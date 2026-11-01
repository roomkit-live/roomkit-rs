//! The in-memory realtime backend: Python's `InMemoryRealtime`, a queue and
//! a task per subscription, in one process.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::{EphemeralCallback, EphemeralEvent, RealtimeBackend, RealtimeFuture};
use crate::unwind::{message, unwinding};

/// How many events a subscription queues by default, as Python's
/// `max_queue_size=100`.
const MAX_QUEUE_SIZE: usize = 100;

/// What [`InMemoryRealtime::new`] takes: Python's
/// `InMemoryRealtime(max_queue_size=100)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InMemoryRealtimeOptions {
    /// Python `max_queue_size=100`: how many events a subscription holds
    /// while its callback runs; past it, the oldest is dropped.
    pub max_queue_size: usize,
}

impl Default for InMemoryRealtimeOptions {
    fn default() -> Self {
        Self {
            max_queue_size: MAX_QUEUE_SIZE,
        }
    }
}

/// A realtime backend in one process: Python's `InMemoryRealtime`. Each
/// subscription holds a queue of at most `max_queue_size` events, the
/// oldest dropped past it, which a task of its own hands to its callback in
/// order. For several processes, a backend over Redis pub/sub or NATS
/// implements [`RealtimeBackend`] instead.
#[derive(Debug)]
pub struct InMemoryRealtime {
    max_queue_size: usize,
    state: Mutex<State>,
    closed: AtomicBool,
}

/// The subscriptions, and the channels they listen to.
#[derive(Debug, Default)]
struct State {
    subscriptions: HashMap<String, Subscription>,
    channels: HashMap<String, HashSet<String>>,
}

/// One subscription: its channel, its queue and the task that drains it.
#[derive(Debug)]
struct Subscription {
    channel: String,
    queue: Arc<Queue>,
    task: JoinHandle<()>,
}

/// A subscription's events waiting for its callback.
#[derive(Debug)]
struct Queue {
    events: Mutex<VecDeque<EphemeralEvent>>,
    ready: Notify,
    stopped: AtomicBool,
    max: usize,
}

impl Queue {
    fn events(&self) -> MutexGuard<'_, VecDeque<EphemeralEvent>> {
        self.events.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues `event`, dropping the oldest past the bound, as Python's
    /// queue keyed by the events' ids: an event of an id the queue holds
    /// takes its place there, so its callback runs once.
    fn enqueue(&self, event: EphemeralEvent) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        {
            let mut events = self.events();
            while events.len() >= self.max.max(1) {
                events.pop_front();
            }
            match events.iter_mut().find(|held| held.id == event.id) {
                Some(held) => *held = event,
                None => events.push_back(event),
            }
        }
        self.ready.notify_one();
    }

    /// The next event, `None` once stopped.
    async fn next(&self) -> Option<EphemeralEvent> {
        loop {
            if self.stopped.load(Ordering::Acquire) {
                return None;
            }
            let next = self.events().pop_front();
            if next.is_some() {
                return next;
            }
            self.ready.notified().await;
        }
    }
}

/// Hands each event of `queue` to `callback`, in order, until the queue
/// stops; a callback that panics is logged, and the next event goes on.
async fn drain(subscription_id: String, queue: Arc<Queue>, callback: EphemeralCallback) {
    while let Some(event) = queue.next().await {
        if let Err(panic) = unwinding(callback(event)).await {
            tracing::error!(
                target: "roomkit::realtime",
                subscription = %subscription_id,
                panic = %message(&*panic),
                "Error in realtime callback for subscription {subscription_id}"
            );
        }
    }
}

impl Drop for InMemoryRealtime {
    /// A backend dropped unclosed stops its subscriptions' tasks.
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(PoisonError::into_inner);
        for subscription in state.subscriptions.values() {
            subscription.queue.stopped.store(true, Ordering::Release);
            subscription.task.abort();
        }
    }
}

impl Default for InMemoryRealtime {
    fn default() -> Self {
        Self::new(InMemoryRealtimeOptions::default())
    }
}

impl InMemoryRealtime {
    /// A backend whose subscriptions queue at most `options.max_queue_size`
    /// events.
    pub fn new(options: InMemoryRealtimeOptions) -> Self {
        Self {
            max_queue_size: options.max_queue_size,
            state: Mutex::new(State::default()),
            closed: AtomicBool::new(false),
        }
    }

    /// How many subscriptions are running: Python's `subscription_count`.
    pub fn subscription_count(&self) -> usize {
        self.state().subscriptions.len()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stops `subscription`'s task, once a callback it runs gave way.
    async fn stop(subscription: Subscription) {
        subscription.queue.stopped.store(true, Ordering::Release);
        subscription.queue.ready.notify_one();
        subscription.task.abort();
        drop(subscription.task.await);
    }
}

impl RealtimeBackend for InMemoryRealtime {
    fn publish<'a>(&'a self, channel: &'a str, event: EphemeralEvent) -> RealtimeFuture<'a, ()> {
        Box::pin(async move {
            if self.closed.load(Ordering::Acquire) {
                return;
            }
            let queues: Vec<Arc<Queue>> = {
                let state = self.state();
                state
                    .channels
                    .get(channel)
                    .into_iter()
                    .flatten()
                    .filter_map(|id| state.subscriptions.get(id))
                    .map(|subscription| Arc::clone(&subscription.queue))
                    .collect()
            };
            for queue in queues {
                queue.enqueue(event.clone());
            }
        })
    }

    fn subscribe<'a>(
        &'a self,
        channel: &'a str,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        Box::pin(async move {
            let id = uuid::Uuid::new_v4().simple().to_string();
            let queue = Arc::new(Queue {
                events: Mutex::new(VecDeque::new()),
                ready: Notify::new(),
                stopped: AtomicBool::new(false),
                max: self.max_queue_size,
            });
            let mut state = self.state();
            if self.closed.load(Ordering::Acquire) {
                // A closed backend runs no subscription: nothing publishes
                // to it any more.
                return id;
            }
            let task = tokio::spawn(drain(id.clone(), Arc::clone(&queue), callback));
            state
                .channels
                .entry(channel.to_owned())
                .or_default()
                .insert(id.clone());
            let subscription = Subscription {
                channel: channel.to_owned(),
                queue,
                task,
            };
            state.subscriptions.insert(id.clone(), subscription);
            id
        })
    }

    fn unsubscribe<'a>(&'a self, subscription_id: &'a str) -> RealtimeFuture<'a, bool> {
        Box::pin(async move {
            let subscription = {
                let mut state = self.state();
                let Some(subscription) = state.subscriptions.remove(subscription_id) else {
                    return false;
                };
                if let Some(ids) = state.channels.get_mut(&subscription.channel) {
                    ids.remove(subscription_id);
                    if ids.is_empty() {
                        state.channels.remove(&subscription.channel);
                    }
                }
                subscription
            };
            Self::stop(subscription).await;
            true
        })
    }

    fn close(&self) -> RealtimeFuture<'_, ()> {
        Box::pin(async move {
            self.closed.store(true, Ordering::Release);
            let subscriptions: Vec<Subscription> = {
                let mut state = self.state();
                state.channels.clear();
                state.subscriptions.drain().map(|(_, sub)| sub).collect()
            };
            for subscription in subscriptions {
                Self::stop(subscription).await;
            }
        })
    }
}
