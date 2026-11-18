//! One running thought and only the newest pending context per room.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use roomkit::turns::ChannelTurnHooks;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

use crate::{SharedAiContext, Thinker, Thought, ThoughtEvent};

type Work = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub(super) type Late = Box<dyn FnOnce(Thought) -> Work + Send>;

struct Pending {
    previous: oneshot::Sender<Thought>,
    work: Work,
}

#[derive(Default)]
struct State {
    thought: Thought,
    asked: u64,
    done: u64,
    spoke: u64,
    pending: Option<Pending>,
    running: bool,
    task: Option<JoinHandle<()>>,
}

pub(super) struct Mind {
    room: String,
    channel: String,
    thinker: Arc<dyn Thinker>,
    state: Mutex<State>,
    changed: Notify,
}

impl Mind {
    pub(super) fn new(room: String, channel: String, thinker: Arc<dyn Thinker>) -> Self {
        Self {
            room,
            channel,
            thinker,
            state: Mutex::default(),
            changed: Notify::new(),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn thought(&self) -> Thought {
        self.state().thought.clone()
    }

    pub(super) fn spoke(&self) -> Thought {
        let mut state = self.state();
        let thought = state.thought.clone();
        state.thought = thought.said();
        state.spoke += 1;
        thought
    }

    /// The caller classifies the result as in-time or late. This handshake
    /// prevents a multi-threaded completion from passing the late registration
    /// between the timeout check and the caller's next instruction.
    pub(super) fn ask(
        self: &Arc<Self>,
        context: SharedAiContext,
        answering: bool,
        callbacks: Option<ChannelTurnHooks>,
        late: Late,
    ) -> (u64, oneshot::Sender<bool>) {
        let (previous, start) = oneshot::channel();
        let (decided, disposition) = oneshot::channel();
        let mut state = self.state();
        state.asked += 1;
        let (ticket, spoke) = (state.asked, state.spoke);
        let mind = Arc::clone(self);
        let work = roomkit::keep_context(async move {
            let Ok(previous) = start.await else {
                return;
            };
            let started = tokio::time::Instant::now();
            let thought = match mind.thinker.think(&previous, &context).await {
                Ok(thought) => thought,
                Err(error) => {
                    tracing::warn!(target: "roomkit::ai", room_id = %mind.room, %error,
                        "Thinker failed; thought kept");
                    previous
                }
            };
            let duration = (started.elapsed().as_secs_f64() * 1000.0).round_ties_even();
            let thought = mind.complete(
                ticket,
                spoke,
                answering,
                thought,
                duration,
                callbacks.as_ref(),
            );
            if disposition.await.unwrap_or(false)
                && mind.state().asked == ticket
                && !thought.want_to_say.is_empty()
            {
                late(thought).await;
            }
        });
        state.pending = Some(Pending {
            previous,
            work: Box::pin(work),
        });
        if !state.running {
            state.running = true;
            let mind = Arc::clone(self);
            state.task = Some(tokio::spawn(async move {
                mind.run().await;
            }));
        }
        (ticket, decided)
    }

    fn complete(
        &self,
        ticket: u64,
        spoke: u64,
        answering: bool,
        mut thought: Thought,
        duration_ms: f64,
        callbacks: Option<&ChannelTurnHooks>,
    ) -> Thought {
        let previous = {
            let mut state = self.state();
            if state.spoke != spoke || answering {
                thought = thought.said();
            }
            std::mem::replace(&mut state.thought, thought.clone())
        };
        if thought != previous {
            report(
                callbacks,
                &self.room,
                &self.channel,
                thought.clone(),
                previous,
                Some(duration_ms),
            );
        }
        // Register the tracked observer before the waiter can end its turn.
        // The thought is already stored; nobody waits for the observer to run.
        self.state().done = ticket;
        self.changed.notify_waiters();
        thought
    }

    async fn run(&self) {
        loop {
            let next = {
                let mut state = self.state();
                if let Some(next) = state.pending.take() {
                    Some((next, state.thought.clone()))
                } else {
                    state.running = false;
                    None
                }
            };
            let Some((pending, thought)) = next else {
                return;
            };
            drop(pending.previous.send(thought));
            pending.work.await;
        }
    }

    pub(super) async fn settled(&self, ticket: u64, timeout: Duration) -> bool {
        let wait = async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.state().done >= ticket {
                    return;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(timeout, wait).await;
        self.state().done >= ticket
    }

    pub(super) async fn close(&self) {
        let task = {
            let mut state = self.state();
            state.pending = None;
            state.task.take()
        };
        if let Some(task) = task {
            task.abort();
            if tokio::task::try_id() != Some(task.id()) {
                drop(task.await);
            }
        }
    }
}

pub(super) fn report(
    callbacks: Option<&ChannelTurnHooks>,
    room: &str,
    channel: &str,
    thought: Thought,
    previous: Thought,
    duration_ms: Option<f64>,
) {
    if let Some(hooks) = callbacks {
        hooks.observe(
            &ThoughtEvent {
                room_id: room.into(),
                channel_id: channel.into(),
                thought,
                previous,
                duration_ms,
            },
            || {},
        );
    }
}
