//! The in-memory delivery backend (RFC §22.2): Python's
//! `InMemoryDeliveryBackend`, a single-process queue whose items are lost
//! with the process.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::backend::{
    Answer, DeliveryBackend, DequeueOptions, GetDeadLetterItemsOptions, NackOptions,
};
use super::item::{DeliveryItem, DeliveryItemStatus, check_retry_delays, retry_delay};
use super::outcome::DeliveryOutcomeStatus;
use super::worker::{RunWorkerLoopOptions, run_for};
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::store::ConversationStore;

/// What [`InMemoryDeliveryBackend::new`] takes: Python's
/// `InMemoryDeliveryBackend(max_queue_size=1000, max_dead_letter_size=1000,
/// *, retry_delay=1.0, max_retry_delay=60.0)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InMemoryDeliveryBackendOptions {
    /// Python `max_queue_size=1000`: the bound on the queue, past which an
    /// enqueue waits.
    pub max_queue_size: usize,
    /// Python `max_dead_letter_size=1000`: the bound on the dead-letter
    /// queue, its oldest items dropped past it.
    pub max_dead_letter_size: usize,
    /// Python `retry_delay=1.0`: the wait before the first retry; each next
    /// one waits twice as long, up to `max_retry_delay`. Zero retries at
    /// once.
    pub retry_delay: Duration,
    /// Python `max_retry_delay=60.0`: the cap on the wait before a retry.
    pub max_retry_delay: Duration,
}

impl Default for InMemoryDeliveryBackendOptions {
    fn default() -> Self {
        Self {
            max_queue_size: 1000,
            max_dead_letter_size: 1000,
            retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(60),
        }
    }
}

/// A delivery queue in the process's memory, for a single process (RFC
/// §22.2): Python's `InMemoryDeliveryBackend`. Items go from the queue to
/// the in-flight set, then are acked or dead-lettered; one nacked with
/// retries left waits out its retry delay before it is queued again. Its
/// worker, started with the kit, executes the queue. Cloning it shares it.
#[derive(Clone)]
pub struct InMemoryDeliveryBackend {
    shared: Arc<Shared>,
}

struct Shared {
    options: InMemoryDeliveryBackendOptions,
    state: Mutex<State>,
    /// Told when an item is queued.
    queued: Notify,
    /// Told when the queue has room again.
    room: Notify,
    worker: Mutex<Option<JoinHandle<()>>>,
    worker_id: String,
}

#[derive(Default)]
struct State {
    queue: VecDeque<DeliveryItem>,
    in_flight: HashMap<String, DeliveryItem>,
    delayed: HashMap<String, (DeliveryItem, JoinHandle<()>)>,
    dead_letter: VecDeque<DeliveryItem>,
}

impl std::fmt::Debug for InMemoryDeliveryBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InMemoryDeliveryBackend")
            .field("options", &self.shared.options)
            .finish_non_exhaustive()
    }
}

impl Default for InMemoryDeliveryBackend {
    fn default() -> Self {
        Self::with(InMemoryDeliveryBackendOptions::default())
    }
}

impl InMemoryDeliveryBackend {
    /// A backend bounded and retrying as `options` say.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`], Python's `ValueError`, when the first
    /// retry's delay is longer than the cap.
    pub fn new(options: InMemoryDeliveryBackendOptions) -> Result<Self, RoomKitError> {
        check_retry_delays(options.retry_delay, options.max_retry_delay)?;
        Ok(Self::with(options))
    }

    fn with(options: InMemoryDeliveryBackendOptions) -> Self {
        let worker_id = uuid::Uuid::new_v4().simple().to_string();
        Self {
            shared: Arc::new(Shared {
                options,
                state: Mutex::new(State::default()),
                queued: Notify::new(),
                room: Notify::new(),
                worker: Mutex::new(None),
                worker_id: worker_id.chars().take(12).collect(),
            }),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The items its workers claimed and did not settle yet. Outside the
    /// public API: the conformance runner's wait for the kit to go idle.
    #[doc(hidden)]
    pub fn in_flight(&self) -> usize {
        self.state().in_flight.len()
    }

    /// Queues `item` again once its retry delay passed, or dead-letters it
    /// when the queue is full.
    fn requeue_later(&self, item: DeliveryItem, delay: Duration) {
        let backend = self.clone();
        let id = item.id.clone();
        let mut state = self.state();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            backend.requeue(&id);
        });
        state.delayed.insert(item.id.clone(), (item, timer));
    }

    fn requeue(&self, item_id: &str) {
        let mut state = self.state();
        let Some((mut item, _timer)) = state.delayed.remove(item_id) else {
            return;
        };
        if state.queue.len() >= self.shared.options.max_queue_size {
            item.status = DeliveryItemStatus::DeadLetter;
            item.error = Some("queue full on retry".to_owned());
            tracing::warn!(target: "roomkit::delivery", item = %item_id, "dead-lettered: the queue is full on its retry");
            self.bury(&mut state, item);
            return;
        }
        state.queue.push_back(item);
        drop(state);
        self.shared.queued.notify_one();
    }

    /// Adds `item` to the dead-letter queue, its oldest item dropped past
    /// the bound.
    fn bury(&self, state: &mut State, item: DeliveryItem) {
        state.dead_letter.push_back(item);
        while state.dead_letter.len() > self.shared.options.max_dead_letter_size {
            state.dead_letter.pop_front();
        }
    }

    /// Claims `item` for `worker_id`.
    fn claim(state: &mut State, mut item: DeliveryItem, worker_id: &str) -> DeliveryItem {
        item.status = DeliveryItemStatus::InProgress;
        item.worker_id = Some(worker_id.to_owned());
        state.in_flight.insert(item.id.clone(), item.clone());
        item
    }
}

impl<S: ConversationStore> DeliveryBackend<S> for InMemoryDeliveryBackend {
    fn enqueue(&self, mut item: DeliveryItem) -> Answer<'_, ()> {
        Box::pin(async move {
            item.status = DeliveryItemStatus::Pending;
            loop {
                let room = self.shared.room.notified();
                {
                    let mut state = self.state();
                    if state.queue.len() < self.shared.options.max_queue_size {
                        state.queue.push_back(item);
                        drop(state);
                        self.shared.queued.notify_one();
                        return Ok(());
                    }
                }
                room.await;
            }
        })
    }

    fn dequeue<'a>(
        &'a self,
        worker_id: &'a str,
        options: DequeueOptions,
    ) -> Answer<'a, Vec<DeliveryItem>> {
        Box::pin(async move {
            let deadline = tokio::time::Instant::now() + options.timeout;
            loop {
                let queued = self.shared.queued.notified();
                {
                    let mut state = self.state();
                    if !state.queue.is_empty() {
                        let mut items = Vec::new();
                        while items.len() < options.batch_size.max(1) {
                            let Some(item) = state.queue.pop_front() else {
                                break;
                            };
                            items.push(Self::claim(&mut state, item, worker_id));
                        }
                        drop(state);
                        self.shared.room.notify_waiters();
                        return Ok(items);
                    }
                }
                if tokio::time::timeout_at(deadline, queued).await.is_err() {
                    return Ok(Vec::new());
                }
            }
        })
    }

    fn executed<'a>(&'a self, item: &'a DeliveryItem) -> Answer<'a, ()> {
        Box::pin(async move {
            if let Some(held) = self.state().in_flight.get_mut(&item.id) {
                held.outcome.clone_from(&item.outcome);
            }
            Ok(())
        })
    }

    fn ack<'a>(&'a self, item_id: &'a str) -> Answer<'a, ()> {
        Box::pin(async move {
            let mut state = self.state();
            if let Some(mut item) = state.in_flight.remove(item_id) {
                let status = item
                    .outcome
                    .as_ref()
                    .map_or(DeliveryOutcomeStatus::Sent, |outcome| outcome.status);
                item.status = match status {
                    DeliveryOutcomeStatus::Sent => DeliveryItemStatus::Delivered,
                    DeliveryOutcomeStatus::Blocked => DeliveryItemStatus::Blocked,
                    _ => DeliveryItemStatus::Unknown,
                };
            }
            Ok(())
        })
    }

    fn nack<'a>(&'a self, item_id: &'a str, options: NackOptions) -> Answer<'a, ()> {
        Box::pin(async move {
            let mut state = self.state();
            let Some(mut item) = state.in_flight.remove(item_id) else {
                return Ok(());
            };
            item.retry_count += 1;
            item.error.clone_from(&options.error);
            if item.retry_count > item.max_retries {
                item.status = DeliveryItemStatus::DeadLetter;
                item.error = Some(
                    options
                        .error
                        .unwrap_or_else(|| "max retries exceeded".to_owned()),
                );
                tracing::warn!(target: "roomkit::delivery", item = %item_id, retries = item.max_retries, "dead-lettered once its retries were spent");
                self.bury(&mut state, item);
                return Ok(());
            }
            item.status = DeliveryItemStatus::Pending;
            item.worker_id = None;
            let options = &self.shared.options;
            let delay = retry_delay(
                item.retry_count,
                options.retry_delay,
                options.max_retry_delay,
            );
            drop(state);
            self.requeue_later(item, delay);
            Ok(())
        })
    }

    fn dead_letter<'a>(&'a self, item_id: &'a str, error: &'a str) -> Answer<'a, ()> {
        Box::pin(async move {
            let mut state = self.state();
            if let Some(mut item) = state.in_flight.remove(item_id) {
                item.status = DeliveryItemStatus::DeadLetter;
                item.error = Some(error.to_owned());
                self.bury(&mut state, item);
            }
            Ok(())
        })
    }

    fn get_queue_depth(&self) -> Answer<'_, usize> {
        Box::pin(async move {
            let state = self.state();
            Ok(state.queue.len() + state.delayed.len())
        })
    }

    fn get_dead_letter_items(
        &self,
        options: GetDeadLetterItemsOptions,
    ) -> Answer<'_, Vec<DeliveryItem>> {
        Box::pin(async move {
            let state = self.state();
            Ok(state
                .dead_letter
                .iter()
                .rev()
                .take(options.limit)
                .cloned()
                .collect())
        })
    }

    fn start(&self, kit: RoomKit<S>) -> Answer<'_, ()> {
        Box::pin(async move {
            let mut worker = self
                .shared
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if worker.is_some() {
                return Ok(());
            }
            let backend: Arc<dyn DeliveryBackend<S>> = Arc::new(self.clone());
            let worker_id = self.shared.worker_id.clone();
            let options = RunWorkerLoopOptions {
                batch_size: 5,
                poll_timeout: Duration::from_secs(2),
            };
            let kit = kit.downgrade();
            *worker = Some(tokio::spawn(async move {
                run_for(backend, kit, &worker_id, options).await;
            }));
            tracing::info!(target: "roomkit::delivery", worker = %self.shared.worker_id, "in-memory delivery worker started");
            Ok(())
        })
    }

    fn close(&self) -> Answer<'_, ()> {
        Box::pin(async move {
            let worker = self
                .shared
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(worker) = worker {
                worker.abort();
                drop(worker.await);
            }
            let mut state = self.state();
            let delayed: Vec<_> = state.delayed.drain().map(|(_, entry)| entry).collect();
            let in_flight: Vec<_> = state.in_flight.drain().map(|(_, item)| item).collect();
            let waiting = delayed.into_iter().map(|(item, timer)| {
                timer.abort();
                item
            });
            for mut item in in_flight.into_iter().chain(waiting) {
                item.status = DeliveryItemStatus::Pending;
                item.worker_id = None;
                if state.queue.len() < self.shared.options.max_queue_size {
                    state.queue.push_back(item);
                } else {
                    item.status = DeliveryItemStatus::DeadLetter;
                    item.error = Some("queue full at shutdown".to_owned());
                    tracing::warn!(target: "roomkit::delivery", item = %item.id, "dead-lettered at shutdown: the queue is full");
                    self.bury(&mut state, item);
                }
            }
            tracing::info!(target: "roomkit::delivery", "in-memory delivery worker stopped");
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_core::model::RoomId;

    use super::*;
    use crate::store::InMemoryStore;

    type Backend = dyn DeliveryBackend<InMemoryStore>;

    fn item(content: &str) -> DeliveryItem {
        DeliveryItem::new(RoomId::new("r1").unwrap(), content)
    }

    fn within(timeout: Duration) -> DequeueOptions {
        DequeueOptions {
            batch_size: 1,
            timeout,
        }
    }

    fn batch(batch_size: usize) -> DequeueOptions {
        DequeueOptions {
            batch_size,
            timeout: Duration::from_secs(1),
        }
    }

    fn retrying(delay: Duration) -> InMemoryDeliveryBackend {
        InMemoryDeliveryBackend::new(InMemoryDeliveryBackendOptions {
            retry_delay: delay,
            ..InMemoryDeliveryBackendOptions::default()
        })
        .unwrap()
    }

    fn nack(error: &str) -> NackOptions {
        NackOptions {
            error: Some(error.to_owned()),
        }
    }

    // Python: tests/delivery/test_memory.py::TestEnqueueDequeue::test_basic_flow
    #[tokio::test]
    async fn test_basic_flow() {
        let backend = InMemoryDeliveryBackend::default();
        let queued = item("hello");
        let id = queued.id.clone();
        Backend::enqueue(&backend, queued).await.unwrap();
        assert_eq!(Backend::get_queue_depth(&backend).await.unwrap(), 1);
        let items = Backend::dequeue(&backend, "w1", within(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].id, id);
        assert_eq!(items[0].status, DeliveryItemStatus::InProgress);
        assert_eq!(items[0].worker_id.as_deref(), Some("w1"));
    }

    // Python: tests/delivery/test_memory.py::TestEnqueueDequeue::test_dequeue_timeout_returns_empty
    #[tokio::test(start_paused = true)]
    async fn test_dequeue_timeout_returns_empty() {
        let backend = InMemoryDeliveryBackend::default();
        let items = Backend::dequeue(&backend, "w1", within(Duration::from_millis(100))).await;
        assert_eq!(items.unwrap(), Vec::new());
    }

    // Python: tests/delivery/test_memory.py::TestEnqueueDequeue::test_batch_dequeue
    // and test_batch_dequeue_partial.
    #[tokio::test]
    async fn test_batch_dequeue() {
        let backend = InMemoryDeliveryBackend::default();
        for n in 0..5 {
            Backend::enqueue(&backend, item(&format!("msg-{n}")))
                .await
                .unwrap();
        }
        assert_eq!(
            Backend::dequeue(&backend, "w1", batch(3))
                .await
                .unwrap()
                .len(),
            3
        );
        assert_eq!(Backend::get_queue_depth(&backend).await.unwrap(), 2);
        assert_eq!(
            Backend::dequeue(&backend, "w1", batch(5))
                .await
                .unwrap()
                .len(),
            2
        );
    }

    // Python: tests/delivery/test_memory.py::TestAck::test_ack_removes_from_in_flight
    // and test_ack_unknown_id_is_noop.
    #[tokio::test]
    async fn test_ack_removes_from_in_flight() {
        let backend = InMemoryDeliveryBackend::default();
        Backend::enqueue(&backend, item("hello")).await.unwrap();
        let items = Backend::dequeue(&backend, "w1", batch(1)).await.unwrap();
        assert_eq!(backend.in_flight(), 1);
        Backend::ack(&backend, &items[0].id).await.unwrap();
        assert_eq!(backend.in_flight(), 0);
        Backend::ack(&backend, "nonexistent").await.unwrap();
    }

    // Python: tests/delivery/test_memory.py::TestNack::test_nack_re_enqueues_after_the_retry_delay
    #[tokio::test(start_paused = true)]
    async fn test_nack_re_enqueues_after_the_retry_delay() {
        let backend = retrying(Duration::from_millis(200));
        Backend::enqueue(&backend, item("hello")).await.unwrap();
        let dequeued = Backend::dequeue(&backend, "w1", batch(1)).await.unwrap();
        Backend::nack(&backend, &dequeued[0].id, nack("transient failure"))
            .await
            .unwrap();
        // Pending, but not attempted again before its delay has passed.
        assert_eq!(Backend::get_queue_depth(&backend).await.unwrap(), 1);
        let early = Backend::dequeue(&backend, "w1", within(Duration::from_millis(50))).await;
        assert_eq!(early.unwrap(), Vec::new());
        let again = Backend::dequeue(&backend, "w1", within(Duration::from_secs(1)))
            .await
            .unwrap();
        assert_eq!(again[0].retry_count, 1);
    }

    // Python: tests/delivery/test_memory.py::TestNack::test_max_retries_counts_retries_after_the_first_attempt
    #[tokio::test]
    async fn test_max_retries_counts_retries_after_the_first_attempt() {
        let backend = retrying(Duration::ZERO);
        let queued = DeliveryItem {
            max_retries: 1,
            ..item("hello")
        };
        Backend::enqueue(&backend, queued).await.unwrap();
        let first = Backend::dequeue(&backend, "w1", batch(1)).await.unwrap();
        Backend::nack(&backend, &first[0].id, nack("first failure"))
            .await
            .unwrap();
        let retried = Backend::dequeue(&backend, "w1", batch(1)).await.unwrap();
        assert_eq!(retried[0].retry_count, 1);
        Backend::nack(&backend, &retried[0].id, nack("permanent failure"))
            .await
            .unwrap();
        assert_eq!(Backend::get_queue_depth(&backend).await.unwrap(), 0);
        let dead = Backend::get_dead_letter_items(&backend, GetDeadLetterItemsOptions::default());
        let dead = dead.await.unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].status, DeliveryItemStatus::DeadLetter);
        assert_eq!(dead[0].error.as_deref(), Some("permanent failure"));
    }

    // Python: tests/delivery/test_memory.py::TestDeadLetter::test_dead_letter_moves_item
    // and test_get_dead_letter_items_limit; tests/delivery/test_dead_letter_order.py:
    // the most recent first.
    #[tokio::test]
    async fn test_dead_letter_moves_item() {
        let backend = InMemoryDeliveryBackend::default();
        for n in 0..10 {
            Backend::enqueue(&backend, item(&format!("msg-{n}")))
                .await
                .unwrap();
            let dequeued = Backend::dequeue(&backend, "w1", batch(1)).await.unwrap();
            Backend::dead_letter(&backend, &dequeued[0].id, "fatal error")
                .await
                .unwrap();
        }
        assert_eq!(backend.in_flight(), 0);
        let limited = GetDeadLetterItemsOptions { limit: 3 };
        let dead = Backend::get_dead_letter_items(&backend, limited)
            .await
            .unwrap();
        let contents: Vec<_> = dead.iter().map(|item| item.content.as_str()).collect();
        assert_eq!(contents, ["msg-9", "msg-8", "msg-7"]);
        assert_eq!(dead[0].error.as_deref(), Some("fatal error"));
    }

    // Python: tests/delivery/test_memory.py::TestLifecycle::test_close_without_start
    // and test_close_re_enqueues_in_flight.
    #[tokio::test]
    async fn test_close_re_enqueues_in_flight() {
        let backend = InMemoryDeliveryBackend::default();
        Backend::close(&backend).await.unwrap();
        Backend::enqueue(&backend, item("hello")).await.unwrap();
        assert_eq!(
            Backend::dequeue(&backend, "w1", batch(1))
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(Backend::get_queue_depth(&backend).await.unwrap(), 0);
        Backend::close(&backend).await.unwrap();
        assert_eq!(Backend::get_queue_depth(&backend).await.unwrap(), 1);
        assert_eq!(backend.in_flight(), 0);
    }

    // Python: tests/delivery/test_memory.py::TestLifecycle::test_nack_queue_full_dead_letters
    #[tokio::test]
    async fn test_nack_queue_full_dead_letters() {
        let backend = InMemoryDeliveryBackend::new(InMemoryDeliveryBackendOptions {
            max_queue_size: 1,
            retry_delay: Duration::ZERO,
            ..InMemoryDeliveryBackendOptions::default()
        })
        .unwrap();
        Backend::enqueue(&backend, item("retry-me")).await.unwrap();
        let retried = Backend::dequeue(&backend, "w1", batch(1)).await.unwrap();
        Backend::enqueue(&backend, item("filler")).await.unwrap();
        Backend::nack(&backend, &retried[0].id, nack("transient"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let dead = Backend::get_dead_letter_items(&backend, GetDeadLetterItemsOptions::default());
        let dead = dead.await.unwrap();
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0].error.as_deref(), Some("queue full on retry"));
    }

    // Python: tests/delivery/test_memory.py::TestRetryDelay::test_close_queues_again_an_item_waiting_out_its_delay
    #[tokio::test]
    async fn test_close_queues_again_an_item_waiting_out_its_delay() {
        let backend = retrying(Duration::from_secs(60));
        Backend::enqueue(&backend, item("hello")).await.unwrap();
        let dequeued = Backend::dequeue(&backend, "w1", batch(1)).await.unwrap();
        Backend::nack(&backend, &dequeued[0].id, nack("transient"))
            .await
            .unwrap();
        Backend::close(&backend).await.unwrap();
        let queued = Backend::dequeue(&backend, "w1", within(Duration::from_millis(100))).await;
        let queued = queued.unwrap();
        assert_eq!(
            (queued[0].id.as_str(), queued[0].retry_count),
            (dequeued[0].id.as_str(), 1)
        );
    }

    // Python: tests/delivery/test_memory.py::TestRetryDelay::test_bad_delays_are_refused
    #[test]
    fn a_delay_longer_than_its_cap_is_refused() {
        let refused = InMemoryDeliveryBackend::new(InMemoryDeliveryBackendOptions {
            retry_delay: Duration::from_secs(2),
            max_retry_delay: Duration::from_secs(1),
            ..InMemoryDeliveryBackendOptions::default()
        });
        assert!(matches!(refused, Err(RoomKitError::Value(_))));
    }
}
