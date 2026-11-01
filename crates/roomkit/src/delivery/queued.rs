//! `Queued` (RFC §22.2): compatible requests batched, each caller told its
//! own outcome, as the Python implementation's `Queued` drains them.
//!
//! One caller drains the queue at a time: it delivers the batch its request
//! heads, then passes the drain to the caller whose request heads the queue
//! next, which delivers that batch in its own task, with its own depth and
//! frames: a request that heads a batch is delivered as its caller made it,
//! and a draining caller that holds the room's lock leaves the callers
//! behind it free to deliver once it returns. A compatible request batched
//! behind another is delivered with it, by that request's caller. A caller
//! holding a room's lock while another caller drains delivers at once,
//! unbatched, as `Immediate` does (RFC §9.5.1). Keyed requests publish
//! individually.

use std::mem;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use roomkit_core::model::{ChannelId, DeliveryError};
use tokio::sync::oneshot;

use super::outcome::{DeliveryOutcome, DeliveryOutcomeStatus};
use super::strategy::{Delivering, DeliveryContext, DeliveryStrategy};
use super::targets::Prepared;
use crate::context::ExecutionContext;
use crate::error::RoomKitError;
use crate::store::{ConversationStore, InMemoryStore};

/// What [`Queued::new`] takes: Python's `Queued(buffer=1.0,
/// playback_timeout=15.0, separator="\n\n")`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueuedOptions {
    /// Python `buffer=1.0`: the wait after the channel went idle.
    pub buffer: Duration,
    /// Python `playback_timeout=15.0`: the longest wait for idle, past which
    /// the batch is delivered anyway.
    pub playback_timeout: Duration,
    /// Python `separator="\n\n"`: what joins a batch's contents.
    pub separator: String,
}

impl Default for QueuedOptions {
    fn default() -> Self {
        Self {
            buffer: Duration::from_secs(1),
            playback_timeout: Duration::from_secs(15),
            separator: "\n\n".to_owned(),
        }
    }
}

/// Batches compatible unkeyed requests at idle, each caller told its own
/// outcome (RFC §22.2): Python's `Queued`. Requests of other rooms,
/// transports, addresses, sessions, intents or metadata stay apart; a keyed
/// request publishes alone. Shared, one strategy queues for every room it
/// delivers into. No channel of this implementation plays voice or hosts a
/// realtime model: a batch waits for no idle and no buffer.
pub struct Queued<S = InMemoryStore> {
    options: QueuedOptions,
    state: Arc<Mutex<Drain<S>>>,
}

impl<S> std::fmt::Debug for Queued<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Queued")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<S> Default for Queued<S> {
    fn default() -> Self {
        Self::new(QueuedOptions::default())
    }
}

/// The queue and who drains it.
struct Drain<S> {
    queue: Vec<Waiting<S>>,
    delivering: bool,
    next_id: u64,
}

/// A request in the queue: its context, its channel, where its caller
/// waits for its outcome and for its turn to drain.
struct Waiting<S> {
    id: u64,
    context: DeliveryContext<S>,
    channel_id: ChannelId,
    result: oneshot::Sender<DeliveryOutcome>,
    turn: Option<oneshot::Sender<()>>,
}

impl<S> Waiting<S> {
    /// Whether its caller stopped waiting for it.
    fn withdrawn(&self) -> bool {
        self.result.is_closed()
    }

    /// Whether `other` may be delivered in one batch with it: neither keyed,
    /// the same room, transport, address, depth, intent, session and
    /// metadata (RFC §22.2).
    fn compatible(&self, other: &Self) -> bool {
        let (left, right) = (&self.context, &other.context);
        left.idempotency_key.is_none()
            && right.idempotency_key.is_none()
            && left.room_id == right.room_id
            && self.channel_id == other.channel_id
            && left.channel_id == right.channel_id
            && left.addressed_to == right.addressed_to
            && left.chain_depth == right.chain_depth
            && left.instruction == right.instruction
            && left.session_id == right.session_id
            && left.metadata == right.metadata
    }
}

impl<S> Queued<S> {
    /// The strategy batching as `options` say.
    pub fn new(options: QueuedOptions) -> Self {
        Self {
            options,
            state: Arc::new(Mutex::new(Drain {
                queue: Vec::new(),
                delivering: false,
                next_id: 0,
            })),
        }
    }

    /// Python's `buffer`: the wait after the channel went idle.
    pub fn buffer(&self) -> Duration {
        self.options.buffer
    }

    /// Python's `playback_timeout`: the longest wait for idle.
    pub fn playback_timeout(&self) -> Duration {
        self.options.playback_timeout
    }

    /// Python's `separator`: what joins a batch's contents.
    pub fn separator(&self) -> &str {
        &self.options.separator
    }
}

impl<S: ConversationStore> DeliveryStrategy<S> for Queued<S> {
    fn deliver(&self, ctx: DeliveryContext<S>) -> Delivering<'_> {
        Box::pin(async move { self.queue(ctx).await.map(Some) })
    }

    fn serialized(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "type": "queued",
            "params": {
                "buffer": self.buffer().as_secs_f64(),
                "playback_timeout": self.playback_timeout().as_secs_f64(),
                "separator": self.separator(),
            },
        }))
    }
}

impl<S: ConversationStore> Queued<S> {
    /// Queues `ctx` and gives its outcome: delivered in the batch it heads,
    /// by its caller once the drain is passed to it, or in another's.
    async fn queue(&self, ctx: DeliveryContext<S>) -> Result<DeliveryOutcome, RoomKitError> {
        let channel_id = match ctx.kit.prepare_delivery(&ctx, None).await? {
            Prepared::Channel(channel_id) => channel_id,
            Prepared::Done(refusal) => return Ok(*refusal),
        };
        if self.lock().delivering && ExecutionContext::current().holds_a_room_lock() {
            return ctx.kit.deliver_to_channel(&ctx, channel_id).await;
        }
        let (result, outcome) = oneshot::channel();
        let (turn, my_turn) = oneshot::channel();
        let (mine, drains) = {
            let mut state = self.lock();
            let id = state.next_id;
            state.next_id += 1;
            state.queue.push(Waiting {
                id,
                context: ctx,
                channel_id,
                result,
                turn: Some(turn),
            });
            let drains = !state.delivering;
            state.delivering = true;
            (id, drains)
        };
        let mut waiter = Waiter {
            state: Arc::clone(&self.state),
            outcome,
            my_turn: Some(my_turn),
        };
        if !drains && let Waited::Outcome(outcome) = waiter.wait().await {
            return outcome;
        }
        waiter.my_turn = None;
        let holds = PassOn {
            state: Arc::clone(&self.state),
            armed: true,
        };
        self.drain(mine).await;
        holds.disarm();
        waiter.outcome().await
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Drain<S>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Delivers the batch `mine` heads, then hands the drain on.
    async fn drain(&self, mine: u64) {
        let batch = {
            let mut state = self.lock();
            state
                .queue
                .retain(|waiting| !waiting.withdrawn() || waiting.id == mine);
            match state.queue.first() {
                Some(head) if head.id == mine => Some(self.take_batch(&mut state)),
                Some(_) | None => None,
            }
        };
        if let Some(batch) = batch {
            self.deliver_batch(batch).await;
        }
        pass_on(&self.state);
    }

    /// The batch the queue's head heads: it, and the compatible requests
    /// behind it, taken out of the queue in order.
    fn take_batch(&self, state: &mut Drain<S>) -> Vec<Waiting<S>> {
        let queue = mem::take(&mut state.queue);
        let mut queue = queue.into_iter();
        let Some(first) = queue.next() else {
            return Vec::new();
        };
        let (mut batch, mut remainder) = (vec![], vec![]);
        for pending in queue {
            if !pending.withdrawn() && first.compatible(&pending) {
                batch.push(pending);
            } else {
                remainder.push(pending);
            }
        }
        batch.insert(0, first);
        state.queue = remainder;
        batch
    }

    /// Delivers `batch` as one request, its contents joined, and gives each
    /// caller its outcome. Cut short as it is sent, the callers it carried
    /// for others learn that its outcome is unknown.
    async fn deliver_batch(&self, batch: Vec<Waiting<S>>) {
        let Some(first) = batch.first() else {
            return;
        };
        let content = batch
            .iter()
            .map(|waiting| waiting.context.content.as_str())
            .collect::<Vec<_>>()
            .join(self.separator());
        let ctx = DeliveryContext {
            content,
            wait_for_turn: false,
            ..first.context.clone()
        };
        let channel_id = first.channel_id.clone();
        let mut carried = Carried { batch: Some(batch) };
        let outcome = match ctx.kit.deliver_to_channel(&ctx, channel_id).await {
            Ok(outcome) => outcome,
            Err(error) => strategy_failure(&error),
        };
        for waiting in carried.batch.take().into_iter().flatten() {
            drop(waiting.result.send(DeliveryOutcome {
                inbound: outcome.inbound.clone(),
                ..outcome.clone()
            }));
        }
    }
}

/// A caller waiting for its request's outcome or for its turn to drain:
/// dropped meanwhile, it withdraws its request, and hands on a drain it was
/// handed (RFC §22.2).
struct Waiter<S> {
    state: Arc<Mutex<Drain<S>>>,
    outcome: oneshot::Receiver<DeliveryOutcome>,
    my_turn: Option<oneshot::Receiver<()>>,
}

/// What a waiting caller waited for.
enum Waited {
    /// Its request's outcome, given by the batch that carried it.
    Outcome(Result<DeliveryOutcome, RoomKitError>),
    /// The drain, handed to it: its request heads the queue.
    Turn,
}

impl<S> Waiter<S> {
    /// Waits for the request's outcome or for the drain to pass to its
    /// caller.
    async fn wait(&mut self) -> Waited {
        let Self {
            outcome, my_turn, ..
        } = self;
        let Some(turn) = my_turn.as_mut() else {
            return Waited::Turn;
        };
        tokio::select! {
            delivered = &mut *outcome => Waited::Outcome(delivered.map_err(|_| withdrawn())),
            passed = turn => match passed {
                Ok(()) => Waited::Turn,
                // Its entry left the queue unhanded: a batch carried it.
                Err(_) => Waited::Outcome(outcome.await.map_err(|_| withdrawn())),
            },
        }
    }

    /// The request's outcome, once its batch gave it.
    async fn outcome(&mut self) -> Result<DeliveryOutcome, RoomKitError> {
        (&mut self.outcome).await.map_err(|_| withdrawn())
    }
}

impl<S> Drop for Waiter<S> {
    fn drop(&mut self) {
        let handed = self
            .my_turn
            .as_mut()
            .is_some_and(|my_turn| my_turn.try_recv().is_ok());
        if handed {
            self.outcome.close();
            pass_on(&self.state);
        }
    }
}

/// The batch a drain carries while it is sent: dropped before its outcome,
/// the batch cut, every caller it carried learns that its outcome is
/// unknown.
struct Carried<S> {
    batch: Option<Vec<Waiting<S>>>,
}

impl<S> Drop for Carried<S> {
    fn drop(&mut self) {
        for waiting in self.batch.take().into_iter().flatten().skip(1) {
            let mut unknown = DeliveryOutcome::new(DeliveryOutcomeStatus::Unknown);
            unknown.reason = Some("strategy_outcome_unknown".to_owned());
            drop(waiting.result.send(unknown));
        }
    }
}

/// The drain a caller holds: dropped before it handed it on, its caller
/// cut, it hands it on to the callers behind it (RFC §22.2).
struct PassOn<S> {
    state: Arc<Mutex<Drain<S>>>,
    armed: bool,
}

impl<S> PassOn<S> {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl<S> Drop for PassOn<S> {
    fn drop(&mut self) {
        if self.armed {
            pass_on(&self.state);
        }
    }
}

/// Hands the drain to the caller whose request heads the queue, or lets it
/// go when none waits.
fn pass_on<S>(state: &Mutex<Drain<S>>) {
    let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
    loop {
        state.queue.retain(|waiting| !waiting.withdrawn());
        let Some(head) = state.queue.first_mut() else {
            state.delivering = false;
            return;
        };
        let Some(turn) = head.turn.take() else {
            // Handed already: its caller drains.
            return;
        };
        if turn.send(()).is_ok() {
            return;
        }
        // Its caller left as the drain came to it: the next one's turn.
        state.queue.remove(0);
    }
}

/// A batch whose delivery raised `error`.
fn strategy_failure(error: &RoomKitError) -> DeliveryOutcome {
    let error = DeliveryError::new(error.class_name(), error.to_string());
    DeliveryOutcome::failed("strategy_failed", error)
}

/// The error of a request whose outcome never came: the strategy dropped
/// it, a defect of this crate.
fn withdrawn() -> RoomKitError {
    RoomKitError::Internal("a queued delivery's outcome never came".to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use roomkit_core::model::hook::AllowOptions;
    use roomkit_core::model::{
        ChannelType, EventContent, HookResult, RoomContext, RoomEvent, RoomId,
    };
    use tokio::sync::Notify;

    use super::*;
    use crate::store::{ConversationStore, InMemoryStore, ListEventsOptions};
    use crate::testing::{RecordingTransport, RecordingTransportOptions};
    use crate::{
        AttachChannelOptions, CreateRoomOptions, DeliverOptions, HookOptions, HookTrigger, RoomKit,
        RoomKitOptions,
    };

    /// A kit whose room `r1` binds the transport `text`, and whose
    /// `BEFORE_BROADCAST` hook holds the message `first` until `release` is
    /// told, telling `reached` once it holds it.
    async fn held_kit(reached: &Arc<Notify>, release: &Arc<Notify>) -> RoomKit {
        let kit = RoomKit::new(RoomKitOptions::default());
        let room = RoomId::new("r1").unwrap();
        let text = ChannelId::new("text").unwrap();
        kit.register_channel(RecordingTransport::new(
            text.clone(),
            ChannelType::WEBSOCKET,
            RecordingTransportOptions::default(),
        ))
        .unwrap();
        let create = CreateRoomOptions {
            room_id: Some(room.clone()),
            ..CreateRoomOptions::default()
        };
        kit.create_room(create).await.unwrap();
        kit.attach_channel(&room, &text, AttachChannelOptions::default())
            .await
            .unwrap();
        let (reached, release) = (Arc::clone(reached), Arc::clone(release));
        kit.hook(
            HookTrigger::BeforeBroadcast,
            move |event: RoomEvent, _context: RoomContext| {
                let first =
                    matches!(&event.content, EventContent::Text(text) if text.body == "first");
                let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
                async move {
                    if first {
                        reached.notify_one();
                        release.notified().await;
                    }
                    HookResult::allow(AllowOptions::default())
                }
            },
            HookOptions::default(),
        )
        .unwrap();
        kit
    }

    fn room() -> RoomId {
        RoomId::new("r1").unwrap()
    }

    /// Starts `content`'s delivery with `strategy`, keyed by `key`.
    fn start(
        kit: &RoomKit,
        strategy: &Arc<Queued>,
        content: &'static str,
        key: Option<&str>,
    ) -> tokio::task::JoinHandle<DeliveryOutcome> {
        let (kit, strategy) = (kit.clone(), Arc::clone(strategy));
        let strategy: Arc<dyn DeliveryStrategy<InMemoryStore>> = strategy;
        let options = DeliverOptions {
            strategy: Some(strategy),
            idempotency_key: key.map(str::to_owned),
            ..DeliverOptions::default()
        };
        tokio::spawn(async move { kit.deliver(&room(), content, options).await.unwrap() })
    }

    /// Waits until `strategy` holds `queued` requests.
    async fn until_queued(strategy: &Queued, queued: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while strategy.lock().queue.len() < queued {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
    }

    async fn texts(kit: &RoomKit) -> Vec<String> {
        let events = kit
            .store()
            .list_events(&room(), ListEventsOptions::default())
            .await
            .unwrap();
        events
            .into_iter()
            .filter_map(|event| match event.content {
                EventContent::Text(text) => Some(text.body),
                _ => None,
            })
            .collect()
    }

    fn separated() -> Arc<Queued> {
        Arc::new(Queued::new(QueuedOptions {
            separator: " | ".to_owned(),
            ..QueuedOptions::default()
        }))
    }

    // Python: tests/test_delivery_strategies.py::TestQueuedStrategy::test_batching_multiple_deliveries:
    // the requests queued while a batch is sent are delivered together, each
    // caller told the one outcome.
    #[tokio::test]
    async fn test_batching_multiple_deliveries() {
        let (reached, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        let kit = held_kit(&reached, &release).await;
        let strategy = separated();
        let first = start(&kit, &strategy, "first", None);
        reached.notified().await;
        let second = start(&kit, &strategy, "second", None);
        let third = start(&kit, &strategy, "third", None);
        until_queued(&strategy, 2).await;
        release.notify_one();
        let first = first.await.unwrap();
        let (second, third) = (second.await.unwrap(), third.await.unwrap());
        assert_eq!(texts(&kit).await, ["first", "second | third"]);
        assert_eq!(second.status, DeliveryOutcomeStatus::Sent);
        assert_eq!(second.event_id, third.event_id);
        assert_ne!(first.event_id, second.event_id);
        assert!(!strategy.lock().delivering);
        kit.close().await.unwrap();
    }

    // Python: tests/delivery/test_targeted.py::test_queued_keeps_incompatible_targets_and_keyed_publications_separate:
    // a keyed request publishes alone, its key its own.
    #[tokio::test]
    async fn a_keyed_request_is_never_batched() {
        let (reached, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        let kit = held_kit(&reached, &release).await;
        let strategy = separated();
        let first = start(&kit, &strategy, "first", None);
        reached.notified().await;
        let keyed = start(&kit, &strategy, "keyed", Some("k"));
        until_queued(&strategy, 1).await;
        let plain = start(&kit, &strategy, "plain", None);
        until_queued(&strategy, 2).await;
        release.notify_one();
        first.await.unwrap();
        let (keyed, plain) = (keyed.await.unwrap(), plain.await.unwrap());
        assert_ne!(keyed.event_id, plain.event_id);
        assert_eq!(texts(&kit).await, ["first", "keyed", "plain"]);
        kit.close().await.unwrap();
    }

    // Python: tests/delivery/test_targeted.py::test_a_drain_cut_short_hands_the_queue_on:
    // the draining caller dropped, the callers behind it get the drain and
    // their outcomes.
    #[tokio::test]
    async fn a_drain_cut_short_hands_the_queue_on() {
        let (reached, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        let kit = held_kit(&reached, &release).await;
        let strategy = separated();
        let first = start(&kit, &strategy, "first", None);
        reached.notified().await;
        let second = start(&kit, &strategy, "second", None);
        until_queued(&strategy, 1).await;
        first.abort();
        drop(first.await);
        release.notify_one();
        let second = second.await.unwrap();
        assert_eq!(second.status, DeliveryOutcomeStatus::Sent);
        assert!(texts(&kit).await.contains(&"second".to_owned()));
        kit.close().await.unwrap();
    }
}
