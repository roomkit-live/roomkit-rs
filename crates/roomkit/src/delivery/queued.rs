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
    /// the same kit, room, transport, address, depth, intent, session and
    /// metadata (RFC §22.2).
    fn compatible(&self, other: &Self) -> bool {
        let (left, right) = (&self.context, &other.context);
        left.idempotency_key.is_none()
            && right.idempotency_key.is_none()
            && Arc::ptr_eq(&left.kit.inner, &right.kit.inner)
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
        let holds_a_lock = ExecutionContext::current().holds_a_room_lock();
        let (result, outcome) = oneshot::channel();
        let (turn, my_turn) = oneshot::channel();
        // Decided in the section that queues it: a caller holding a room lock
        // never queues behind a drain begun meanwhile.
        let queued = {
            let mut state = self.lock();
            if state.delivering && holds_a_lock {
                Err((ctx, channel_id))
            } else {
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
                Ok((id, drains))
            }
        };
        let (mine, drains) = match queued {
            Ok(queued) => queued,
            Err((ctx, channel_id)) => return ctx.kit.deliver_to_channel(&ctx, channel_id).await,
        };
        let mut waiter = Waiter {
            state: Arc::clone(&self.state),
            outcome,
            my_turn: Some(my_turn),
        };
        if !drains && let Waited::Outcome(outcome) = waiter.wait().await {
            return *outcome;
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
                Some(head) if head.id == mine => Some(Self::take_batch(&mut state)),
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
    fn take_batch(state: &mut Drain<S>) -> Vec<Waiting<S>> {
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
    Outcome(Box<Result<DeliveryOutcome, RoomKitError>>),
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
            delivered = &mut *outcome => Waited::Outcome(Box::new(delivered.map_err(|_| withdrawn()))),
            passed = turn => match passed {
                Ok(()) => Waited::Turn,
                // Its entry left the queue unhanded: a batch carried it.
                Err(_) => Waited::Outcome(Box::new(outcome.await.map_err(|_| withdrawn()))),
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
        self.outcome.close();
        let Some(my_turn) = self.my_turn.as_mut() else {
            return;
        };
        // Closed first, so a drain handed on from now on skips it, and one
        // handed before is read here and handed on.
        my_turn.close();
        if my_turn.try_recv().is_ok() {
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

    use std::future::Future;
    use std::sync::Mutex;

    use roomkit_core::model::hook::AllowOptions;
    use roomkit_core::model::{
        ChannelBinding, ChannelCategory, ChannelOutput, ChannelType, EventContent, HookResult,
        InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
    };
    use serde_json::{Value, json};
    use tokio::sync::Notify;

    use super::*;
    use crate::store::{ConversationStore, InMemoryStore, ListEventsOptions};
    use crate::testing::{RecordingTransport, RecordingTransportOptions};
    use crate::{
        AttachChannelOptions, CreateRoomOptions, DeliverOptions, HookOptions, HookTrigger,
        ProcessInboundOptions, RoomKit, RoomKitOptions,
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
            .filter_map(|event| {
                let EventContent::Text(text) = event.content else {
                    return None;
                };
                Some(text.body)
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

    // Python: delivery.py's _QueuedRequest.compatible, `left.kit is right.kit`:
    // one strategy shared by two kits batches neither kit's request into the
    // other's room, though their rooms and channels have the same ids.
    #[tokio::test]
    async fn two_kits_sharing_a_strategy_keep_their_requests_apart() {
        let (reached, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        let kit = held_kit(&reached, &release).await;
        let other = held_kit(&Arc::new(Notify::new()), &Arc::new(Notify::new())).await;
        let strategy = separated();
        let first = start(&kit, &strategy, "first", None);
        reached.notified().await;
        let second = start(&kit, &strategy, "second", None);
        until_queued(&strategy, 1).await;
        let elsewhere = start(&other, &strategy, "elsewhere", None);
        until_queued(&strategy, 2).await;
        release.notify_one();
        first.await.unwrap();
        let (second, elsewhere) = (second.await.unwrap(), elsewhere.await.unwrap());
        assert_eq!(texts(&kit).await, ["first", "second"]);
        assert_eq!(texts(&other).await, ["elsewhere"]);
        assert_eq!(
            (second.status, elsewhere.status),
            (DeliveryOutcomeStatus::Sent, DeliveryOutcomeStatus::Sent)
        );
        kit.close().await.unwrap();
        other.close().await.unwrap();
    }

    /// A transport whose parse holds the message `gated` until `release`
    /// is told, telling `reached` once it holds it: a delivery of that
    /// text waits there, before its room's lock, as Python's waits for
    /// voice idle.
    struct ParseGated {
        id: ChannelId,
        gated: &'static str,
        reached: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl crate::Channel for ParseGated {
        fn channel_id(&self) -> ChannelId {
            self.id.clone()
        }

        fn channel_type(&self) -> ChannelType {
            ChannelType::WEBSOCKET
        }

        async fn handle_inbound(
            &self,
            message: InboundMessage,
            context: RoomContext,
        ) -> Result<RoomEvent, crate::ChannelError> {
            if matches!(&message.content, EventContent::Text(text) if text.body == self.gated) {
                self.reached.notify_one();
                self.release.notified().await;
            }
            Ok(crate::default_inbound_event(self, &message, &context))
        }

        fn deliver(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> impl Future<Output = Result<ChannelOutput, crate::ChannelError>> + Send {
            std::future::ready(Ok(ChannelOutput::empty()))
        }
    }

    /// A kit, its `process_timeout` `timeout`, its rooms `rooms` binding
    /// the transport `human`, which holds the delivery of `gated` in its
    /// parse; and the gate's two sides, `reached` and `release`.
    async fn gated_kit(
        options: RoomKitOptions,
        rooms: &[&str],
        gated: &'static str,
    ) -> (RoomKit, Arc<Notify>, Arc<Notify>) {
        let kit = RoomKit::new(options);
        let (reached, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        let human = ChannelId::new("human").unwrap();
        let transport = ParseGated {
            id: human.clone(),
            gated,
            reached: Arc::clone(&reached),
            release: Arc::clone(&release),
        };
        kit.register_channel(transport).unwrap();
        for room in rooms {
            let room = RoomId::new(*room).unwrap();
            let create = CreateRoomOptions {
                room_id: Some(room.clone()),
                ..CreateRoomOptions::default()
            };
            kit.create_room(create).await.unwrap();
            kit.attach_channel(&room, &human, AttachChannelOptions::default())
                .await
                .unwrap();
        }
        (kit, reached, release)
    }

    /// The options of a delivery addressed to no agent with `strategy`,
    /// its metadata `metadata`.
    fn unaddressed(strategy: Option<&Arc<Queued>>, metadata: Value) -> DeliverOptions {
        let strategy = strategy.map(|strategy| {
            let strategy: Arc<Queued> = Arc::clone(strategy);
            let strategy: Arc<dyn DeliveryStrategy<InMemoryStore>> = strategy;
            strategy
        });
        DeliverOptions {
            strategy,
            addressed_to: Some(Vec::new()),
            metadata: serde_json::from_value(metadata).unwrap(),
            ..DeliverOptions::default()
        }
    }

    async fn texts_in(kit: &RoomKit, room: &str) -> Vec<(String, u32)> {
        let room = RoomId::new(room).unwrap();
        let events = kit
            .store()
            .list_events(&room, ListEventsOptions::default())
            .await
            .unwrap();
        events
            .into_iter()
            .filter_map(|event| {
                let EventContent::Text(text) = event.content else {
                    return None;
                };
                Some((text.body, event.chain_depth))
            })
            .collect()
    }

    fn said(body: &str) -> InboundMessage {
        InboundMessage::new(
            ChannelId::new("human").unwrap(),
            "u",
            TextContent::new(body),
        )
    }

    fn into(room: &str) -> ProcessInboundOptions {
        ProcessInboundOptions {
            room_id: Some(RoomId::new(room).unwrap()),
            ..ProcessInboundOptions::default()
        }
    }

    // Python: tests/delivery/test_targeted.py::test_a_queued_follower_is_delivered_at_its_own_depth:
    // one caller drains a shared strategy for all; a follower's delivery
    // runs as the follower made it, never inside the leader's work, 3 deep.
    #[tokio::test]
    async fn test_a_queued_follower_is_delivered_at_its_own_depth() {
        let (kit, reached, release) = gated_kit(RoomKitOptions::default(), &["r1"], "leader").await;
        let strategy = Arc::new(Queued::new(QueuedOptions::default()));
        let leader = {
            let (kit, options) = (kit.clone(), unaddressed(Some(&strategy), json!({})));
            let leading = async move { kit.deliver(&room(), "leader", options).await };
            tokio::spawn(ExecutionContext::default().at_least(3).scope(leading))
        };
        reached.notified().await;
        let follower = {
            let (kit, options) = (kit.clone(), unaddressed(Some(&strategy), json!({})));
            tokio::spawn(async move { kit.deliver(&room(), "app", options).await })
        };
        until_queued(&strategy, 1).await;
        release.notify_one();
        let (leader, follower) = (leader.await.unwrap(), follower.await.unwrap());
        assert_eq!(
            (leader.unwrap().status, follower.unwrap().status),
            (DeliveryOutcomeStatus::Sent, DeliveryOutcomeStatus::Sent)
        );
        let mut depths = texts_in(&kit, "r1").await;
        depths.sort();
        assert_eq!(depths, [("app".to_owned(), 0), ("leader".to_owned(), 3)]);
        kit.close().await.unwrap();
    }

    // Python: tests/delivery/test_targeted.py::test_a_queue_drained_under_the_room_lock_lets_its_followers_deliver:
    // a locked BEFORE_BROADCAST hook drains a shared strategy; an
    // application's request its batch cannot take (another metadata) waits
    // behind it, and delivers once the hook's section is over, never
    // waiting on a lock the hook holds.
    #[tokio::test]
    async fn test_a_queue_drained_under_the_room_lock_lets_its_followers_deliver() {
        let (kit, reached, release) = gated_kit(RoomKitOptions::default(), &["r1"], "hook").await;
        let strategy = Arc::new(Queued::new(QueuedOptions::default()));
        let got = Arc::new(Mutex::new(None));
        let (lead_kit, lead_strategy, lead_got) =
            (kit.clone(), Arc::clone(&strategy), Arc::clone(&got));
        kit.hook(
            HookTrigger::BeforeBroadcast,
            move |event: RoomEvent, _context: RoomContext| {
                let go = matches!(&event.content, EventContent::Text(text) if text.body == "go");
                let (kit, options, got) = (
                    lead_kit.clone(),
                    unaddressed(Some(&lead_strategy), json!({})),
                    Arc::clone(&lead_got),
                );
                async move {
                    if go {
                        let outcome = kit.deliver(&room(), "hook", options).await;
                        *got.lock().unwrap() = Some(outcome.map(|outcome| outcome.status));
                    }
                    HookResult::allow(AllowOptions::default())
                }
            },
            HookOptions::default(),
        )
        .unwrap();
        let go = {
            let kit = kit.clone();
            tokio::spawn(async move { kit.process_inbound(said("go"), into("r1")).await })
        };
        reached.notified().await;
        let app = {
            let (kit, options) = (
                kit.clone(),
                unaddressed(Some(&strategy), json!({"from": "app"})),
            );
            tokio::spawn(async move { kit.deliver(&room(), "app", options).await })
        };
        until_queued(&strategy, 1).await;
        release.notify_one();
        let within = Duration::from_secs(2);
        let inbound = tokio::time::timeout(within, go)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let delivered = tokio::time::timeout(within, app)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let hook = got.lock().unwrap().take().unwrap().unwrap();
        assert_eq!(
            (inbound.blocked, hook, delivered.status),
            (
                false,
                DeliveryOutcomeStatus::Sent,
                DeliveryOutcomeStatus::Sent
            )
        );
        kit.close().await.unwrap();
    }

    // Python: tests/delivery/test_targeted.py::test_a_caller_holding_the_room_lock_delivers_past_a_drain_under_way,
    // into its room and another room, drained by another caller or by the
    // kit's strategy delivering the very message whose pass runs the hook:
    // the hook, holding the room's lock, delivers at once, unbatched, and
    // every delivery completes.
    #[tokio::test]
    async fn test_a_caller_holding_the_room_lock_delivers_past_a_drain_under_way() {
        for target in ["r1", "r2"] {
            for another_caller in [true, false] {
                a_caller_holding_the_lock_delivers(target, another_caller).await;
            }
        }
    }

    async fn a_caller_holding_the_lock_delivers(target: &'static str, another_caller: bool) {
        let options = RoomKitOptions {
            process_timeout: Duration::from_millis(500),
            delivery_strategy: Some(Arc::new(Queued::new(QueuedOptions::default()))),
            ..RoomKitOptions::default()
        };
        let (kit, reached, release) = gated_kit(options, &["r1", "r2"], "app").await;
        let strategy = another_caller.then(|| Arc::new(Queued::new(QueuedOptions::default())));
        let (got, hooked) = (Arc::new(Mutex::new(None)), Arc::new(Notify::new()));
        let (hook_kit, hook_strategy, hook_got, hook_told) = (
            kit.clone(),
            strategy.clone(),
            Arc::clone(&got),
            Arc::clone(&hooked),
        );
        kit.hook(
            HookTrigger::BeforeBroadcast,
            move |event: RoomEvent, _context: RoomContext| {
                let go = matches!(&event.content, EventContent::Text(text) if text.body == "go");
                let (kit, got, told) = (
                    hook_kit.clone(),
                    Arc::clone(&hook_got),
                    Arc::clone(&hook_told),
                );
                let options = unaddressed(hook_strategy.as_ref(), json!({"h": 1}));
                async move {
                    if go {
                        let target = RoomId::new(target).unwrap();
                        let outcome = kit.deliver(&target, "from the hook", options).await;
                        *got.lock().unwrap() = Some(outcome.map(|outcome| outcome.status));
                        told.notify_one();
                    }
                    HookResult::allow(AllowOptions::default())
                }
            },
            HookOptions::default(),
        )
        .unwrap();
        let within = Duration::from_secs(2);
        let mut expected = vec!["from the hook", "go"];
        if another_caller {
            let first = {
                let (kit, options) = (kit.clone(), unaddressed(strategy.as_ref(), json!({})));
                tokio::spawn(async move { kit.deliver(&room(), "app", options).await })
            };
            reached.notified().await;
            let go = {
                let kit = kit.clone();
                tokio::spawn(async move { kit.process_inbound(said("go"), into("r1")).await })
            };
            // The app's drain still waits: the hook delivered past it.
            tokio::time::timeout(Duration::from_secs(1), hooked.notified())
                .await
                .unwrap();
            release.notify_one();
            let first = tokio::time::timeout(within, first).await.unwrap().unwrap();
            let go = tokio::time::timeout(within, go).await.unwrap().unwrap();
            assert_eq!(first.unwrap().status, DeliveryOutcomeStatus::Sent);
            assert!(!go.unwrap().blocked);
            expected.push("app");
        } else {
            let room = room();
            let first = kit.deliver(&room, "go", unaddressed(None, json!({})));
            let first = tokio::time::timeout(within, first).await.unwrap();
            assert_eq!(first.unwrap().status, DeliveryOutcomeStatus::Sent);
        }
        let hook = got.lock().unwrap().take().unwrap().unwrap();
        let mut bodies: Vec<String> = Vec::new();
        for room in ["r1", "r2"] {
            bodies.extend(texts_in(&kit, room).await.into_iter().map(|(body, _)| body));
        }
        bodies.sort();
        expected.sort_unstable();
        assert_eq!(
            (hook, bodies),
            (
                DeliveryOutcomeStatus::Sent,
                expected.into_iter().map(str::to_owned).collect()
            ),
            "into {target}, drained by another caller: {another_caller}"
        );
        kit.close().await.unwrap();
    }

    /// An agent whose turn on `first` delivers `second` into its room with
    /// `strategy`, before it answers nothing.
    struct Following {
        kit: Arc<Mutex<Option<crate::WeakRoomKit>>>,
        strategy: Arc<Queued>,
    }

    impl crate::Channel for Following {
        fn channel_id(&self) -> ChannelId {
            ChannelId::new("a").unwrap()
        }

        fn channel_type(&self) -> ChannelType {
            ChannelType::AI
        }

        fn category(&self) -> ChannelCategory {
            ChannelCategory::Intelligence
        }

        fn handle_inbound(
            &self,
            message: InboundMessage,
            context: RoomContext,
        ) -> impl Future<Output = Result<RoomEvent, crate::ChannelError>> + Send {
            std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
        }

        async fn on_event(
            &self,
            event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> Result<ChannelOutput, crate::ChannelError> {
            let first = matches!(&event.content, EventContent::Text(text) if text.body == "first");
            let kit = self
                .kit
                .lock()
                .unwrap()
                .clone()
                .and_then(|kit| kit.upgrade());
            if let (true, Some(kit)) = (first, kit) {
                let options = unaddressed(Some(&self.strategy), json!({}));
                kit.deliver(&room(), "second", options).await.unwrap();
            }
            Ok(ChannelOutput::empty())
        }

        fn deliver(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> impl Future<Output = Result<ChannelOutput, crate::ChannelError>> + Send {
            std::future::ready(Ok(ChannelOutput::empty()))
        }
    }

    // Python: tests/delivery/test_targeted.py::test_queued_accepts_reentrant_delivery_from_agent_without_deadlocking
    #[tokio::test]
    async fn test_queued_accepts_reentrant_delivery_from_agent_without_deadlocking() {
        let (kit, _reached, _release) = gated_kit(RoomKitOptions::default(), &["r1"], "").await;
        let strategy = Arc::new(Queued::new(QueuedOptions::default()));
        let agent = Following {
            kit: Arc::new(Mutex::new(Some(kit.downgrade()))),
            strategy: Arc::clone(&strategy),
        };
        kit.register_channel(agent).unwrap();
        let attach = AttachChannelOptions {
            category: Some(ChannelCategory::Intelligence),
            ..AttachChannelOptions::default()
        };
        kit.attach_channel(&room(), &ChannelId::new("a").unwrap(), attach)
            .await
            .unwrap();
        let options = DeliverOptions {
            addressed_to: Some(vec![ChannelId::new("a").unwrap()]),
            ..unaddressed(Some(&strategy), json!({}))
        };
        let within = Duration::from_secs(2);
        let result = tokio::time::timeout(within, kit.deliver(&room(), "first", options))
            .await
            .unwrap()
            .unwrap();
        let delivery = result.inbound.and_then(|inbound| inbound.delivery).unwrap();
        tokio::time::timeout(within, delivery.wait())
            .await
            .unwrap()
            .unwrap();
        let bodies: Vec<String> = texts_in(&kit, "r1")
            .await
            .into_iter()
            .map(|(body, _)| body)
            .collect();
        assert_eq!(bodies, ["first", "second"]);
        kit.close().await.unwrap();
    }

    // Python: tests/delivery/test_instruction_delivery.py::test_queued_never_merges_an_instruction_with_a_message
    #[tokio::test]
    async fn test_queued_never_merges_an_instruction_with_a_message() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let request = |instruction: bool| {
            let context = DeliveryContext {
                instruction,
                ..DeliveryContext::new(kit.clone(), room(), "x".to_owned())
            };
            let (result, _outcome) = oneshot::channel();
            Waiting {
                id: 0,
                context,
                channel_id: ChannelId::new("phone").unwrap(),
                result,
                turn: None,
            }
        };
        assert!(!request(true).compatible(&request(false)));
        assert!(request(true).compatible(&request(true)));
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
