//! The reentrant lock and direct injection on the tokio driver (RFC §13.5,
//! §10.5, §9.5.1 Reentrance, §10.1 step 18): code that runs inside a room's
//! own work, a hook under the lock, an off-lock hook, an agent's turn, a
//! transport's delivery, calls into that room and never waits for itself.
//!
//! Every call is bounded: a test fails instead of hanging when a call waits
//! for its own caller.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a test fails on a broken fixture"
)]

use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, EventDirection, EventSource, EventStatus, EventType, HookResult, InboundMessage,
    RoomContext, RoomEvent, RoomId, RoomStatus, Timestamp,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CloseRoomOptions,
    CommitEventOptions, ConversationStore, CreateRoomOptions, DetachChannelOptions, EventDraft,
    FrameworkEvent, HookExecution, HookSpec, HookTrigger, InboundError, InboundRoomRouter,
    KitError, MutationError, MuteOptions, ProcessInboundOptions, RoomFilter, RoomKit,
    RoomKitOptions, RouteOptions, RouterFuture, SendEventOptions, UpdateEventOptions,
    default_inbound_event, text_of,
};
use tokio::sync::Notify;

/// How long a call that should not wait may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

/// Runs the scenario `$scenario` as the test `$single` on a single-threaded
/// runtime, and as `$threaded` on a multi-threaded one, where the calls of
/// one room's work run in parallel.
macro_rules! on_both_runtimes {
    ($single:ident, $threaded:ident, $scenario:ident) => {
        #[tokio::test]
        async fn $single() {
            $scenario().await;
        }

        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn $threaded() {
            $scenario().await;
        }
    };
}

type Answer = Pin<Box<dyn Future<Output = Result<ChannelOutput, ChannelError>> + Send>>;
type Call = Arc<dyn Fn(RoomEvent) -> Answer + Send + Sync>;

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// What runs a message into `room`.
fn in_room(room: RoomId) -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room),
        ..ProcessInboundOptions::default()
    }
}

fn message(text: &str) -> InboundMessage {
    InboundMessage::new(channel("sms"), "+15551234567", EventContent::text(text))
}

/// A SYNC `BEFORE_BROADCAST` hook named `name`, under the lock or off it.
fn before_broadcast(name: &str, needs_lock: bool) -> HookSpec {
    let mut spec = HookSpec::new(name, HookTrigger::BeforeBroadcast, HookExecution::Sync);
    spec.needs_lock = needs_lock;
    spec
}

/// A channel whose parse, turn or delivery runs a scripted call; a
/// scripted parse runs before the default one.
struct Scripted {
    declaration: ChannelDeclaration,
    parse: Option<Call>,
    on_event: Option<Call>,
    deliver: Option<Call>,
}

impl Scripted {
    /// An agent whose turn runs `on_event`.
    fn agent(id: &str, on_event: Call) -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel(id),
                ChannelType::AI,
                ChannelCategory::Intelligence,
                ChannelDirection::Bidirectional,
            ),
            parse: None,
            on_event: Some(on_event),
            deliver: None,
        }
    }

    /// A transport needing no recipient whose delivery runs `deliver`.
    fn transport(id: &str, deliver: Call) -> Self {
        let mut declaration = ChannelDeclaration::new(
            channel(id),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.recipient_key = None;
        Self {
            declaration,
            parse: None,
            on_event: None,
            deliver: Some(deliver),
        }
    }

    /// The same channel, running `parse` on each message before its default
    /// parse.
    fn parsing(mut self, parse: Call) -> Self {
        self.parse = Some(parse);
        self
    }
}

impl Channel for Scripted {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        _context: RoomContext,
        draft: EventDraft,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let event = default_inbound_event(
            &message,
            &self.declaration,
            draft.room_id,
            draft.id,
            draft.now,
        );
        let parse = self.parse.clone();
        async move {
            if let Some(parse) = parse {
                parse(event.clone()).await?;
            }
            Ok(event)
        }
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        match &self.on_event {
            Some(call) => call(event),
            None => Box::pin(ready(Ok(ChannelOutput::default()))),
        }
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        match &self.deliver {
            Some(call) => call(event),
            None => Box::pin(ready(Ok(ChannelOutput::default()))),
        }
    }
}

/// A kit with the customer's SMS, the advisor's console `ws` and an
/// `audit` transport that records every event of `room_1`, in order. The
/// three attaches' lifecycle records take the room's indexes 0 to 2 (gap
/// G-046).
async fn room() -> (RoomKit, RecordingTransport) {
    room_with(RoomKitOptions::default()).await
}

/// [`room`] on a kit built with `options`.
async fn room_with(options: RoomKitOptions) -> (RoomKit, RecordingTransport) {
    let kit = RoomKit::new(options);
    let audit = RecordingTransport::new(channel("audit"), ChannelType::WEBSOCKET);
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.register_channel(RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
    ))
    .unwrap();
    kit.register_channel(audit.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ws", "audit"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, audit)
}

/// Runs `message` into `room_1`, failing the test past [`BOUND`].
async fn send(kit: &RoomKit, text: &str) -> Result<roomkit::InboundResult, KitError> {
    tokio::time::timeout(
        BOUND,
        kit.process_inbound(message(text), in_room(room_id())),
    )
    .await
    .unwrap_or_else(|_| panic!("the call waited for itself"))
}

/// The texts `audit` delivered, once it delivered `count` of them or the
/// bound passed: a call answered at its decision is delivered after its
/// caller's call, and nothing awaits its delivery.
async fn delivered(audit: &RecordingTransport, count: usize) -> Vec<String> {
    let deadline = tokio::time::Instant::now() + BOUND;
    loop {
        let texts = audit.texts();
        if texts.len() >= count || tokio::time::Instant::now() >= deadline {
            return texts;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The room's timeline, BLOCKED records included, as `index source status
/// text` lines.
async fn timeline(kit: &RoomKit) -> Vec<String> {
    kit.store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
        .iter()
        .map(|event| {
            let status = if event.status == EventStatus::Blocked {
                "blocked"
            } else {
                "delivered"
            };
            format!(
                "#{} {} {status} {}",
                event.index,
                event.source.channel_id,
                text_of(&event.content)
            )
        })
        .collect()
}

/// RFC §13.5, §7.5 rule 2: a hook under the lock changes the room through
/// the kit's own calls, which take the lock again at once.
mod a_locked_hook {
    use super::*;
    use pretty_assertions::assert_eq;

    on_both_runtimes!(
        that_mutes_the_source_blocks_the_event,
        that_mutes_the_source_blocks_the_event_on_threads,
        mutes_the_source
    );

    /// The hook mutes the event's source: the event is stored
    /// `source_muted`.
    async fn mutes_the_source() {
        let (kit, _) = room().await;
        let hook_kit = kit.clone();
        kit.hook(before_broadcast("moderator", true), move |_, _| {
            let kit = hook_kit.clone();
            async move {
                kit.mute(&room_id(), &channel("sms"), MuteOptions::default())
                    .await?;
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        assert_eq!(
            (result.reason, timeline(&kit).await),
            (
                Some("source_muted".to_owned()),
                vec![
                    "#0 system delivered Channel sms attached (read_write, all)".to_owned(),
                    "#1 system delivered Channel ws attached (read_write, all)".to_owned(),
                    "#2 system delivered Channel audit attached (read_write, all)".to_owned(),
                    "#3 system delivered Channel sms muted".to_owned(),
                    "#4 sms blocked hello".to_owned()
                ]
            )
        );
    }

    /// The hook closes the room: the event it decides on is refused, as a
    /// closed room refuses any (RFC §5.1, gap G-070).
    #[tokio::test]
    async fn that_closes_the_room_refuses_the_event() {
        let (kit, _) = room().await;
        let mut events = kit.subscribe();
        let hook_kit = kit.clone();
        kit.hook(before_broadcast("farewell", true), move |_, _| {
            let kit = hook_kit.clone();
            async move {
                kit.close_room(&room_id(), CloseRoomOptions::default())
                    .await?;
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        let mut names = Vec::new();
        while let Ok(event) = events.try_recv() {
            if matches!(event, FrameworkEvent::RoomClosed { .. }) {
                names.push("room_closed");
            } else if matches!(event, FrameworkEvent::RoomRefusedEvent { .. }) {
                names.push("room_refused_event");
            }
        }
        assert_eq!(
            (result.reason, timeline(&kit).await.len(), names),
            // The attaches' records; the close writes none (gap G-046).
            (
                Some("room_closed".to_owned()),
                3,
                vec!["room_closed", "room_refused_event"]
            )
        );
    }

    on_both_runtimes!(
        that_injects_commits_its_event_ahead,
        that_injects_commits_its_event_ahead_on_threads,
        injects_ahead
    );

    /// The hook injects into its own room: its event commits ahead of the
    /// one the hook decides on, which takes the next index, and the hook
    /// receives it at once. The delivery sets keep index order.
    async fn injects_ahead() {
        let (kit, audit) = room().await;
        let seen = Arc::new(Mutex::new(None));
        let (hook_kit, hook_seen) = (kit.clone(), Arc::clone(&seen));
        kit.hook(before_broadcast("notifier", true), move |event, _| {
            let (kit, seen) = (hook_kit.clone(), Arc::clone(&hook_seen));
            async move {
                if event.source.channel_id == channel("sms") {
                    let injected = kit
                        .send_event(
                            &room_id(),
                            &channel("ws"),
                            EventContent::text("Heads up"),
                            SendEventOptions::default(),
                        )
                        .await?;
                    *seen.lock().unwrap() = Some(injected.index);
                }
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        assert_eq!(
            (
                *seen.lock().unwrap(),
                result.event.map(|event| event.index),
                audit.texts()
            ),
            (
                Some(3),
                Some(4),
                vec!["Heads up".to_owned(), "hello".to_owned()]
            )
        );
    }

    /// The hook injects twice at once: the two entries run one after the
    /// other within the hook's section, each with its own index.
    #[tokio::test]
    async fn that_injects_twice_at_once_commits_both_ahead() {
        let (kit, audit) = room().await;
        let hook_kit = kit.clone();
        kit.hook(before_broadcast("notifier", true), move |event, _| {
            let kit = hook_kit.clone();
            async move {
                if event.source.channel_id == channel("sms") {
                    let (room, ws) = (room_id(), channel("ws"));
                    let (first, second) = tokio::join!(
                        kit.send_event(
                            &room,
                            &ws,
                            EventContent::text("First"),
                            SendEventOptions::default()
                        ),
                        kit.send_event(
                            &room,
                            &ws,
                            EventContent::text("Second"),
                            SendEventOptions::default()
                        )
                    );
                    first?;
                    second?;
                }
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        let mut ahead = audit.texts();
        ahead.truncate(2);
        ahead.sort();
        assert_eq!(
            (
                result.event.map(|event| event.index),
                ahead,
                timeline(&kit).await.len()
            ),
            (Some(5), vec!["First".to_owned(), "Second".to_owned()], 6)
        );
    }

    /// RFC §10.5 `commit_event` under the lock: the record takes the next
    /// index, ahead of the event the hook decides on, and reaches no one.
    #[tokio::test]
    async fn that_commits_a_record_leaves_the_event_the_next_index() {
        let (kit, audit) = room().await;
        let hook_kit = kit.clone();
        kit.hook(before_broadcast("tracer", true), move |event, _| {
            let kit = hook_kit.clone();
            async move {
                let mut record = event.clone();
                record.id = "trace_1".parse()?;
                record.content = EventContent::text("trace");
                kit.commit_event(&room_id(), record, CommitEventOptions::default())
                    .await?;
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.index),
                timeline(&kit).await,
                audit.texts()
            ),
            (
                Some(4),
                vec![
                    "#0 system delivered Channel sms attached (read_write, all)".to_owned(),
                    "#1 system delivered Channel ws attached (read_write, all)".to_owned(),
                    "#2 system delivered Channel audit attached (read_write, all)".to_owned(),
                    "#3 sms delivered trace".to_owned(),
                    "#4 sms delivered hello".to_owned()
                ],
                vec!["hello".to_owned()]
            )
        );
    }

    /// The lock taken again by the hook's own call is let go with the
    /// section's release only: a call from outside, which holds no lease,
    /// waits for the section, so the event is decided before the room
    /// closes.
    #[tokio::test]
    async fn keeps_the_lock_after_its_call_releases_it() {
        let (kit, _) = room().await;
        let muted = Arc::new(Notify::new());
        let (hook_kit, hook_muted) = (kit.clone(), Arc::clone(&muted));
        kit.hook(before_broadcast("moderator", true), move |_, _| {
            let (kit, muted) = (hook_kit.clone(), Arc::clone(&hook_muted));
            async move {
                kit.mute(&room_id(), &channel("sms"), MuteOptions::default())
                    .await?;
                muted.notify_one();
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let outside = {
            let (kit, muted) = (kit.clone(), Arc::clone(&muted));
            tokio::spawn(async move {
                muted.notified().await;
                kit.close_room(&room_id(), CloseRoomOptions::default())
                    .await
                    .unwrap();
            })
        };
        let result = send(&kit, "hello").await.unwrap();
        tokio::time::timeout(BOUND, outside).await.unwrap().unwrap();
        assert_eq!(
            (result.reason, timeline(&kit).await),
            (
                Some("source_muted".to_owned()),
                vec![
                    "#0 system delivered Channel sms attached (read_write, all)".to_owned(),
                    "#1 system delivered Channel ws attached (read_write, all)".to_owned(),
                    "#2 system delivered Channel audit attached (read_write, all)".to_owned(),
                    "#3 system delivered Channel sms muted".to_owned(),
                    "#4 sms blocked hello".to_owned()
                ]
            )
        );
    }
}

/// RFC §9.5.1 Reentrance: an off-lock hook's injection takes no ticket, so
/// it waits for none its own call holds, and commits ahead.
#[tokio::test]
async fn an_off_lock_hook_injects_without_a_ticket() {
    let (kit, audit) = room().await;
    let hook_kit = kit.clone();
    kit.hook(before_broadcast("scanner", false), move |event, _| {
        let kit = hook_kit.clone();
        async move {
            if event.source.channel_id == channel("sms") {
                kit.send_event(
                    &room_id(),
                    &channel("ws"),
                    EventContent::text("Scanned"),
                    SendEventOptions::default(),
                )
                .await?;
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    assert_eq!(
        (result.event.map(|event| event.index), audit.texts()),
        (Some(4), vec!["Scanned".to_owned(), "hello".to_owned()])
    );
}

/// RFC §9.5.1, §10.1 step 18: an off-lock hook's body, the event's
/// admission work, makes a deferred call into the room, adopted, and waits
/// its handle: the call's delivery set runs ahead of the event, and the
/// wait returns with its delivery results; a wait from outside, later,
/// reads them too, as Python's (tasks/probes/step5-lot1-review-A probe 1).
/// Under the lock the wait returns unwaited, the delivery following.
#[tokio::test]
async fn a_wait_from_admission_work_returns_with_its_delivery_results() {
    let off_lock = waited_from_a_hook(false).await;
    let under_lock = waited_from_a_hook(true).await;
    assert_eq!(
        (off_lock, under_lock),
        (
            (
                vec!["audit".to_owned(), "ws".to_owned()],
                vec!["audit".to_owned(), "ws".to_owned()],
                vec!["side".to_owned(), "hello".to_owned()]
            ),
            // Committed ahead of the section's event, it is delivered first
            // (`a_locked_hook::injects_ahead`).
            (
                vec![],
                vec!["audit".to_owned(), "ws".to_owned()],
                vec!["side".to_owned(), "hello".to_owned()]
            )
        )
    );
}

/// What a hook of `hello`, under the lock or off it, read of the delivery
/// of the deferred `side` it sent and waited for; what a wait from outside
/// read once the call was over; and what `audit` delivered.
async fn waited_from_a_hook(needs_lock: bool) -> (Vec<String>, Vec<String>, Vec<String>) {
    let (kit, audit) = room().await;
    let handles = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (hook_kit, hook_handles, hook_seen) =
        (kit.clone(), Arc::clone(&handles), Arc::clone(&seen));
    kit.hook(before_broadcast("side", needs_lock), move |event, _| {
        let (kit, handles, seen) = (
            hook_kit.clone(),
            Arc::clone(&hook_handles),
            Arc::clone(&hook_seen),
        );
        async move {
            if text_of(&event.content) == "hello" {
                let deferred = ProcessInboundOptions {
                    defer_delivery: true,
                    ..in_room(room_id())
                };
                let side = kit.process_inbound(message("side"), deferred).await?;
                let handle = side.delivery.unwrap();
                let waited = handle.wait().await?;
                *seen.lock().unwrap() = delivered_to(&waited);
                handles.lock().unwrap().push(handle);
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    send(&kit, "hello").await.unwrap();
    let handle = handles.lock().unwrap().pop().unwrap();
    let later = tokio::time::timeout(BOUND, handle.wait())
        .await
        .unwrap()
        .unwrap();
    let texts = delivered(&audit, 2).await;
    let waited = seen.lock().unwrap().clone();
    (waited, delivered_to(&later), texts)
}

/// The channels `result` reports a delivery to.
fn delivered_to(result: &roomkit::InboundResult) -> Vec<String> {
    result
        .delivery_results
        .keys()
        .map(|id| id.as_str().to_owned())
        .collect()
}

/// RFC §10.1 step 18: the own-lane short-circuit holds while the delivery
/// is not done, as Python's `if not self.done`: a delivery of the room
/// waiting on the handle of an earlier call, whose delivery ran after its
/// commit and is over, reads its delivery results.
#[tokio::test]
async fn a_wait_from_its_room_s_lane_on_a_delivery_done_reads_its_results() {
    let (kit, _audit) = room().await;
    let (gate, handle_slot) = (Arc::new(Notify::new()), Arc::new(Mutex::new(None)));
    let seen = Arc::new(Mutex::new(None));
    let (bot_gate, bot_slot, bot_seen) = (
        Arc::clone(&gate),
        Arc::clone(&handle_slot),
        Arc::clone(&seen),
    );
    let bot = Scripted::transport(
        "bot",
        Arc::new(move |event: RoomEvent| {
            let (gate, slot, seen) = (
                Arc::clone(&bot_gate),
                Arc::clone(&bot_slot),
                Arc::clone(&bot_seen),
            );
            Box::pin(async move {
                match text_of(&event.content).as_str() {
                    "first" => gate.notified().await,
                    "hello" => {
                        let handle: roomkit::DeliveryHandle = slot.lock().unwrap().clone().unwrap();
                        let waited = handle.wait().await?;
                        *seen.lock().unwrap() = Some(delivered_to(&waited));
                    }
                    _ => {}
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(bot).unwrap();
    kit.attach_channel(&room_id(), &channel("bot"), AttachChannelOptions::default())
        .await
        .unwrap();
    let first = ProcessInboundOptions {
        defer_delivery: true,
        ..in_room(room_id())
    };
    let first = kit.process_inbound(message("first"), first).await.unwrap();
    let handle = first.delivery.unwrap();
    let at_commit = (first.delivery_results.len(), handle.done());
    gate.notify_one();
    tokio::time::timeout(BOUND, handle.wait())
        .await
        .unwrap()
        .unwrap();
    *handle_slot.lock().unwrap() = Some(handle);
    send(&kit, "hello").await.unwrap();
    assert_eq!(
        (at_commit, seen.lock().unwrap().clone()),
        (
            (0, false),
            Some(vec!["audit".to_owned(), "bot".to_owned(), "ws".to_owned()])
        )
    );
}

on_both_runtimes!(
    an_agent_s_turn_injects_into_its_own_room_and_receives_its_event_at_once,
    an_agent_s_turn_injects_into_its_own_room_on_threads,
    agent_injects
);

/// RFC §10.1 step 18, §9.5.1 Reentrance: an agent's turn injects into its
/// own room, a new arrival, and receives its event at once, at its
/// decision; the event's delivery follows in the lane, after the turn's own
/// set.
async fn agent_injects() {
    let (kit, audit) = room().await;
    let seen = Arc::new(Mutex::new(None));
    let (turn_kit, turn_seen, turn_audit) = (kit.clone(), Arc::clone(&seen), audit.clone());
    let agent = Scripted::agent(
        "ai",
        Arc::new(move |event: RoomEvent| {
            let (kit, seen, audit) = (turn_kit.clone(), Arc::clone(&turn_seen), turn_audit.clone());
            Box::pin(async move {
                if event.source.channel_id == channel("sms") {
                    let thought = SendEventOptions {
                        event_type: EventType::Message,
                        ..Default::default()
                    };
                    let injected = kit
                        .send_event(
                            &room_id(),
                            &channel("ai"),
                            EventContent::text("Let me check."),
                            thought,
                        )
                        .await?;
                    let delivered_yet = audit.texts().contains(&"Let me check.".to_owned());
                    *seen.lock().unwrap() = Some((
                        injected.index,
                        injected.status,
                        injected.source.direction,
                        delivered_yet,
                    ));
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(agent).unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    let texts = delivered(&audit, 2).await;
    assert_eq!(
        (*seen.lock().unwrap(), texts),
        (
            Some((5, EventStatus::Delivered, EventDirection::Outbound, false)),
            vec!["hello".to_owned(), "Let me check.".to_owned()]
        )
    );
}

/// RFC §10.1, §13.5: a channel's call to `process_inbound` for its own room
/// from its delivery is a new arrival, returns at its decision with no
/// delivery result, and its delivery follows in the lane.
#[tokio::test]
async fn process_inbound_from_a_channel_into_its_own_room_returns_at_its_commit() {
    into_its_own_room_from_its_delivery(false).await;
}

/// RFC §10.1 step 18: deferred, the same call's handle waited for there
/// returns the result at once, unwaited, as the call that waits returns:
/// its delivery could only wait for that wait.
#[tokio::test]
async fn a_deferred_wait_from_a_delivery_into_its_own_room_returns_unwaited() {
    into_its_own_room_from_its_delivery(true).await;
}

/// A channel's delivery of `hello` calls into its own room, deferring the
/// call's delivery and waiting its handle when `defer`.
async fn into_its_own_room_from_its_delivery(defer: bool) {
    let (kit, audit) = room().await;
    let seen = Arc::new(Mutex::new(None));
    let (bot_kit, bot_seen) = (kit.clone(), Arc::clone(&seen));
    let bot = Scripted::transport(
        "bot",
        Arc::new(move |event: RoomEvent| {
            let (kit, seen) = (bot_kit.clone(), Arc::clone(&bot_seen));
            Box::pin(async move {
                if text_of(&event.content) == "hello" {
                    let options = ProcessInboundOptions {
                        defer_delivery: defer,
                        ..in_room(room_id())
                    };
                    let mut result = kit.process_inbound(message("follow-up"), options).await?;
                    if let Some(delivery) = result.delivery.take() {
                        result = delivery.wait().await?;
                    }
                    *seen.lock().unwrap() = Some((
                        result.event.map(|event| event.index),
                        result.delivery_results.len(),
                    ));
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(bot).unwrap();
    kit.attach_channel(&room_id(), &channel("bot"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    let texts = delivered(&audit, 2).await;
    assert_eq!(
        (*seen.lock().unwrap(), texts),
        (
            Some((Some(5), 0)),
            vec!["hello".to_owned(), "follow-up".to_owned()]
        )
    );
}

/// RFC §10.1 step 18: room 1's delivery of `hello` calls into room 2 and
/// goes on; once it is over, room 2's delivery calls back into room 1 and
/// waits for that call's delivery. Room 2's run carries the frame of room
/// 1's delivery, which no longer runs: it works in room 1 no more, so the
/// call back waits for its delivery, as Python's does.
#[tokio::test]
async fn a_call_back_from_another_room_s_delivery_waits_for_its_delivery() {
    let (kit, _audit) = room().await;
    let room_2 = RoomId::new("room_2").unwrap();
    let (go, seen) = (Arc::new(Notify::new()), Arc::new(Mutex::new(None)));
    let (relay_kit, into_2) = (kit.clone(), in_room(room_2.clone()));
    let relay = Scripted::transport(
        "relay",
        Arc::new(move |event: RoomEvent| {
            let (kit, options) = (relay_kit.clone(), into_2.clone());
            Box::pin(async move {
                if text_of(&event.content) == "hello" {
                    let deferred = ProcessInboundOptions {
                        defer_delivery: true,
                        ..options
                    };
                    kit.process_inbound(message("to room 2"), deferred).await?;
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    let (back_kit, back_go, back_seen) = (kit.clone(), Arc::clone(&go), Arc::clone(&seen));
    let back = Scripted::transport(
        "back",
        Arc::new(move |event: RoomEvent| {
            let (kit, go, seen) = (
                back_kit.clone(),
                Arc::clone(&back_go),
                Arc::clone(&back_seen),
            );
            Box::pin(async move {
                if text_of(&event.content) == "to room 2" {
                    go.notified().await;
                    let result = kit
                        .process_inbound(message("back"), in_room(room_id()))
                        .await?;
                    let delivered: Vec<String> = result
                        .delivery_results
                        .keys()
                        .map(|id| id.as_str().to_owned())
                        .collect();
                    *seen.lock().unwrap() = Some(delivered);
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(relay).unwrap();
    kit.register_channel(back).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("relay"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_2.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "back"] {
        kit.attach_channel(&room_2, &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    send(&kit, "hello").await.unwrap();
    go.notify_one();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    assert_eq!(
        seen.lock().unwrap().clone(),
        Some(vec![
            "audit".to_owned(),
            "relay".to_owned(),
            "ws".to_owned()
        ])
    );
}

/// RFC §17.2, §8.3: a call from a delivery into its room naming another
/// organization reads as not found and draws nothing on the delivering
/// cascade's reentry budget, as the Python implementation's draws nothing:
/// sixty such calls, past the cap of fifty, leave the next call accepted.
#[tokio::test]
async fn a_call_naming_another_organization_draws_nothing_on_the_budget() {
    let (kit, _audit) = room().await;
    let seen = Arc::new(Mutex::new(None));
    let (bot_kit, bot_seen) = (kit.clone(), Arc::clone(&seen));
    let bot = Scripted::transport(
        "bot",
        Arc::new(move |event: RoomEvent| {
            let (kit, seen) = (bot_kit.clone(), Arc::clone(&bot_seen));
            Box::pin(async move {
                if text_of(&event.content) != "hello" {
                    return Ok(ChannelOutput::default());
                }
                let mut not_found = 0;
                for _ in 0..60 {
                    let other = ProcessInboundOptions {
                        organization_id: Some("other".to_owned()),
                        ..in_room(room_id())
                    };
                    let refused = kit.process_inbound(message("other"), other).await;
                    not_found += usize::from(matches!(
                        refused,
                        Err(KitError::Inbound(InboundError::RoomNotFound(_)))
                    ));
                }
                let last = kit
                    .process_inbound(message("last"), in_room(room_id()))
                    .await?;
                *seen.lock().unwrap() = Some((not_found, last.blocked, last.reason));
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(bot).unwrap();
    kit.attach_channel(&room_id(), &channel("bot"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    assert_eq!(seen.lock().unwrap().clone(), Some((60, false, None)));
}

/// RFC §8.3, gap G-070: a transport whose delivery sends a follow-up into
/// its room on every delivery stops at the reentry cap. Each follow-up is a
/// new arrival drawing on the budget of the cascade that delivers, the
/// chain's first call's, ten times the chain-depth limit, and the one past
/// it is refused with no event.
#[tokio::test]
async fn a_delivery_that_calls_into_its_room_on_every_delivery_stops_at_the_reentry_cap() {
    let (kit, _) = room_with(RoomKitOptions {
        max_chain_depth: 1,
        ..Default::default()
    })
    .await;
    let refused = Arc::new(Mutex::new(Vec::new()));
    let (bot_kit, bot_refused) = (kit.clone(), Arc::clone(&refused));
    let bot = Scripted::transport(
        "bot",
        Arc::new(move |_| {
            let (kit, refused) = (bot_kit.clone(), Arc::clone(&bot_refused));
            Box::pin(async move {
                let result = kit
                    .process_inbound(message("follow-up"), in_room(room_id()))
                    .await?;
                if result.event.is_none() {
                    refused.lock().unwrap().push(result.reason);
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(bot).unwrap();
    kit.attach_channel(&room_id(), &channel("bot"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    // The follow-ups are answered at their decisions: nothing awaits the
    // chain but its refusal.
    let deadline = tokio::time::Instant::now() + BOUND;
    while refused.lock().unwrap().is_empty() {
        let stored = timeline(&kit).await.len();
        assert!(
            tokio::time::Instant::now() < deadline,
            "the chain never stopped: {stored} events stored"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let stored = timeline(&kit).await.len();
    assert_eq!(
        (refused.lock().unwrap().clone(), stored),
        // Four lifecycle records, then the eleven events (gap G-046).
        (vec![Some("reentry_loop_cap".to_owned())], 15)
    );
}

/// RFC §9.5.1 Past the cap, §13.4: a delivery's injection whose key its
/// channel holds is a replay, its key read before the budget it draws on.
/// The delivery injects under the key `ECHO`, spends the budget with
/// unkeyed events, then injects under `ECHO` again: past the budget, it
/// receives the event its key committed, and nothing is stored under the
/// key again.
#[tokio::test]
async fn a_delivery_s_keyed_injection_past_the_reentry_cap_is_a_replay() {
    let (kit, _) = room_with(RoomKitOptions {
        max_chain_depth: 1,
        ..Default::default()
    })
    .await;
    let returned = Arc::new(Mutex::new(Vec::new()));
    let (bot_kit, bot_returned) = (kit.clone(), Arc::clone(&returned));
    let bot = Scripted::transport(
        "bot",
        Arc::new(move |event: RoomEvent| {
            let (kit, returned) = (bot_kit.clone(), Arc::clone(&bot_returned));
            Box::pin(async move {
                if event.source.channel_id != channel("sms") {
                    return Ok(ChannelOutput::default());
                }
                for call in 0..12 {
                    let mut echo = SendEventOptions::default();
                    if call == 0 || call == 11 {
                        echo.idempotency_key = Some("ECHO".to_owned());
                    }
                    let injected = kit
                        .send_event(
                            &room_id(),
                            &channel("ws"),
                            EventContent::text(format!("echo {call}")),
                            echo,
                        )
                        .await?;
                    returned
                        .lock()
                        .unwrap()
                        .push((call, injected.id, injected.status));
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(bot).unwrap();
    kit.attach_channel(&room_id(), &channel("bot"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    roomkit::testing::idle(&kit).await;
    let returned = returned.lock().unwrap().clone();
    let first = returned.first().map(|(_, id, _)| id.clone());
    let last = returned
        .last()
        .map(|(call, id, status)| (*call, id.clone(), *status));
    let keyed = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.idempotency_key.as_deref() == Some("ECHO"))
        .count();
    assert_eq!(
        (last, keyed, timeline(&kit).await.last().cloned()),
        (
            Some((11, first.unwrap(), EventStatus::Delivered)),
            1,
            Some("#15 ws blocked echo 10".to_owned())
        )
    );
}

/// RFC §13.5: the lock is reentrant within one execution context only.
mod a_context {
    use super::*;
    use pretty_assertions::assert_eq;

    /// A task a hook spawns without awaiting it as its own runs outside the
    /// hook's context: its call waits for the lock until the section is
    /// over, and the event is decided before the room closes.
    #[tokio::test]
    async fn is_not_handed_to_a_task_a_hook_spawns() {
        let (kit, _) = room().await;
        let spawned = Arc::new(Mutex::new(None));
        let (hook_kit, hook_spawned) = (kit.clone(), Arc::clone(&spawned));
        kit.hook(before_broadcast("closer", true), move |event, _| {
            let (kit, spawned) = (hook_kit.clone(), Arc::clone(&hook_spawned));
            async move {
                if event.source.channel_id == channel("sms") {
                    let task = tokio::spawn(async move {
                        kit.close_room(&room_id(), CloseRoomOptions::default())
                            .await
                    });
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    *spawned.lock().unwrap() = Some(task);
                }
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let result = send(&kit, "hello").await.unwrap();
        let task = spawned.lock().unwrap().take().unwrap();
        let closed = tokio::time::timeout(BOUND, task).await.unwrap().unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.status),
                closed.map(|room| room.status).ok()
            ),
            (Some(EventStatus::Delivered), Some(RoomStatus::Closed))
        );
    }

    on_both_runtimes!(
        after_the_release_does_not_bypass_the_lock,
        after_the_release_does_not_bypass_the_lock_on_threads,
        does_not_bypass
    );

    /// An agent's turn runs after its trigger's section released the lock:
    /// its context holds no lease, so its call waits for the section an
    /// entry it adopted holds, rather than run within it. The entry is
    /// decided on the room as it was, then the room closes.
    async fn does_not_bypass() {
        let (kit, _) = room().await;
        let in_hook = Arc::new(Notify::new());
        let hook_entered = Arc::clone(&in_hook);
        kit.hook(before_broadcast("slow_policy", true), move |event, _| {
            let entered = Arc::clone(&hook_entered);
            async move {
                if event.source.channel_id == channel("ai") {
                    entered.notify_one();
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        let outcome = Arc::new(Mutex::new(None));
        let (turn_kit, turn_outcome, turn_in_hook) =
            (kit.clone(), Arc::clone(&outcome), Arc::clone(&in_hook));
        let agent = Scripted::agent(
            "ai",
            Arc::new(move |event: RoomEvent| {
                let (kit, outcome, in_hook) = (
                    turn_kit.clone(),
                    Arc::clone(&turn_outcome),
                    Arc::clone(&turn_in_hook),
                );
                Box::pin(async move {
                    if event.source.channel_id != channel("sms") {
                        return Ok(ChannelOutput::default());
                    }
                    let thought = SendEventOptions::default();
                    let (room, ai) = (room_id(), channel("ai"));
                    let close = async {
                        in_hook.notified().await;
                        kit.close_room(&room, CloseRoomOptions::default()).await
                    };
                    let (injected, closed) = tokio::join!(
                        kit.send_event(&room, &ai, EventContent::text("Noted."), thought),
                        close
                    );
                    *outcome.lock().unwrap() = Some((
                        injected.map(|event| event.status).ok(),
                        closed.map(|room| room.status).ok(),
                    ));
                    Ok(ChannelOutput::default())
                })
            }),
        );
        kit.register_channel(agent).unwrap();
        kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
            .await
            .unwrap();
        send(&kit, "hello").await.unwrap();
        assert_eq!(
            *outcome.lock().unwrap(),
            Some((Some(EventStatus::Delivered), Some(RoomStatus::Closed)))
        );
    }
}

/// Gap G-050, RFC §13.5: an `ON_ROOM_CREATED` hook step 2 awaits runs
/// within the call that created the room: its injection is adopted, and
/// commits ahead of the message that created the room.
#[tokio::test]
async fn an_awaited_creation_hook_injects_into_the_room_being_created() {
    let (kit, _) = room().await;
    let new_room = RoomId::new("room_2").unwrap();
    let observer_kit = kit.clone();
    kit.observe("welcome", HookTrigger::OnRoomCreated, move |event, _| {
        let kit = observer_kit.clone();
        async move {
            kit.attach_channel(
                &event.room_id,
                &channel("ws"),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
            kit.send_event(
                &event.room_id,
                &channel("ws"),
                EventContent::text("Welcome"),
                SendEventOptions::default(),
            )
            .await
            .unwrap();
        }
    })
    .unwrap();
    let created = kit.process_inbound(message("hello"), in_room(new_room.clone()));
    let result = tokio::time::timeout(BOUND, created)
        .await
        .unwrap_or_else(|_| panic!("the creation hook waited for its own call"))
        .unwrap();
    let texts: Vec<(u64, String)> = kit
        .store()
        .list_events(&new_room, &every_row())
        .await
        .unwrap()
        .iter()
        .map(|event| (event.index, text_of(&event.content)))
        .collect();
    assert_eq!(
        (result.event.map(|event| event.index), texts),
        // The hook's attach, its event, step 2's attach, then the message:
        // each attach is recorded (gap G-046).
        (
            Some(3),
            vec![
                (0, "Channel ws attached (read_write, all)".to_owned()),
                (1, "Welcome".to_owned()),
                (2, "Channel sms attached (read_write, all)".to_owned()),
                (3, "hello".to_owned())
            ]
        )
    );
}

/// Gap G-050: the integrator's creation of a room awaits its hooks too, and
/// holds no lane meanwhile: a hook that attaches a channel and sends into
/// the room it observes does not wait for the creation, and its event is
/// the room's first.
async fn an_integrator_creation_hook_sends_into_the_room_being_created() {
    let (kit, _) = room().await;
    let new_room = RoomId::new("room_2").unwrap();
    let observer_kit = kit.clone();
    kit.observe("welcome", HookTrigger::OnRoomCreated, move |event, _| {
        let kit = observer_kit.clone();
        async move {
            kit.attach_channel(
                &event.room_id,
                &channel("ws"),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
            kit.send_event(
                &event.room_id,
                &channel("ws"),
                EventContent::text("Welcome"),
                SendEventOptions::default(),
            )
            .await
            .unwrap();
        }
    })
    .unwrap();
    tokio::time::timeout(
        BOUND,
        kit.create_room(CreateRoomOptions {
            room_id: Some(new_room.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap_or_else(|_| panic!("the creation hook waited for its own creation"))
    .unwrap();
    let texts: Vec<String> = kit
        .store()
        .list_events(&new_room, &every_row())
        .await
        .unwrap()
        .iter()
        .map(|event| text_of(&event.content))
        .collect();
    // The hook's attach is recorded first (gap G-046).
    assert_eq!(texts, ["Channel ws attached (read_write, all)", "Welcome"]);
}

on_both_runtimes!(
    an_integrator_creation_hook_sends_into_its_room_on_one_thread,
    an_integrator_creation_hook_sends_into_its_room_on_many_threads,
    an_integrator_creation_hook_sends_into_the_room_being_created
);

/// RFC §13.5: the lock is reentrant all the way down a call chain: a
/// locked hook sends a message into its room, which is nested in the hook's
/// section, and the parse of that message injects into the room too, nested
/// in turn. Each commits ahead of the event whose section it runs in.
#[tokio::test]
async fn an_entry_made_by_a_nested_entry_s_parse_is_nested_in_turn() {
    let (kit, _) = room().await;
    let parse_kit = kit.clone();
    let bot = Scripted::transport(
        "bot",
        Arc::new(|_| Box::pin(ready(Ok(ChannelOutput::default())))),
    )
    .parsing(Arc::new(move |_| {
        let kit = parse_kit.clone();
        Box::pin(async move {
            kit.send_event(
                &room_id(),
                &channel("ws"),
                EventContent::text("Parsed"),
                SendEventOptions::default(),
            )
            .await?;
            Ok(ChannelOutput::default())
        })
    }));
    kit.register_channel(bot).unwrap();
    kit.attach_channel(&room_id(), &channel("bot"), AttachChannelOptions::default())
        .await
        .unwrap();
    let hook_kit = kit.clone();
    kit.hook(before_broadcast("relay", true), move |event, _| {
        let kit = hook_kit.clone();
        async move {
            if event.source.channel_id == channel("sms") {
                let relayed =
                    InboundMessage::new(channel("bot"), "relay", EventContent::text("Relayed"));
                kit.process_inbound(relayed, in_room(room_id())).await?;
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    send(&kit, "hello").await.unwrap();
    assert_eq!(
        timeline(&kit).await,
        [
            "#0 system delivered Channel sms attached (read_write, all)",
            "#1 system delivered Channel ws attached (read_write, all)",
            "#2 system delivered Channel audit attached (read_write, all)",
            "#3 system delivered Channel bot attached (read_write, all)",
            "#4 ws delivered Parsed",
            "#5 bot delivered Relayed",
            "#6 sms delivered hello"
        ]
    );
}

/// RFC §13.5: a task the turn spawns and awaits as its own keeps the turn's
/// context when its future is wrapped with `keep_context`: its injection is
/// adopted, and returns at its commit.
#[tokio::test]
async fn a_task_a_turn_awaits_as_its_own_keeps_the_turn_s_context() {
    let (kit, audit) = room().await;
    let seen = Arc::new(Mutex::new(None));
    let (turn_kit, turn_seen) = (kit.clone(), Arc::clone(&seen));
    let agent = Scripted::agent(
        "ai",
        Arc::new(move |event: RoomEvent| {
            let (kit, seen) = (turn_kit.clone(), Arc::clone(&turn_seen));
            Box::pin(async move {
                if event.source.channel_id != channel("sms") {
                    return Ok(ChannelOutput::default());
                }
                let tool = tokio::spawn(roomkit::keep_context(async move {
                    let thought = SendEventOptions::default();
                    kit.send_event(
                        &room_id(),
                        &channel("ai"),
                        EventContent::text("Looked up."),
                        thought,
                    )
                    .await
                }));
                let injected = tool.await??;
                *seen.lock().unwrap() = Some(injected.index);
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(agent).unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    let texts = delivered(&audit, 2).await;
    assert_eq!(
        (*seen.lock().unwrap(), texts),
        (Some(5), vec!["hello".to_owned(), "Looked up.".to_owned()])
    );
}

/// The messages of the timeline, by source, from the room's first message
/// on: its lifecycle records left out.
async fn messages(kit: &RoomKit) -> Vec<String> {
    kit.store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
        .iter()
        .filter(|event| event.event_type == EventType::Message)
        .map(|event| format!("#{} {}", event.index, event.source.channel_id))
        .collect()
}

/// Decision 2 of step 3 (probe 7), RFC §13.5, §9.5.1: a task a locked hook
/// spawns within the hook's context (`keep_context`) acts for the hook
/// while the hook runs, whether the hook awaits it or not, as a task
/// spawned in a Python hook does: its injection is adopted and commits
/// ahead of the message. A task spawned bare holds no context, as tokio
/// hands a spawned task none: its injection is a new arrival, committed
/// after the message.
#[tokio::test]
async fn a_task_a_locked_hook_spawns_in_its_context_acts_for_it_while_it_runs() {
    let mut orders = Vec::new();
    for kept in [true, false] {
        let (kit, _) = room().await;
        let hook_kit = kit.clone();
        kit.hook(before_broadcast("notifier", true), move |event, _| {
            let kit = hook_kit.clone();
            async move {
                if event.source.channel_id == channel("sms") {
                    let call = async move {
                        kit.send_event(
                            &room_id(),
                            &channel("ws"),
                            EventContent::text("Heads up"),
                            SendEventOptions::default(),
                        )
                        .await
                        .map(|_| ())
                    };
                    // Fired and not awaited; the hook runs on meanwhile.
                    if kept {
                        drop(tokio::spawn(roomkit::keep_context(call)));
                    } else {
                        drop(tokio::spawn(call));
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(HookResult::allow())
            }
        })
        .unwrap();
        send(&kit, "hello").await.unwrap();
        tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
            .await
            .unwrap();
        orders.push(messages(&kit).await);
    }
    assert_eq!(
        orders,
        [["#3 ws", "#4 sms"], ["#3 sms", "#4 ws"]].map(|order| order.map(str::to_owned).to_vec())
    );
}

/// RFC §9.5.1, decision 2 of step 3: once the hook is over, the task it
/// left running within its context acts for it no more, though the pass
/// still holds the room's lock for its next hook: its injection is a new
/// arrival, committed after the message, where it was refused as a call
/// waiting for itself.
#[tokio::test]
async fn a_task_a_locked_hook_left_running_calls_as_a_new_arrival_once_it_is_over() {
    let (kit, _) = room().await;
    let (hook_over, calling) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let sent = Arc::new(Mutex::new(None));
    let (hook_kit, notify_over, notify_calling, task_sent) = (
        kit.clone(),
        Arc::clone(&hook_over),
        Arc::clone(&calling),
        Arc::clone(&sent),
    );
    kit.hook(before_broadcast("notifier", true), move |event, _| {
        let (kit, over, calling, sent) = (
            hook_kit.clone(),
            Arc::clone(&notify_over),
            Arc::clone(&notify_calling),
            Arc::clone(&task_sent),
        );
        async move {
            if event.source.channel_id == channel("sms") {
                let task_over = Arc::clone(&over);
                drop(tokio::spawn(roomkit::keep_context(async move {
                    task_over.notified().await;
                    calling.notify_one();
                    let result = kit
                        .send_event(
                            &room_id(),
                            &channel("ws"),
                            EventContent::text("Heads up"),
                            SendEventOptions::default(),
                        )
                        .await;
                    *sent.lock().unwrap() = Some(
                        result
                            .map(|event| event.index)
                            .map_err(|error| error.to_string()),
                    );
                })));
                over.notify_one();
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    // The pass's next hook, under the same lock, runs once the task calls.
    let mut next = before_broadcast("next", true);
    next.priority = 10;
    let next_calling = Arc::clone(&calling);
    kit.hook(next, move |event, _| {
        let calling = Arc::clone(&next_calling);
        async move {
            if event.source.channel_id == channel("sms") {
                calling.notified().await;
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    send(&kit, "hello").await.unwrap();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let sent = sent.lock().unwrap().take();
    assert_eq!(
        (sent, messages(&kit).await),
        (Some(Ok(4)), vec!["#3 sms".to_owned(), "#4 ws".to_owned()])
    );
}

/// Decided for step 3 milestone 2 (the review's probe
/// `a_later_hook_awaits_the_task_an_earlier_one_left`): a task a finished
/// hook left running calls as an ordinary new arrival and waits for the
/// lock, even when a later hook of the same pass awaits it. That hook waits
/// until its own timeout, and fails open; the message commits, then the
/// task's injection, which the later hook never sees.
#[tokio::test]
async fn a_later_hook_awaiting_a_task_an_earlier_one_left_waits_until_its_timeout() {
    let (kit, _) = room().await;
    let left: Arc<Mutex<Option<tokio::task::JoinHandle<Option<u64>>>>> = Arc::default();
    let first_over = Arc::new(Notify::new());
    let (hook_kit, put, over) = (kit.clone(), Arc::clone(&left), Arc::clone(&first_over));
    kit.hook(before_broadcast("first", true), move |event, _| {
        let (kit, put, over) = (hook_kit.clone(), Arc::clone(&put), Arc::clone(&over));
        async move {
            if event.source.channel_id == channel("sms") {
                let task = tokio::spawn(roomkit::keep_context(async move {
                    over.notified().await;
                    let sent = kit
                        .send_event(
                            &room_id(),
                            &channel("ws"),
                            EventContent::text("Heads up"),
                            SendEventOptions::default(),
                        )
                        .await;
                    sent.ok().map(|event| event.index)
                }));
                *put.lock().unwrap() = Some(task);
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let seen: Arc<Mutex<Option<Option<u64>>>> = Arc::default();
    let (take, over, saw) = (
        Arc::clone(&left),
        Arc::clone(&first_over),
        Arc::clone(&seen),
    );
    let mut second = before_broadcast("second", true);
    second.priority = 10;
    second.timeout = Duration::from_millis(50);
    kit.hook(second, move |event, _| {
        let (take, over, saw) = (Arc::clone(&take), Arc::clone(&over), Arc::clone(&saw));
        async move {
            if event.source.channel_id == channel("sms") {
                over.notify_one();
                let task = take.lock().unwrap().take();
                if let Some(task) = task {
                    *saw.lock().unwrap() = Some(task.await.unwrap());
                }
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let seen = *seen.lock().unwrap();
    assert_eq!(
        (result.blocked, seen, messages(&kit).await),
        (false, None, vec!["#3 sms".to_owned(), "#4 ws".to_owned()])
    );
}

/// RFC §8.3: a channel whose parse sends every message it parses back into
/// its room is bounded by the reentry budget: each message is charged as it
/// arrives, before its parse, and the one past the budget is refused.
#[tokio::test]
async fn a_parse_that_calls_into_its_room_for_every_message_is_bounded() {
    let (kit, _) = room_with(RoomKitOptions {
        max_chain_depth: 1,
        ..Default::default()
    })
    .await;
    let refused = Arc::new(Mutex::new(Vec::new()));
    let (relay_kit, relay_refused) = (kit.clone(), Arc::clone(&refused));
    let relay = Scripted::transport(
        "relay",
        Arc::new(|_| Box::pin(ready(Ok(ChannelOutput::default())))),
    )
    .parsing(Arc::new(move |_| {
        let (kit, refused) = (relay_kit.clone(), Arc::clone(&relay_refused));
        Box::pin(async move {
            let again = InboundMessage::new(channel("relay"), "relay", EventContent::text("again"));
            let result = kit.process_inbound(again, in_room(room_id())).await?;
            if result.event.is_none() {
                refused.lock().unwrap().push(result.reason);
            }
            Ok(ChannelOutput::default())
        })
    }));
    kit.register_channel(relay).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("relay"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let first = InboundMessage::new(channel("relay"), "relay", EventContent::text("again"));
    tokio::time::timeout(BOUND, kit.process_inbound(first, in_room(room_id())))
        .await
        .unwrap_or_else(|_| panic!("the parse recursed past the budget"))
        .unwrap();
    let stored = timeline(&kit).await.len();
    assert_eq!(
        (refused.lock().unwrap().clone(), stored),
        // Four lifecycle records, then the eleven events (gap G-046).
        (vec![Some("reentry_loop_cap".to_owned())], 15)
    );
}

/// RFC §13.6, gap G-076: a call dropped while its hook works under the
/// lock, once the event the hook sent into the room is committed and its
/// delivery under way, expires: the hook is given up on, the lock let go,
/// nothing more of the call's event written, and the hook's event is
/// delivered all the same. The next call goes through.
#[tokio::test]
async fn a_call_dropped_after_its_hook_s_event_committed_delivers_that_event() {
    let (kit, _) = room().await;
    let (delivering, go) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let (started, released, kept) = (
        Arc::clone(&delivering),
        Arc::clone(&go),
        Arc::clone(&delivered),
    );
    let slow = Scripted::transport(
        "slow",
        Arc::new(move |event| {
            let (started, released, kept) = (
                Arc::clone(&started),
                Arc::clone(&released),
                Arc::clone(&kept),
            );
            Box::pin(async move {
                let text = text_of(&event.content);
                if text == "Heads up" {
                    started.notify_one();
                    released.notified().await;
                }
                kept.lock().unwrap().push(text);
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(slow).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("slow"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let hook_kit = kit.clone();
    kit.hook(before_broadcast("stuck", true), move |event, _| {
        let kit = hook_kit.clone();
        async move {
            if text_of(&event.content) == "first" {
                kit.send_event(
                    &room_id(),
                    &channel("ws"),
                    EventContent::text("Heads up"),
                    SendEventOptions::default(),
                )
                .await?;
                std::future::pending::<()>().await;
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let room = room_id();
    let call = kit.process_inbound(message("first"), in_room(room.clone()));
    tokio::select! {
        result = call => panic!("the call ended before its hook's event: {result:?}"),
        () = delivering.notified() => {}
    }
    go.notify_one();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let delivered = delivered.lock().unwrap().clone();
    let next = send(&kit, "second").await.unwrap();
    assert_eq!(
        (
            delivered,
            next.event.map(|event| (event.index, event.status))
        ),
        (
            vec!["Heads up".to_owned()],
            Some((5, EventStatus::Delivered))
        )
    );
}

/// RFC §13.6: the call's lock request, held back while an entry its
/// off-lock hook made holds the lock, is given up on once the call's
/// `process_timeout` passed meanwhile, not granted.
#[tokio::test]
async fn a_lock_request_held_back_past_the_deadline_is_given_up_on() {
    let (kit, _) = room_with(RoomKitOptions {
        process_timeout: Duration::from_millis(100),
        ..Default::default()
    })
    .await;
    kit.hook(before_broadcast("slow", true), |event, _| async move {
        if text_of(&event.content) == "Heads up" {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        Ok(HookResult::allow())
    })
    .unwrap();
    let hook_kit = kit.clone();
    kit.hook(before_broadcast("notifier", false), move |event, _| {
        let kit = hook_kit.clone();
        async move {
            if event.source.channel_id == channel("sms") {
                // Fired and not awaited: the entry runs beside the check.
                drop(tokio::spawn(roomkit::keep_context(async move {
                    kit.send_event(
                        &room_id(),
                        &channel("ws"),
                        EventContent::text("Heads up"),
                        SendEventOptions::default(),
                    )
                    .await
                })));
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    assert_eq!(
        (result.reason, timeline(&kit).await),
        (
            Some("process_timeout".to_owned()),
            vec![
                "#0 system delivered Channel sms attached (read_write, all)".to_owned(),
                "#1 system delivered Channel ws attached (read_write, all)".to_owned(),
                "#2 system delivered Channel audit attached (read_write, all)".to_owned(),
                "#3 ws delivered Heads up".to_owned()
            ]
        )
    );
}

/// RFC §9.3, §13.5: a hook that times out while the entry it made still
/// works under its lock is heard only once the entry is settled: the entry
/// commits and is delivered, then the event the hook decided on, the hook
/// failing open.
#[tokio::test]
async fn a_hook_that_times_out_while_its_entry_works_is_heard_after_it() {
    let (kit, audit) = room().await;
    kit.hook(before_broadcast("slow", true), |event, _| async move {
        if text_of(&event.content) == "Heads up" {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Ok(HookResult::allow())
    })
    .unwrap();
    let hook_kit = kit.clone();
    let mut spec = before_broadcast("notifier", true);
    spec.timeout = Duration::from_millis(50);
    kit.hook(spec, move |event, _| {
        let kit = hook_kit.clone();
        async move {
            if event.source.channel_id == channel("sms") {
                kit.send_event(
                    &room_id(),
                    &channel("ws"),
                    EventContent::text("Heads up"),
                    SendEventOptions::default(),
                )
                .await?;
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    assert_eq!(
        (
            result.event.map(|event| (event.index, event.status)),
            audit.texts()
        ),
        (
            Some((4, EventStatus::Delivered)),
            vec!["Heads up".to_owned(), "hello".to_owned()]
        )
    );
}

/// Routes every message to `room_1`.
struct ToRoom1;

impl InboundRoomRouter for ToRoom1 {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        _options: RouteOptions,
    ) -> RouterFuture<'a> {
        Box::pin(ready(Ok(Some(room_id()))))
    }
}

/// Routes every message to the room it names.
struct ToRoom(RoomId);

impl InboundRoomRouter for ToRoom {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        _options: RouteOptions,
    ) -> RouterFuture<'a> {
        Box::pin(ready(Ok(Some(self.0.clone()))))
    }
}

/// RFC §9.5.1, gap G-070: a message a delivery's code routes draws on the
/// delivering cascade's budget once routing places it in the room the set
/// delivers in, charged before its parse; one routed into another room has
/// a budget of its own.
#[tokio::test]
async fn a_routed_message_draws_on_the_delivering_budget_in_the_delivering_room_only() {
    const LIMIT: u32 = 2;
    let mut refused_in = Vec::new();
    for into in ["room_1", "room_2"] {
        let (kit, _) = room_with(RoomKitOptions {
            max_chain_depth: LIMIT,
            inbound_router: Some(Arc::new(ToRoom(RoomId::new(into).unwrap()))),
            ..Default::default()
        })
        .await;
        kit.create_room(CreateRoomOptions {
            room_id: Some((RoomId::new("room_2").unwrap()).clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        let refusals = Arc::new(Mutex::new(Vec::new()));
        let (bot_kit, bot_refusals) = (kit.clone(), Arc::clone(&refusals));
        let bot = Scripted::transport(
            "bot",
            Arc::new(move |event: RoomEvent| {
                let (kit, refusals) = (bot_kit.clone(), Arc::clone(&bot_refusals));
                Box::pin(async move {
                    if text_of(&event.content) != "hello" {
                        return Ok(ChannelOutput::default());
                    }
                    for _ in 0..=LIMIT * 10 {
                        let result = kit
                            .process_inbound(message("routed"), ProcessInboundOptions::default())
                            .await?;
                        if result.event.is_none() {
                            refusals.lock().unwrap().push(result.reason);
                        }
                    }
                    Ok(ChannelOutput::default())
                })
            }),
        );
        kit.register_channel(bot).unwrap();
        kit.attach_channel(&room_id(), &channel("bot"), AttachChannelOptions::default())
            .await
            .unwrap();
        send(&kit, "hello").await.unwrap();
        let refused = refusals.lock().unwrap().clone();
        refused_in.push((into, refused.first().cloned().flatten()));
    }
    let cap = Some("reentry_loop_cap".to_owned());
    assert_eq!(refused_in, [("room_1", cap), ("room_2", None)]);
}

/// Finds no room for any message: each one opens a new room.
struct NoRoom;

impl InboundRoomRouter for NoRoom {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        _options: RouteOptions,
    ) -> RouterFuture<'a> {
        Box::pin(ready(Ok(None)))
    }
}

/// Names, for each message, a room the store does not hold.
#[derive(Default)]
struct FreshRoom(AtomicU32);

impl InboundRoomRouter for FreshRoom {
    fn route<'a>(
        &'a self,
        _channel_id: &'a ChannelId,
        _channel_type: &'a ChannelType,
        _options: RouteOptions,
    ) -> RouterFuture<'a> {
        let room = self.0.fetch_add(1, AtomicOrdering::Relaxed);
        Box::pin(ready(Ok(Some(
            RoomId::new(format!("fresh_{room}")).unwrap(),
        ))))
    }
}

/// How a delivery's message opens a room.
#[derive(Clone, Copy, Debug)]
enum Opens {
    /// The router answers none.
    NoRoom,
    /// The router names a room the store lacks.
    FreshIds,
    /// The call names a room the store lacks.
    Named,
}

/// The maintainer's rule (RMK-930, gap G-070): a message a delivery's code
/// sends that creates a room is that delivery's own work, and draws on the
/// delivering cascade's budget, whichever way the room is opened (no
/// answer from the router, an id the store lacks, a named room the store
/// lacks): a transport whose every delivery opens a room it is then bound
/// in stops at the cap, rather than create rooms without end.
async fn opening_a_room_on_every_delivery_stops_at_the_reentry_cap(opens: Opens) {
    let inbound_router: Arc<dyn InboundRoomRouter> = match opens {
        Opens::FreshIds => Arc::new(FreshRoom::default()),
        Opens::NoRoom | Opens::Named => Arc::new(NoRoom),
    };
    let (kit, _) = room_with(RoomKitOptions {
        max_chain_depth: 1,
        inbound_router: Some(inbound_router),
        ..Default::default()
    })
    .await;
    let named = Arc::new(AtomicU32::new(0));
    let binder = kit.clone();
    kit.observe("bind_bot", HookTrigger::OnRoomCreated, move |event, _| {
        let kit = binder.clone();
        async move {
            kit.attach_channel(
                &event.room_id,
                &channel("bot"),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
        }
    })
    .unwrap();
    let refused = Arc::new(Mutex::new(Vec::new()));
    let (bot_kit, bot_refused) = (kit.clone(), Arc::clone(&refused));
    let bot = Scripted::transport(
        "bot",
        Arc::new(move |_| {
            let (kit, refused) = (bot_kit.clone(), Arc::clone(&bot_refused));
            let fresh = named.fetch_add(1, AtomicOrdering::Relaxed);
            Box::pin(async move {
                let result = match opens {
                    Opens::Named => {
                        let room = RoomId::new(format!("named_{fresh}")).unwrap();
                        kit.process_inbound(message("again"), in_room(room.clone()))
                            .await?
                    }
                    Opens::NoRoom | Opens::FreshIds => {
                        kit.process_inbound(message("again"), ProcessInboundOptions::default())
                            .await?
                    }
                };
                if result.event.is_none() {
                    refused.lock().unwrap().push(result.reason);
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(bot).unwrap();
    kit.attach_channel(&room_id(), &channel("bot"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    let deadline = tokio::time::Instant::now() + BOUND;
    while refused.lock().unwrap().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the transport opened rooms without end"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let rooms = kit
        .store()
        .list_rooms(&RoomFilter::default())
        .await
        .unwrap();
    assert_eq!(
        (refused.lock().unwrap().clone(), rooms.len()),
        (vec![Some("reentry_loop_cap".to_owned())], 11),
        "{opens:?}"
    );
}

#[tokio::test]
async fn a_transport_opening_a_room_on_every_delivery_stops_at_the_reentry_cap() {
    opening_a_room_on_every_delivery_stops_at_the_reentry_cap(Opens::NoRoom).await;
}

#[tokio::test]
async fn a_transport_routing_to_a_missing_room_on_every_delivery_stops_at_the_reentry_cap() {
    opening_a_room_on_every_delivery_stops_at_the_reentry_cap(Opens::FreshIds).await;
}

#[tokio::test]
async fn a_transport_naming_a_missing_room_on_every_delivery_stops_at_the_reentry_cap() {
    opening_a_room_on_every_delivery_stops_at_the_reentry_cap(Opens::Named).await;
}

/// The maintainer's rule (RMK-930, gap G-070): a delivery's call into an
/// existing room it does not deliver in has a budget of its own.
#[tokio::test]
async fn a_delivery_s_call_into_another_existing_room_has_a_budget_of_its_own() {
    const LIMIT: u32 = 1;
    let (kit, _) = room_with(RoomKitOptions {
        max_chain_depth: LIMIT,
        ..Default::default()
    })
    .await;
    let other = RoomId::new("room_2").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(other.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    let results = Arc::new(Mutex::new(Vec::new()));
    let (bot_kit, bot_results, bot_other) = (kit.clone(), Arc::clone(&results), other.clone());
    let bot = Scripted::transport(
        "bot",
        Arc::new(move |event: RoomEvent| {
            let (kit, results, other) =
                (bot_kit.clone(), Arc::clone(&bot_results), bot_other.clone());
            Box::pin(async move {
                if text_of(&event.content) != "hello" {
                    return Ok(ChannelOutput::default());
                }
                for _ in 0..=LIMIT * 10 {
                    let result = kit
                        .process_inbound(message("there"), in_room(other.clone()))
                        .await?;
                    results.lock().unwrap().push(result.event.is_some());
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(bot).unwrap();
    kit.attach_channel(&room_id(), &channel("bot"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    assert_eq!(
        results.lock().unwrap().clone(),
        vec![true; (LIMIT * 10 + 1) as usize]
    );
}

/// RFC §13.5, §9.5.1 Reentrance: a message routed back into the room from
/// under its lock cannot be adopted, the room being known only once it is
/// routed: it fails at the lock its own caller's section holds, instead of
/// committing within that section's tenure.
#[tokio::test]
async fn a_call_that_could_only_wait_for_itself_fails() {
    let (kit, _) = room_with(RoomKitOptions {
        inbound_router: Some(Arc::new(ToRoom1)),
        ..Default::default()
    })
    .await;
    let refused = Arc::new(Mutex::new(None));
    let (hook_kit, hook_refused) = (kit.clone(), Arc::clone(&refused));
    kit.hook(before_broadcast("forwarder", true), move |event, _| {
        let (kit, refused) = (hook_kit.clone(), Arc::clone(&hook_refused));
        async move {
            if text_of(&event.content) == "hello" {
                let routed = kit
                    .process_inbound(message("forwarded"), ProcessInboundOptions::default())
                    .await;
                *refused.lock().unwrap() = Some(routed.err());
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    assert_eq!(
        (
            refused.lock().unwrap().take(),
            result.event.map(|event| event.index)
        ),
        (
            Some(Some(KitError::Reentrant {
                room_id: room_id(),
                call: "process_inbound",
            })),
            Some(3)
        )
    );
}

/// RFC §9.5.1 Reentrance, gap G-070 decision 4: a parse that routes a
/// message back into its room cannot have it adopted, the room being known
/// only once routed: it fails at the lock step 2's record of the routed
/// sender takes, the parse standing in the room's admission order, rather
/// than recurse on every message.
#[tokio::test]
async fn a_parse_that_routes_a_message_back_into_its_room_fails() {
    let (kit, _) = room_with(RoomKitOptions {
        inbound_router: Some(Arc::new(ToRoom1)),
        ..Default::default()
    })
    .await;
    let failed = Arc::new(Mutex::new(Vec::new()));
    let (relay_kit, relay_failed) = (kit.clone(), Arc::clone(&failed));
    let relay = Scripted::transport(
        "relay",
        Arc::new(|_| Box::pin(ready(Ok(ChannelOutput::default())))),
    )
    .parsing(Arc::new(move |_| {
        let (kit, failed) = (relay_kit.clone(), Arc::clone(&relay_failed));
        Box::pin(async move {
            let again = InboundMessage::new(channel("relay"), "relay", EventContent::text("again"));
            let routed = kit
                .process_inbound(again, ProcessInboundOptions::default())
                .await;
            failed.lock().unwrap().push(routed.err());
            Ok(ChannelOutput::default())
        })
    }));
    kit.register_channel(relay).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("relay"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let first = InboundMessage::new(channel("relay"), "relay", EventContent::text("again"));
    let result = tokio::time::timeout(BOUND, kit.process_inbound(first, in_room(room_id())))
        .await
        .unwrap_or_else(|_| panic!("the parse recursed"))
        .unwrap();
    let reentrant = KitError::Reentrant {
        room_id: room_id(),
        call: "process_inbound",
    };
    let stored = timeline(&kit).await.len();
    assert_eq!(
        (
            std::mem::take(&mut *failed.lock().unwrap()),
            result.event.map(|event| event.index),
            stored
        ),
        (vec![Some(reentrant)], Some(4), 5)
    );
}

/// RFC §9.5.1 Reentrance, gap G-070 decision 4: an off-lock hook's body
/// that routes a message back into its room fails as well, even when no
/// off-lock hook fires for that message, which then takes no ticket: the
/// hook stands in the room's admission order, and the message would commit
/// ahead of the event the hook runs for, outside every order.
#[tokio::test]
async fn an_off_lock_hook_routing_back_a_message_no_check_fires_for_fails() {
    let (kit, _) = room_with(RoomKitOptions {
        inbound_router: Some(Arc::new(ToRoom1)),
        ..Default::default()
    })
    .await;
    let refused = Arc::new(Mutex::new(None));
    let (hook_kit, hook_refused) = (kit.clone(), Arc::clone(&refused));
    let mut customer_only = before_broadcast("forwarder", false);
    customer_only.filter.channel_ids = Some([channel("sms")].into());
    kit.hook(customer_only, move |event, _| {
        let (kit, refused) = (hook_kit.clone(), Arc::clone(&hook_refused));
        async move {
            if text_of(&event.content) == "hello" {
                let forwarded =
                    InboundMessage::new(channel("ws"), "advisor", EventContent::text("forwarded"));
                let routed = kit
                    .process_inbound(forwarded, ProcessInboundOptions::default())
                    .await;
                *refused.lock().unwrap() = Some(routed.err());
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    let reentrant = KitError::Reentrant {
        room_id: room_id(),
        call: "process_inbound",
    };
    let stored = timeline(&kit).await.len();
    assert_eq!(
        (
            refused.lock().unwrap().take(),
            result.event.map(|event| event.index),
            stored
        ),
        (Some(Some(reentrant)), Some(3), 4)
    );
}

/// RFC §9.5.1 Reentrance, gap G-070: a frame stands in a room's admission
/// order only while its call runs. A parse in `room_1`, run for a delivery
/// in `room_2`, injects into `room_2`: that new arrival is answered at its
/// decision and goes on detached, its context keeping the parse's frame.
/// Once the parse is over, the turn the arrival's delivery runs calls into
/// `room_1`: no finished parse takes the call nor refuses its ticket's turn,
/// and it commits.
#[tokio::test]
async fn a_detached_run_s_turn_is_not_taken_by_a_finished_parse() {
    let (kit, _) = room().await;
    let room_2 = RoomId::new("room_2").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_2.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.hook(before_broadcast("scanner", false), |_, _| async {
        Ok(HookResult::allow())
    })
    .unwrap();
    let bot_kit = kit.clone();
    let bot = Scripted::transport(
        "bot",
        Arc::new(move |event: RoomEvent| {
            let kit = bot_kit.clone();
            Box::pin(async move {
                if text_of(&event.content) == "hello" {
                    let ping =
                        InboundMessage::new(channel("relay"), "relay", EventContent::text("ping"));
                    kit.process_inbound(ping, in_room(room_id())).await?;
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    let (relay_kit, relay_room) = (kit.clone(), room_2.clone());
    let relay = Scripted::transport(
        "relay",
        Arc::new(|_| Box::pin(ready(Ok(ChannelOutput::default())))),
    )
    .parsing(Arc::new(move |_| {
        let (kit, room_2) = (relay_kit.clone(), relay_room.clone());
        Box::pin(async move {
            kit.send_event(
                &room_2,
                &channel("ws"),
                EventContent::text("pong"),
                SendEventOptions::default(),
            )
            .await?;
            Ok(ChannelOutput::default())
        })
    }));
    let seen = Arc::new(Mutex::new(None));
    let (turn_kit, turn_seen) = (kit.clone(), Arc::clone(&seen));
    let agent = Scripted::agent(
        "ai",
        Arc::new(move |event: RoomEvent| {
            let (kit, seen) = (turn_kit.clone(), Arc::clone(&turn_seen));
            Box::pin(async move {
                if text_of(&event.content) == "pong" {
                    let noted = kit
                        .send_event(
                            &room_id(),
                            &channel("ws"),
                            EventContent::text("Noted"),
                            SendEventOptions::default(),
                        )
                        .await;
                    *seen.lock().unwrap() = Some(noted.map(|event| event.index));
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    for channel_ in [bot, relay, agent] {
        kit.register_channel(channel_).unwrap();
    }
    kit.attach_channel(
        &room_id(),
        &channel("relay"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    for id in ["sms", "ws", "bot", "ai"] {
        kit.attach_channel(&room_2, &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    tokio::time::timeout(
        BOUND,
        kit.process_inbound(message("hello"), in_room(room_2.clone())),
    )
    .await
    .unwrap_or_else(|_| panic!("the call waited for itself"))
    .unwrap();
    // The arrival goes on detached: nothing awaits its turn.
    let deadline = tokio::time::Instant::now() + BOUND;
    while seen.lock().unwrap().is_none() {
        assert!(tokio::time::Instant::now() < deadline, "the turn never ran");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(*seen.lock().unwrap(), Some(Ok(5)));
}

/// An agent whose turn, on the customer's `hello`, runs `call` with the kit
/// and records what it returned.
fn calling_agent<T: Send + 'static>(
    kit: &RoomKit,
    seen: &Arc<Mutex<Option<T>>>,
    call: impl Fn(RoomKit) -> Pin<Box<dyn Future<Output = T> + Send>> + Send + Sync + 'static,
) -> Scripted {
    let (turn_kit, turn_seen) = (kit.clone(), Arc::clone(seen));
    let call = Arc::new(call);
    Scripted::agent(
        "ai",
        Arc::new(move |event: RoomEvent| {
            let (kit, seen, call) = (turn_kit.clone(), Arc::clone(&turn_seen), Arc::clone(&call));
            Box::pin(async move {
                if text_of(&event.content) == "hello" {
                    *seen.lock().unwrap() = Some(call(kit).await);
                }
                Ok(ChannelOutput::default())
            })
        }),
    )
}

/// RFC §9.5.1 Reentrance: a turn's call is a new arrival, which takes the
/// room's admission ticket behind a message whose check started first, and
/// commits after it, while the turn's own set, ahead in the lane, waits for
/// the call's decision; nothing waits for itself.
#[tokio::test]
async fn a_turn_s_call_ticketed_behind_another_message_commits_after_it() {
    let (kit, _) = room().await;
    let (a_checking, e_checking) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (hook_a, hook_e) = (Arc::clone(&a_checking), Arc::clone(&e_checking));
    kit.hook(before_broadcast("scanner", false), move |event, _| {
        let (a_checking, e_checking) = (Arc::clone(&hook_a), Arc::clone(&hook_e));
        async move {
            match text_of(&event.content).as_str() {
                "A" => {
                    a_checking.notify_one();
                    e_checking.notified().await;
                }
                "E" => e_checking.notify_one(),
                _ => {}
            }
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let seen = Arc::new(Mutex::new(None));
    let turning = Arc::new(Notify::new());
    let (turn_turning, turn_a) = (Arc::clone(&turning), Arc::clone(&a_checking));
    let agent = calling_agent(&kit, &seen, move |kit| {
        let (turning, a_checking) = (Arc::clone(&turn_turning), Arc::clone(&turn_a));
        Box::pin(async move {
            turning.notify_one();
            a_checking.notified().await;
            kit.send_event(
                &room_id(),
                &channel("ws"),
                EventContent::text("E"),
                SendEventOptions::default(),
            )
            .await
            .map(|event| event.index)
        })
    });
    kit.register_channel(agent).unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    let c = {
        let kit = kit.clone();
        tokio::spawn(async move { send(&kit, "hello").await })
    };
    // C is committed and its turn runs, in C's set, ahead in the lane.
    tokio::time::timeout(BOUND, turning.notified())
        .await
        .unwrap();
    let a = send(&kit, "A").await.unwrap();
    tokio::time::timeout(BOUND, c)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let lines = timeline(&kit).await;
    assert_eq!(
        (
            a.event.map(|event| event.index),
            seen.lock().unwrap().take(),
            lines
        ),
        (
            Some(5),
            Some(Ok(6)),
            vec![
                "#0 system delivered Channel sms attached (read_write, all)".to_owned(),
                "#1 system delivered Channel ws attached (read_write, all)".to_owned(),
                "#2 system delivered Channel audit attached (read_write, all)".to_owned(),
                "#3 system delivered Channel ai attached (read_write, all)".to_owned(),
                "#4 sms delivered hello".to_owned(),
                "#5 sms delivered A".to_owned(),
                "#6 ws delivered E".to_owned()
            ]
        )
    );
}

/// RFC §13.6, §9.5.1 Reentrance: a turn's call is bounded by its own
/// `process_timeout`: its slow check refuses it, nothing written, while the
/// turn's trigger stays delivered.
#[tokio::test]
async fn a_turn_s_call_meets_its_own_process_timeout() {
    let (kit, _) = room_with(RoomKitOptions {
        process_timeout: Duration::from_millis(100),
        ..Default::default()
    })
    .await;
    kit.hook(before_broadcast("scanner", false), |event, _| async move {
        if text_of(&event.content) == "Let me check." {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
        Ok(HookResult::allow())
    })
    .unwrap();
    let seen = Arc::new(Mutex::new(None));
    let agent = calling_agent(&kit, &seen, |kit| {
        Box::pin(async move {
            let thought = SendEventOptions::default();
            kit.send_event(
                &room_id(),
                &channel("ai"),
                EventContent::text("Let me check."),
                thought,
            )
            .await
            .err()
        })
    });
    kit.register_channel(agent).unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    let result = send(&kit, "hello").await.unwrap();
    let lines = timeline(&kit).await;
    assert_eq!(
        (
            result.event.map(|event| event.status),
            seen.lock().unwrap().take(),
            lines
        ),
        (
            Some(EventStatus::Delivered),
            Some(Some(KitError::Inbound(InboundError::Refused {
                reason: "process_timeout".to_owned()
            }))),
            vec![
                "#0 system delivered Channel sms attached (read_write, all)".to_owned(),
                "#1 system delivered Channel ws attached (read_write, all)".to_owned(),
                "#2 system delivered Channel audit attached (read_write, all)".to_owned(),
                "#3 system delivered Channel ai attached (read_write, all)".to_owned(),
                "#4 sms delivered hello".to_owned()
            ]
        )
    );
}

/// RFC §8.3, gap G-070: a turn's calls draw on the reentry budget of the
/// cascade that delivers to it, ten times the chain-depth limit: the call
/// past it is stored `BLOCKED` with `reentry_loop_cap`, as an injected event
/// past the cap is, and so is the turn's own response, which finds the
/// budget spent.
#[tokio::test]
async fn a_turn_s_calls_draw_on_the_reentry_budget_of_the_cascade_that_delivers() {
    const LIMIT: u32 = 2;
    let (kit, _) = room_with(RoomKitOptions {
        max_chain_depth: LIMIT,
        ..Default::default()
    })
    .await;
    let (turn_kit, last_call) = (kit.clone(), Arc::new(Mutex::new(None)));
    let turn_last_call = Arc::clone(&last_call);
    let agent = Scripted::agent(
        "ai",
        Arc::new(move |event: RoomEvent| {
            let (kit, last_call) = (turn_kit.clone(), Arc::clone(&turn_last_call));
            Box::pin(async move {
                if text_of(&event.content) != "hello" {
                    return Ok(ChannelOutput::default());
                }
                for _ in 0..=LIMIT * 10 {
                    let thought = SendEventOptions::default();
                    let injected = kit
                        .send_event(
                            &room_id(),
                            &channel("ai"),
                            EventContent::text("Checking."),
                            thought,
                        )
                        .await
                        .unwrap();
                    *last_call.lock().unwrap() = Some((injected.status, injected.blocked_by));
                }
                let source =
                    EventSource::new(channel("ai"), ChannelType::AI, EventDirection::Outbound);
                let reply = RoomEvent::new(
                    "ai_reply".parse().unwrap(),
                    room_id(),
                    EventType::Message,
                    source,
                    EventContent::text("Hello back"),
                    Timestamp::UNIX_EPOCH,
                );
                Ok(ChannelOutput::respond(vec![reply]))
            })
        }),
    );
    kit.register_channel(agent).unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    let lines = timeline(&kit).await;
    let cap = Some("reentry_loop_cap".to_owned());
    assert_eq!(
        (
            last_call.lock().unwrap().clone(),
            lines.get(24).cloned(),
            lines.get(25..).map(<[String]>::to_vec)
        ),
        // After four lifecycle records and the message (gap G-046).
        (
            Some((EventStatus::Blocked, cap)),
            Some("#24 ai delivered Checking.".to_owned()),
            Some(vec![
                "#25 ai blocked Checking.".to_owned(),
                "#26 ai blocked Hello back".to_owned()
            ])
        )
    );
}

/// RFC §10.6, §8.3: a call an agent's turn makes is at least as deep as the
/// turn's response, whatever depth it was given: an agent that injects
/// through another channel on every turn stops at the chain-depth limit,
/// where it is no longer asked, as for its own responses.
#[tokio::test]
async fn an_agent_injecting_on_every_turn_stops_at_the_chain_depth_limit() {
    let (kit, _) = room_with(RoomKitOptions {
        max_chain_depth: 3,
        ..Default::default()
    })
    .await;
    let (turn_kit, depths) = (kit.clone(), Arc::new(Mutex::new(Vec::new())));
    let turn_depths = Arc::clone(&depths);
    let agent = Scripted::agent(
        "ai",
        Arc::new(move |event: RoomEvent| {
            let (kit, depths) = (turn_kit.clone(), Arc::clone(&turn_depths));
            Box::pin(async move {
                let injected = kit
                    .send_event(
                        &room_id(),
                        &channel("ws"),
                        EventContent::text("Again"),
                        SendEventOptions::default(),
                    )
                    .await?;
                depths
                    .lock()
                    .unwrap()
                    .push((event.chain_depth, injected.chain_depth));
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(agent).unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    // The chain ends in the record of the agent not asked; nothing awaits it.
    let deadline = tokio::time::Instant::now() + BOUND;
    let mut lines = timeline(&kit).await;
    while !lines.iter().any(|line| line.contains("blocked")) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the chain never ended: {lines:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
        lines = timeline(&kit).await;
    }
    let depths = depths.lock().unwrap().clone();
    assert_eq!(
        (depths, lines),
        (
            vec![(0, 1), (1, 2)],
            vec![
                "#0 system delivered Channel sms attached (read_write, all)".to_owned(),
                "#1 system delivered Channel ws attached (read_write, all)".to_owned(),
                "#2 system delivered Channel audit attached (read_write, all)".to_owned(),
                "#3 system delivered Channel ai attached (read_write, all)".to_owned(),
                "#4 sms delivered hello".to_owned(),
                "#5 ws delivered Again".to_owned(),
                "#6 ws delivered Again".to_owned(),
                "#7 ai blocked ".to_owned()
            ]
        )
    );
}

/// The calls, as `(chain depth, status)`, of work a turn began, which sends
/// `count` events into `room` through the console.
type Sent = Arc<Mutex<Vec<(u32, EventStatus)>>>;

async fn send_through_console(kit: &RoomKit, room: &RoomId, count: u32, sent: &Sent) {
    for _ in 0..count {
        let event = kit
            .send_event(
                room,
                &channel("ws"),
                EventContent::text("Later"),
                SendEventOptions::default(),
            )
            .await
            .unwrap();
        sent.lock().unwrap().push((event.chain_depth, event.status));
    }
}

/// Decided 2026-10-23 (RMK-930, gap G-070): work a turn began starts
/// fresh once the turn is over. A task the turn spawned within its context
/// and left running, which sends after the turn, is at depth 0 and on a
/// budget of its own for each call: the frame of a call that is over lends
/// neither its depth nor its budget.
#[tokio::test]
async fn work_a_turn_began_starts_fresh_once_the_turn_is_over() {
    const LIMIT: u32 = 2;
    let (kit, _) = room_with(RoomKitOptions {
        max_chain_depth: LIMIT,
        ..Default::default()
    })
    .await;
    let (turn_over, sent): (Arc<Notify>, Sent) = (Arc::new(Notify::new()), Sent::default());
    let (turn_kit, turn_notify, turn_sent) =
        (kit.clone(), Arc::clone(&turn_over), Arc::clone(&sent));
    let agent = Scripted::agent(
        "ai",
        Arc::new(move |event: RoomEvent| {
            let (kit, over, sent) = (
                turn_kit.clone(),
                Arc::clone(&turn_notify),
                Arc::clone(&turn_sent),
            );
            Box::pin(async move {
                if text_of(&event.content) == "hello" {
                    // Left running past the turn, within the turn's context.
                    drop(tokio::spawn(roomkit::keep_context(async move {
                        over.notified().await;
                        send_through_console(&kit, &room_id(), LIMIT * 10 + 1, &sent).await;
                    })));
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(agent).unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    turn_over.notify_one();
    let deadline = tokio::time::Instant::now() + BOUND;
    while sent.lock().unwrap().len() < (LIMIT * 10 + 1) as usize {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the task never sent"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        sent.lock().unwrap().clone(),
        vec![(0, EventStatus::Delivered); (LIMIT * 10 + 1) as usize]
    );
}

/// Decided 2026-10-23 (RMK-930, gap G-070): a turn's call into another
/// room is none of this room's work, and has a budget of its own; it is
/// still at least as deep as the turn's response, as any call made while
/// the turn runs.
#[tokio::test]
async fn a_turn_s_call_into_another_room_has_a_budget_of_its_own() {
    const LIMIT: u32 = 2;
    let (kit, _) = room_with(RoomKitOptions {
        max_chain_depth: LIMIT,
        ..Default::default()
    })
    .await;
    let other = RoomId::new("room_2").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(other.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&other, &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    let (turn_kit, sent, turn_other) = (kit.clone(), Sent::default(), other.clone());
    let turn_sent = Arc::clone(&sent);
    let agent = Scripted::agent(
        "ai",
        Arc::new(move |event: RoomEvent| {
            let (kit, sent, other) = (turn_kit.clone(), Arc::clone(&turn_sent), turn_other.clone());
            Box::pin(async move {
                if text_of(&event.content) == "hello" {
                    send_through_console(&kit, &other, LIMIT * 10 + 1, &sent).await;
                }
                Ok(ChannelOutput::default())
            })
        }),
    );
    kit.register_channel(agent).unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    assert_eq!(
        sent.lock().unwrap().clone(),
        vec![(1, EventStatus::Delivered); (LIMIT * 10 + 1) as usize]
    );
}

/// RFC §10.1 step 18: a message a turn routes back into its own room is a
/// new arrival, and returns at its decision, its delivery following the
/// turn's own set in the lane: it waits for nothing its caller holds.
#[tokio::test]
async fn a_message_a_turn_routes_into_its_own_room_returns_at_its_decision() {
    let (kit, _) = room_with(RoomKitOptions {
        inbound_router: Some(Arc::new(ToRoom1)),
        ..Default::default()
    })
    .await;
    let seen = Arc::new(Mutex::new(None));
    let agent = calling_agent(&kit, &seen, |kit| {
        Box::pin(async move {
            kit.process_inbound(message("forwarded"), ProcessInboundOptions::default())
                .await
                .map(|result| result.event.map(|event| event.index))
        })
    });
    kit.register_channel(agent).unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    send(&kit, "hello").await.unwrap();
    assert_eq!(*seen.lock().unwrap(), Some(Ok(Some(5))));
}

/// RFC §10.5: from outside, `send_event` waits for its delivery set and
/// returns the committed event; a refusal before the commit is raised.
mod send_event {
    use super::*;
    use pretty_assertions::assert_eq;

    #[tokio::test]
    async fn returns_the_committed_event_once_delivered() {
        let (kit, audit) = room().await;
        let event = tokio::time::timeout(
            BOUND,
            kit.send_event(
                &room_id(),
                &channel("ws"),
                EventContent::text("Hi"),
                SendEventOptions::default(),
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            (event.index, event.status, audit.texts()),
            (3, EventStatus::Delivered, vec!["Hi".to_owned()])
        );
    }

    #[tokio::test]
    async fn into_a_closed_room_is_refused() {
        let (kit, _) = room().await;
        kit.close_room(&room_id(), CloseRoomOptions::default())
            .await
            .unwrap();
        let refused = kit
            .send_event(
                &room_id(),
                &channel("ws"),
                EventContent::text("Hi"),
                SendEventOptions::default(),
            )
            .await;
        assert_eq!(
            refused,
            Err(KitError::Inbound(InboundError::Refused {
                reason: "room_closed".to_owned()
            }))
        );
    }

    /// Gap G-069: the host named a channel the room does not bind; nothing
    /// is written.
    #[tokio::test]
    async fn on_a_channel_the_room_does_not_bind_is_refused() {
        let (kit, _) = room().await;
        kit.detach_channel(&room_id(), &channel("ws"), DetachChannelOptions::default())
            .await
            .unwrap();
        let refused = kit
            .send_event(
                &room_id(),
                &channel("ws"),
                EventContent::text("Hi"),
                SendEventOptions::default(),
            )
            .await;
        assert_eq!(
            (refused, timeline(&kit).await),
            (
                Err(KitError::Inbound(InboundError::Refused {
                    reason: "source_unbound".to_owned()
                })),
                vec![
                    "#0 system delivered Channel sms attached (read_write, all)".to_owned(),
                    "#1 system delivered Channel ws attached (read_write, all)".to_owned(),
                    "#2 system delivered Channel audit attached (read_write, all)".to_owned(),
                    "#3 system delivered Channel ws detached".to_owned()
                ]
            )
        );
    }

    /// RFC §17.2, gaps G-069 and G-077: a caller acting for an organization
    /// reaches its rooms only, another's failing as not found.
    #[tokio::test]
    async fn for_another_organization_than_the_room_s_fails_as_not_found() {
        let (kit, _) = room().await;
        let clinic = SendEventOptions {
            organization_id: Some("clinic".to_owned()),
            ..Default::default()
        };
        let refused = kit
            .send_event(&room_id(), &channel("ws"), EventContent::text("Hi"), clinic)
            .await;
        assert_eq!(
            (refused, timeline(&kit).await),
            (
                Err(KitError::Inbound(InboundError::RoomNotFound(room_id()))),
                vec![
                    "#0 system delivered Channel sms attached (read_write, all)".to_owned(),
                    "#1 system delivered Channel ws attached (read_write, all)".to_owned(),
                    "#2 system delivered Channel audit attached (read_write, all)".to_owned()
                ]
            )
        );
    }

    #[tokio::test]
    async fn for_the_room_s_organization_is_committed() {
        let (kit, audit) = room().await;
        let room = RoomId::new("room_2").unwrap();
        let clinic = || Some("clinic".to_owned());
        let options = CreateRoomOptions {
            room_id: Some(room.clone()),
            organization_id: clinic(),
            ..Default::default()
        };
        kit.create_room(options).await.unwrap();
        for id in [channel("ws"), audit.id().clone()] {
            let options = AttachChannelOptions {
                organization_id: clinic(),
                ..Default::default()
            };
            kit.attach_channel(&room, &id, options).await.unwrap();
        }
        let options = SendEventOptions {
            organization_id: clinic(),
            ..Default::default()
        };
        let event = kit
            .send_event(&room, &channel("ws"), EventContent::text("Hi"), options)
            .await
            .unwrap();
        assert_eq!((event.index, audit.texts()), (2, vec!["Hi".to_owned()]));
    }
}

/// The record `id` the console's channel holds, as `commit_event` takes it.
fn console_record(id: &str, text: &str) -> RoomEvent {
    let source = EventSource::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        EventDirection::Inbound,
    );
    RoomEvent::new(
        id.parse().unwrap(),
        room_id(),
        EventType::Message,
        source,
        EventContent::text(text),
        Timestamp::UNIX_EPOCH,
    )
}

/// RFC §10.5: `commit_event` commits a record no one receives.
#[tokio::test]
async fn commit_event_reaches_no_channel() {
    let (kit, audit) = room().await;
    let committed = kit
        .commit_event(
            &room_id(),
            console_record("snap_1", "Snapshot"),
            CommitEventOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        (
            committed.index,
            audit.texts().len(),
            timeline(&kit).await.len()
        ),
        (3, 0, 4)
    );
}

/// RFC §10.5, §14.3: a record is never a replay. Under a key its channel
/// holds in the room, `commit_event` fails as the store refuses the
/// duplicate key, nothing written, and the event the key names is kept.
#[tokio::test]
async fn commit_event_under_a_key_its_channel_holds_is_refused() {
    let (kit, _) = room().await;
    let sent = SendEventOptions {
        idempotency_key: Some("WS1".to_owned()),
        ..Default::default()
    };
    let sent = kit
        .send_event(&room_id(), &channel("ws"), EventContent::text("Hi"), sent)
        .await
        .unwrap();
    let mut record = console_record("snap_1", "Snapshot");
    record.idempotency_key = Some("WS1".to_owned());
    let refused = kit
        .commit_event(&room_id(), record, CommitEventOptions::default())
        .await;
    let held = kit
        .store()
        .get_event_by_idempotency_key(&room_id(), Some(&channel("ws")), "WS1")
        .await
        .unwrap()
        .map(|event| event.id);
    let error_type = match refused {
        Err(KitError::Mutation(MutationError::Store(record))) => record.error_type,
        other => panic!("the record was not refused as a duplicate key: {other:?}"),
    };
    assert_eq!(
        (error_type.as_str(), held, timeline(&kit).await.len()),
        ("IdempotencyKeyTaken", Some(sent.id), 4)
    );
}

/// RFC §13.4: a blank key, empty or whitespace only, is no key: two
/// messages, two injections and two records carrying one are each
/// processed and stored as carrying none.
#[tokio::test]
async fn a_blank_key_is_no_key() {
    for blank in ["", "   "] {
        let (kit, _) = room().await;
        for text in ["one", "two"] {
            let mut keyed = message(text);
            keyed.idempotency_key = Some(blank.to_owned());
            kit.process_inbound(keyed, in_room(room_id()))
                .await
                .unwrap();
        }
        for text in ["three", "four"] {
            let injected = SendEventOptions {
                idempotency_key: Some(blank.to_owned()),
                ..Default::default()
            };
            kit.send_event(
                &room_id(),
                &channel("ws"),
                EventContent::text(text),
                injected,
            )
            .await
            .unwrap();
        }
        for id in ["snap_1", "snap_2"] {
            let mut record = console_record(id, "Snapshot");
            record.idempotency_key = Some(blank.to_owned());
            kit.commit_event(&room_id(), record, CommitEventOptions::default())
                .await
                .unwrap();
        }
        let mut stored = kit
            .store()
            .list_events(&room_id(), &every_row())
            .await
            .unwrap();
        // The attaches' lifecycle records carry no key either (gap G-046).
        stored.drain(..3);
        let keys: Vec<Option<String>> = stored.iter().map(|e| e.idempotency_key.clone()).collect();
        assert_eq!((stored.len(), keys), (6, vec![None; 6]), "blank {blank:?}");
    }
}

/// RFC §10.5, §14.3: a record is stored with the `delivery_results` it
/// brings, which a later update keeps.
#[tokio::test]
async fn commit_event_keeps_the_delivery_results_it_brings() {
    let (kit, _) = room().await;
    let mut record = console_record("snap_1", "Snapshot");
    record
        .delivery_results
        .insert("sms".to_owned(), "failed".into());
    kit.commit_event(&room_id(), record, CommitEventOptions::default())
        .await
        .unwrap();
    let updates = UpdateEventOptions {
        content: Some(EventContent::text("Snapshot, corrected")),
        ..UpdateEventOptions::default()
    };
    let corrected = kit
        .update_event(&room_id(), &"snap_1".parse().unwrap(), updates)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        corrected.delivery_results.get("sms").cloned(),
        Some("failed".into())
    );
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::EventQuery {
    roomkit::EventQuery {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::EventQuery::default()
    }
}
