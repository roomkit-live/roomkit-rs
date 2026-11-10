//! A room's lane and lock on the tokio driver (RFC §10.2, §13.5): calls
//! from several tasks into one room, and the lane and lock left free for
//! the next call whatever ended the last one: a channel that panicked, or a
//! caller that dropped its call before its decision, which then expires
//! and lets go of what it holds. A call dropped once decided goes on
//! detached and delivers its event (RFC §13.6, decision 1 of step 2b, gap
//! G-076).

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a test fails on a broken fixture"
)]
use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::{AllowOptions, BlockOptions};
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventDirection, EventSource, EventStatus, EventType, HookResult, InboundMessage, RoomContext,
    RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, EventTap, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    HookExecution, HookOptions, HookTrigger, MuteOptions, PauseRoomOptions, ProcessInboundOptions,
    RoomKit, RoomKitOptions,
};
use roomkit_core::ops::ChannelConfig;
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::{Barrier, Notify};

#[path = "lanes/cancellation.rs"]
mod cancellation;

/// How long a call that should not wait may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn message(from: &str, sender: &str, text: &str) -> InboundMessage {
    InboundMessage::new(channel(from), sender, TextContent::new(text))
}

/// A kit with an SMS transport and an echo agent, both attached to `room_1`.
async fn room_with_agent() -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(EchoAgent::new(
        channel("ai"),
        "You said: ",
        EchoAgentOptions { quote: true },
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id(), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.attach_channel(&room_id(), &channel("ai"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

/// The next call into `room_1` finishes in time: the lane and the lock are
/// free.
async fn next_call_goes_through(kit: &RoomKit) -> roomkit::InboundResult {
    let room = room_id();
    let next = kit.process_inbound(
        message("sms", "+15551234567", "Still there?"),
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        },
    );
    tokio::time::timeout(BOUND, next)
        .await
        .unwrap_or_else(|_| panic!("the next call waited on a lane or a lock left held"))
        .unwrap()
}

/// The framework events step 2 emits, which `tap` reads in order.
const STEP_2_EVENTS: [&str; 3] = ["room_created", "room_channel_attached", "session_started"];

/// The names of step 2's framework events `tap` saw, in order, once the
/// kit's background work is over.
async fn event_names(kit: &RoomKit, tap: &EventTap) -> Vec<String> {
    roomkit::testing::idle(kit).await;
    tap.types()
}

/// Two tasks on a multi-threaded runtime send at the same instant into one
/// missing room, overlapping up to the room lock (RFC §10.2). The room is
/// created, attached and its session started once, the creation's awaited
/// hook leaving the other message time to get in: that one finds the room
/// created and may attach the channel first, at its defaults (step 2's read
/// again after a creation refused), and the attach of the call that created
/// the room then finds it bound. Each message commits once, with its own
/// index.
///
/// The creating call reports `room_created` before `session_started`
/// (RFC §10.1 step 2). RFC §8.2 orders no framework events across calls:
/// the other call's `room_channel_attached` may come before, between or
/// after them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_tasks_naming_one_missing_room_at_once_create_it_once() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.hook(
        HookTrigger::OnRoomCreated,
        |_: RoomEvent, _| tokio::time::sleep(Duration::from_millis(50)),
        HookOptions {
            execution: HookExecution::Async,
            name: "welcome".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let tap = EventTap::on(&kit, &STEP_2_EVENTS);
    let start = Arc::new(Barrier::new(2));
    let tasks = ["+15550000001", "+15550000002"].map(|sender| {
        let (kit, start) = (kit.clone(), Arc::clone(&start));
        tokio::spawn(async move {
            start.wait().await;
            kit.process_inbound(
                message("sms", sender, "Hi"),
                ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                },
            )
            .await
        })
    });
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    let rooms = kit
        .store()
        .list_rooms(roomkit::store::ListRoomsOptions::default())
        .await
        .unwrap();
    let timeline = kit
        .store()
        .list_events(&room_id(), every_row())
        .await
        .unwrap();
    let bindings = kit.store().list_bindings(&room_id()).await.unwrap();
    let names = event_names(&kit, &tap).await;
    let count = |name: &str| names.iter().filter(|seen| *seen == name).count();
    let position = |name: &str| names.iter().position(|seen| seen == name);
    assert_eq!(
        (
            rooms.len(),
            ["room_created", "room_channel_attached", "session_started"].map(count),
            position("room_created") < position("session_started"),
            timeline.iter().map(|event| event.index).collect::<Vec<_>>(),
            bindings.len(),
        ),
        // The attach's lifecycle record, then the two messages (gap G-046).
        (1, [1, 1, 1], true, vec![0, 1, 2], 1),
        "{names:?}"
    );
}

/// A transport that panics in the call it is built to panic in.
struct Panicking {
    declaration: ChannelConfig,
    in_on_event: bool,
}

impl Panicking {
    fn new(in_on_event: bool) -> Self {
        Self {
            declaration: ChannelConfig::new(
                channel("broken"),
                ChannelType::WEBSOCKET,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
            ),
            in_on_event,
        }
    }
}

impl Channel for Panicking {
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        assert!(!self.in_on_event, "the provider SDK panicked in on_event");
        ready(Ok(ChannelOutput::default()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        assert!(self.in_on_event, "the provider SDK panicked in deliver");
        ready(Ok(ChannelOutput::default()))
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
}

async fn after_a_panic_in(in_on_event: bool) -> roomkit::InboundResult {
    let kit = room_with_agent().await;
    kit.register_channel(Panicking::new(in_on_event)).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("broken"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    kit.process_inbound(
        message("sms", "+15551234567", "Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    next_call_goes_through(&kit).await
}

#[tokio::test]
async fn a_channel_that_panics_in_on_event_leaves_the_lane_and_the_lock_free() {
    let result = after_a_panic_in(true).await;
    assert_eq!(
        result.event.map(|event| (event.index, event.status)),
        Some((5, EventStatus::Delivered))
    );
}

#[tokio::test]
async fn a_channel_that_panics_in_deliver_leaves_the_lane_and_the_lock_free() {
    let result = after_a_panic_in(false).await;
    assert_eq!(
        result.event.map(|event| (event.index, event.status)),
        Some((5, EventStatus::Delivered))
    );
}

/// A transport whose first delivery tells `delivering` it started, then
/// waits for `go`, as a slow provider; it keeps the texts it delivered.
#[derive(Clone)]
struct Gated {
    declaration: ChannelConfig,
    /// Whether the first delivery started.
    started: Arc<AtomicBool>,
    delivering: Arc<Notify>,
    go: Arc<Notify>,
    delivered: Arc<Mutex<Vec<String>>>,
}

impl Gated {
    fn new() -> Self {
        Self {
            declaration: ChannelConfig::new(
                channel("slow"),
                ChannelType::WEBSOCKET,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
            ),
            started: Arc::default(),
            delivering: Arc::default(),
            go: Arc::default(),
            delivered: Arc::default(),
        }
    }
}

impl Channel for Gated {
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let first = !self.started.swap(true, Ordering::SeqCst);
        let gate = self.clone();
        async move {
            if first {
                gate.delivering.notify_one();
                gate.go.notified().await;
            }
            gate.delivered.lock().unwrap().push(text_of(&event.content));
            Ok(ChannelOutput::default())
        }
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
}

/// Whether `events` reports `event_processed` for the room's first message
/// within the bound: the events before it are its attaches' lifecycle
/// records (gap G-046).
async fn first_event_processed(kit: &RoomKit, tap: &EventTap) -> bool {
    let timeline = kit
        .store()
        .list_events(&room_id(), every_row())
        .await
        .unwrap();
    let first = timeline
        .iter()
        .find(|event| event.r#type == EventType::Message)
        .unwrap()
        .id
        .clone();
    let processed = tap.wait_for(|event| event.event_id.as_ref() == Some(&first));
    tokio::time::timeout(BOUND, processed).await.is_ok()
}

/// RFC §13.6, decision 1 of step 2b: the caller gives up once its event is
/// committed and delivered, as a `tokio::time::timeout` that expires then
/// drops its call. The run goes on detached: the event is delivered, its
/// `AFTER_BROADCAST` hooks run, and `event_processed` is emitted.
#[tokio::test]
async fn a_call_dropped_during_its_delivery_still_delivers_its_event() {
    let kit = room_with_agent().await;
    let slow = Gated::new();
    kit.register_channel(slow.clone()).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("slow"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let broadcast_over = Arc::new(Notify::new());
    let observed = Arc::clone(&broadcast_over);
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            if text_of(&event.content) == "Bonjour" {
                observed.notify_one();
            }
            ready(())
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "audit".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let tap = EventTap::on(&kit, &["event_processed"]);
    let room = room_id();
    let call = kit.process_inbound(
        message("sms", "+15551234567", "Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        },
    );
    tokio::select! {
        result = call => panic!("the call ended before its delivery: {result:?}"),
        () = slow.delivering.notified() => {}
    }
    slow.go.notify_one();
    let processed = first_event_processed(&kit, &tap).await;
    let observed = tokio::time::timeout(BOUND, broadcast_over.notified()).await;
    assert_eq!(
        (
            slow.delivered.lock().unwrap().first().cloned(),
            observed.is_ok(),
            processed
        ),
        (Some("Bonjour".to_owned()), true, true)
    );
}

/// An agent whose turn says it started, waits for `go`, then answers
/// "late answer"; `log` records whether the turn answered or its work was
/// dropped unfinished.
#[derive(Clone)]
struct SlowAgent {
    declaration: ChannelConfig,
    turning: Arc<Notify>,
    go: Arc<Notify>,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl SlowAgent {
    fn new() -> Self {
        Self {
            declaration: ChannelConfig::new(
                channel("ai"),
                ChannelType::AI,
                ChannelCategory::Intelligence,
                ChannelDirection::Bidirectional,
            ),
            turning: Arc::default(),
            go: Arc::default(),
            log: Arc::default(),
        }
    }
}

/// Logs that the turn's work was dropped unfinished, unless it answered.
struct Unfinished {
    log: Arc<Mutex<Vec<&'static str>>>,
    answered: bool,
}

impl Drop for Unfinished {
    fn drop(&mut self) {
        if !self.answered {
            self.log.lock().unwrap().push("dropped unfinished");
        }
    }
}

impl Channel for SlowAgent {
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let agent = self.clone();
        async move {
            let mut unfinished = Unfinished {
                log: Arc::clone(&agent.log),
                answered: false,
            };
            agent.turning.notify_one();
            agent.go.notified().await;
            unfinished.answered = true;
            agent.log.lock().unwrap().push("answered");
            let source = EventSource {
                direction: EventDirection::Outbound,
                ..EventSource::new(channel("ai"), ChannelType::AI)
            };
            let answer = RoomEvent {
                id: event.id.clone(),
                created_at: event.created_at,
                ..RoomEvent::new(
                    event.room_id.clone(),
                    source,
                    TextContent::new("late answer"),
                )
            };
            Ok(ChannelOutput {
                responded: true,
                response_events: vec![answer],
                ..ChannelOutput::empty()
            })
        }
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl std::future::Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
}

/// RFC §10.1 "Explicit delivery cancellation", decision 2 of step 3, gap
/// G-076: the caller gives up during the agent's turn, as a
/// `tokio::time::timeout` that expires then drops its call (probe 6 of
/// step 3). The turn is cut and joined: its work is dropped unfinished
/// before the run ends, and no answer is committed, even once the agent
/// could have given one. The event stays delivered, the advisor's console
/// receives it, and its `AFTER_BROADCAST` hooks run.
#[tokio::test]
async fn a_call_dropped_during_its_agent_s_turn_cuts_the_turn_and_delivers_its_event() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let ws = RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    let agent = SlowAgent::new();
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(ws.clone()).unwrap();
    kit.register_channel(agent.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ws", "ai"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let observed = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&observed);
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |event: RoomEvent, _| {
            seen.lock().unwrap().push(text_of(&event.content));
            ready(())
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "audit".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let room = room_id();
    let call = kit.process_inbound(
        message("sms", "+15551234567", "Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        },
    );
    tokio::select! {
        result = call => panic!("the call ended before its agent's turn: {result:?}"),
        () = agent.turning.notified() => {}
    }
    // The agent could answer now: a turn left running would.
    agent.go.notify_one();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    let timeline: Vec<(String, EventStatus)> = kit
        .store()
        .list_events(&room_id(), every_row())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.r#type == EventType::Message)
        .map(|event| (text_of(&event.content), event.status))
        .collect();
    assert_eq!(
        (
            timeline,
            ws.texts(),
            agent.log.lock().unwrap().clone(),
            observed.lock().unwrap().clone()
        ),
        (
            vec![("Bonjour".to_owned(), EventStatus::Delivered)],
            vec!["Bonjour".to_owned()],
            vec!["dropped unfinished"],
            vec!["Bonjour".to_owned()]
        )
    );
}

/// RFC §13.5 "Delivery lanes across processes", §5.1, gap G-091: the
/// room's delivery cursor follows its lane, one index at a time, as the
/// Python implementation's does: the attaches' lifecycle records once
/// committed; a message only once its set has run, not while it delivers;
/// a mute's record, a message stored `BLOCKED`, and the next message each
/// in turn. It reads the room's latest index once its sets have run.
#[tokio::test]
async fn the_delivery_cursor_follows_the_lane() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let slow = Gated::new();
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(slow.clone()).unwrap();
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |event: RoomEvent, _| {
            let spam = text_of(&event.content) == "spam";
            ready(Ok(if spam {
                HookResult::block("spam", BlockOptions::default())
            } else {
                HookResult::allow(AllowOptions::default())
            }))
        },
        HookOptions {
            name: "no_spam".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let room = room_id();
    let store = kit.store();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    let mut cursors = vec![store.get_delivered_index(&room).await.unwrap()];
    kit.attach_channel(&room, &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit.attach_channel(&room, &channel("slow"), AttachChannelOptions::default())
        .await
        .unwrap();
    cursors.push(store.get_delivered_index(&room).await.unwrap());
    let delivering = async {
        slow.delivering.notified().await;
        let during = store.get_delivered_index(&room).await.unwrap();
        slow.go.notify_one();
        during
    };
    let (sent, during) = tokio::join!(
        kit.process_inbound(
            message("sms", "+15551234567", "one"),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            }
        ),
        delivering
    );
    sent.unwrap();
    cursors.push(during);
    cursors.push(store.get_delivered_index(&room).await.unwrap());
    kit.mute(&room, &channel("slow"), MuteOptions::default())
        .await
        .unwrap();
    cursors.push(store.get_delivered_index(&room).await.unwrap());
    for text in ["spam", "two"] {
        kit.process_inbound(
            message("sms", "+15551234567", text),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        cursors.push(store.get_delivered_index(&room).await.unwrap());
    }
    let latest = kit
        .store()
        .get_room(&room)
        .await
        .unwrap()
        .unwrap()
        .latest_index;
    assert_eq!((cursors, latest), (vec![-1, 1, 1, 2, 3, 4, 5], Some(5)));
}

/// The caller gives up while a `BEFORE_BROADCAST` hook runs, under the
/// room's lock.
#[tokio::test]
async fn a_call_dropped_under_the_room_lock_leaves_the_lane_and_the_lock_free() {
    let kit = room_with_agent().await;
    let hung = Arc::new(AtomicBool::new(false));
    kit.hook(
        HookTrigger::BeforeBroadcast,
        move |_: RoomEvent, _| {
            let first = !hung.swap(true, Ordering::SeqCst);
            async move {
                if first {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                }
                Ok(HookResult::allow(AllowOptions::default()))
            }
        },
        HookOptions {
            name: "slow_scan".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let dropped = tokio::time::timeout(
        Duration::from_millis(50),
        kit.process_inbound(
            message("sms", "+15551234567", "Bonjour"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        ),
    )
    .await;
    let paused = tokio::time::timeout(
        BOUND,
        kit.pause_room(&room_id(), PauseRoomOptions::default()),
    )
    .await;
    let next = next_call_goes_through(&kit).await;
    assert_eq!(
        (
            dropped.is_err(),
            paused.is_ok(),
            next.event.map(|event| event.index)
        ),
        // The two attaches' lifecycle records take the first indexes; the
        // integrator's pause writes none (gap G-046).
        (true, true, Some(2))
    );
}

/// Every row of a room's timeline, the refused ones included.
fn every_row() -> roomkit::store::ListEventsOptions {
    roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::store::ListEventsOptions::default()
    }
}
