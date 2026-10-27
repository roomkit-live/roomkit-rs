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
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, EventDirection, EventSource, EventStatus, EventType, HookResult, InboundMessage,
    RoomContext, RoomEvent, RoomId,
};
use roomkit::testing::{EchoAgent, RecordingTransport};
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, ConversationStore,
    CreateRoomOptions, FrameworkEvent, HookTrigger, MuteOptions, PauseRoomOptions,
    ProcessInboundOptions, RoomFilter, RoomKit, RoomKitOptions, text_of,
};
use tokio::sync::{Barrier, Notify, broadcast};

/// How long a call that should not wait may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn message(from: &str, sender: &str, text: &str) -> InboundMessage {
    InboundMessage::new(channel(from), sender, EventContent::text(text))
}

/// A kit with an SMS transport and an echo agent, both attached to `room_1`.
async fn room_with_agent() -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.register_channel(EchoAgent::new(channel("ai"), "You said: ").quoting())
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

/// The names of step 2's framework events among those `events` holds, in
/// order.
fn event_names(events: &mut broadcast::Receiver<FrameworkEvent>) -> Vec<String> {
    let mut names = Vec::new();
    while let Ok(event) = events.try_recv() {
        let name = if matches!(event, FrameworkEvent::RoomCreated { .. }) {
            "room_created"
        } else if matches!(event, FrameworkEvent::RoomChannelAttached { .. }) {
            "room_channel_attached"
        } else if matches!(event, FrameworkEvent::SessionStarted { .. }) {
            "session_started"
        } else {
            continue;
        };
        names.push(name.to_owned());
    }
    names
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
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.observe("welcome", HookTrigger::OnRoomCreated, |_, _| {
        tokio::time::sleep(Duration::from_millis(50))
    })
    .unwrap();
    let mut events = kit.subscribe();
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
        .list_rooms(&RoomFilter::default())
        .await
        .unwrap();
    let timeline = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap();
    let bindings = kit.store().list_bindings(&room_id()).await.unwrap();
    let names = event_names(&mut events);
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
    declaration: ChannelDeclaration,
    in_on_event: bool,
}

impl Panicking {
    fn new(in_on_event: bool) -> Self {
        Self {
            declaration: ChannelDeclaration::new(
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
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
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
    declaration: ChannelDeclaration,
    /// Whether the first delivery started.
    started: Arc<AtomicBool>,
    delivering: Arc<Notify>,
    go: Arc<Notify>,
    delivered: Arc<Mutex<Vec<String>>>,
}

impl Gated {
    fn new() -> Self {
        Self {
            declaration: ChannelDeclaration::new(
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
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
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
}

/// Whether `events` reports `event_processed` for the room's first message
/// within the bound: the events before it are its attaches' lifecycle
/// records (gap G-046).
async fn first_event_processed(
    kit: &RoomKit,
    events: &mut broadcast::Receiver<FrameworkEvent>,
) -> bool {
    let timeline = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap();
    let first = timeline
        .iter()
        .find(|event| event.event_type == EventType::Message)
        .unwrap()
        .id
        .clone();
    let processed = async {
        loop {
            if let Ok(FrameworkEvent::EventProcessed { event_id, .. }) = events.recv().await
                && event_id == first
            {
                return;
            }
        }
    };
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
    kit.observe("audit", HookTrigger::AfterBroadcast, move |event, _| {
        if text_of(&event.content) == "Bonjour" {
            observed.notify_one();
        }
        ready(())
    })
    .unwrap();
    let mut events = kit.subscribe();
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
    let processed = first_event_processed(&kit, &mut events).await;
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
    declaration: ChannelDeclaration,
    turning: Arc<Notify>,
    go: Arc<Notify>,
    log: Arc<Mutex<Vec<&'static str>>>,
}

impl SlowAgent {
    fn new() -> Self {
        Self {
            declaration: ChannelDeclaration::new(
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
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
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
            let source = EventSource::new(channel("ai"), ChannelType::AI, EventDirection::Outbound);
            let answer = RoomEvent::new(
                event.id.clone(),
                event.room_id.clone(),
                EventType::Message,
                source,
                EventContent::text("late answer"),
                event.created_at,
            );
            Ok(ChannelOutput::respond(vec![answer]))
        }
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
    let ws = RecordingTransport::new(channel("ws"), ChannelType::WEBSOCKET);
    let agent = SlowAgent::new();
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
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
    kit.observe("audit", HookTrigger::AfterBroadcast, move |event, _| {
        seen.lock().unwrap().push(text_of(&event.content));
        ready(())
    })
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
    tokio::time::timeout(BOUND, kit.idle()).await.unwrap();
    let timeline: Vec<(String, EventStatus)> = kit
        .store()
        .list_events(&room_id(), &every_row())
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.event_type == EventType::Message)
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
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.register_channel(slow.clone()).unwrap();
    kit.before_broadcast("no_spam", |event, _| {
        let spam = text_of(&event.content) == "spam";
        ready(Ok(if spam {
            HookResult::block("spam")
        } else {
            HookResult::allow()
        }))
    })
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
    kit.before_broadcast("slow_scan", move |_, _| {
        let first = !hung.swap(true, Ordering::SeqCst);
        async move {
            if first {
                tokio::time::sleep(Duration::from_secs(3600)).await;
            }
            Ok(HookResult::allow())
        }
    })
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
