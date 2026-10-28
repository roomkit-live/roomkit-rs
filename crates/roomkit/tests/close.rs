//! The kit's one shutdown, and a close called from the kit's own work (RFC
//! §12.10.4 as RMK-1015 amended it; RMK-1017), as the Python
//! implementation's `TestOneShutdown` (`tests/test_framework.py`) and
//! `tests/test_kit_close_doors.py` prove them:
//!
//! - every `close` joins the kit's one shutdown, a caller that stops
//!   waiting stopping only its own wait, and it never runs twice;
//! - a `close` from the kit's own work neither waits for that work nor
//!   cuts it, whether it started the shutdown or joined one under way;
//! - a `close` from the shutdown's own work returns at once.
//!
//! The kit's own work here is what roomkit-rs has of Python's doors: a
//! SYNC hook's body, an ASYNC hook a delivery fires, a transport's
//! delivery, an ASYNC observer run after the fact and an agent's tool
//! call. Python's other doors, a voice, video or realtime session's end, a
//! delegation's `on_complete`, a discussion's announcement and a held
//! write, are features roomkit-rs does not have yet.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CreateRoomOptions,
    HookExecution, HookSpec, HookTrigger, InboundResult, KitError, ProcessInboundOptions, RoomKit,
    RoomKitOptions, text_of,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiAgent, ToolDefinition, ToolOutput, Tools};
use serde_json::json;
use tokio::sync::{Barrier, Notify};
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// How long a wait that should end may take before the test fails.
const BOUND: Duration = Duration::from_secs(5);

/// Less than the close's grace, 5 seconds, which a close that waited for
/// the work it was called from would take.
const AT_ONCE: Duration = Duration::from_secs(1);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn message(text: &str) -> InboundMessage {
    InboundMessage::new(channel("sms"), "+15551234567", EventContent::text(text))
}

fn into(room_id: RoomId) -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room_id),
        ..Default::default()
    }
}

fn transport(id: &str) -> ChannelDeclaration {
    ChannelDeclaration::new(
        channel(id),
        ChannelType::WEBSOCKET,
        ChannelCategory::Transport,
        ChannelDirection::Bidirectional,
    )
}

/// A channel whose close waits until released, counting its closes; given
/// a kit, it closes that kit from its own close.
#[derive(Clone)]
struct GatedClose {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    closes: Arc<AtomicUsize>,
    finished: Arc<AtomicBool>,
    closes_the_kit: Arc<Mutex<Option<RoomKit>>>,
}

impl GatedClose {
    fn new() -> Self {
        Self {
            entered: Arc::default(),
            release: Arc::default(),
            closes: Arc::default(),
            finished: Arc::default(),
            closes_the_kit: Arc::default(),
        }
    }
}

impl Channel for GatedClose {
    fn declaration(&self) -> ChannelDeclaration {
        transport("sms1")
    }

    fn close(&self) -> impl Future<Output = ()> + Send {
        let this = self.clone();
        async move {
            this.closes.fetch_add(1, Ordering::SeqCst);
            this.entered.notify_one();
            this.release.notified().await;
            let kit = this.closes_the_kit.lock().unwrap().clone();
            if let Some(kit) = kit {
                kit.close().await;
            }
            this.finished.store(true, Ordering::SeqCst);
        }
    }
}

/// RFC §12.10.4: a close called while another runs, its caller still
/// waiting or its future dropped, waits for the same shutdown, which runs
/// to its end; a close once it ended returns at once; the channels close
/// once.
#[tokio::test]
async fn a_close_during_another_returns_once_the_shutdown_ends() {
    let mut cases = Vec::new();
    for first_dropped in [false, true] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let gated = GatedClose::new();
        kit.register_channel(gated.clone()).unwrap();
        let closes = || {
            let kit = kit.clone();
            tokio::spawn(async move { kit.close().await })
        };
        let first = closes();
        tokio::time::timeout(BOUND, gated.entered.notified())
            .await
            .unwrap();
        if first_dropped {
            first.abort();
        }
        let mut second = closes();
        let early = tokio::time::timeout(Duration::from_millis(20), &mut second).await;
        gated.release.notify_one();
        let waited = early.is_err();
        let ended = waited && tokio::time::timeout(BOUND, second).await.is_ok();
        let later = tokio::time::timeout(BOUND, kit.close()).await.is_ok();
        cases.push((
            first_dropped,
            waited,
            ended,
            later,
            gated.closes.load(Ordering::SeqCst),
            gated.finished.load(Ordering::SeqCst),
        ));
    }
    assert_eq!(
        cases,
        [
            (false, true, true, true, 1, true),
            (true, true, true, true, 1, true)
        ]
    );
}

/// RFC §12.10.4: a close called from the shutdown's own work, a channel's
/// close, returns at once rather than wait for itself.
#[tokio::test]
async fn a_close_from_the_shutdown_s_own_work_returns_at_once() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let gated = GatedClose::new();
    *gated.closes_the_kit.lock().unwrap() = Some(kit.clone());
    gated.release.notify_one();
    kit.register_channel(gated.clone()).unwrap();
    tokio::time::timeout(BOUND, kit.close()).await.unwrap();
    assert!(gated.finished.load(Ordering::SeqCst));
}

/// A channel that says whether it was closed and keeps the texts it
/// delivered; a closer set on it closes the kit from its first delivery.
#[derive(Clone)]
struct Witness {
    closed: Arc<AtomicBool>,
    delivered: Arc<Mutex<Vec<String>>>,
    closes_on_delivery: Arc<Mutex<Option<Closer>>>,
}

impl Witness {
    fn new() -> Self {
        Self {
            closed: Arc::default(),
            delivered: Arc::default(),
            closes_on_delivery: Arc::default(),
        }
    }

    fn delivered(&self) -> Vec<String> {
        self.delivered.lock().unwrap().clone()
    }
}

impl Channel for Witness {
    fn declaration(&self) -> ChannelDeclaration {
        transport("witness")
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let this = self.clone();
        async move {
            this.delivered.lock().unwrap().push(text_of(&event.content));
            let closer = this.closes_on_delivery.lock().unwrap().take();
            if let Some(closer) = closer {
                closer.close().await;
            }
            Ok(ChannelOutput::default())
        }
    }

    fn close(&self) -> impl Future<Output = ()> + Send {
        let closed = Arc::clone(&self.closed);
        async move { closed.store(true, Ordering::SeqCst) }
    }
}

/// The kit's own work closing the kit, once, once let go when it is gated;
/// it keeps whether the witness was closed when its close returned.
#[derive(Clone)]
struct Closer {
    kit: RoomKit,
    witness: Arc<AtomicBool>,
    gated: bool,
    entered: Arc<Notify>,
    go: Arc<Notify>,
    once: Arc<AtomicBool>,
    /// Set as the close returns: whether the witness was closed then.
    returned: Arc<Mutex<Option<bool>>>,
    returned_now: Arc<Notify>,
}

impl Closer {
    fn new(kit: &RoomKit, witness: &Witness, gated: bool) -> Self {
        Self {
            kit: kit.clone(),
            witness: Arc::clone(&witness.closed),
            gated,
            entered: Arc::default(),
            go: Arc::default(),
            once: Arc::default(),
            returned: Arc::default(),
            returned_now: Arc::default(),
        }
    }

    async fn close(&self) {
        if self.once.swap(true, Ordering::SeqCst) {
            return;
        }
        self.entered.notify_one();
        if self.gated {
            self.go.notified().await;
        }
        self.kit.close().await;
        *self.returned.lock().unwrap() = Some(self.witness.load(Ordering::SeqCst));
        self.returned_now.notify_one();
    }

    /// Whether the work's close returned within the bound, the witness
    /// closed by then.
    async fn returned_once_closed(&self) -> Option<bool> {
        drop(tokio::time::timeout(BOUND, self.returned_now.notified()).await);
        *self.returned.lock().unwrap()
    }
}

/// A kit with an SMS transport and the witness attached to `room_1`.
async fn kit_with(witness: &Witness) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.register_channel(witness.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "witness"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}

/// Whether the kit is sealed: a room is refused.
async fn sealed(kit: &RoomKit) -> bool {
    let options = CreateRoomOptions {
        room_id: Some(RoomId::new("after").unwrap()),
        ..Default::default()
    };
    matches!(kit.create_room(options).await, Err(KitError::ShutDown))
}

/// Where the kit's own work closes the kit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Door {
    /// A SYNC `BEFORE_BROADCAST` hook's body, under the room's lock.
    SyncHook,
    /// An ASYNC `AFTER_BROADCAST` hook the delivery fires.
    DeliveryHook,
    /// The witness's delivery.
    Delivery,
    /// An ASYNC `BEFORE_BROADCAST` observer, run after the fact.
    Observer,
    /// An agent's tool call, in its room's turn.
    ToolCall,
}

const DOORS: [Door; 5] = [
    Door::SyncHook,
    Door::DeliveryHook,
    Door::Delivery,
    Door::Observer,
    Door::ToolCall,
];

/// The door's kit, its closer, and the message whose work closes the kit,
/// running.
async fn open(door: Door, witness: &Witness, gated: bool) -> (RoomKit, Closer, Call) {
    let kit = kit_with(witness).await;
    let closer = Closer::new(&kit, witness, gated);
    let hook = |trigger, execution| {
        let work = closer.clone();
        let spec = HookSpec::new("quit", trigger, execution);
        kit.hook(spec, move |_event, _context| {
            let work = work.clone();
            async move {
                work.close().await;
                Ok(HookResult::allow())
            }
        })
        .unwrap();
    };
    match door {
        Door::SyncHook => hook(HookTrigger::BeforeBroadcast, HookExecution::Sync),
        Door::DeliveryHook => hook(HookTrigger::AfterBroadcast, HookExecution::Async),
        Door::Observer => hook(HookTrigger::BeforeBroadcast, HookExecution::Async),
        Door::Delivery => *witness.closes_on_delivery.lock().unwrap() = Some(closer.clone()),
        Door::ToolCall => closing_agent(&kit, &closer).await,
    }
    let call = spawn_message(&kit, "Bye.", room_id());
    (kit, closer, call)
}

/// An agent in `room_1` whose one tool closes the kit, then answers.
async fn closing_agent(kit: &RoomKit, closer: &Closer) {
    let work = closer.clone();
    let tools = Tools::new().tool(
        ToolDefinition::new("shut_down", "Shut down.", json!({ "type": "object" })),
        move |_call, _context| {
            let work = work.clone();
            async move {
                work.close().await;
                Ok(ToolOutput::Text("closed".to_owned()))
            }
        },
    );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "shut_down", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let agent = AiAgent::new(channel("agent"), model)
        .with_tools(tools)
        .unwrap();
    kit.register_channel(agent).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("agent"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
}

type Call = JoinHandle<Result<InboundResult, KitError>>;

fn spawn_message(kit: &RoomKit, text: &str, room_id: RoomId) -> Call {
    let kit = kit.clone();
    let message = message(text);
    tokio::spawn(async move { kit.process_inbound(message, into(room_id)).await })
}

/// RFC §12.10.4: the kit's own work closes the kit: its close neither
/// waits for nor cuts the work it runs within, and returns once every
/// channel is closed; the kit is sealed, and the work's call ends.
#[tokio::test(start_paused = true)]
async fn a_close_from_the_kit_s_own_work_returns_and_closes_the_kit() {
    let mut doors = Vec::new();
    for door in DOORS {
        let witness = Witness::new();
        let started = Instant::now();
        let (kit, closer, call) = open(door, &witness, false).await;
        let returned = closer.returned_once_closed().await;
        let at_once = started.elapsed() < AT_ONCE;
        let call_ended = tokio::time::timeout(BOUND, call).await.is_ok();
        doors.push((door, returned, at_once, sealed(&kit).await, call_ended));
    }
    assert_eq!(
        doors,
        DOORS.map(|door| (door, Some(true), true, true, true))
    );
}

/// RFC §12.10.4: another close runs, and waits for the work (the drain),
/// when that work closes the kit too: from then on it is spared, and both
/// closes return once the kit is closed. The tool's door is RMK-1019's:
/// the close that joins spares its own caller's work, not only the first
/// caller's.
#[tokio::test(start_paused = true)]
async fn a_close_from_the_kit_s_own_work_joining_another_spares_that_work() {
    let mut doors = Vec::new();
    for door in DOORS {
        let witness = Witness::new();
        let (kit, closer, call) = open(door, &witness, true).await;
        tokio::time::timeout(BOUND, closer.entered.notified())
            .await
            .unwrap();
        let started = Instant::now();
        let first = tokio::spawn({
            let kit = kit.clone();
            async move { kit.close().await }
        });
        // The first close's drain reaches the work, and waits for it.
        tokio::time::sleep(Duration::from_millis(100)).await;
        closer.go.notify_one();
        let returned = closer.returned_once_closed().await;
        let first_ended = tokio::time::timeout(BOUND, first).await.is_ok();
        let at_once = started.elapsed() < AT_ONCE;
        let call_ended = tokio::time::timeout(BOUND, call).await.is_ok();
        doors.push((door, returned, first_ended, at_once, call_ended));
    }
    assert_eq!(
        doors,
        DOORS.map(|door| (door, Some(true), true, true, true))
    );
}

/// A transport whose deliveries each close the kit, once both are in
/// their delivery.
#[derive(Clone)]
struct ClosingTransport {
    kit: Arc<Mutex<Option<RoomKit>>>,
    together: Arc<Barrier>,
    returned: Arc<AtomicUsize>,
}

impl Channel for ClosingTransport {
    fn declaration(&self) -> ChannelDeclaration {
        transport("closing")
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let this = self.clone();
        async move {
            this.together.wait().await;
            let kit = this.kit.lock().unwrap().clone();
            if let Some(kit) = kit {
                kit.close().await;
                this.returned.fetch_add(1, Ordering::SeqCst);
            }
            Ok(ChannelOutput::default())
        }
    }
}

/// RFC §12.10.4: two deliveries, in two rooms, each close the kit at once:
/// the second close joins the shutdown the first started, which spares
/// both, and both return once the kit is closed (Python's
/// `test_two_sessions_ending_at_once_each_close_the_kit`).
#[tokio::test(start_paused = true)]
async fn two_deliveries_closing_the_kit_at_once_each_close_it() {
    let witness = Witness::new();
    let kit = kit_with(&witness).await;
    let closing = ClosingTransport {
        kit: Arc::new(Mutex::new(Some(kit.clone()))),
        together: Arc::new(Barrier::new(2)),
        returned: Arc::default(),
    };
    kit.register_channel(closing.clone()).unwrap();
    let rooms = [room_id(), RoomId::new("room_2").unwrap()];
    for room in &rooms {
        if room != &room_id() {
            kit.create_room(CreateRoomOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        kit.attach_channel(room, &channel("closing"), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let started = Instant::now();
    let calls = rooms.map(|room| spawn_message(&kit, "Bye.", room));
    let mut ended = 0;
    for call in calls {
        ended += usize::from(tokio::time::timeout(BOUND, call).await.is_ok());
    }
    assert_eq!(
        (
            ended,
            closing.returned.load(Ordering::SeqCst),
            started.elapsed() < AT_ONCE,
            witness.closed.load(Ordering::SeqCst)
        ),
        (2, 2, true, true)
    );
}

/// RFC §12.10.4: a delivery closes the kit while the room's next message
/// waits behind it in the room's lane: the delivery is spared, not drained,
/// so that message is never delivered, and its caller is answered at once,
/// `cancellation_reason` `kit_closed`, rather than at the close's grace.
#[tokio::test(start_paused = true)]
async fn the_deliveries_queued_behind_a_delivery_a_close_runs_in_are_dropped() {
    let witness = Witness::new();
    let (kit, closer, call) = open(Door::Delivery, &witness, true).await;
    tokio::time::timeout(BOUND, closer.entered.notified())
        .await
        .unwrap();
    let queued = spawn_message(&kit, "Again.", room_id());
    // Queued behind the delivery that closes the kit.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = Instant::now();
    closer.go.notify_one();
    let returned = closer.returned_once_closed().await;
    let queued = tokio::time::timeout(BOUND, queued).await.unwrap().unwrap();
    let at_once = started.elapsed() < AT_ONCE;
    tokio::time::timeout(BOUND, call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            returned,
            at_once,
            queued.map(|result| result.cancellation_reason),
            witness.delivered()
        ),
        (
            Some(true),
            true,
            Ok(Some("kit_closed".to_owned())),
            vec!["Bye.".to_owned()]
        )
    );
}
