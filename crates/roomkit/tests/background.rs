//! The kit's background work on the tokio driver (decision 3 of step 2b):
//! the test seam `idle` waits for the observer hooks that run after the
//! fact (RFC §9.3) and for the calls that go on detached (RFC §10.1 step
//! 18, §13.6); `close` drains them, as the Python implementation's `close`
//! does, ends what outlives its grace, and every call made after it is
//! refused.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, InboundMessage, RoomContext, RoomEvent, RoomId,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CheckAllTimersOptions,
    CreateRoomOptions, HookTrigger, KitError, PauseRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, text_of,
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// How long a call that should not wait may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

/// How long a wait that should not end is given before the test takes it
/// that it does not.
const BRIEF: Duration = Duration::from_millis(20);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

fn message(text: &str) -> InboundMessage {
    InboundMessage::new(channel("sms"), "+15551234567", EventContent::text(text))
}

/// A kit with an SMS transport attached to `room_1`.
async fn room() -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
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
    kit
}

/// A gate a hook or a channel stops at: it tells `reached` it got there,
/// then waits for `open`.
#[derive(Clone, Default)]
struct Gate {
    reached: Arc<Notify>,
    open: Arc<Notify>,
}

impl Gate {
    async fn pass(&self) {
        self.reached.notify_one();
        self.open.notified().await;
    }
}

/// A transport whose first delivery stops at its gate; it keeps the texts
/// it delivered.
#[derive(Clone)]
struct Gated {
    declaration: ChannelDeclaration,
    gate: Gate,
    /// Whether the first delivery started.
    started: Arc<AtomicBool>,
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
            gate: Gate::default(),
            started: Arc::default(),
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
        let gated = self.clone();
        async move {
            if first {
                gated.gate.pass().await;
            }
            gated
                .delivered
                .lock()
                .unwrap()
                .push(text_of(&event.content));
            Ok(ChannelOutput::default())
        }
    }
}

/// RFC §9.3: an observer runs after the call that fired it returned; `idle`
/// waits for it.
#[tokio::test]
async fn idle_waits_for_an_observer_hook_running_after_its_call() {
    let kit = room().await;
    let gate = Gate::default();
    let observed = Arc::new(AtomicBool::new(false));
    let (hook_gate, hook_observed) = (gate.clone(), Arc::clone(&observed));
    kit.observe("audit", HookTrigger::AfterBroadcast, move |_, _| {
        let (gate, observed) = (hook_gate.clone(), Arc::clone(&hook_observed));
        async move {
            gate.pass().await;
            observed.store(true, Ordering::SeqCst);
        }
    })
    .unwrap();
    kit.process_inbound(
        message("Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    tokio::time::timeout(BOUND, gate.reached.notified())
        .await
        .unwrap();
    let busy = tokio::time::timeout(BRIEF, roomkit::testing::idle(&kit))
        .await
        .is_err();
    gate.open.notify_one();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    assert_eq!((busy, observed.load(Ordering::SeqCst)), (true, true));
}

/// RFC §13.6, decision 1 of step 2b: a call its caller stopped waiting for
/// once it was decided goes on delivering; `idle` waits for its delivery.
#[tokio::test]
async fn idle_waits_for_a_call_that_goes_on_detached() {
    let kit = room().await;
    let slow = Gated::new();
    kit.register_channel(slow.clone()).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("slow"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let room = room_id();
    let call = kit.process_inbound(
        message("Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        },
    );
    tokio::select! {
        result = call => panic!("the call ended before its delivery: {result:?}"),
        () = slow.gate.reached.notified() => {}
    }
    let busy = tokio::time::timeout(BRIEF, roomkit::testing::idle(&kit))
        .await
        .is_err();
    slow.gate.open.notify_one();
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    assert_eq!(
        (busy, slow.delivered.lock().unwrap().clone()),
        (true, vec!["Bonjour".to_owned()])
    );
}

/// Decision 15: a delivery under way when the kit closes runs to its end:
/// its event is delivered, its caller receives its result, and `close`
/// returns once it is over. A second `close` meanwhile returns at once.
#[tokio::test]
async fn close_drains_a_queued_delivery() {
    let (kit, slow) = slow_room().await;
    let call = spawn_bonjour(&kit);
    tokio::time::timeout(BOUND, slow.gate.reached.notified())
        .await
        .unwrap();
    let closing = tokio::spawn({
        let kit = kit.clone();
        async move { kit.close().await }
    });
    // Once the first close began, which refuses the calls, the second
    // returns at once, the first still draining.
    while kit
        .check_all_timers(CheckAllTimersOptions::default())
        .await
        .is_ok()
    {
        tokio::task::yield_now().await;
    }
    tokio::time::timeout(BOUND, kit.close()).await.unwrap();
    slow.gate.open.notify_one();
    tokio::time::timeout(BOUND, closing).await.unwrap().unwrap();
    let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    assert_eq!(
        (
            ended.map(|result| result.event.is_some()),
            slow.delivered.lock().unwrap().clone()
        ),
        (Ok(true), vec!["Bonjour".to_owned()])
    );
}

/// Decision 15: a delivery that never ends is aborted 5 seconds into the
/// close, which then returns: its caller receives `ShutDown`, the event
/// undelivered.
#[tokio::test(start_paused = true)]
async fn close_aborts_what_outlives_the_lane_grace() {
    let (kit, slow) = slow_room().await;
    let call = spawn_bonjour(&kit);
    slow.gate.reached.notified().await;
    let started = tokio::time::Instant::now();
    kit.close().await;
    let took = started.elapsed();
    let ended = call.await.unwrap();
    assert_eq!(
        (
            took >= Duration::from_secs(5) && took < Duration::from_secs(6),
            ended.err(),
            slow.delivered.lock().unwrap().is_empty()
        ),
        (true, Some(KitError::ShutDown), true)
    );
}

/// Decision 15: an observer still running when the kit closes finishes
/// within its hook's timeout before the close returns; one that would run
/// longer is dropped at its timeout, 30 seconds unless its hook names
/// another.
#[tokio::test(start_paused = true)]
async fn close_lets_an_observer_finish_within_its_hook_s_timeout() {
    let kit = room().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    for (name, runs) in [("brief", 1), ("endless", 3600)] {
        let observed = Arc::clone(&observed);
        kit.observe(name, HookTrigger::AfterBroadcast, move |_, _| {
            let observed = Arc::clone(&observed);
            async move {
                tokio::time::sleep(Duration::from_secs(runs)).await;
                observed.lock().unwrap().push(name);
            }
        })
        .unwrap();
    }
    kit.process_inbound(
        message("Bonjour"),
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let started = tokio::time::Instant::now();
    kit.close().await;
    let took = started.elapsed();
    assert_eq!(
        (
            took >= Duration::from_secs(30) && took < Duration::from_secs(31),
            observed.lock().unwrap().clone()
        ),
        (true, vec!["brief"])
    );
}

/// The kit with its SMS and the gated `slow` transport attached.
async fn slow_room() -> (RoomKit, Gated) {
    let kit = room().await;
    let slow = Gated::new();
    kit.register_channel(slow.clone()).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("slow"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    (kit, slow)
}

/// `Bonjour` into `room_1`, on a task of its own.
fn spawn_bonjour(kit: &RoomKit) -> JoinHandle<Result<roomkit::InboundResult, KitError>> {
    let kit = kit.clone();
    tokio::spawn(async move {
        kit.process_inbound(
            message("Bonjour"),
            ProcessInboundOptions {
                room_id: Some(room_id()),
                ..Default::default()
            },
        )
        .await
    })
}

/// Every call that runs an operation is refused once the kit is closed;
/// registering a channel still works.
#[tokio::test]
async fn a_call_made_after_the_close_is_refused() {
    let kit = room().await;
    kit.close().await;
    let room = room_id();
    let refused = (
        kit.process_inbound(
            message("Bonjour"),
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await
        .err(),
        kit.process_inbound(message("Bonjour"), ProcessInboundOptions::default())
            .await
            .err(),
        kit.create_room(CreateRoomOptions {
            room_id: Some((RoomId::new("room_2").unwrap()).clone()),
            ..Default::default()
        })
        .await
        .err(),
        kit.pause_room(&room, PauseRoomOptions::default())
            .await
            .err(),
        kit.check_all_timers(CheckAllTimersOptions::default())
            .await
            .err(),
    );
    let registered = kit
        .register_channel(RecordingTransport::new(
            channel("ws"),
            ChannelType::WEBSOCKET,
        ))
        .is_ok();
    assert_eq!(
        (refused, registered),
        (
            (
                Some(KitError::ShutDown),
                Some(KitError::ShutDown),
                Some(KitError::ShutDown),
                Some(KitError::ShutDown),
                Some(KitError::ShutDown)
            ),
            true
        )
    );
}

/// A channel that records each close it is asked for into a shared log.
struct Closing {
    declaration: ChannelDeclaration,
    closed: Arc<Mutex<Vec<String>>>,
}

impl Closing {
    fn new(id: &str, closed: &Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel(id),
                ChannelType::WEBSOCKET,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
            ),
            closed: Arc::clone(closed),
        }
    }
}

impl Channel for Closing {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    async fn close(&self) {
        tokio::time::sleep(BRIEF).await;
        let id = self.declaration.channel_id.as_str().to_owned();
        self.closed.lock().unwrap().push(id);
    }
}

/// The close closes each channel registered, one after the other in the
/// order of their ids (RFC §6.1 `close`), once.
#[tokio::test]
async fn close_closes_each_channel_once_in_the_order_of_their_ids() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let closed = Arc::new(Mutex::new(Vec::new()));
    for id in ["ws_b", "ws_a"] {
        kit.register_channel(Closing::new(id, &closed)).unwrap();
    }
    let (first, second) = tokio::join!(kit.close(), kit.close());
    let ((), ()) = (first, second);
    kit.close().await;
    assert_eq!(*closed.lock().unwrap(), ["ws_a", "ws_b"]);
}
