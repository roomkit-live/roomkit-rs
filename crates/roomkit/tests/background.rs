//! The kit's background work on the tokio driver (decision 3 of step 2b):
//! the test seam `idle` waits for the observer hooks that run after the
//! fact (RFC §9.3) and for the calls that go on detached (RFC §10.1 step
//! 18, §13.6); `close` drains them, as the Python implementation's `close`
//! does, ends what outlives its grace, and every call made after it is
//! refused.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::{Future, pending};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, HookResult, InboundMessage, RoomContext, RoomEvent, RoomId,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CheckAllTimersOptions,
    CreateRoomOptions, GetTimelineOptions, HookExecution, HookSpec, HookTrigger, KitError,
    PauseRoomOptions, ProcessInboundOptions, RoomKit, RoomKitOptions, SendEventOptions, text_of,
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

/// Decision 15: a delivery in progress when the kit closes runs to its
/// end: the close waits for it, its event is delivered, its caller
/// receives its result, and `close` returns once it is over. A second
/// `close` meanwhile waits for the same shutdown (RFC §12.10.4).
#[tokio::test]
async fn close_drains_a_delivery_in_progress() {
    let (kit, slow) = slow_room().await;
    let call = spawn_bonjour(&kit);
    tokio::time::timeout(BOUND, slow.gate.reached.notified())
        .await
        .unwrap();
    let closes = || {
        let kit = kit.clone();
        tokio::spawn(async move { kit.close().await })
    };
    let (mut first, mut second) = (closes(), closes());
    // The close drains: both closes are still running.
    let draining = tokio::time::timeout(BRIEF, &mut first).await.is_err()
        && tokio::time::timeout(BRIEF, &mut second).await.is_err();
    assert!(
        draining,
        "a close returned at once: it did not wait for the delivery in progress"
    );
    slow.gate.open.notify_one();
    tokio::time::timeout(BOUND, first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(BOUND, second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    assert_eq!(
        (
            draining,
            ended.map(|result| (result.event.is_some(), result.cancellation_reason)),
            slow.delivered.lock().unwrap().clone()
        ),
        (true, Ok((true, None)), vec!["Bonjour".to_owned()])
    );
}

/// Decision 15: a delivery that never ends is cut 5 seconds into the
/// close, which then returns: its caller receives its result, the event
/// committed and undelivered, its `cancellation_reason` `lane_cancelled`,
/// as Python's (tasks/probes/step5-lot1/fixpass.out P2).
#[tokio::test(start_paused = true)]
async fn close_aborts_what_outlives_the_lane_grace() {
    let (kit, slow) = slow_room().await;
    let call = spawn_bonjour(&kit);
    tokio::time::timeout(BOUND, slow.gate.reached.notified())
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    let took = started.elapsed();
    let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    assert_eq!(
        (
            took >= Duration::from_secs(5) && took < Duration::from_secs(6),
            ended.map(|result| (
                result.event.is_some(),
                result.delivery_results.len(),
                result.cancellation_reason
            )),
            slow.delivered.lock().unwrap().is_empty()
        ),
        (true, Ok((true, 0, Some("lane_cancelled".to_owned()))), true)
    );
}

/// Decision 15: a call stuck in a SYNC hook before its commit when the kit
/// closes is ended at the grace: its caller receives `ShutDown`, nothing of
/// its message stored, as Python's caller is refused with nothing stored
/// (tasks/probes/step5-lot1/fixpass.out P1).
#[tokio::test(start_paused = true)]
async fn close_ends_a_call_stuck_before_its_commit() {
    let kit = room().await;
    kit.before_broadcast("stuck", |_, _| async {
        pending::<()>().await;
        Ok(HookResult::allow())
    })
    .unwrap();
    let call = spawn_bonjour(&kit);
    tokio::time::sleep(BRIEF).await;
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    let stored = kit
        .get_timeline(&room_id(), GetTimelineOptions::default())
        .await;
    assert_eq!(
        (
            ended.err(),
            stored.map(|events| events
                .iter()
                .any(|event| text_of(&event.content) == "Bonjour"))
        ),
        (Some(KitError::ShutDown), Ok(false))
    );
}

/// Decision 15, as Python's close does (tasks/probes/step5-lot1
/// review-A probe 7): a call made while the close drains is taken, its
/// event stored and its delivery dropped, `kit_closed`; a room is created;
/// once the close is over, both are refused.
#[tokio::test]
async fn a_call_while_the_close_drains_is_taken_its_delivery_dropped() {
    let (kit, slow) = slow_room().await;
    let call = spawn_bonjour(&kit);
    tokio::time::timeout(BOUND, slow.gate.reached.notified())
        .await
        .unwrap();
    let mut closing = tokio::spawn({
        let kit = kit.clone();
        async move { kit.close().await }
    });
    let draining = tokio::time::timeout(BRIEF, &mut closing).await.is_err();
    assert!(
        draining,
        "the close returned at once: it did not wait for the delivery in progress"
    );
    let during = kit
        .process_inbound(message("During"), in_room())
        .await
        .map(|result| (result.event.is_some(), result.cancellation_reason));
    let created = kit.create_room(CreateRoomOptions::default()).await.is_ok();
    slow.gate.open.notify_one();
    tokio::time::timeout(BOUND, closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(BOUND, call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let after = kit.process_inbound(message("After"), in_room()).await.err();
    let created_after = kit.create_room(CreateRoomOptions::default()).await.err();
    assert_eq!(
        (draining, during, created, after, created_after),
        (
            true,
            Ok((true, Some("kit_closed".to_owned()))),
            true,
            Some(KitError::ShutDown),
            Some(KitError::ShutDown)
        )
    );
}

/// A transport whose delivery of `Bonjour` stops at its gate, then calls
/// into its own room: a direct injection and a message, whose outcomes it
/// keeps.
#[derive(Clone)]
struct Calling {
    declaration: ChannelDeclaration,
    gate: Gate,
    kit: Arc<Mutex<Option<RoomKit>>>,
    outcomes: Arc<Mutex<Vec<String>>>,
}

impl Channel for Calling {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let calling = self.clone();
        async move {
            if text_of(&event.content) != "Bonjour" {
                return Ok(ChannelOutput::default());
            }
            calling.gate.pass().await;
            let kit = calling.kit.lock().unwrap().clone().unwrap();
            let sent = kit
                .send_event(
                    &room_id(),
                    &channel("caller"),
                    EventContent::text("Sent"),
                    SendEventOptions::default(),
                )
                .await
                .map(|event| text_of(&event.content));
            let again = kit
                .process_inbound(message("Again"), in_room())
                .await
                .map(|result| result.cancellation_reason);
            let mut outcomes = calling.outcomes.lock().unwrap();
            outcomes.push(format!("{sent:?}"));
            outcomes.push(format!("{again:?}"));
            Ok(ChannelOutput::default())
        }
    }
}

/// Decision 15, as Python's close does (tasks/probes/step5-lot1/
/// fixpass.out P3): a delivery draining when the kit closes calls into its
/// own room, and both calls are taken: the injection returns its event, the
/// message its result, its delivery dropped `kit_closed`.
#[tokio::test]
async fn a_draining_delivery_s_calls_into_its_room_are_taken() {
    let kit = room().await;
    let mut declaration = ChannelDeclaration::new(
        channel("caller"),
        ChannelType::WEBSOCKET,
        ChannelCategory::Transport,
        ChannelDirection::Bidirectional,
    );
    declaration.recipient_key = None;
    let calling = Calling {
        declaration,
        gate: Gate::default(),
        kit: Arc::new(Mutex::new(Some(kit.clone()))),
        outcomes: Arc::default(),
    };
    kit.register_channel(calling.clone()).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("caller"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let call = spawn_bonjour(&kit);
    tokio::time::timeout(BOUND, calling.gate.reached.notified())
        .await
        .unwrap();
    let mut closing = tokio::spawn({
        let kit = kit.clone();
        async move { kit.close().await }
    });
    let draining = tokio::time::timeout(BRIEF, &mut closing).await.is_err();
    assert!(
        draining,
        "the close returned at once: it did not wait for the delivery in progress"
    );
    calling.gate.open.notify_one();
    tokio::time::timeout(BOUND, closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    tokio::time::timeout(BOUND, call)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    calling.kit.lock().unwrap().take();
    assert_eq!(
        (draining, calling.outcomes.lock().unwrap().clone()),
        (
            true,
            vec![
                r#"Ok("Sent")"#.to_owned(),
                r#"Ok(Some("kit_closed"))"#.to_owned()
            ]
        )
    );
}

/// Decision 15: an observer a delivery fires is that delivery's work, as
/// Python's lane runs it: the close drains it within its grace, one of 3 s
/// finishing and one of 9 s cut at 5 s (`tasks/probes/step5-lot1/
/// observers_close.py`).
#[tokio::test(start_paused = true)]
async fn close_cuts_an_after_broadcast_observer_at_the_grace() {
    let kit = room().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    for (name, runs) in [("three", 3), ("nine", 9)] {
        let observed = Arc::clone(&observed);
        let mut spec = HookSpec::new(name, HookTrigger::AfterBroadcast, HookExecution::Async);
        spec.timeout = Duration::from_secs(30);
        kit.hook(spec, move |_event, _| {
            let observed = Arc::clone(&observed);
            async move {
                tokio::time::sleep(Duration::from_secs(runs)).await;
                observed.lock().unwrap().push(name);
                Ok(HookResult::allow())
            }
        })
        .unwrap();
    }
    kit.process_inbound(message("Bonjour"), in_room())
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    let took = started.elapsed();
    assert_eq!(
        (
            took >= Duration::from_secs(5) && took < Duration::from_secs(6),
            observed.lock().unwrap().clone()
        ),
        (true, vec!["three"])
    );
}

/// Decision 15: an observer no delivery fired, a `BEFORE_BROADCAST`
/// observer of the event its chain allowed, still running when the kit
/// closes finishes within its hook's timeout before the close returns, as
/// Python's close drains its observers; one that would run longer is
/// dropped at its timeout, 30 seconds unless its hook names another.
#[tokio::test(start_paused = true)]
async fn close_lets_an_observer_finish_within_its_hook_s_timeout() {
    let kit = room().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    for (name, runs) in [("brief", 1), ("endless", 3600)] {
        let observed = Arc::clone(&observed);
        kit.observe(name, HookTrigger::BeforeBroadcast, move |_, _| {
            let observed = Arc::clone(&observed);
            async move {
                tokio::time::sleep(Duration::from_secs(runs)).await;
                observed.lock().unwrap().push(name);
            }
        })
        .unwrap();
    }
    kit.process_inbound(message("Bonjour"), in_room())
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    let took = started.elapsed();
    assert_eq!(
        (
            took >= Duration::from_secs(30) && took < Duration::from_secs(31),
            observed.lock().unwrap().clone()
        ),
        (true, vec!["brief"])
    );
}

/// A transport whose every delivery takes `delay`, 500 ms by default; it
/// keeps the texts it delivered.
#[derive(Clone)]
struct Slow {
    delay: Duration,
    delivered: Arc<Mutex<Vec<String>>>,
}

impl Default for Slow {
    fn default() -> Self {
        Self {
            delay: Duration::from_millis(500),
            delivered: Arc::default(),
        }
    }
}

impl Channel for Slow {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            channel("slow"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        )
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let (delivered, delay) = (Arc::clone(&self.delivered), self.delay);
        async move {
            tokio::time::sleep(delay).await;
            delivered.lock().unwrap().push(text_of(&event.content));
            Ok(ChannelOutput::default())
        }
    }
}

/// Decision 15, plan step 2: twenty deliveries queued in a room's lane, of
/// 500 ms each, and an observer of the first that runs 9 s: the close
/// returns at its 5 s grace, every waiter answered, what was delivered
/// reported, the delivery cut running `lane_cancelled`, the ones still
/// queued `kit_closed` (tasks/probes/step5-lot1-review-A probe 12).
#[tokio::test(start_paused = true)]
async fn close_ends_the_queued_deliveries_at_the_grace() {
    let kit = room().await;
    let slow = Slow::default();
    kit.register_channel(slow.clone()).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("slow"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let mut spec = HookSpec::new("long", HookTrigger::AfterBroadcast, HookExecution::Async);
    spec.timeout = Duration::from_secs(30);
    kit.hook(spec, |event, _| async move {
        if text_of(&event.content) == "m0" {
            tokio::time::sleep(Duration::from_secs(9)).await;
        }
        Ok(HookResult::allow())
    })
    .unwrap();
    let deferred = ProcessInboundOptions {
        defer_delivery: true,
        ..in_room()
    };
    let mut handles = Vec::new();
    for n in 0..20 {
        let result = kit
            .process_inbound(message(&format!("m{n}")), deferred.clone())
            .await
            .unwrap();
        handles.push(result.delivery.unwrap());
    }
    let started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap()
        .unwrap();
    let took = started.elapsed();
    let mut reasons = Vec::new();
    for handle in handles {
        let waited = tokio::time::timeout(BOUND, handle.wait())
            .await
            .unwrap()
            .unwrap();
        reasons.push((waited.delivery_results.len(), waited.cancellation_reason));
    }
    // The deliveries over by the grace report what they delivered; the one
    // cut running, its transport done or not, reports none.
    let over = reasons
        .iter()
        .take_while(|reason| reason.1.is_none())
        .count();
    let delivered = slow.delivered.lock().unwrap().len();
    let expected: Vec<(usize, Option<String>)> = (0..20)
        .map(|n| match n {
            n if n < over => (1, None),
            n if n == over => (0, Some("lane_cancelled".to_owned())),
            _ => (0, Some("kit_closed".to_owned())),
        })
        .collect();
    assert_eq!(
        (
            took < Duration::from_secs(6),
            (8..=10).contains(&over) && (over..=over + 1).contains(&delivered),
            reasons
        ),
        (true, true, expected)
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

/// What runs a message into `room_1`.
fn in_room() -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    }
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
    kit.close().await.unwrap();
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
    assert_eq!((first, second), (Ok(()), Ok(())));
    kit.close().await.unwrap();
    assert_eq!(*closed.lock().unwrap(), ["ws_a", "ws_b"]);
}

/// Decision 15: a call whose delivery ends while the close drains reports
/// its result, the close's cut read at the run's end, not when its end is
/// seen: on threads, the close in a task of its own, none of fifty such
/// calls reads as cut (tasks/probes/step5-lot1-review-A probe 13b, where
/// eighteen of two hundred did).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_whose_delivery_ends_during_the_drain_reports_its_result() {
    let mut cut = 0;
    for _ in 0..50 {
        let kit = room().await;
        let slow = Slow {
            delay: Duration::from_millis(5),
            ..Slow::default()
        };
        kit.register_channel(slow).unwrap();
        kit.attach_channel(
            &room_id(),
            &channel("slow"),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
        let call = spawn_bonjour(&kit);
        let closing = tokio::spawn({
            let kit = kit.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
                kit.close().await.unwrap();
            }
        });
        let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
        tokio::time::timeout(BOUND, closing).await.unwrap().unwrap();
        cut += usize::from(!matches!(ended, Ok(result) if result.cancellation_reason.is_none()));
    }
    assert_eq!(cut, 0);
}

/// Decision 15, plan step 2: once the grace runs out, the queued work is
/// aborted before the other observers drain: an observer of another
/// trigger that runs 30 s does not leave twenty queued deliveries of
/// 500 ms going on meanwhile; past the grace nothing more is delivered.
#[tokio::test(start_paused = true)]
async fn the_queued_work_is_aborted_before_the_observers_drain() {
    let kit = room().await;
    let slow = Slow::default();
    kit.register_channel(slow.clone()).unwrap();
    kit.attach_channel(
        &room_id(),
        &channel("slow"),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let mut spec = HookSpec::new("long", HookTrigger::BeforeBroadcast, HookExecution::Async);
    spec.timeout = Duration::from_secs(60);
    kit.hook(spec, |event, _| async move {
        if text_of(&event.content) == "m0" {
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
        Ok(HookResult::allow())
    })
    .unwrap();
    let deferred = ProcessInboundOptions {
        defer_delivery: true,
        ..in_room()
    };
    for n in 0..20 {
        let message = message(&format!("m{n}"));
        tokio::time::timeout(BOUND, kit.process_inbound(message, deferred.clone()))
            .await
            .unwrap()
            .unwrap();
    }
    let started = tokio::time::Instant::now();
    tokio::time::timeout(Duration::from_secs(120), kit.close())
        .await
        .unwrap()
        .unwrap();
    let took = started.elapsed();
    let delivered = slow.delivered.lock().unwrap().len();
    assert_eq!(
        (took >= Duration::from_secs(29), delivered <= 11),
        (true, true),
        "{delivered} delivered by the close's end, after {took:?}"
    );
}
