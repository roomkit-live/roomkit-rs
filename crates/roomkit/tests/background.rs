//! The kit's background work on the tokio driver (decision 3 of step 2b):
//! the test seam `idle` waits for the observer hooks that run after the
//! fact (RFC §9.3) and for the calls that go on detached (RFC §10.1 step
//! 18, §13.6); `close` gives them its grace, then cuts them (RFC §12.10.4),
//! as the Python implementation's `close` does.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#[path = "support/warnings.rs"]
mod warnings;

use std::future::{Future, pending};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::hook::AllowOptions;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    HookResult, InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, GetTimelineOptions,
    HookExecution, HookOptions, HookTrigger, ProcessInboundOptions, RoomKit, RoomKitError,
    RoomKitOptions,
};
use roomkit_core::ops::ChannelConfig;
use roomkit_core::rules::transcoding::text_of;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use warnings::Warnings;

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
    InboundMessage::new(channel("sms"), "+15551234567", TextContent::new(text))
}

/// A kit with an SMS transport attached to `room_1`.
async fn room() -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
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
    declaration: ChannelConfig,
    gate: Gate,
    /// Whether the first delivery started.
    started: Arc<AtomicBool>,
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
            gate: Gate::default(),
            started: Arc::default(),
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

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = &self.declaration;
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
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

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
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
    kit.hook(
        HookTrigger::AfterBroadcast,
        move |_: RoomEvent, _| {
            let (gate, observed) = (hook_gate.clone(), Arc::clone(&hook_observed));
            async move {
                gate.pass().await;
                observed.store(true, Ordering::SeqCst);
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "audit".to_owned(),
            ..Default::default()
        },
    )
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

/// RFC §9.3: what an observer returns is discarded, as Python discards an
/// async hook's: one that returns nothing logs no warning, one that fails
/// is logged.
#[tokio::test]
async fn an_observer_returning_nothing_logs_no_warning() {
    let (warnings, _capturing) = Warnings::capture();
    let kit = room().await;
    let observer = |name: &str| HookOptions {
        execution: HookExecution::Async,
        name: name.to_owned(),
        ..Default::default()
    };
    kit.hook(
        HookTrigger::AfterBroadcast,
        |_: RoomEvent, _| async {},
        observer("quiet"),
    )
    .unwrap();
    kit.hook(
        HookTrigger::AfterBroadcast,
        |_: RoomEvent, _| async { Err::<HookResult, _>("audit failed".into()) },
        observer("failing"),
    )
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
    tokio::time::timeout(BOUND, roomkit::testing::idle(&kit))
        .await
        .unwrap();
    kit.close().await.unwrap();
    assert_eq!(
        (
            warnings.containing("hook=quiet").len(),
            warnings.containing("hook=failing").len()
        ),
        (0, 1)
    );
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

/// RFC §12.10.4: a delivery in progress when the kit closes gets its
/// grace: the close waits for it, its event is delivered, its caller
/// receives its result, `kit_closed`, the call under way as the close
/// started, and `close` returns once it is over. A second `close`
/// meanwhile waits for the same shutdown.
#[tokio::test]
async fn close_gives_a_delivery_in_progress_its_grace() {
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
    // Both closes wait for the delivery in progress.
    let waiting = tokio::time::timeout(BRIEF, &mut first).await.is_err()
        && tokio::time::timeout(BRIEF, &mut second).await.is_err();
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
            waiting,
            ended.map(|result| (result.event.is_some(), result.cancellation_reason)),
            slow.delivered.lock().unwrap().clone()
        ),
        (
            true,
            Ok((true, Some("kit_closed".to_owned()))),
            vec!["Bonjour".to_owned()]
        )
    );
}

/// RFC §12.10.4: a call stuck in a SYNC hook before its commit when the
/// kit closes is cut once the grace ran out: its caller reads it cut before
/// its commit, blocked, `kit_closed`, nothing of its message stored, as
/// Python's `InboundResult(blocked=True, reason="kit_closed")`. On fefbfac
/// it failed `KitClosed`.
#[tokio::test(start_paused = true)]
async fn close_ends_a_call_stuck_before_its_commit() {
    let kit = room().await;
    kit.hook(
        HookTrigger::BeforeBroadcast,
        |_: RoomEvent, _| async {
            pending::<()>().await;
            Ok(HookResult::allow(AllowOptions::default()))
        },
        HookOptions {
            name: "stuck".to_owned(),
            ..Default::default()
        },
    )
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
    let cut = ended.map(|result| {
        let reasons = (result.reason, result.cancellation_reason);
        (result.blocked, reasons)
    });
    let kit_closed = || Some("kit_closed".to_owned());
    assert_eq!(
        (
            cut,
            stored.map(|events| events
                .iter()
                .any(|event| text_of(&event.content) == "Bonjour"))
        ),
        (Ok((true, (kit_closed(), kit_closed()))), Ok(false))
    );
}

/// RFC §12.10.4: an observer a delivery fires gets the close's grace, as
/// Python's lane's hooks do: one of 3 s finishing and one of 9 s cut at 5 s
/// (`tasks/probes/step5-lot1/observers_close.py`; Python's
/// `test_a_hook_outlasting_the_grace_is_cut`).
#[tokio::test(start_paused = true)]
async fn close_cuts_an_after_broadcast_observer_at_the_grace() {
    let kit = room().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    for (name, runs) in [("three", 3), ("nine", 9)] {
        let observed = Arc::clone(&observed);
        let spec = HookOptions {
            execution: HookExecution::Async,
            name: name.to_owned(),
            timeout: Duration::from_secs(30),
            ..Default::default()
        };
        kit.hook(
            HookTrigger::AfterBroadcast,
            move |_event: RoomEvent, _| {
                let observed = Arc::clone(&observed);
                async move {
                    tokio::time::sleep(Duration::from_secs(runs)).await;
                    observed.lock().unwrap().push(name);
                    Ok(HookResult::allow(AllowOptions::default()))
                }
            },
            spec,
        )
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

/// RFC §12.10.4: an observer no delivery fired, a `BEFORE_BROADCAST`
/// observer of the event its chain allowed, still running when the kit
/// closes gets the close's grace, then is cut: one of 1 s finishes, one of
/// an hour is cut at 5 s. On fefbfac the close waited for it up to its
/// hook's timeout, 30 s.
#[tokio::test(start_paused = true)]
async fn close_gives_an_observer_its_grace() {
    let kit = room().await;
    let observed = Arc::new(Mutex::new(Vec::new()));
    for (name, runs) in [("brief", 1), ("endless", 3600)] {
        let observed = Arc::clone(&observed);
        kit.hook(
            HookTrigger::BeforeBroadcast,
            move |_: RoomEvent, _| {
                let observed = Arc::clone(&observed);
                async move {
                    tokio::time::sleep(Duration::from_secs(runs)).await;
                    observed.lock().unwrap().push(name);
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                name: name.to_owned(),
                ..Default::default()
            },
        )
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

/// What runs a message into `room_1`.
fn in_room() -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    }
}

/// `Bonjour` into `room_1`, on a task of its own.
fn spawn_bonjour(kit: &RoomKit) -> JoinHandle<Result<roomkit::InboundResult, RoomKitError>> {
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

/// A channel that records each close it is asked for into a shared log.
struct Closing {
    declaration: ChannelConfig,
    closed: Arc<Mutex<Vec<String>>>,
}

impl Closing {
    fn new(id: &str, closed: &Arc<Mutex<Vec<String>>>) -> Self {
        Self {
            declaration: ChannelConfig::new(
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

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = &self.declaration;
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    async fn close(&self) {
        tokio::time::sleep(BRIEF).await;
        let id = self.declaration.channel_id.as_str().to_owned();
        self.closed.lock().unwrap().push(id);
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl std::future::Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
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
