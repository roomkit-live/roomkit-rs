//! The kit's background work on the tokio driver (decision 3 of step 2b):
//! `idle` waits for the observer hooks that run after the fact (RFC §9.3)
//! and for the calls that go on detached (RFC §10.1 step 18, §13.6);
//! `shutdown` ends them, and every call made after it is refused.

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
use roomkit::{Channel, ChannelDeclaration, ChannelError, HookTrigger, KitError, RoomKit, text_of};
use tokio::sync::Notify;

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
    let kit = RoomKit::new();
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.create_room(&room_id()).await.unwrap();
    kit.attach(&room_id(), &channel("sms")).await.unwrap();
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
    kit.process_inbound_in(&room_id(), message("Bonjour"))
        .await
        .unwrap();
    tokio::time::timeout(BOUND, gate.reached.notified())
        .await
        .unwrap();
    let busy = tokio::time::timeout(BRIEF, kit.idle()).await.is_err();
    gate.open.notify_one();
    tokio::time::timeout(BOUND, kit.idle()).await.unwrap();
    assert_eq!((busy, observed.load(Ordering::SeqCst)), (true, true));
}

/// RFC §13.6, decision 1 of step 2b: a call its caller stopped waiting for
/// once it was decided goes on delivering; `idle` waits for its delivery.
#[tokio::test]
async fn idle_waits_for_a_call_that_goes_on_detached() {
    let kit = room().await;
    let slow = Gated::new();
    kit.register_channel(slow.clone()).unwrap();
    kit.attach(&room_id(), &channel("slow")).await.unwrap();
    let room = room_id();
    let call = kit.process_inbound_in(&room, message("Bonjour"));
    tokio::select! {
        result = call => panic!("the call ended before its delivery: {result:?}"),
        () = slow.gate.reached.notified() => {}
    }
    let busy = tokio::time::timeout(BRIEF, kit.idle()).await.is_err();
    slow.gate.open.notify_one();
    tokio::time::timeout(BOUND, kit.idle()).await.unwrap();
    assert_eq!(
        (busy, slow.delivered.lock().unwrap().clone()),
        (true, vec!["Bonjour".to_owned()])
    );
}

/// A call whose run the shutdown ends receives `ShutDown`; the shutdown
/// returns once it is over.
#[tokio::test]
async fn shutdown_ends_a_call_running_and_its_caller_receives_shut_down() {
    let kit = room().await;
    let gate = Gate::default();
    let hook_gate = gate.clone();
    kit.before_broadcast("stuck", move |_, _| {
        let gate = hook_gate.clone();
        async move {
            gate.pass().await;
            Ok(HookResult::allow())
        }
    })
    .unwrap();
    let call = {
        let kit = kit.clone();
        tokio::spawn(async move { kit.process_inbound_in(&room_id(), message("Bonjour")).await })
    };
    tokio::time::timeout(BOUND, gate.reached.notified())
        .await
        .unwrap();
    tokio::time::timeout(BOUND, kit.shutdown()).await.unwrap();
    let ended = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    assert_eq!(ended, Err(KitError::ShutDown));
}

/// An observer still running when the kit shuts down is dropped: the
/// shutdown does not wait for it to finish.
#[tokio::test]
async fn shutdown_drops_an_observer_hook_still_running() {
    let kit = room().await;
    let gate = Gate::default();
    let hook_gate = gate.clone();
    kit.observe("audit", HookTrigger::AfterBroadcast, move |_, _| {
        let gate = hook_gate.clone();
        async move {
            gate.reached.notify_one();
            pending::<()>().await;
        }
    })
    .unwrap();
    kit.process_inbound_in(&room_id(), message("Bonjour"))
        .await
        .unwrap();
    tokio::time::timeout(BOUND, gate.reached.notified())
        .await
        .unwrap();
    let shut = tokio::time::timeout(BOUND, kit.shutdown()).await;
    let idle = tokio::time::timeout(BOUND, kit.idle()).await;
    assert_eq!((shut.is_ok(), idle.is_ok()), (true, true));
}

/// Every call that runs an operation is refused once the kit is shut down;
/// registering a channel still works.
#[tokio::test]
async fn a_call_made_after_the_shutdown_is_refused() {
    let kit = room().await;
    kit.shutdown().await;
    let room = room_id();
    let refused = (
        kit.process_inbound_in(&room, message("Bonjour"))
            .await
            .err(),
        kit.process_inbound(message("Bonjour")).await.err(),
        kit.create_room(&RoomId::new("room_2").unwrap()).await.err(),
        kit.pause_room(&room).await.err(),
        kit.check_timers().await.err(),
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
