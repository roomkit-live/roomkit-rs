//! A call that defers its delivery (RFC §10.1 step 18, detached
//! completion), the Python implementation's `defer_delivery`: it returns at
//! its commit, its blocking decisions made, and its result's delivery
//! handle waits for the rest.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, EventStatus, EventType, HookResult, InboundMessage, RoomContext, RoomEvent,
    RoomId,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, Channel, ChannelDeclaration, ChannelError, CreateRoomOptions,
    HookExecution, HookSpec, HookTrigger, ProcessInboundOptions, RoomKit, RoomKitOptions,
};
use tokio::sync::Notify;

/// How long a wait that should end may take before the test fails.
const BOUND: Duration = Duration::from_secs(5);

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// A transport whose delivery waits until the test releases it, telling it
/// when it started.
#[derive(Clone)]
struct Held {
    delivering: Arc<Notify>,
    release: Arc<Notify>,
}

impl Channel for Held {
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration::new(
            channel("held"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        )
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let (delivering, release) = (Arc::clone(&self.delivering), Arc::clone(&self.release));
        async move {
            delivering.notify_one();
            release.notified().await;
            Ok(ChannelOutput::default())
        }
    }
}

/// A room binding an SMS and the held transport.
async fn room() -> (RoomKit, RecordingTransport, Held) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let sms = RecordingTransport::new(channel("sms"), ChannelType::SMS);
    let held = Held {
        delivering: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    kit.register_channel(sms.clone()).unwrap();
    kit.register_channel(held.clone()).unwrap();
    let options = CreateRoomOptions {
        room_id: Some(room_id()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(options).await.unwrap();
    for id in [sms.id().clone(), channel("held")] {
        kit.attach_channel(&room_id(), &id, AttachChannelOptions::default())
            .await
            .unwrap();
    }
    (kit, sms, held)
}

fn deferred() -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room_id()),
        defer_delivery: true,
        ..ProcessInboundOptions::default()
    }
}

fn hello(sms: &RecordingTransport) -> InboundMessage {
    InboundMessage::new(
        sms.id().clone(),
        "+15551234567",
        EventContent::text("Hello"),
    )
}

/// The call returns at its commit while the held transport still delivers;
/// its handle's `wait` then reports the delivery, as a call that waited
/// would have, the handle kept on the result; another wait, from a clone,
/// reads the same end, as Python's handle does.
#[tokio::test]
async fn deferred_delivery_returns_at_the_commit() {
    let (kit, sms, held) = room().await;
    let call = kit.process_inbound(hello(&sms), deferred());
    let result = tokio::time::timeout(BOUND, call).await.unwrap().unwrap();
    let at_commit = (
        result.event.as_ref().map(|event| event.status),
        result.delivery_results.len(),
        result.delivery.as_ref().map(roomkit::DeliveryHandle::done),
    );
    tokio::time::timeout(BOUND, held.delivering.notified())
        .await
        .unwrap();
    held.release.notify_one();
    let delivery = result.delivery.clone().unwrap();
    let waited = tokio::time::timeout(BOUND, delivery.wait())
        .await
        .unwrap()
        .unwrap();
    let again = tokio::time::timeout(BOUND, result.delivery.unwrap().wait())
        .await
        .unwrap()
        .unwrap();
    let delivered: Vec<&str> = waited
        .delivery_results
        .keys()
        .map(ChannelId::as_str)
        .collect();
    assert_eq!(
        (
            at_commit,
            delivered,
            waited.delivery.as_ref() == Some(&delivery),
            again == waited
        ),
        (
            (Some(EventStatus::Delivered), 0, Some(false)),
            // The source's own channel is not delivered to.
            vec!["held"],
            true,
            true
        )
    );
}

/// RFC §10.1 step 18: the blocking decisions are made before a deferred
/// call returns: a hook's refusal is its result, its handle done.
#[tokio::test]
async fn a_deferred_call_s_refusal_is_returned_at_once() {
    let (kit, sms, _held) = room().await;
    kit.before_broadcast("refuse", |_, _| async { Ok(HookResult::block("spam")) })
        .unwrap();
    let result = kit.process_inbound(hello(&sms), deferred()).await.unwrap();
    let delivery = result.delivery.clone().unwrap();
    let done = delivery.done();
    let waited = tokio::time::timeout(BOUND, delivery.wait())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            result.blocked,
            result.reason.as_deref(),
            done,
            waited.blocked
        ),
        (true, Some("spam"), true, true)
    );
}

/// RFC §10.1 step 18: "a call refused before the locked region reports the
/// refusal alone": an instruction that cannot be one, and a call past its
/// `process_timeout` before the room's lock, get no handle, as Python's
/// get none (tasks/probes/step5-lot1/fixpass.out P6, review-A probe 4e).
#[tokio::test(start_paused = true)]
async fn a_deferred_call_refused_before_the_lock_has_no_handle() {
    let (kit, sms, _held) = room().await;
    let mut unaddressed = hello(&sms);
    unaddressed.event_type = EventType::Instruction;
    let instruction = kit.process_inbound(unaddressed, deferred()).await.unwrap();
    let mut off_lock = HookSpec::new("slow", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    off_lock.needs_lock = false;
    let options = RoomKitOptions {
        process_timeout: Duration::from_millis(100),
        ..RoomKitOptions::default()
    };
    let timed = RoomKit::new(options);
    timed
        .hook(off_lock, |_, _| async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(HookResult::allow())
        })
        .unwrap();
    let sms_2 = RecordingTransport::new(channel("sms"), ChannelType::SMS);
    timed.register_channel(sms_2.clone()).unwrap();
    let options = CreateRoomOptions {
        room_id: Some(room_id()),
        ..CreateRoomOptions::default()
    };
    timed.create_room(options).await.unwrap();
    timed
        .attach_channel(&room_id(), sms_2.id(), AttachChannelOptions::default())
        .await
        .unwrap();
    let late = timed
        .process_inbound(hello(&sms_2), deferred())
        .await
        .unwrap();
    let refused =
        |result: &roomkit::InboundResult| (result.reason.clone(), result.delivery.is_none());
    assert_eq!(
        (refused(&instruction), refused(&late)),
        (
            (Some("instruction_unaddressed".to_owned()), true),
            (Some("process_timeout".to_owned()), true)
        )
    );
}

/// A call that does not defer has no delivery handle: it returned once its
/// delivery set ran.
#[tokio::test]
async fn a_call_that_waits_has_no_delivery_handle() {
    let (kit, sms, held) = room().await;
    let release = Arc::clone(&held.release);
    let releasing = tokio::spawn(async move {
        held.delivering.notified().await;
        release.notify_one();
    });
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..ProcessInboundOptions::default()
    };
    let result = kit.process_inbound(hello(&sms), options).await.unwrap();
    releasing.await.unwrap();
    assert_eq!(
        (result.delivery.is_none(), result.delivery_results.len()),
        (true, 1)
    );
}

/// A deferred delivery the kit's close cuts past its grace reports it as
/// Python's handle does: the result, the event committed, its delivery
/// `lane_cancelled` (tasks/probes/step5-lot1-review-A probe 3).
#[tokio::test(start_paused = true)]
async fn a_deferred_delivery_the_close_cuts_reports_it() {
    let (kit, sms, held) = room().await;
    let result = kit.process_inbound(hello(&sms), deferred()).await.unwrap();
    tokio::time::timeout(BOUND, held.delivering.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(60), kit.close())
        .await
        .unwrap();
    let delivery = result.delivery.unwrap();
    let waited = tokio::time::timeout(BOUND, delivery.wait()).await.unwrap();
    assert_eq!(
        waited.map(|result| (
            result.event.map(|event| event.status),
            result.delivery_results.len(),
            result.cancellation_reason
        )),
        Ok((
            Some(EventStatus::Delivered),
            0,
            Some("lane_cancelled".to_owned())
        ))
    );
}

/// A deferred delivery that ran to its end before the kit's close reports
/// what it delivered, whenever its wait reads it, after the close
/// included, as Python's handle does (tasks/probes/step5-lot1-review-A
/// probe 6, review-B `probe_wait_after_close`).
#[tokio::test]
async fn a_delivery_over_before_the_close_reports_it_after() {
    let (kit, sms, held) = room().await;
    let result = kit.process_inbound(hello(&sms), deferred()).await.unwrap();
    tokio::time::timeout(BOUND, held.delivering.notified())
        .await
        .unwrap();
    held.release.notify_one();
    let delivery = result.delivery.unwrap();
    tokio::time::timeout(BOUND, async {
        while !delivery.done() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(BOUND, kit.close()).await.unwrap();
    let waited = tokio::time::timeout(BOUND, delivery.wait()).await.unwrap();
    assert_eq!(
        waited.map(|result| (result.delivery_results.len(), result.cancellation_reason)),
        Ok((1, None))
    );
}
