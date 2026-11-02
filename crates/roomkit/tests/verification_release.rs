//! A held message's delivery named by its key until its release is
//! answered (RFC §10.1 "Explicit delivery cancellation", §11.7.6): what
//! the shared scenarios (roomkit-specs `conformance/scenarios/
//! verification/`) cannot say, two processes on one store and a call's
//! completion handle, each test translating the Python implementation's
//! test of its name.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#![expect(
    clippy::expect_used,
    reason = "a test fails on a condition not met in time"
)]

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use roomkit::identity::{MockIdentityResolver, MockIdentityResolverOptions};
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, EventId, Identity, IdentityId,
    InboundMessage, RoomContext, RoomEvent, RoomId, TextContent, VerificationPolicy,
};
use roomkit::store::ListEventsOptions;
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{
    AttachChannelOptions, CancelDeliveryOptions, CancelOptions, Channel, ChannelError,
    CompleteVerificationOptions, ConversationStore, CreateRoomOptions, InMemoryStore,
    InboundResult, ProcessInboundOptions, RequestVerificationOptions, RoomKit, RoomKitOptions,
};
use serde_json::Value;
use tokio::sync::watch;

const ALICE: &str = "+15550000001";
const KEY: &str = "task-42";
const HELD_KEY: &str = "roomkit:verification_held";
const BOUND: Duration = Duration::from_secs(2);

fn alice() -> IdentityId {
    IdentityId::new("id-alice").unwrap()
}

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room() -> RoomId {
    RoomId::new("room-1").unwrap()
}

/// A kit over `store` verifying its senders, Alice's number resolving to
/// `id-alice`, its room binding the SMS channel and an agent answering
/// `answer` (Python's `_kit`).
async fn kit_on(store: Arc<InMemoryStore>) -> RoomKit {
    let resolver = MockIdentityResolver::new(MockIdentityResolverOptions {
        mapping: BTreeMap::from([(ALICE.to_owned(), Identity::new(alice()))]),
        ..MockIdentityResolverOptions::default()
    });
    let kit = RoomKit::new(RoomKitOptions {
        store,
        identity_resolver: Some(Arc::new(resolver)),
        verification_policy: Some(VerificationPolicy::default()),
        ..RoomKitOptions::default()
    });
    let sms = RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let agent = EchoAgent::new(channel("ai"), "answer", EchoAgentOptions::default());
    kit.register_channel(agent).unwrap();
    if kit.store().get_room(&room()).await.unwrap().is_none() {
        let create = CreateRoomOptions {
            room_id: Some(room()),
            ..CreateRoomOptions::default()
        };
        kit.create_room(create).await.unwrap();
        for id in ["sms", "ai"] {
            kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
    }
    kit
}

/// Alice's message `body`, its delivery named `key`.
fn message(body: &str, key: Option<&str>) -> InboundMessage {
    let mut message = InboundMessage::new(channel("sms"), ALICE, TextContent::new(body));
    message.delivery_key = key.map(str::to_owned);
    message
}

async fn send(kit: &RoomKit, message: InboundMessage, defer_delivery: bool) -> InboundResult {
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        defer_delivery,
        ..ProcessInboundOptions::default()
    };
    kit.process_inbound(message, options).await.unwrap()
}

/// Alice verifies through the integrator's page, the request opened from
/// `first`'s time on (Python's `_verify`).
async fn verify(kit: &RoomKit, first: &RoomEvent) {
    let options = RequestVerificationOptions {
        window_start: Some(first.created_at),
        ..RequestVerificationOptions::default()
    };
    let (request, secret) = kit
        .request_verification(&alice(), &room(), &channel("sms"), options)
        .await
        .unwrap();
    kit.complete_verification(&request.id, &secret, CompleteVerificationOptions::default())
        .await
        .unwrap();
}

/// The agent's answers in the room.
async fn answers(kit: &RoomKit) -> usize {
    let options = ListEventsOptions {
        limit: 50,
        ..ListEventsOptions::default()
    };
    let events = kit.store().list_events(&room(), options).await.unwrap();
    events
        .iter()
        .filter(|event| event.source.channel_id == channel("ai"))
        .count()
}

/// Until the held event's mark records its delivery's cancellation: written
/// off the canceller, it is what another process's release reads.
async fn cancellation_recorded(kit: &RoomKit, event_id: &EventId) {
    let recorded = async {
        loop {
            let event = kit.store().get_event(event_id).await.unwrap().unwrap();
            let mark = event.metadata.get(HELD_KEY).and_then(Value::as_object);
            if mark.is_some_and(|mark| mark.get("cancelled").is_some()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::time::timeout(BOUND, recorded)
        .await
        .expect("the cancellation was never recorded on the held event");
}

/// A handle's cancel for `user_stopped`, as Python's tests give it.
fn user_stopped() -> CancelOptions {
    CancelOptions {
        reason: "user_stopped".to_owned(),
        ..CancelOptions::default()
    }
}

fn stopped() -> CancelDeliveryOptions {
    CancelDeliveryOptions {
        reason: "user_stopped".to_owned(),
        ..CancelDeliveryOptions::default()
    }
}

async fn release_in_another_process(cancel: bool) {
    let store = Arc::new(InMemoryStore::new());
    let first = kit_on(Arc::clone(&store)).await;
    let second = kit_on(store).await;
    let held = send(&first, message("Wire 500 to Bob", Some(KEY)), false).await;
    let event = held.event.unwrap();
    if cancel {
        assert_eq!(first.cancel_delivery(KEY, stopped()).await, 1);
        cancellation_recorded(&first, &event.id).await;
    }

    verify(&second, &event).await;

    assert_eq!(answers(&second).await, usize::from(!cancel));
    first.close().await.unwrap();
    second.close().await.unwrap();
}

// Python: tests/test_verification_release_cancel.py::test_a_release_in_another_process_reads_the_cancellation[memory-True]
#[tokio::test]
async fn a_release_in_another_process_reads_the_cancellation() {
    release_in_another_process(true).await;
}

// Python: tests/test_verification_release_cancel.py::test_a_release_in_another_process_reads_the_cancellation[memory-False]
#[tokio::test]
async fn a_release_in_another_process_answers_what_nobody_cancelled() {
    release_in_another_process(false).await;
}

// Python: tests/test_verification_release_cancel.py::test_a_deferred_held_message_cancelled_by_key_is_released_with_no_answer
#[tokio::test]
async fn a_deferred_held_message_cancelled_by_key_is_released_with_no_answer() {
    let kit = kit_on(Arc::new(InMemoryStore::new())).await;
    let held = send(&kit, message("Wire 500 to Bob", Some(KEY)), true).await;
    held.delivery.as_ref().unwrap().wait().await.unwrap();

    assert_eq!(kit.cancel_delivery(KEY, stopped()).await, 1);
    verify(&kit, held.event.as_ref().unwrap()).await;

    assert_eq!(answers(&kit).await, 0);
    assert_eq!(kit.cancel_delivery(KEY, stopped()).await, 0);
    kit.close().await.unwrap();
}

/// A WebSocket transport whose delivery of Alice's message waits until `go`
/// opens: the message's delivery set runs meanwhile (Python's slow
/// `AFTER_BROADCAST` hook, which holds Python's set).
struct Holding {
    go: watch::Receiver<bool>,
    inner: RecordingTransport,
}

impl Channel for Holding {
    fn channel_id(&self) -> ChannelId {
        channel("ws")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        self.inner.handle_inbound(message, context)
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let mut go = self.go.clone();
        let delivered = self.inner.deliver(event, binding, context);
        async move {
            drop(go.wait_for(|open| *open).await);
            delivered.await
        }
    }
}

/// A kit whose WebSocket transport holds Alice's message's delivery set
/// until `go` opens.
async fn held_delivery(go: watch::Receiver<bool>) -> RoomKit {
    let kit = kit_on(Arc::new(InMemoryStore::new())).await;
    let inner = RecordingTransport::new(
        channel("ws"),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(Holding { go, inner }).unwrap();
    kit.attach_channel(&room(), &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    kit
}

// Python: tests/test_verification_release_cancel.py::test_the_handle_reaches_a_held_message_while_its_delivery_set_runs[while its delivery set runs]
#[tokio::test]
async fn the_handle_reaches_a_held_message_while_its_delivery_set_runs() {
    let (go, wait) = watch::channel(false);
    let kit = held_delivery(wait).await;
    let held = send(&kit, message("Wire 500 to Bob", None), true).await;
    let delivery = held.delivery.as_ref().unwrap();

    delivery.cancel(user_stopped()).await.unwrap();
    go.send_replace(true);
    delivery.wait().await.unwrap();
    cancellation_recorded(&kit, &held.event.as_ref().unwrap().id).await;
    verify(&kit, held.event.as_ref().unwrap()).await;

    assert_eq!(answers(&kit).await, 0);
    kit.close().await.unwrap();
}

// Python: tests/test_verification_release_cancel.py::test_the_handle_reaches_a_held_message_while_its_delivery_set_runs[once it ran]
#[tokio::test]
async fn the_handle_does_not_reach_a_held_message_once_its_delivery_set_ran() {
    let (go, wait) = watch::channel(true);
    let kit = held_delivery(wait).await;
    let held = send(&kit, message("Wire 500 to Bob", None), true).await;
    let delivery = held.delivery.as_ref().unwrap();
    delivery.wait().await.unwrap();

    delivery.cancel(user_stopped()).await.unwrap();
    drop(go);
    verify(&kit, held.event.as_ref().unwrap()).await;

    assert_eq!(answers(&kit).await, 1);
    kit.close().await.unwrap();
}
