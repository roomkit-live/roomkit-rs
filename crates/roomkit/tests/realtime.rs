//! Ephemeral events (RFC §8.4): the event, the in-memory backend and the
//! kit's publications and subscriptions, each test named after the Python
//! test it translates (Python's `tests/test_realtime.py`, and the scoped
//! ephemerals of `tests/test_room_org_scoping.py`).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, Metadata, RoomId};
use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, InMemoryRealtime,
    InMemoryRealtimeOptions, RealtimeBackend, RealtimeFuture,
};
use roomkit::{
    CreateRoomOptions, PublishPresenceOptions, PublishReactionOptions, PublishReadReceiptOptions,
    PublishToolCallOptions, PublishTypingOptions, RoomKit, RoomKitError, RoomKitOptions,
    SubscribeRoomOptions,
};
use serde_json::json;
use tokio::sync::{Notify, mpsc};

/// How long a test waits for an event it expects, far above its arrival.
const WAIT: Duration = Duration::from_secs(5);

fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

fn event(r#type: EphemeralEventType, user_id: &str) -> EphemeralEvent {
    EphemeralEvent::new(room("room-1"), r#type, user_id)
}

/// A callback sending each event it receives to the returned receiver.
fn recording() -> (EphemeralCallback, mpsc::UnboundedReceiver<EphemeralEvent>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let callback: EphemeralCallback = Arc::new(move |event| {
        let sender = sender.clone();
        Box::pin(async move {
            drop(sender.send(event));
        })
    });
    (callback, receiver)
}

/// The next event `receiver` gets, failing past [`WAIT`].
async fn next(receiver: &mut mpsc::UnboundedReceiver<EphemeralEvent>) -> EphemeralEvent {
    tokio::time::timeout(WAIT, receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

/// Python: `tests/test_realtime.py::TestEphemeralEvent::test_to_dict_and_from_dict`.
#[test]
fn to_dict_and_from_dict() {
    let event = EphemeralEvent {
        channel_id: Some(ChannelId::new("ch-1").unwrap()),
        data: Metadata::from_iter([("key".to_owned(), json!("value"))]),
        ..event(EphemeralEventType::TypingStart, "user-1")
    };
    let restored = EphemeralEvent::from_dict(event.to_dict()).unwrap();
    assert_eq!(restored, event);
}

/// Python: `tests/test_realtime.py::TestEphemeralEvent::test_default_values`.
#[test]
fn default_values() {
    let event = event(EphemeralEventType::PresenceOnline, "user-1");
    assert_eq!(
        (event.id.is_empty(), event.channel_id, event.data),
        (false, None, Metadata::new())
    );
}

/// Python: `tests/test_realtime.py::TestInMemoryRealtime::test_publish_subscribe`
/// and `test_multiple_subscribers`: every subscription of a channel
/// receives what is published to it.
#[tokio::test]
async fn publish_subscribe_and_multiple_subscribers() {
    let backend = InMemoryRealtime::default();
    let (first, mut first_received) = recording();
    let (second, mut second_received) = recording();
    backend.subscribe("ch", first).await;
    backend.subscribe("ch", second).await;
    let published = event(EphemeralEventType::TypingStart, "user-1");

    backend.publish("ch", published.clone()).await;

    let received = (
        next(&mut first_received).await.id,
        next(&mut second_received).await.id,
    );
    backend.close().await;
    assert_eq!(received, (published.id.clone(), published.id));
}

/// Python: `tests/test_realtime.py::TestInMemoryRealtime::test_unsubscribe_stops_delivery`
/// and `test_unsubscribe_nonexistent`.
#[tokio::test]
async fn unsubscribe_stops_delivery() {
    let backend = InMemoryRealtime::default();
    let (callback, mut received) = recording();
    let id = backend.subscribe("ch", callback).await;
    backend
        .publish("ch", event(EphemeralEventType::TypingStart, "user-1"))
        .await;
    let first = next(&mut received).await.r#type;

    let removed = backend.unsubscribe(&id).await;
    backend
        .publish("ch", event(EphemeralEventType::TypingStop, "user-1"))
        .await;
    let unknown = backend.unsubscribe("nonexistent").await;

    let after = received.try_recv().ok().map(|event| event.r#type);
    backend.close().await;
    assert_eq!(
        (first, removed, unknown, after),
        (EphemeralEventType::TypingStart, true, false, None)
    );
}

/// Python: `tests/test_realtime.py::TestInMemoryRealtime::test_room_convenience_methods`.
#[tokio::test]
async fn room_convenience_methods() {
    let backend = InMemoryRealtime::default();
    let (callback, mut received) = recording();
    backend.subscribe_to_room(&room("room-1"), callback).await;
    let receipt = EphemeralEvent {
        data: Metadata::from_iter([("event_id".to_owned(), json!("evt-123"))]),
        ..event(EphemeralEventType::ReadReceipt, "user-1")
    };

    backend.publish_to_room(&room("room-1"), receipt).await;

    let data = next(&mut received).await.data;
    backend.close().await;
    assert_eq!(data.get("event_id"), Some(&json!("evt-123")));
}

/// Python: `tests/test_realtime.py::TestInMemoryRealtime::test_queue_overflow_drops_oldest`.
/// Three events published while the callback is held, on a runtime of one
/// thread: past the bound of two, the oldest is dropped.
#[tokio::test]
async fn queue_overflow_drops_oldest() {
    let backend = InMemoryRealtime::new(InMemoryRealtimeOptions { max_queue_size: 2 });
    let gate = Arc::new(Notify::new());
    let (sender, mut received) = mpsc::unbounded_channel();
    let held = Arc::clone(&gate);
    let slow: EphemeralCallback = Arc::new(move |event: EphemeralEvent| {
        let (gate, sender) = (Arc::clone(&held), sender.clone());
        Box::pin(async move {
            gate.notified().await;
            drop(sender.send(event.user_id));
        })
    });
    backend.subscribe("ch", slow).await;

    for user in ["user-0", "user-1", "user-2"] {
        backend
            .publish("ch", event(EphemeralEventType::TypingStart, user))
            .await;
    }
    let mut users = Vec::new();
    for _ in 0..2 {
        gate.notify_one();
        users.push(
            tokio::time::timeout(WAIT, received.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }

    backend.close().await;
    assert_eq!(users, ["user-1", "user-2"]);
}

/// Python's `_Subscription`, its queue keyed by the events' ids: an event
/// published twice before its callback drains it reaches the callback once.
#[tokio::test]
async fn an_event_published_twice_before_its_callback_runs_reaches_it_once() {
    let backend = InMemoryRealtime::default();
    let (callback, mut received) = recording();
    backend.subscribe("ch", callback).await;
    let twice = event(EphemeralEventType::TypingStart, "user-1");
    let after = event(EphemeralEventType::TypingStop, "user-1");

    backend.publish("ch", twice.clone()).await;
    backend.publish("ch", twice.clone()).await;
    backend.publish("ch", after.clone()).await;

    let ids = [next(&mut received).await.id, next(&mut received).await.id];
    backend.close().await;
    assert_eq!(ids, [twice.id, after.id]);
}

/// Python: `tests/test_realtime.py::TestInMemoryRealtime::test_subscription_count`
/// and `test_close_stops_all_subscriptions`: a publish after the close
/// does nothing.
#[tokio::test]
async fn subscription_count_and_close_stops_all_subscriptions() {
    let backend = InMemoryRealtime::default();
    let mut counts = vec![backend.subscription_count()];
    let (callback, mut received) = recording();
    let first = backend.subscribe("ch1", Arc::clone(&callback)).await;
    counts.push(backend.subscription_count());
    backend.subscribe("ch2", callback).await;
    counts.push(backend.subscription_count());
    backend.unsubscribe(&first).await;
    counts.push(backend.subscription_count());

    backend.close().await;
    counts.push(backend.subscription_count());
    backend
        .publish("ch2", event(EphemeralEventType::TypingStart, "user-1"))
        .await;

    assert_eq!(
        (counts, received.try_recv().is_err()),
        (vec![0, 1, 2, 1, 0], true)
    );
}

/// Python: `tests/test_realtime.py::TestInMemoryRealtime::test_callback_error_does_not_stop_subscription`:
/// a callback that panics is logged, and the next event reaches it.
#[tokio::test]
async fn callback_error_does_not_stop_subscription() {
    let backend = InMemoryRealtime::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let (sender, mut received) = mpsc::unbounded_channel();
    let counted = Arc::clone(&calls);
    let flaky: EphemeralCallback = Arc::new(move |event: EphemeralEvent| {
        let (calls, sender) = (Arc::clone(&counted), sender.clone());
        Box::pin(async move {
            assert!(calls.fetch_add(1, Ordering::SeqCst) > 0, "Simulated error");
            sender.send(event.r#type).ok();
        })
    });
    backend.subscribe("ch", flaky).await;

    backend
        .publish("ch", event(EphemeralEventType::TypingStart, "user-1"))
        .await;
    backend
        .publish("ch", event(EphemeralEventType::TypingStop, "user-1"))
        .await;

    let second = tokio::time::timeout(WAIT, received.recv()).await.unwrap();
    backend.close().await;
    assert_eq!(
        (second, calls.load(Ordering::SeqCst)),
        (Some(EphemeralEventType::TypingStop), 2)
    );
}

/// A kit subscribed to `room-1`, and what the subscription receives.
async fn subscribed() -> (RoomKit, mpsc::UnboundedReceiver<EphemeralEvent>) {
    let kit = RoomKit::new(RoomKitOptions::default());
    let (sender, receiver) = mpsc::unbounded_channel();
    kit.subscribe_room(
        &room("room-1"),
        move |event| {
            let sender = sender.clone();
            async move {
                drop(sender.send(event));
            }
        },
        SubscribeRoomOptions::default(),
    )
    .await
    .unwrap();
    (kit, receiver)
}

/// Python: `tests/test_realtime.py::TestRoomKitRealtimeIntegration::test_publish_typing`
/// and `test_publish_typing_stop`.
#[tokio::test]
async fn publish_typing() {
    let (kit, mut received) = subscribed().await;
    kit.publish_typing(&room("room-1"), "user-1", PublishTypingOptions::default())
        .await
        .unwrap();
    let stop = PublishTypingOptions {
        is_typing: false,
        ..Default::default()
    };
    kit.publish_typing(&room("room-1"), "user-1", stop)
        .await
        .unwrap();

    let start = next(&mut received).await;
    let stopped = next(&mut received).await;
    kit.close().await.unwrap();
    assert_eq!(
        (start.r#type, start.user_id, start.room_id, stopped.r#type),
        (
            EphemeralEventType::TypingStart,
            "user-1".to_owned(),
            room("room-1"),
            EphemeralEventType::TypingStop
        )
    );
}

/// Python: `tests/test_realtime.py::TestRoomKitRealtimeIntegration::test_publish_presence`
/// and `test_publish_presence_custom_status`.
#[tokio::test]
async fn publish_presence() {
    let (kit, mut received) = subscribed().await;
    for status in ["online", "away", "offline", "busy"] {
        kit.publish_presence(
            &room("room-1"),
            "user-1",
            status,
            PublishPresenceOptions::default(),
        )
        .await
        .unwrap();
    }

    let mut types = Vec::new();
    for _ in 0..4 {
        let event = next(&mut received).await;
        types.push((event.r#type, event.data.get("status").cloned()));
    }
    kit.close().await.unwrap();
    assert_eq!(
        types,
        [
            (EphemeralEventType::PresenceOnline, None),
            (EphemeralEventType::PresenceAway, None),
            (EphemeralEventType::PresenceOffline, None),
            (EphemeralEventType::Custom, Some(json!("busy"))),
        ]
    );
}

/// Python: `tests/test_realtime.py::TestRoomKitRealtimeIntegration::test_publish_read_receipt`,
/// and a reaction (`publish_reaction`).
#[tokio::test]
async fn publish_read_receipt() {
    let (kit, mut received) = subscribed().await;
    kit.publish_read_receipt(
        &room("room-1"),
        "user-1",
        "evt-123",
        PublishReadReceiptOptions::default(),
    )
    .await
    .unwrap();
    kit.publish_reaction(
        &room("room-1"),
        "user-1",
        "evt-123",
        "+1",
        PublishReactionOptions::default(),
    )
    .await
    .unwrap();

    let receipt = next(&mut received).await;
    let reaction = next(&mut received).await;
    kit.close().await.unwrap();
    assert_eq!(
        (
            (receipt.r#type, receipt.user_id, receipt.data),
            (reaction.r#type, reaction.data)
        ),
        (
            (
                EphemeralEventType::ReadReceipt,
                "user-1".to_owned(),
                Metadata::from_iter([("event_id".to_owned(), json!("evt-123"))])
            ),
            (
                EphemeralEventType::Reaction,
                Metadata::from_iter([
                    ("target_event_id".to_owned(), json!("evt-123")),
                    ("emoji".to_owned(), json!("+1")),
                    ("action".to_owned(), json!("add")),
                ])
            )
        )
    );
}

/// Python: `tests/test_realtime.py::TestRoomKitToolCallEvents::test_publish_tool_call_start`
/// and `test_publish_tool_call_end`.
#[tokio::test]
async fn publish_tool_call() {
    let (kit, mut received) = subscribed().await;
    let ai = ChannelId::new("ai1").unwrap();
    let call = |extra: (&str, serde_json::Value)| {
        Metadata::from_iter([
            ("id".to_owned(), json!("tc1")),
            ("name".to_owned(), json!("search")),
            (extra.0.to_owned(), extra.1),
        ])
    };
    let start = vec![call(("arguments", json!({ "q": "test" })))];
    kit.publish_tool_call(
        &room("room-1"),
        &ai,
        start,
        PublishToolCallOptions::default(),
    )
    .await
    .unwrap();
    let end = PublishToolCallOptions {
        event_type: EphemeralEventType::ToolCallEnd,
        duration_ms: Some(1234),
        ..Default::default()
    };
    let ended = vec![call(("result", json!("found it")))];
    kit.publish_tool_call(&room("room-1"), &ai, ended, end)
        .await
        .unwrap();

    let started = next(&mut received).await;
    let finished = next(&mut received).await;
    kit.close().await.unwrap();
    assert_eq!(
        (
            started.r#type,
            started.channel_id,
            started
                .data
                .get("tool_calls")
                .map(|calls| calls[0]["name"].clone()),
            started.data.get("channel_id").cloned(),
            started.data.contains_key("duration_ms"),
        ),
        (
            EphemeralEventType::ToolCallStart,
            Some(ai),
            Some(json!("search")),
            Some(json!("ai1")),
            false
        )
    );
    assert_eq!(
        (
            finished.r#type,
            finished.data.get("duration_ms").cloned(),
            finished
                .data
                .get("tool_calls")
                .map(|calls| calls[0]["result"].clone()),
        ),
        (
            EphemeralEventType::ToolCallEnd,
            Some(json!(1234)),
            Some(json!("found it"))
        )
    );
}

/// Python: `tests/test_realtime.py::TestRoomKitRealtimeIntegration::test_realtime_property`,
/// `test_custom_realtime_backend` and `test_context_manager_closes_realtime`:
/// the kit publishes with the backend it was given, and its close ends the
/// backend's subscriptions.
#[tokio::test]
async fn custom_realtime_backend_closed_with_the_kit() {
    let backend = Arc::new(InMemoryRealtime::new(InMemoryRealtimeOptions {
        max_queue_size: 50,
    }));
    let given: Arc<dyn RealtimeBackend> = Arc::<InMemoryRealtime>::clone(&backend);
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::clone(&given)),
        ..Default::default()
    });
    let held = Arc::ptr_eq(&kit.realtime(), &given);
    kit.subscribe_room(
        &room("room-1"),
        |_| async {},
        SubscribeRoomOptions::default(),
    )
    .await
    .unwrap();
    let before = backend.subscription_count();

    kit.close().await.unwrap();

    assert_eq!((held, before, backend.subscription_count()), (true, 1, 0));
}

/// A backend recording the channels the kit publishes to and subscribes
/// to, read without waiting on a subscription's queue.
#[derive(Debug, Default)]
struct Recording {
    inner: InMemoryRealtime,
    reached: Mutex<Vec<String>>,
}

impl RealtimeBackend for Recording {
    fn publish<'a>(&'a self, channel: &'a str, event: EphemeralEvent) -> RealtimeFuture<'a, ()> {
        self.reached.lock().unwrap().push(channel.to_owned());
        self.inner.publish(channel, event)
    }

    fn subscribe<'a>(
        &'a self,
        channel: &'a str,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        self.reached.lock().unwrap().push(channel.to_owned());
        self.inner.subscribe(channel, callback)
    }

    fn unsubscribe<'a>(&'a self, subscription_id: &'a str) -> RealtimeFuture<'a, bool> {
        self.inner.unsubscribe(subscription_id)
    }

    fn close(&self) -> RealtimeFuture<'_, ()> {
        self.inner.close()
    }
}

/// Each of the kit's ephemeral calls on `globex-room`, for `organization`:
/// its name and what it came to.
async fn every_ephemeral(
    kit: &RoomKit,
    organization: Option<&str>,
) -> Vec<(&'static str, Result<(), RoomKitError>)> {
    let room = room("globex-room");
    let org = || organization.map(str::to_owned);
    let call = || vec![Metadata::from_iter([("id".to_owned(), json!("c1"))])];
    vec![
        (
            "publish_typing",
            kit.publish_typing(
                &room,
                "u1",
                PublishTypingOptions {
                    organization_id: org(),
                    ..Default::default()
                },
            )
            .await,
        ),
        (
            "publish_tool_call",
            kit.publish_tool_call(
                &room,
                &ChannelId::new("ai").unwrap(),
                call(),
                PublishToolCallOptions {
                    organization_id: org(),
                    ..Default::default()
                },
            )
            .await,
        ),
        (
            "publish_presence",
            kit.publish_presence(
                &room,
                "u1",
                "online",
                PublishPresenceOptions {
                    organization_id: org(),
                },
            )
            .await,
        ),
        (
            "publish_read_receipt",
            kit.publish_read_receipt(
                &room,
                "u1",
                "e1",
                PublishReadReceiptOptions {
                    organization_id: org(),
                },
            )
            .await,
        ),
        (
            "publish_reaction",
            kit.publish_reaction(
                &room,
                "u1",
                "e1",
                "+1",
                PublishReactionOptions {
                    organization_id: org(),
                    ..Default::default()
                },
            )
            .await,
        ),
        (
            "subscribe_room",
            kit.subscribe_room(
                &room,
                |_| async {},
                SubscribeRoomOptions {
                    organization_id: org(),
                },
            )
            .await
            .map(drop),
        ),
    ]
}

/// A kit on a recording backend holding `globex-room` of `globex`.
async fn tenants() -> (RoomKit, Arc<Recording>) {
    let realtime = Arc::new(Recording::default());
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::<Recording>::clone(&realtime)),
        ..Default::default()
    });
    kit.create_room(CreateRoomOptions {
        room_id: Some(room("globex-room")),
        organization_id: Some("globex".to_owned()),
        ..Default::default()
    })
    .await
    .unwrap();
    (kit, realtime)
}

/// Python: `tests/test_room_org_scoping.py::TestScopedEphemerals::test_another_organizations_room_is_not_found_and_nothing_is_published`:
/// RFC §17.2, nothing is published to, or heard from, another
/// organization's room.
#[tokio::test]
async fn another_organizations_room_is_not_found_and_nothing_is_published() {
    let (kit, realtime) = tenants().await;
    let refused: Vec<(&str, bool)> = every_ephemeral(&kit, Some("acme"))
        .await
        .into_iter()
        .map(|(name, outcome)| (name, matches!(outcome, Err(RoomKitError::RoomNotFound(_)))))
        .collect();
    let reached = realtime.reached.lock().unwrap().clone();
    kit.close().await.unwrap();
    let all: Vec<(&str, bool)> = refused.iter().map(|&(name, _)| (name, true)).collect();
    assert_eq!((refused, reached), (all, Vec::<String>::new()));
}

/// Python: `tests/test_room_org_scoping.py::TestScopedEphemerals::test_the_rooms_own_organization_or_none_reaches_the_room`.
#[tokio::test]
async fn the_rooms_own_organization_or_none_reaches_the_room() {
    for organization in [Some("globex"), None] {
        let (kit, realtime) = tenants().await;
        let outcomes = every_ephemeral(&kit, organization).await;
        let reached = realtime.reached.lock().unwrap().clone();
        kit.close().await.unwrap();
        assert!(
            outcomes.iter().all(|(_, outcome)| outcome.is_ok()),
            "{outcomes:?}"
        );
        assert_eq!(reached, vec!["room:globex-room".to_owned(); outcomes.len()]);
    }
}
