//! The `store.<operation>` spans: Python's `PostgresStore._query_span` and
//! `tests/test_postgres_store.py::test_query_span_on_success`,
//! `test_query_span_on_error`, on a real database. Each test skips itself without
//! `POSTGRES_DSN`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use std::sync::Arc;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventId, EventSource, Identity, IdentityId,
    InboundMessage, Metadata, Participant, ParticipantId, Room, RoomEvent, RoomId, RoomStatus,
    TextContent,
};
use roomkit::store::{
    AdvanceDeliveredIndexOptions, CommitEventOptions, DeleteEventOptions, FindRoomsOptions,
    GetEventCountOptions, ListEventsOptions, ListRoomsOptions, PatchRoomMetadataOptions,
};
use roomkit::telemetry::{
    MockTelemetryProvider, Span, SpanKind, TelemetryConfig, TelemetryProvider,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, GetRoomOptions,
    ProcessInboundOptions, RoomKit, RoomKitOptions, StoreError,
};
use roomkit_postgres::{PostgresStore, PostgresStoreOptions};
use serde_json::json;

use crate::support::{TempSchema, dsn};

fn room_id(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

fn channel() -> ChannelId {
    ChannelId::new("ch1").unwrap()
}

fn event(room: &str, body: &str) -> RoomEvent {
    RoomEvent::new(
        room_id(room),
        EventSource::new(channel(), ChannelType::SMS),
        TextContent::new(body),
    )
}

fn binding(room: &str) -> ChannelBinding {
    ChannelBinding::new(channel(), room_id(room), ChannelType::SMS)
}

/// A store reporting to a mock provider, on a schema of its own.
async fn reporting(
    sensitive: bool,
) -> Option<(TempSchema, PostgresStore, Arc<MockTelemetryProvider>)> {
    dsn()?;
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    let provider = Arc::new(MockTelemetryProvider::default());
    let handed: Arc<dyn TelemetryProvider> = Arc::<MockTelemetryProvider>::clone(&provider);
    store.set_telemetry(handed, sensitive);
    Some((schema, store, provider))
}

/// The operation and the table a span names.
fn named(span: &Span) -> (String, String) {
    let member = |key: &str| {
        span.attributes
            .get(key)
            .and_then(|value| value.as_str())
            .unwrap_or_default()
            .to_owned()
    };
    (member("store.operation"), member("store.table"))
}

/// Each operation's span, in the order Python's store lists them, each named
/// `store.<operation>`, of kind `store.query`, ended ok, its parent none: no span
/// is current here. `SPANS` is Python's list, which the next test reads again in its
/// source.
const SPANS: [(&str, &str); 37] = [
    ("create_room", "rooms"),
    ("get_room", "rooms"),
    ("update_room", "rooms"),
    ("update_room_if", "rooms"),
    ("room_exists", "rooms"),
    ("binding_exists", "bindings"),
    ("get_delivered_index", "rooms"),
    ("advance_delivered_index", "rooms"),
    ("get_private_state", "private_state"),
    ("set_private_state", "private_state"),
    ("patch_room_metadata", "rooms"),
    ("list_rooms", "rooms"),
    ("find_rooms", "rooms"),
    ("add_event", "events"),
    ("get_event", "events"),
    ("update_event", "events"),
    ("write_change_target", "events"),
    ("record_delivery_results", "events"),
    ("list_events", "events"),
    ("get_thread_summaries", "events"),
    ("get_event_by_idempotency_key", "events"),
    ("get_event_count", "events"),
    ("add_event_auto_index", "events"),
    ("commit_event", "events"),
    ("delete_event", "events"),
    ("add_binding", "bindings"),
    ("get_binding", "bindings"),
    ("update_binding", "bindings"),
    ("remove_binding", "bindings"),
    ("list_bindings", "bindings"),
    ("add_participant", "participants"),
    ("get_participant", "participants"),
    ("update_participant", "participants"),
    ("list_participants", "participants"),
    ("delete_participant", "participants"),
    ("load_room_context", "rooms"),
    ("delete_room", "rooms"),
];

/// Calls each spanned operation once, in the order of [`SPANS`].
async fn every_operation(store: &PostgresStore) {
    let room = room_id("r1");
    let created = store.create_room(Room::new(room.clone())).await.unwrap();
    store.get_room(&room).await.unwrap();
    store.update_room(created.clone()).await.unwrap();
    store
        .update_room_if(created, RoomStatus::Active, None, None)
        .await
        .unwrap();
    store.room_exists(&room).await.unwrap();
    store.binding_exists(&room, &channel()).await.unwrap();
    store.get_delivered_index(&room).await.unwrap();
    store
        .advance_delivered_index(&room, 1, AdvanceDeliveredIndexOptions::default())
        .await
        .unwrap();
    store.get_private_state(&room, "k").await.unwrap();
    store
        .set_private_state(&room, "k", Some(Metadata::new()))
        .await
        .unwrap();
    store
        .patch_room_metadata(&room, Metadata::new(), PatchRoomMetadataOptions::default())
        .await
        .unwrap();
    store.list_rooms(ListRoomsOptions::default()).await.unwrap();
    store.find_rooms(FindRoomsOptions::default()).await.unwrap();
    let mut own = event("r1", "x");
    own.index = 7;
    let added = store.add_event(own).await.unwrap();
    store.get_event(&added.id).await.unwrap();
    store.update_event(added.clone()).await.unwrap();
    store
        .write_change_target(&room, added.clone())
        .await
        .unwrap();
    store
        .record_delivery_results(&room, &added.id, Metadata::new())
        .await
        .unwrap();
    store
        .list_events(&room, ListEventsOptions::default())
        .await
        .unwrap();
    store
        .get_thread_summaries(&room, std::slice::from_ref(&added.id))
        .await
        .unwrap();
    store
        .get_event_by_idempotency_key(&room, "k", None)
        .await
        .unwrap();
    store
        .get_event_count(&room, GetEventCountOptions::default())
        .await
        .unwrap();
    store
        .add_event_auto_index(&room, event("r1", "y"))
        .await
        .unwrap();
    store
        .commit_event(&room, event("r1", "z"), CommitEventOptions::default())
        .await
        .unwrap();
    store
        .delete_event(&room, &added.id, DeleteEventOptions::default())
        .await
        .unwrap();
    let bound = store.add_binding(binding("r1")).await.unwrap();
    store.get_binding(&room, &channel()).await.unwrap();
    store.update_binding(bound).await.unwrap();
    store.remove_binding(&room, &channel()).await.unwrap();
    store.list_bindings(&room).await.unwrap();
    let id = ParticipantId::new("p1").unwrap();
    let participant = store
        .add_participant(Participant::new(id.clone(), room.clone(), channel()))
        .await
        .unwrap();
    store.get_participant(&room, &id).await.unwrap();
    store.update_participant(participant).await.unwrap();
    store.list_participants(&room).await.unwrap();
    store.delete_participant(&room, &id).await.unwrap();
    store.load_room_context(&room).await.unwrap();
    store.delete_room(&room).await.unwrap();
}

/// Python: `test_query_span_on_success`: each of Python's operations opens its span,
/// and one Python does not span opens none.
#[tokio::test]
async fn each_operation_opens_its_query_span() {
    let Some((_schema, store, provider)) = reporting(false).await else {
        return;
    };
    every_operation(&store).await;
    let room = room_id("kept");
    store.create_room(Room::new(room.clone())).await.unwrap();
    let before = provider.spans().len();
    store
        .mark_read(&room, &channel(), &added_event_id(&store, &room).await)
        .await
        .unwrap();
    store
        .create_identity(Identity::new(IdentityId::new("i1").unwrap()))
        .await
        .unwrap();
    store
        .get_identity(&IdentityId::new("i1").unwrap())
        .await
        .unwrap();
    let after = provider.spans();
    let unspanned: Vec<&str> = after
        .get(before + 1..)
        .unwrap_or_default()
        .iter()
        .map(|span| span.name.as_str())
        .collect();
    assert_eq!(unspanned, Vec::<&str>::new());
    let spans = provider.spans();
    let read: Vec<(String, String)> = spans.iter().take(SPANS.len()).map(named).collect();
    let expected: Vec<(String, String)> = SPANS
        .iter()
        .map(|(operation, table)| ((*operation).to_owned(), (*table).to_owned()))
        .collect();
    assert_eq!(read, expected);
    for span in spans.iter().take(SPANS.len()) {
        assert_eq!(span.kind, SpanKind::StoreQuery, "{span:?}");
        assert_eq!(span.name, format!("store.{}", named(span).0));
        assert_eq!(span.status, "ok", "{span:?}");
        assert_eq!(span.parent_id, None, "{span:?}");
    }
}

/// An event's id, stored in `room`.
async fn added_event_id(store: &PostgresStore, room: &RoomId) -> EventId {
    store
        .add_event_auto_index(room, event(room.as_str(), "x"))
        .await
        .unwrap()
        .id
}

/// `SPANS` is what Python's store opens, read in its source when `ROOMKIT_PYTHON`
/// names a roomkit checkout's interpreter.
#[test]
fn the_operations_are_python_s() {
    let Ok(python) = std::env::var("ROOMKIT_PYTHON") else {
        return;
    };
    let script = r#"
import ast, json, roomkit.store.postgres as store
found = []
for cls in ast.parse(open(store.__file__).read()).body:
    if isinstance(cls, ast.ClassDef) and cls.name == "PostgresStore":
        for method in cls.body:
            if isinstance(method, ast.AsyncFunctionDef):
                for node in ast.walk(method):
                    if (
                        isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr == "_query_span"
                    ):
                        found.append([method.name, *(arg.value for arg in node.args)])
print(json.dumps(sorted(found)))
"#;
    let output = std::process::Command::new(python)
        .arg("-c")
        .arg(script)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let read: Vec<(String, String, String)> = serde_json::from_slice(&output.stdout).unwrap();
    let mut ours: Vec<(String, String, String)> = SPANS
        .iter()
        .map(|(operation, table)| {
            (
                (*operation).to_owned(),
                (*operation).to_owned(),
                (*table).to_owned(),
            )
        })
        .collect();
    ours.sort();
    assert_eq!(read, ours);
}

/// Python: `test_query_span_on_error[by_default]` and `[when_asked]`, RFC §15.7: a
/// driver's error can quote a row. A span reads its class, asyncpg's, unless the
/// kit's `record_sensitive_data` asks for its text.
#[tokio::test]
async fn a_query_span_s_error_reads_its_text_only_when_asked() {
    for (sensitive, quotes) in [(false, false), (true, true)] {
        let Some((_schema, store, provider)) = reporting(sensitive).await else {
            return;
        };
        store.create_room(Room::new(room_id("r1"))).await.unwrap();
        let mut held = event("r1", "x");
        held.id = EventId::new("+15145550123").unwrap();
        held.index = 1;
        store.add_event(held.clone()).await.unwrap();
        held.index = 2;
        store.add_event(held).await.unwrap_err();
        let spans = provider.spans();
        let span = spans.last().unwrap();
        assert_eq!(
            (named(span).0.as_str(), span.status.as_str()),
            ("add_event", "error")
        );
        let message = span.error_message.clone().unwrap();
        assert_eq!(message.contains("+15145550123"), quotes, "{message}");
        if !sensitive {
            assert_eq!(message, "UniqueViolationError");
        }
    }
}

/// A refusal in the span reads as Python's class, whatever the store's text.
#[tokio::test]
async fn a_refusal_reads_as_python_s_class() {
    let Some((_schema, store, provider)) = reporting(false).await else {
        return;
    };
    store
        .update_room(Room::new(room_id("gone")))
        .await
        .unwrap_err();
    store.get_event(&EventId::new("e1").unwrap()).await.unwrap();
    store.update_event(event("gone", "x")).await.unwrap_err();
    let read: Vec<(String, Option<String>)> = provider
        .spans()
        .iter()
        .filter(|span| span.status == "error")
        .map(|span| (named(span).0, span.error_message.clone()))
        .collect();
    assert_eq!(
        read,
        [
            (
                "update_room".to_owned(),
                Some("RoomNotFoundError".to_owned())
            ),
            (
                "update_event".to_owned(),
                Some("EventNotFoundError".to_owned())
            ),
        ]
    );
}

/// Python raises these two once the span ended: it reads `ok`, and the caller its
/// refusal.
#[tokio::test]
async fn a_refusal_python_raises_after_its_span_reads_ok() {
    let Some((_schema, store, provider)) = reporting(false).await else {
        return;
    };
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    assert_eq!(
        store.create_room(Room::new(room_id("r1"))).await,
        Err(StoreError::RoomAlreadyExists(room_id("r1")))
    );
    let missing = Participant::new(ParticipantId::new("p1").unwrap(), room_id("r1"), channel());
    store.update_participant(missing).await.unwrap_err();
    for span in provider.spans() {
        assert_eq!(
            (span.status.as_str(), &span.error_message),
            ("ok", &None),
            "{span:?}"
        );
    }
}

/// What Python refuses at a model's construction or before its span opens no span:
/// an event or a binding the model refuses, two cursors, no root to read.
#[tokio::test]
async fn what_python_refuses_before_its_span_opens_none() {
    let Some((_schema, store, provider)) = reporting(false).await else {
        return;
    };
    let room = room_id("r1");
    store.create_room(Room::new(room.clone())).await.unwrap();
    let before = provider.spans().len();
    store
        .list_events(
            &room,
            ListEventsOptions {
                after_index: Some(1),
                before_index: Some(2),
                ..ListEventsOptions::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        store.get_thread_summaries(&room, &[]).await.unwrap().len(),
        0
    );
    assert_eq!(provider.spans().len(), before);
}

/// A failed wait for a connection is an error span, as Python's: `RuntimeError`
/// before `init`, its text the store's words only when asked.
#[tokio::test]
async fn a_store_before_init_reads_as_python_s_runtime_error() {
    for (sensitive, read) in [
        (false, "RuntimeError"),
        (true, "PostgresStore.init() must be called before use"),
    ] {
        let store = PostgresStore::new(PostgresStoreOptions::default());
        let provider = Arc::new(MockTelemetryProvider::default());
        store.set_telemetry(
            Arc::<MockTelemetryProvider>::clone(&provider) as Arc<dyn TelemetryProvider>,
            sensitive,
        );
        store.get_room(&room_id("r1")).await.unwrap_err();
        let spans = provider.spans();
        assert_eq!(spans[0].error_message.as_deref(), Some(read));
    }
}

/// A store two kits share reports to the last one, as Python's attribute.
#[tokio::test]
async fn a_shared_store_reports_to_the_last_kit() {
    let Some((_schema, store, first)) = reporting(false).await else {
        return;
    };
    let last = Arc::new(MockTelemetryProvider::default());
    store.set_telemetry(
        Arc::<MockTelemetryProvider>::clone(&last) as Arc<dyn TelemetryProvider>,
        false,
    );
    store.get_room(&room_id("r1")).await.unwrap();
    assert_eq!((first.spans().len(), last.spans().len()), (0, 1));
}

/// What Python's `self._store._telemetry = self._telemetry` gives: a kit on this
/// store reports its reads, outside a message and with no current span, and a
/// message's commit under the span of the message's work.
#[tokio::test]
async fn a_kit_gives_the_store_its_provider() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    let provider = Arc::new(MockTelemetryProvider::default());
    let kit = RoomKit::new(RoomKitOptions {
        telemetry: Some(TelemetryConfig {
            record_sensitive_data: true,
            ..TelemetryConfig::from(
                Arc::<MockTelemetryProvider>::clone(&provider) as Arc<dyn TelemetryProvider>
            )
        }),
        ..RoomKitOptions::with_store(store)
    });
    let sms = RecordingTransport::new(
        ChannelId::new("sms").unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    let room = room_id("r1");
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    kit.attach_channel(
        &room,
        &ChannelId::new("sms").unwrap(),
        AttachChannelOptions::default(),
    )
    .await
    .unwrap();
    let before = provider.spans().len();
    kit.get_room(&room, GetRoomOptions::default())
        .await
        .unwrap();
    let spans = provider.spans();
    let read = spans.get(before).unwrap();
    assert_eq!(read.name, "store.get_room");
    assert_eq!(read.parent_id, None);
    assert_eq!(read.attributes.get("store.table"), Some(&json!("rooms")));
    kit.process_inbound(
        InboundMessage::new(
            ChannelId::new("sms").unwrap(),
            "+15550001",
            TextContent::new("Hello"),
        ),
        ProcessInboundOptions {
            room_id: Some(room),
            ..ProcessInboundOptions::default()
        },
    )
    .await
    .unwrap();
    let spans = provider.spans();
    let commit = spans
        .iter()
        .rfind(|span| span.name == "store.commit_event")
        .unwrap();
    let parent = commit.parent_id.as_deref().unwrap();
    let under = spans.iter().find(|span| span.id == parent).unwrap();
    assert_eq!(under.name, "framework.inbound");
    kit.close().await.unwrap();
}
