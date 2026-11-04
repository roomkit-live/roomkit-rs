//! The shared schema (RMK-989): rows the Python implementation's
//! `PostgresStore` wrote are read and rewritten by [`PostgresStore`] with
//! nothing lost, and Python reads the rows this crate writes as it reads its
//! own.
//!
//! The rows are `tests/fixtures/python-rows.json`, written by
//! `tests/fixtures/make_fixture.py` (`make postgres-fixture`), at the roomkit
//! commit `tests/fixtures/ROOMKIT_COMMIT` names. Each test skips itself without
//! `POSTGRES_DSN`; the Python reader runs when `ROOMKIT_PYTHON` names a roomkit
//! checkout's interpreter.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#![expect(
    clippy::indexing_slicing,
    reason = "serde_json's index reads null for a missing member"
)]

mod support;

use std::collections::BTreeMap;
use std::path::Path;

use pretty_assertions::assert_eq;
use roomkit::model::{ChannelId, ChannelType, IdentityId, Room, RoomEvent, RoomId};
use roomkit::store::{
    AdvanceDeliveredIndexOptions, ListEventsOptions, ListRoomsOptions, ResolveIdentityOptions,
};
use roomkit::{ConversationStore, EventFilter};
use roomkit_postgres::PostgresStore;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::support::{TempSchema, dsn};

const ROOM: &str = "room-full";

/// The tables the fixture fills, children after parents, each with its key.
const TABLES: [(&str, &str); 11] = [
    ("rooms", "id"),
    ("events", "id"),
    ("event_sequences", "room_id"),
    ("bindings", "room_id, channel_id"),
    ("participants", "room_id, id"),
    ("identities", "id"),
    (
        "identity_addresses",
        "channel_type, address, organization_id",
    ),
    ("tasks", "id"),
    ("observations", "id"),
    ("read_markers", "room_id, channel_id"),
    ("private_state", "room_id, key"),
];

fn fixtures() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures"))
}

fn room_id(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

/// The fixture is the one `make postgres-fixture` wrote, at a roomkit commit.
#[test]
fn the_fixture_is_the_one_its_generator_wrote() {
    let rows = std::fs::read(fixtures().join("python-rows.json")).unwrap();
    let digest: String = Sha256::digest(&rows)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let recorded = std::fs::read_to_string(fixtures().join("FIXTURE_SHA256")).unwrap();
    assert_eq!(digest, recorded.trim());
}

/// A schema holding the rows Python wrote, and its store.
async fn holding_python_rows() -> Option<(TempSchema, PostgresStore)> {
    dsn()?;
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    let rows: Value =
        serde_json::from_slice(&std::fs::read(fixtures().join("python-rows.json")).unwrap())
            .unwrap();
    let client = schema.client().await;
    for (table, _) in TABLES {
        client
            .execute(
                &format!(
                    "INSERT INTO {table} SELECT * FROM jsonb_populate_recordset(NULL::{table}, $1)"
                ),
                &[&rows[table]],
            )
            .await
            .unwrap();
    }
    Some((schema, store))
}

/// Every row of `schema`'s tables, whole, by table and key: what a rewrite must
/// leave as it found.
async fn every_row(schema: &TempSchema) -> BTreeMap<String, Vec<Value>> {
    let client = schema.client().await;
    let mut rows = BTreeMap::new();
    for (table, key) in TABLES {
        let found = client
            .query(
                &format!("SELECT to_jsonb(t) FROM {table} t ORDER BY {key}"),
                &[],
            )
            .await
            .unwrap();
        rows.insert(
            table.to_owned(),
            found.iter().map(|row| row.get::<_, Value>(0)).collect(),
        );
    }
    rows
}

/// The filter that reads every event of a room.
fn every_event() -> ListEventsOptions {
    ListEventsOptions {
        limit: 1000,
        event_filter: Some(EventFilter {
            include_blocked: true,
            ..EventFilter::default()
        }),
        ..ListEventsOptions::default()
    }
}

/// Every identity id in the rows.
async fn identity_ids(schema: &TempSchema) -> Vec<IdentityId> {
    schema
        .client()
        .await
        .query("SELECT id FROM identities ORDER BY id", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| IdentityId::new(row.get::<_, String>(0)).unwrap())
        .collect()
}

/// Every record of every kind Python wrote reads, each as its model says.
#[tokio::test]
async fn every_record_python_wrote_reads() {
    let Some((schema, store)) = holding_python_rows().await else {
        return;
    };
    let room = store.get_room(&room_id(ROOM)).await.unwrap().unwrap();
    assert_eq!(room.organization_id.as_deref(), Some("org-1"));
    assert_eq!(room.timers.inactive_after_seconds, Some(1800));
    assert!(room.event_count > 0);
    let events = store.list_events(&room.id, every_event()).await.unwrap();
    let stored: i64 = schema
        .client()
        .await
        .query_one("SELECT count(*) FROM events", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(i64::try_from(events.len()).unwrap(), stored);
    let blocked = events
        .iter()
        .find(|event| event.id.as_str() == "evt-blocked")
        .unwrap();
    assert_eq!(blocked.blocked_by.as_deref(), Some("pii_hook"));
    let sms = store
        .get_binding(&room.id, &ChannelId::new("sms-main").unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sms.capabilities.max_length, Some(160));
    assert_eq!(sms.last_read_index, Some(2));
    let jean = store
        .get_participant(
            &room.id,
            &roomkit::model::ParticipantId::new("p-jean").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        jean.candidates.as_ref().map(Vec::len),
        Some(2),
        "the participant's candidates"
    );
    let identity = store
        .resolve_identity(
            &ChannelType::SMS,
            "+15551234567",
            ResolveIdentityOptions {
                organization_id: Some("org-1".to_owned()),
            },
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(identity.phone.as_deref(), Some("+15551234567"));
    assert_eq!(identity.external_id.as_deref(), Some("crm-42"));
    assert_eq!(identity.external_ids.len(), 2);
    let observations = store.list_observations(&room.id).await.unwrap();
    assert!(
        (observations[0].confidence.value() - 0.75).abs() < f64::EPSILON,
        "{observations:?}"
    );
    assert_eq!(identity_ids(&schema).await.len(), 2);
}

/// Each record, read and written back, leaves every row as Python wrote it,
/// what the model does not read among it.
#[tokio::test]
async fn a_rewrite_keeps_everything_python_wrote() {
    let Some((schema, store)) = holding_python_rows().await else {
        return;
    };
    let before = every_row(&schema).await;
    let rooms: Vec<Room> = store
        .list_rooms(ListRoomsOptions {
            limit: 1000,
            ..ListRoomsOptions::default()
        })
        .await
        .unwrap();
    for room in rooms {
        let id = room.id.clone();
        store.update_room(room).await.unwrap();
        for binding in store.list_bindings(&id).await.unwrap() {
            store.update_binding(binding.clone()).await.unwrap();
            store.add_binding(binding).await.unwrap();
        }
        for participant in store.list_participants(&id).await.unwrap() {
            store.update_participant(participant).await.unwrap();
        }
        for event in store.list_events(&id, every_event()).await.unwrap() {
            store.update_event(event).await.unwrap();
        }
        for task in store
            .list_tasks(&id, roomkit::store::ListTasksOptions::default())
            .await
            .unwrap()
        {
            store.update_task(task).await.unwrap();
        }
    }
    assert_eq!(every_row(&schema).await, before);
}

/// `schema`'s rows as Python's store reads them.
fn python_reads(python: &str, schema: &TempSchema) -> Value {
    let output = std::process::Command::new(python)
        .arg("-I")
        .arg(fixtures().join("read_rows.py"))
        .arg(dsn().unwrap())
        .arg(schema.name())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

/// Writes in `target` every record `source` holds, through this crate alone.
async fn written_again(source: &PostgresStore, target: &PostgresStore, ids: &[IdentityId]) {
    let rooms = source
        .list_rooms(ListRoomsOptions {
            limit: 1000,
            ..ListRoomsOptions::default()
        })
        .await
        .unwrap();
    for room in rooms {
        let (id, delivered) = (room.id.clone(), room.delivered_index);
        target.create_room(room).await.unwrap();
        if let Ok(delivered) = u64::try_from(delivered) {
            target
                .advance_delivered_index(
                    &id,
                    delivered,
                    AdvanceDeliveredIndexOptions { force: true },
                )
                .await
                .unwrap();
        }
        for binding in source.list_bindings(&id).await.unwrap() {
            target.add_binding(binding).await.unwrap();
        }
        for participant in source.list_participants(&id).await.unwrap() {
            target.add_participant(participant).await.unwrap();
        }
        let events: Vec<RoomEvent> = source.list_events(&id, every_event()).await.unwrap();
        for event in events {
            target.add_event(event).await.unwrap();
        }
        for task in source
            .list_tasks(&id, roomkit::store::ListTasksOptions::default())
            .await
            .unwrap()
        {
            target.add_task(task).await.unwrap();
        }
        for observation in source.list_observations(&id).await.unwrap() {
            target.add_observation(observation).await.unwrap();
        }
    }
    for id in ids {
        let identity = source.get_identity(id).await.unwrap().unwrap();
        target.create_identity(identity).await.unwrap();
    }
}

/// Removes from what Python read of `id-lena` the key that is no channel type's
/// name for roomkit-rs: the model never read it, so a copy through the model
/// cannot hold it (a rewrite keeps it, `a_rewrite_keeps_everything_python_wrote`).
fn without_the_unread_key(read: &mut Value) {
    for identity in read["identities"].as_array_mut().into_iter().flatten() {
        if let Some(addresses) = identity["channel_addresses"].as_object_mut() {
            addresses.shift_remove("Slack");
        }
    }
}

/// Python reads the rows this crate wrote as it reads the rows it wrote itself.
#[tokio::test]
async fn python_reads_what_this_crate_wrote_as_its_own() {
    let Ok(python) = std::env::var("ROOMKIT_PYTHON") else {
        return;
    };
    let Some((schema, store)) = holding_python_rows().await else {
        return;
    };
    let ids = identity_ids(&schema).await;
    let target_schema = TempSchema::new().await;
    let target = target_schema.initialized_store().await;
    written_again(&store, &target, &ids).await;
    let mut own = python_reads(&python, &schema);
    let mut written = python_reads(&python, &target_schema);
    without_the_unread_key(&mut own);
    without_the_unread_key(&mut written);
    assert_eq!(written, own);
}
