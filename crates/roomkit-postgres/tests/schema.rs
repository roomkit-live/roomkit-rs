//! `init` and the explicit maintenance on a real database, Python's
//! `tests/test_store_postgres.py` and `tests/test_postgres_store.py`'s init
//! and migrate groups, and `tests/test_postgres_multiprocess.py`'s dedupe
//! group. Each test skips itself without `POSTGRES_DSN`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use std::collections::BTreeMap;
use std::error::Error;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelType, EventSource, Identity, IdentityId, RateLimit,
    RetryPolicy, Room, RoomEvent, RoomId, TextContent, Visibility,
};
use roomkit::{ConversationStore, StoreError};
use roomkit_postgres::{
    DedupeEventIndicesOptions, DropLegacyParentIndexOptions, InitOptions, MigrateOptions,
    PostgresSchemaError, PostgresStore, PostgresStoreOptions,
};
use serde_json::{Value, json};
use tokio_postgres::NoTls;

use crate::support::{SMALL_POOL, TempSchema, dsn};

fn room_id(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

fn event(room: &str, body: &str) -> RoomEvent {
    RoomEvent::new(
        room_id(room),
        EventSource::new(ChannelId::new("ch1").unwrap(), ChannelType::SMS),
        TextContent::new(body),
    )
}

fn binding(room: &str) -> ChannelBinding {
    ChannelBinding::new(
        ChannelId::new("sms").unwrap(),
        room_id(room),
        ChannelType::SMS,
    )
}

/// A schema and its store, or `None` without `POSTGRES_DSN`.
async fn fresh() -> Option<(TempSchema, PostgresStore)> {
    dsn()?;
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    Some((schema, store))
}

/// The cause a [`StoreError::Unavailable`] wraps, as text.
fn schema_error(error: &StoreError) -> Option<&PostgresSchemaError> {
    let mut cause = error.source();
    while let Some(current) = cause {
        if let Some(found) = current.downcast_ref::<PostgresSchemaError>() {
            return Some(found);
        }
        cause = current.source();
    }
    None
}

/// Python: `test_an_empty_room_stores_no_latest_index`.
#[tokio::test]
async fn an_empty_room_stores_no_latest_index() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    let client = schema.client().await;
    let row = client
        .query_one("SELECT latest_index FROM rooms WHERE id = 'r1'", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<i32>>(0), None);
}

/// Python: `test_init_clears_the_zero_an_empty_room_was_stored_with`.
#[tokio::test]
async fn init_clears_the_zero_an_empty_room_was_stored_with() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let client = schema.client().await;
    client
        .batch_execute(
            "INSERT INTO rooms (id, event_count, latest_index) VALUES ('old', 0, 0);
             ALTER TABLE rooms ALTER COLUMN latest_index SET NOT NULL;
             ALTER TABLE rooms ALTER COLUMN latest_index SET DEFAULT 0;",
        )
        .await
        .unwrap();
    store.init(SMALL_POOL).await.unwrap();
    let row = client
        .query_one("SELECT latest_index FROM rooms WHERE id = 'old'", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<i32>>(0), None);
}

/// Python: `test_init_stamps_the_visibility_of_events_stored_before`.
#[tokio::test]
async fn init_stamps_the_visibility_of_events_stored_before() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    let mut narrowed = ChannelBinding::new(
        ChannelId::new("ch1").unwrap(),
        room_id("r1"),
        ChannelType::SMS,
    );
    narrowed.visibility = Visibility::Internal;
    store.add_binding(narrowed).await.unwrap();
    let mut under = event("r1", "under the binding");
    under.visibility = Visibility::Internal;
    let mut own = event("r1", "its own scope");
    own.visibility = Visibility::Transport;
    own.index = 1;
    let mut held = event("r1", "held");
    held.visibility = Visibility::Transport;
    held.index = 2;
    held.metadata.insert(
        "roomkit:verification_held".to_owned(),
        json!({ "visibility": "all", "identity_id": "i1" }),
    );
    for event in [under, own, held] {
        store.add_event(event).await.unwrap();
    }
    let client = schema.client().await;
    client
        .batch_execute(
            "UPDATE events SET visibility = 'all' WHERE visibility = 'internal';
             DELETE FROM schema_version WHERE version >= 3;",
        )
        .await
        .unwrap();
    store.init(SMALL_POOL).await.unwrap();
    let rows = client
        .query(
            "SELECT visibility, metadata -> 'roomkit:verification_held' ->> 'visibility' \
             FROM events ORDER BY index",
            &[],
        )
        .await
        .unwrap();
    let read: Vec<(String, Option<String>)> =
        rows.iter().map(|row| (row.get(0), row.get(1))).collect();
    assert_eq!(
        read,
        [
            ("internal".to_owned(), None),
            ("transport".to_owned(), None),
            ("transport".to_owned(), Some("internal".to_owned())),
        ]
    );
}

/// Python: `test_binding_policy_migration_preserves_old_rows`.
#[tokio::test]
async fn binding_policy_migration_preserves_old_rows() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    let mut old = binding("r1");
    old.attached_at = "2026-10-22T14:00:00.250000Z".parse().unwrap();
    store.add_binding(old.clone()).await.unwrap();
    schema
        .client()
        .await
        .batch_execute("ALTER TABLE bindings DROP COLUMN rate_limit, DROP COLUMN retry_policy")
        .await
        .unwrap();
    store.init(SMALL_POOL).await.unwrap();
    store.init(SMALL_POOL).await.unwrap();
    assert_eq!(
        store
            .get_binding(&room_id("r1"), &old.channel_id)
            .await
            .unwrap(),
        Some(old.clone())
    );
    let mut changed = old;
    changed.rate_limit = Some(RateLimit {
        max_per_minute: Some(30.0),
        ..RateLimit::default()
    });
    changed.retry_policy = Some(RetryPolicy {
        max_retries: 7,
        ..RetryPolicy::default()
    });
    store.update_binding(changed.clone()).await.unwrap();
    assert_eq!(
        store
            .get_binding(&room_id("r1"), &changed.channel_id)
            .await
            .unwrap(),
        Some(changed)
    );
}

/// Each index of the tables the store owns, its name to its definition.
async fn indexes(client: &tokio_postgres::Client) -> BTreeMap<String, String> {
    client
        .query(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema()",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect()
}

/// Python: `test_init_creates_each_index_it_lacks_and_leaves_the_others_be`.
#[tokio::test]
async fn init_creates_each_index_it_lacks_and_leaves_the_others_be() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let client = schema.client().await;
    let before = indexes(&client).await;
    for name in before.keys().filter(|name| name.starts_with("idx_")) {
        client
            .batch_execute(&format!("DROP INDEX \"{name}\""))
            .await
            .unwrap();
    }
    store.init(SMALL_POOL).await.unwrap();
    store.init(SMALL_POOL).await.unwrap();
    let after = indexes(&client).await;
    assert_eq!(after, before);
    assert!(after["idx_events_parent_index"].contains("(parent_event_id, index) WHERE"));
}

/// Python: `test_the_legacy_parent_index_is_dropped_on_request_only`.
#[tokio::test]
async fn the_legacy_parent_index_is_dropped_on_request_only() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let client = schema.client().await;
    client
        .batch_execute("CREATE INDEX idx_events_parent ON events(parent_event_id)")
        .await
        .unwrap();
    store.init(SMALL_POOL).await.unwrap();
    let action = |report: serde_json::Map<String, Value>| report["action"].clone();
    let dry = store
        .drop_legacy_parent_index(DropLegacyParentIndexOptions::default())
        .await
        .unwrap();
    assert!(indexes(&client).await.contains_key("idx_events_parent"));
    let run = DropLegacyParentIndexOptions { dry_run: false };
    let dropped = store.drop_legacy_parent_index(run).await.unwrap();
    let again = store.drop_legacy_parent_index(run).await.unwrap();
    assert!(!indexes(&client).await.contains_key("idx_events_parent"));
    assert_eq!(
        (action(dry), action(dropped), action(again)),
        (json!("dry_run"), json!("dropped"), json!("noop"))
    );
}

/// Python: `test_init_seeds_the_index_marks_of_a_store_from_before_them`.
#[tokio::test]
async fn init_seeds_the_index_marks_of_a_store_from_before_them() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    store.create_room(Room::new(room_id("r2"))).await.unwrap();
    for number in 0..3 {
        store
            .commit_event(
                &room_id("r1"),
                event("r1", &format!("{number}")),
                roomkit::store::CommitEventOptions::default(),
            )
            .await
            .unwrap();
    }
    let client = schema.client().await;
    client
        .batch_execute("DROP TABLE event_sequences")
        .await
        .unwrap();
    store.init(SMALL_POOL).await.unwrap();
    let committed = store
        .commit_event(
            &room_id("r1"),
            event("r1", "next"),
            roomkit::store::CommitEventOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(committed.index, 3);
    let seeded: BTreeMap<String, i32> = client
        .query("SELECT room_id, next_index FROM event_sequences", &[])
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        seeded,
        BTreeMap::from([("r1".to_owned(), 4), ("r2".to_owned(), 0)])
    );
}

/// Python: `test_a_store_from_before_group_bindings_upgrades_its_rows`.
#[tokio::test]
async fn a_store_from_before_group_bindings_upgrades_its_rows() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    let mut old = binding("r1");
    old.attached_at = "2026-10-22T14:00:00.250000Z".parse().unwrap();
    store.add_binding(old.clone()).await.unwrap();
    schema
        .client()
        .await
        .batch_execute("ALTER TABLE bindings DROP COLUMN is_group")
        .await
        .unwrap();
    store.init(SMALL_POOL).await.unwrap();
    store.init(SMALL_POOL).await.unwrap();
    assert_eq!(
        store
            .get_binding(&room_id("r1"), &old.channel_id)
            .await
            .unwrap(),
        Some(old.clone())
    );
    let mut grouped = old;
    grouped.group = true;
    store.update_binding(grouped.clone()).await.unwrap();
    assert_eq!(
        store
            .get_binding(&room_id("r1"), &grouped.channel_id)
            .await
            .unwrap(),
        Some(grouped)
    );
}

/// Python: `test_init_adds_the_column_to_an_events_table_without_it`.
#[tokio::test]
async fn init_adds_the_column_to_an_events_table_without_it() {
    for column in ["addressed_to", "responds_to", "delivery_results"] {
        let Some((schema, store)) = fresh().await else {
            return;
        };
        store.create_room(Room::new(room_id("r1"))).await.unwrap();
        let old = store.add_event(event("r1", "avant")).await.unwrap();
        schema
            .client()
            .await
            .batch_execute(&format!(
                "DROP INDEX IF EXISTS idx_events_room_responds_to;
                 ALTER TABLE events DROP COLUMN {column};"
            ))
            .await
            .unwrap();
        let upgraded = schema.initialized_store().await;
        let kept = upgraded.get_event(&old.id).await.unwrap().unwrap();
        assert_eq!(kept.addressed_to, None, "{column}");
        assert_eq!(kept.responds_to, None, "{column}");
        assert!(kept.delivery_results.is_empty(), "{column}");
    }
}

/// Python: `test_init_records_when_a_room_stored_paused_before_paused_at_paused`.
#[tokio::test]
async fn init_records_when_a_room_stored_paused_before_paused_at_paused() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let mut paused = Room::new(room_id("r1"));
    paused.status = roomkit::model::RoomStatus::Paused;
    store.create_room(paused.clone()).await.unwrap();
    store.create_room(Room::new(room_id("r2"))).await.unwrap();
    schema
        .client()
        .await
        .batch_execute("ALTER TABLE rooms DROP COLUMN paused_at")
        .await
        .unwrap();
    let upgraded = schema.initialized_store().await;
    let was_paused = upgraded.get_room(&room_id("r1")).await.unwrap().unwrap();
    let active = upgraded.get_room(&room_id("r2")).await.unwrap().unwrap();
    assert_eq!(was_paused.paused_at, Some(was_paused.updated_at));
    assert_eq!(active.paused_at, None);
}

/// Python: `test_init_moves_the_author_registers_to_the_private_state`.
#[tokio::test]
async fn init_moves_the_author_registers_to_the_private_state() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let register = json!({ "version": 3, "names": { "aaaa": "bbbb:1" } });
    let later = json!({ "version": 3, "names": { "aaaa": "bbbb:1 cccc:2" } });
    for (id, value) in [
        ("r1", register.clone()),
        ("r2", json!("junk")),
        ("r3", later.clone()),
    ] {
        let mut room = Room::new(room_id(id));
        room.metadata.insert("author_register".to_owned(), value);
        store.create_room(room).await.unwrap();
    }
    let client = schema.client().await;
    client
        .batch_execute("DELETE FROM schema_version WHERE version >= 4")
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO private_state (room_id, key, value) \
             VALUES ('r3', 'author_register', $1::jsonb)",
            &[&register],
        )
        .await
        .unwrap();
    let upgraded = schema.initialized_store().await;
    let state = async |id: &str| {
        upgraded
            .get_private_state(&room_id(id), "author_register")
            .await
            .unwrap()
            .map(Value::Object)
    };
    let metadata = async |id: &str| {
        Value::Object(
            upgraded
                .get_room(&room_id(id))
                .await
                .unwrap()
                .unwrap()
                .metadata,
        )
    };
    assert_eq!(state("r1").await, Some(register.clone()));
    assert_eq!(metadata("r1").await, json!({}));
    assert_eq!(state("r2").await, None);
    assert_eq!(metadata("r2").await, json!({}));
    assert_eq!(state("r3").await, Some(register));
    assert_eq!(metadata("r3").await, json!({ "author_register": later }));
}

/// Python: `test_init_moves_an_unscoped_address_to_its_identity_s_organization`
/// (HITL 0f96a597, RFC §17.2): a row the upgrade left unscoped moves to the
/// organization of its identity, once, if the identity still declares the address;
/// one whose organization holds the address already yields to that row. A link
/// since revoked is not restored: its row goes (HITL 6928c418). An identity of no
/// organization keeps its unscoped row, and a row of another organization is left
/// alone, which no read serves.
#[tokio::test]
async fn init_moves_an_unscoped_address_to_its_identity_s_organization() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    for (id, organization, address) in [
        ("alice", Some("acme"), None),
        ("bob", Some("acme"), Some("+2")),
        ("carol", None, Some("+3")),
    ] {
        let mut identity = Identity::new(IdentityId::new(id).unwrap());
        identity.organization_id = organization.map(str::to_owned);
        if let Some(address) = address {
            identity.link(ChannelType::SMS, address);
        }
        store.create_identity(identity).await.unwrap();
    }
    let client = schema.client().await;
    client
        .batch_execute(
            "UPDATE identities SET channel_addresses = '{\"sms\": [\"+1\", \"+2\"]}' \
             WHERE id = 'alice';
             INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id) \
             VALUES ('sms', '+1', 'alice', ''), ('sms', '+2', 'alice', ''), \
                    ('sms', '+4', 'alice', 'globex'), ('sms', '+5', 'alice', '');
             DELETE FROM schema_version WHERE version >= 5;",
        )
        .await
        .unwrap();
    store.init(SMALL_POOL).await.unwrap();
    store.init(SMALL_POOL).await.unwrap();
    let stored: Vec<(String, String, String)> = client
        .query(
            "SELECT address, identity_id, organization_id FROM identity_addresses ORDER BY 1, 3",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect();
    let row = |address: &str, id: &str, organization: &str| {
        (address.to_owned(), id.to_owned(), organization.to_owned())
    };
    assert_eq!(
        stored,
        [
            row("+1", "alice", "acme"),
            row("+2", "bob", "acme"),
            row("+3", "carol", ""),
            row("+4", "alice", "globex"),
        ]
    );
}

/// Python: `test_a_second_init_waits_on_no_open_transaction`: on an up-to-date
/// schema `init` takes no lock an open transaction on its tables holds
/// against it, a second's lock timeout proves it.
#[tokio::test]
async fn a_second_init_waits_on_no_open_transaction() {
    for holder in [
        "SELECT count(*) FROM events",
        "SELECT count(*) FROM rooms",
        "SELECT count(*) FROM bindings",
        "SELECT count(*) FROM participants",
        "SELECT count(*) FROM identity_addresses",
        "UPDATE rooms SET updated_at = now() WHERE id = 'r1'",
        "UPDATE event_sequences SET next_index = next_index + 1 WHERE room_id = 'r1'",
    ] {
        let Some((schema, store)) = fresh().await else {
            return;
        };
        store.create_room(Room::new(room_id("r1"))).await.unwrap();
        store
            .commit_event(
                &room_id("r1"),
                event("r1", "x"),
                roomkit::store::CommitEventOptions::default(),
            )
            .await
            .unwrap();
        let other = schema.client().await;
        other.batch_execute("BEGIN").await.unwrap();
        other.batch_execute(holder).await.unwrap();
        let again = PostgresStore::new(PostgresStoreOptions {
            dsn: Some(schema.dsn_with("lock_timeout=500")),
            ..PostgresStoreOptions::default()
        });
        let started = again.init(SMALL_POOL).await;
        other.batch_execute("ROLLBACK").await.unwrap();
        assert!(started.is_ok(), "{holder}: {started:?}");
    }
}

/// Python: `test_init_in_a_second_schema_keeps_each_schema_its_own_indexes`:
/// two deployments in one database, one per schema, the first's on the
/// `search_path` behind: the tenant reads and writes its own schema only.
#[tokio::test]
async fn init_in_a_second_schema_keeps_each_schema_its_own_indexes() {
    let Some((home, _store)) = fresh().await else {
        return;
    };
    let tenant = TempSchema::new().await;
    let home_client = home.client().await;
    home_client
        .batch_execute(
            "DROP INDEX idx_events_room_index;
             CREATE INDEX idx_events_room_index ON events(room_id, index);
             CREATE INDEX idx_events_parent ON events(parent_event_id);",
        )
        .await
        .unwrap();
    let before = indexes(&home_client).await;
    let store = PostgresStore::new(PostgresStoreOptions {
        dsn: Some(format!("{}%2C{}", tenant.dsn(), home.name())),
        ..PostgresStoreOptions::default()
    });
    store.init(SMALL_POOL).await.unwrap();
    let dry = store
        .drop_legacy_parent_index(DropLegacyParentIndexOptions::default())
        .await
        .unwrap();
    assert_eq!(dry["action"], json!("noop"));
    let tenant_indexes = indexes(&tenant.client().await).await;
    assert_eq!(indexes(&home_client).await, before);
    assert!(tenant_indexes["idx_events_room_index"].contains("UNIQUE"));
    assert!(!tenant_indexes.contains_key("idx_events_parent"));
}

/// Writes the tables of a version 1 schema, its record in `rooms.data`.
async fn v1_schema(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "DROP TABLE rooms CASCADE;
             CREATE TABLE rooms (id TEXT PRIMARY KEY, data JSONB NOT NULL);
             INSERT INTO rooms VALUES ('old', '{}');",
        )
        .await
        .unwrap();
}

/// Python: `test_init_refuses_v1_schema`.
#[tokio::test]
async fn init_refuses_a_version_1_schema_and_leaves_it() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let client = schema.client().await;
    v1_schema(&client).await;
    let refused = store.init(SMALL_POOL).await.unwrap_err();
    assert!(schema_error(&refused).is_some(), "{refused:?}");
    let kept: i64 = client
        .query_one("SELECT count(*) FROM rooms", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(kept, 1);
}

/// Python: `test_migrate_dry_run_reports_without_dropping`,
/// `test_migrate_requires_confirm`, `test_migrate_drops_and_recreates_when_confirmed`.
#[tokio::test]
async fn a_migration_drops_only_when_it_is_run_and_confirmed() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let client = schema.client().await;
    v1_schema(&client).await;
    let count = async || -> Option<i64> {
        client
            .query_one("SELECT count(*) FROM rooms", &[])
            .await
            .ok()
            .map(|row| row.get(0))
    };
    let dry = store.migrate(MigrateOptions::default()).await.unwrap();
    assert_eq!(dry["action"], json!("dry_run"));
    assert_eq!(dry["detected_version"], json!(1));
    assert_eq!(count().await, Some(1));
    let refused = store
        .migrate(MigrateOptions {
            dry_run: false,
            confirm: false,
        })
        .await
        .unwrap_err();
    assert!(schema_error(&refused).is_some(), "{refused:?}");
    assert_eq!(count().await, Some(1));
    let migrated = store
        .migrate(MigrateOptions {
            dry_run: false,
            confirm: true,
        })
        .await
        .unwrap();
    assert_eq!(migrated["action"], json!("migrated"));
    assert_eq!(
        migrated["dropped_tables"].as_array().map(Vec::len),
        Some(11)
    );
    assert_eq!(count().await, Some(0));
}

/// Python: `init` keeps its pool before it reads the schema: a version 1 database
/// migrates on the store whose `init` refused it, as its refusal tells the caller.
#[tokio::test]
async fn a_store_whose_init_refused_a_version_1_schema_migrates_it() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let client = schema.client().await;
    client
        .batch_execute(
            "CREATE TABLE rooms (id TEXT PRIMARY KEY, data JSONB NOT NULL);
             INSERT INTO rooms VALUES ('old', '{}');",
        )
        .await
        .unwrap();
    let store = schema.store();
    let refused = store.init(SMALL_POOL).await.unwrap_err();
    assert!(schema_error(&refused).is_some(), "{refused:?}");
    let dry = store.migrate(MigrateOptions::default()).await.unwrap();
    assert_eq!(dry["action"], json!("dry_run"));
    let run = MigrateOptions {
        dry_run: false,
        confirm: true,
    };
    assert_eq!(
        store.migrate(run).await.unwrap()["action"],
        json!("migrated")
    );
    store.init(SMALL_POOL).await.unwrap();
    assert!(!store.room_exists(&room_id("old")).await.unwrap());
}

/// Python: `test_migrate_noop_on_v2`.
#[tokio::test]
async fn a_migration_of_a_version_2_schema_does_nothing() {
    let Some((_schema, store)) = fresh().await else {
        return;
    };
    let report = store
        .migrate(MigrateOptions {
            dry_run: false,
            confirm: true,
        })
        .await
        .unwrap();
    assert_eq!(
        Value::Object(report),
        json!({ "detected_version": 2, "action": "noop", "dropped_tables": [] })
    );
}

/// Python: `test_migrate_in_a_second_schema_drops_its_own_tables_only`.
#[tokio::test]
async fn a_migration_in_a_second_schema_drops_its_own_tables_only() {
    let Some((_home, home_store)) = fresh().await else {
        return;
    };
    home_store
        .create_room(Room::new(room_id("kept")))
        .await
        .unwrap();
    let tenant = TempSchema::new().await;
    let tenant_store = tenant.initialized_store().await;
    v1_schema(&tenant.client().await).await;
    let report = tenant_store
        .migrate(MigrateOptions {
            dry_run: false,
            confirm: true,
        })
        .await
        .unwrap();
    assert_eq!(report["action"], json!("migrated"));
    assert!(home_store.room_exists(&room_id("kept")).await.unwrap());
}

/// Writes duplicate indexes in a room, as a release before could, the unique
/// index given way to the older one.
async fn duplicates(store: &PostgresStore, client: &tokio_postgres::Client) {
    client
        .batch_execute(
            "DROP INDEX idx_events_room_index;
             CREATE INDEX idx_events_room_index ON events(room_id, index);",
        )
        .await
        .unwrap();
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    for _ in 0..3 {
        store.add_event(event("r1", "same index")).await.unwrap();
    }
}

/// Python: `test_dedupe_dry_run_reports_without_changing`.
#[tokio::test]
async fn a_dedupe_dry_run_reports_without_changing() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let client = schema.client().await;
    duplicates(&store, &client).await;
    let report = store
        .dedupe_event_indices(DedupeEventIndicesOptions::default())
        .await
        .unwrap();
    assert_eq!(
        Value::Object(report),
        json!({
            "action": "dry_run", "duplicate_rows": 2, "affected_rooms": 1, "now_unique": false,
        })
    );
    let distinct: i64 = client
        .query_one("SELECT count(DISTINCT index) FROM events", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(distinct, 1);
}

/// Python: `test_dedupe_repairs_and_enforces_unique`,
/// `test_dedupe_noop_on_clean`, `test_unique_index_constraint_rejects_duplicate`.
#[tokio::test]
async fn a_dedupe_repairs_and_enforces_unique() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let client = schema.client().await;
    duplicates(&store, &client).await;
    let run = DedupeEventIndicesOptions { dry_run: false };
    let report = store.dedupe_event_indices(run).await.unwrap();
    assert_eq!(report["action"], json!("repaired"));
    assert_eq!(report["now_unique"], json!(true));
    let room = store.get_room(&room_id("r1")).await.unwrap().unwrap();
    assert_eq!((room.event_count, room.latest_index), (3, Some(2)));
    assert_eq!(
        store.dedupe_event_indices(run).await.unwrap()["action"],
        json!("noop")
    );
    let refused = store
        .add_event(event("r1", "same index"))
        .await
        .unwrap_err();
    assert!(matches!(refused, StoreError::Unavailable(_)), "{refused:?}");
}

/// Python: `test_init_serializes_its_ddl_under_the_migration_advisory_lock`:
/// while another session holds the lock, `init` waits for it.
#[tokio::test]
async fn init_serializes_its_ddl_under_the_migration_advisory_lock() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let server = dsn().unwrap();
    let (holder, connection) = tokio_postgres::connect(&server, NoTls).await.unwrap();
    tokio::spawn(connection);
    holder
        .batch_execute("SELECT pg_advisory_lock(1919905645)")
        .await
        .unwrap();
    let waiting = tokio::spawn({
        let store = store.clone();
        async move { store.init(SMALL_POOL).await }
    });
    let client = schema.client().await;
    let mut seen_waiting = false;
    for _ in 0..200 {
        let waiting_now: i64 = client
            .query_one(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if waiting_now > 0 {
            seen_waiting = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    holder
        .batch_execute("SELECT pg_advisory_unlock(1919905645)")
        .await
        .unwrap();
    waiting.await.unwrap().unwrap();
    assert!(seen_waiting);
}

/// A size asyncpg refuses is refused before any connection.
#[tokio::test]
async fn sizes_asyncpg_refuses_are_refused() {
    let store = PostgresStore::new(PostgresStoreOptions {
        dsn: Some("postgresql://nobody@127.0.0.1:1/none".to_owned()),
        ..PostgresStoreOptions::default()
    });
    for sizes in [
        InitOptions {
            min_size: 1,
            max_size: 0,
        },
        InitOptions {
            min_size: 3,
            max_size: 2,
        },
    ] {
        let refused = store.init(sizes).await.unwrap_err();
        assert!(matches!(refused, StoreError::Value(_)), "{refused:?}");
    }
}
