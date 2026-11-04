//! What only this store holds, on a real database: Python's
//! `tests/test_store_postgres.py` and `tests/test_postgres_store.py`, for what
//! the shared store suite and the scenarios do not say. Each test skips
//! itself without `POSTGRES_DSN`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelId, ChannelType, EventSource, IdentityId, Metadata,
    Participant, ParticipantId, Room, RoomEvent, RoomId, TextContent, Verification,
};
use roomkit::store::{CommitEventOptions, PatchRoomMetadataOptions};
use roomkit::{ConversationStore, RoomKit, RoomKitOptions, StoreError};
use roomkit_postgres::{PostgresStore, PostgresStoreOptions};
use serde_json::{Value, json};
use tokio_postgres::NoTls;

use crate::support::{SMALL_POOL, TempSchema, dsn, full_text};

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

/// A schema and its store, or `None` without `POSTGRES_DSN`.
async fn fresh() -> Option<(TempSchema, PostgresStore)> {
    dsn()?;
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    Some((schema, store))
}

/// Python: `test_ensure_pool_raises_without_init`: no operation runs before
/// `init`, and no I/O happens before it.
#[tokio::test]
async fn an_operation_before_init_fails_and_says_so() {
    let store = PostgresStore::new(PostgresStoreOptions {
        dsn: Some("postgresql://nobody@127.0.0.1:1/none".to_owned()),
        ..PostgresStoreOptions::default()
    });
    let refused = store.get_room(&room_id("r1")).await.unwrap_err();
    assert!(
        full_text(&refused).contains("PostgresStore.init() must be called before use"),
        "{refused:?}"
    );
}

/// A DSN holds a password: neither the options nor the store write one.
#[test]
fn no_debug_form_writes_the_dsn() {
    let options = PostgresStoreOptions {
        dsn: Some("postgresql://user:hunter2@host/db".to_owned()),
        ..PostgresStoreOptions::default()
    };
    assert!(!format!("{options:?}").contains("hunter2"));
    assert!(!format!("{:?}", PostgresStore::new(options)).contains("hunter2"));
}

/// A connection that cannot be made fails without the DSN's password in the
/// message or its causes.
#[tokio::test]
async fn a_failed_connection_says_nothing_of_the_password() {
    let store = PostgresStore::new(PostgresStoreOptions {
        dsn: Some("postgresql://user:hunter2@127.0.0.1:1/db?sslmode=disable".to_owned()),
        ..PostgresStoreOptions::default()
    });
    let refused = store.init(SMALL_POOL).await.unwrap_err();
    assert!(!full_text(&refused).contains("hunter2"), "{refused:?}");
}

/// Python: `test_close_releases_pool`: a closed store takes no operation, a
/// second close does nothing, and `init` opens a new pool.
#[tokio::test]
async fn a_closed_store_fails_until_it_is_initialized_again() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    store.close().await;
    store.close().await;
    assert!(matches!(
        store.get_room(&room_id("r1")).await,
        Err(StoreError::Unavailable(_))
    ));
    store.init(SMALL_POOL).await.unwrap();
    assert!(store.room_exists(&room_id("r1")).await.unwrap());
    drop(schema);
}

/// Python: `test_close_skips_external_pool`: a pool handed in stays its
/// owner's, and its store works with one alone.
#[tokio::test]
async fn a_pool_handed_in_stays_open_after_the_store_s_close() {
    use deadpool_postgres::{Manager, Pool};
    let Some((schema, _store)) = fresh().await else {
        return;
    };
    let config: tokio_postgres::Config = schema.dsn().parse().unwrap();
    let pool = Pool::builder(Manager::new(config, NoTls))
        .max_size(2)
        .build()
        .unwrap();
    let store = PostgresStore::new(PostgresStoreOptions {
        pool: Some(pool.clone()),
        ..PostgresStoreOptions::default()
    });
    store.init(SMALL_POOL).await.unwrap();
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    store.close().await;
    assert!(!pool.is_closed());
    assert!(store.room_exists(&room_id("r1")).await.unwrap());
}

/// Python: `TestEventFromRow::test_a_raw_row_is_the_event_get_event_reads`.
#[tokio::test]
async fn a_raw_row_is_the_event_get_event_reads() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    let committed = store
        .commit_event(
            &room_id("r1"),
            event("r1", "from a page"),
            CommitEventOptions::default(),
        )
        .await
        .unwrap();
    let row = schema
        .client()
        .await
        .query_one(
            "SELECT events.*, 'page-1' AS page FROM events WHERE id = $1",
            &[&committed.id.as_str()],
        )
        .await
        .unwrap();
    assert_eq!(
        PostgresStore::event_from_row(&row).unwrap(),
        store.get_event(&committed.id).await.unwrap().unwrap()
    );
}

/// Python: `test_a_private_state_write_racing_the_room_s_deletion_writes_nothing`.
#[tokio::test]
async fn a_private_state_write_racing_the_room_s_deletion_writes_nothing() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    let deleter = schema.client().await;
    deleter.batch_execute("BEGIN").await.unwrap();
    deleter
        .batch_execute("DELETE FROM rooms WHERE id = 'r1'")
        .await
        .unwrap();
    let write = tokio::spawn({
        let store = store.clone();
        async move {
            let mut value = Metadata::new();
            value.insert("version".to_owned(), json!(3));
            store
                .set_private_state(&room_id("r1"), "author_register", Some(value))
                .await
        }
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    deleter.batch_execute("COMMIT").await.unwrap();
    write.await.unwrap().unwrap();
    assert_eq!(
        store
            .get_private_state(&room_id("r1"), "author_register")
            .await
            .unwrap(),
        None
    );
}

/// Python: `test_a_held_id_is_the_driver_s_refusal_whatever_the_index_order`:
/// after a reindex the primary key is the newest index, checked last: a second
/// write of a stored event stays the driver's refusal, and another event under
/// the key a taken key.
#[tokio::test]
async fn a_held_id_is_the_driver_s_refusal_whatever_the_index_order() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    schema
        .client()
        .await
        .batch_execute(&format!(
            "REINDEX INDEX CONCURRENTLY {}.events_pkey",
            schema.name()
        ))
        .await
        .unwrap();
    store.create_room(Room::new(room_id("r1"))).await.unwrap();
    let mut keyed = event("r1", "x");
    keyed.idempotency_key = Some("k".to_owned());
    let held = store
        .commit_event(&room_id("r1"), keyed, CommitEventOptions::default())
        .await
        .unwrap();
    let mut other = event("r1", "y");
    other.idempotency_key = Some("k".to_owned());
    let mut other_at_99 = other.clone();
    other_at_99.index = 99;
    let mut held_at_99 = held.clone();
    held_at_99.index = 99;
    let room = room_id("r1");

    let second = [
        store
            .commit_event(&room, held.clone(), CommitEventOptions::default())
            .await,
        store.add_event(held_at_99).await,
        store.add_event_auto_index(&room, held.clone()).await,
    ];
    for refused in second {
        assert!(
            matches!(refused, Err(StoreError::Unavailable(_))),
            "{refused:?}"
        );
    }
    let taken = [
        store
            .commit_event(&room, other.clone(), CommitEventOptions::default())
            .await,
        store.add_event(other_at_99).await,
        store.add_event_auto_index(&room, other).await,
    ];
    for refused in taken {
        assert!(
            matches!(refused, Err(StoreError::IdempotencyKeyTaken { .. })),
            "{refused:?}"
        );
    }
    let stored = store
        .list_events(&room, roomkit::store::ListEventsOptions::default())
        .await
        .unwrap();
    assert_eq!(
        stored
            .iter()
            .map(|event| event.id.clone())
            .collect::<Vec<_>>(),
        vec![held.id]
    );
}

/// Python: `test_patch_room_metadata_concurrent_with_commits`,
/// `test_patch_room_metadata_does_not_touch_counters`: one partial update, so
/// neither loses the other's write, across connections.
#[tokio::test]
async fn metadata_patches_and_commits_lose_nothing_of_each_other() {
    let Some((_schema, store)) = fresh().await else {
        return;
    };
    let room = room_id("r1");
    store.create_room(Room::new(room.clone())).await.unwrap();
    let mut work = Vec::new();
    for number in 0..10 {
        let (patched, in_room) = (store.clone(), room.clone());
        work.push(tokio::spawn(async move {
            let mut patch = Metadata::new();
            patch.insert(format!("k{number}"), json!(number));
            patched
                .patch_room_metadata(&in_room, patch, PatchRoomMetadataOptions::default())
                .await
                .unwrap();
        }));
        let (store, room) = (store.clone(), room.clone());
        work.push(tokio::spawn(async move {
            store
                .commit_event(&room, event("r1", "x"), CommitEventOptions::default())
                .await
                .unwrap();
        }));
    }
    for done in work {
        done.await.unwrap();
    }
    let stored = store.get_room(&room).await.unwrap().unwrap();
    assert_eq!(stored.event_count, 10);
    assert_eq!(stored.latest_index, Some(9));
    assert_eq!(stored.metadata.len(), 10);
}

/// Python: `TestDeleteRoomCleanup::test_cascading_delete`.
#[tokio::test]
async fn a_deleted_room_takes_its_rows_with_it() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let room = room_id("r1");
    store.create_room(Room::new(room.clone())).await.unwrap();
    store
        .add_binding(ChannelBinding::new(
            channel(),
            room.clone(),
            ChannelType::SMS,
        ))
        .await
        .unwrap();
    store
        .add_participant(Participant::new(
            ParticipantId::new("p1").unwrap(),
            room.clone(),
            channel(),
        ))
        .await
        .unwrap();
    let committed = store
        .commit_event(&room, event("r1", "x"), CommitEventOptions::default())
        .await
        .unwrap();
    store
        .mark_read(&room, &channel(), &committed.id)
        .await
        .unwrap();
    let mut state = Metadata::new();
    state.insert("v".to_owned(), json!(1));
    store
        .set_private_state(&room, "k", Some(state))
        .await
        .unwrap();
    assert!(store.delete_room(&room).await.unwrap());
    let client = schema.client().await;
    for table in [
        "events",
        "bindings",
        "participants",
        "read_markers",
        "event_sequences",
        "private_state",
    ] {
        let left: i64 = client
            .query_one(&format!("SELECT count(*) FROM {table}"), &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(left, 0, "{table}");
    }
}

/// Python: `test_connected_via_repairs_a_row_that_predates_it`.
#[tokio::test]
async fn an_empty_channel_list_reads_with_its_primary() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let room = room_id("r1");
    store.create_room(Room::new(room.clone())).await.unwrap();
    let id = ParticipantId::new("p1").unwrap();
    store
        .add_participant(Participant::new(id.clone(), room.clone(), channel()))
        .await
        .unwrap();
    let client = schema.client().await;
    client
        .batch_execute("UPDATE participants SET connected_via = '{}'")
        .await
        .unwrap();
    let read = store.get_participant(&room, &id).await.unwrap().unwrap();
    assert_eq!(read.connected_via, vec![channel()]);
    // Python: `test_init_backfills_connected_via_on_a_legacy_row`.
    store.init(SMALL_POOL).await.unwrap();
    let stored: Vec<String> = client
        .query_one("SELECT connected_via FROM participants", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(stored, vec!["ch1".to_owned()]);
}

/// A member of a JSONB column the model does not read stays when the record
/// is rewritten; a member the model reads and the record cleared is cleared
/// (RMK-989).
#[tokio::test]
async fn a_rewrite_keeps_what_the_model_does_not_read() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let room = room_id("r1");
    store.create_room(Room::new(room.clone())).await.unwrap();
    let capabilities = ChannelCapabilities {
        max_length: Some(160),
        ..ChannelCapabilities::default()
    };
    let mut binding = ChannelBinding::new(channel(), room.clone(), ChannelType::SMS);
    binding.capabilities = capabilities;
    store.add_binding(binding.clone()).await.unwrap();
    let committed = store
        .commit_event(&room, event("r1", "hi"), CommitEventOptions::default())
        .await
        .unwrap();
    let client = schema.client().await;
    client
        .batch_execute(
            "UPDATE rooms SET timers = timers || '{\"later\": 1}';
             UPDATE bindings SET capabilities = capabilities || '{\"later\": 2}';
             UPDATE events SET content = content || '{\"later\": 3}';",
        )
        .await
        .unwrap();
    let mut read_room = store.get_room(&room).await.unwrap().unwrap();
    read_room.timers.inactive_after_seconds = Some(5);
    store.update_room(read_room).await.unwrap();
    binding.capabilities.max_length = None;
    store.update_binding(binding).await.unwrap();
    let mut read_event = store.get_event(&committed.id).await.unwrap().unwrap();
    read_event.metadata.insert("seen".to_owned(), json!(true));
    store.update_event(read_event).await.unwrap();
    let row = client
        .query_one(
            "SELECT (SELECT timers FROM rooms), (SELECT capabilities FROM bindings), \
             (SELECT content FROM events)",
            &[],
        )
        .await
        .unwrap();
    let (timers, capabilities, content): (Value, Value, Value) =
        (row.get(0), row.get(1), row.get(2));
    assert_eq!(timers["later"], json!(1));
    assert_eq!(timers["inactive_after_seconds"], json!(5));
    assert_eq!(capabilities["later"], json!(2));
    assert_eq!(capabilities["max_length"], Value::Null);
    assert_eq!(content["later"], json!(3));
}

/// A blank key is no key (RFC §13.4), written by an update too: two events of a
/// channel never collide on one.
#[tokio::test]
async fn a_blank_key_written_by_an_update_is_no_key() {
    let Some((_schema, store)) = fresh().await else {
        return;
    };
    let room = room_id("r1");
    store.create_room(Room::new(room.clone())).await.unwrap();
    let mut first = store
        .commit_event(&room, event("r1", "one"), CommitEventOptions::default())
        .await
        .unwrap();
    first.idempotency_key = Some("  ".to_owned());
    store.update_event(first).await.unwrap();
    let mut second = store
        .commit_event(&room, event("r1", "two"), CommitEventOptions::default())
        .await
        .unwrap();
    second.idempotency_key = Some("  ".to_owned());
    assert!(store.update_event(second).await.is_ok());
}

/// Python: `check_idempotency` reads no row: a key is found on an event this
/// version's model cannot read, a later version's type among them.
#[tokio::test]
async fn a_key_is_found_on_a_row_the_model_cannot_read() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let room = room_id("r1");
    store.create_room(Room::new(room.clone())).await.unwrap();
    let mut keyed = event("r1", "x");
    keyed.idempotency_key = Some("k".to_owned());
    store
        .commit_event(&room, keyed, CommitEventOptions::default())
        .await
        .unwrap();
    schema
        .client()
        .await
        .batch_execute("UPDATE events SET type = 'from_a_later_version'")
        .await
        .unwrap();
    let channel = channel();
    for scope in [None, Some(&channel)] {
        assert!(store.check_idempotency(&room, "k", scope).await.unwrap());
    }
    assert!(!store.check_idempotency(&room, "  ", None).await.unwrap());
}

/// A commit's timers and a verification's document keep, on a rewrite, what the
/// model does not read (RMK-989).
#[tokio::test]
async fn a_commit_and_a_verification_keep_what_the_model_does_not_read() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let room = room_id("r1");
    store.create_room(Room::new(room.clone())).await.unwrap();
    let identity = IdentityId::new("alice").unwrap();
    let verification = Verification::new(identity.clone());
    store.put_verification(verification.clone()).await.unwrap();
    let client = schema.client().await;
    client
        .batch_execute(
            "UPDATE rooms SET timers = timers || '{\"later\": 1}';
             UPDATE verifications SET data = data || '{\"later\": 2}';",
        )
        .await
        .unwrap();
    store
        .commit_event(&room, event("r1", "x"), CommitEventOptions::default())
        .await
        .unwrap();
    store.put_verification(verification).await.unwrap();
    let row = client
        .query_one(
            "SELECT (SELECT timers FROM rooms), (SELECT data FROM verifications)",
            &[],
        )
        .await
        .unwrap();
    let (timers, data): (Value, Value) = (row.get(0), row.get(1));
    assert_eq!(timers["later"], json!(1));
    assert_eq!(data["later"], json!(2));
}

/// Python: `test_without_cross_process_lock_the_constraint_catches_races` and
/// `test_inbound_pipeline_atomic_commit_serializes_without_advisory_lock`: two
/// stores, each its own pool, as two processes hold them, commit to one room
/// at once: no index is given twice and the counters are exact.
#[tokio::test]
async fn two_processes_commit_to_one_room_with_no_index_given_twice() {
    let Some((schema, first)) = fresh().await else {
        return;
    };
    let second = schema.initialized_store().await;
    let room = room_id("r1");
    first.create_room(Room::new(room.clone())).await.unwrap();
    let mut work = Vec::new();
    for store in [first.clone(), second.clone()] {
        for _ in 0..15 {
            let (store, room) = (store.clone(), room.clone());
            work.push(tokio::spawn(async move {
                store
                    .commit_event(&room, event("r1", "x"), CommitEventOptions::default())
                    .await
                    .unwrap()
                    .index
            }));
        }
    }
    let mut indexes = Vec::new();
    for done in work {
        indexes.push(done.await.unwrap());
    }
    indexes.sort_unstable();
    assert_eq!(indexes, (0..30).collect::<Vec<u64>>());
    let stored = first.get_room(&room).await.unwrap().unwrap();
    assert_eq!((stored.event_count, stored.latest_index), (30, Some(29)));
}

/// The kit runs on the store, its close closing the pool the store opened.
#[tokio::test]
async fn the_kit_closes_the_store_it_runs_on() {
    let Some((_schema, store)) = fresh().await else {
        return;
    };
    let kit = RoomKit::new(RoomKitOptions::with_store(store.clone()));
    kit.close().await.unwrap();
    assert!(matches!(
        store.room_exists(&room_id("r1")).await,
        Err(StoreError::Unavailable(_))
    ));
}
