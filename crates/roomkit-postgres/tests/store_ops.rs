//! What only this store holds, on a real database: Python's
//! `tests/test_store_postgres.py` and `tests/test_postgres_store.py`, for what
//! the shared store suite and the scenarios do not say. Each test skips
//! itself without `POSTGRES_DSN`.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelId, ChannelType, EditContent, EventContent,
    EventSource, EventType, Identity, IdentityId, InboundMessage, Metadata, Participant,
    ParticipantId, Room, RoomEvent, RoomId, RoomStatus, TextContent, Verification,
};
use roomkit::store::{
    CommitEventOptions, DeleteEventOptions, LinkAddressOptions, PatchRoomMetadataOptions,
    ResolveIdentityOptions,
};
use roomkit::testing::{EventTap, RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitOptions, StoreError,
};
use roomkit_postgres::{InitOptions, PostgresStore, PostgresStoreOptions};
use serde_json::{Value, json};
use tokio_postgres::NoTls;

use crate::support::{SMALL_POOL, TempSchema, dsn, full_text, until_a_session, until_blocked_by};

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

/// An identity of `organization`, at the `address` given on SMS.
fn identity(name: &str, organization: Option<&str>, address: Option<&str>) -> Identity {
    let mut identity = Identity {
        organization_id: organization.map(str::to_owned),
        ..Identity::new(IdentityId::new(name).unwrap())
    };
    if let Some(address) = address {
        identity.link(ChannelType::SMS, address);
    }
    identity
}

/// Where `address` resolves in `organization`.
async fn resolved(store: &PostgresStore, address: &str, organization: &str) -> Option<Identity> {
    store
        .resolve_identity(
            &ChannelType::SMS,
            address,
            ResolveIdentityOptions {
                organization_id: Some(organization.to_owned()),
            },
        )
        .await
        .unwrap()
}

/// Python: `test_a_creation_racing_another_organization_s_is_refused`: the
/// refusal is the write's own, so a creation that waits on another's uncommitted
/// row reads it once that commits, and writes nothing.
#[tokio::test]
async fn a_creation_racing_another_organization_s_is_refused() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let creator = schema.client().await;
    creator.batch_execute("BEGIN").await.unwrap();
    creator
        .batch_execute("INSERT INTO identities (id, organization_id) VALUES ('i1', 'acme')")
        .await
        .unwrap();
    let racing = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .create_identity(identity("i1", Some("globex"), None))
                .await
        }
    });
    // Python's `_until_blocked_by`: a session waits on this one's transaction.
    let creator_pid: i32 = creator
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let watcher = schema.client().await;
    let mut blocked = 0_i64;
    for _ in 0..400 {
        blocked = watcher
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
                &[&creator_pid],
            )
            .await
            .unwrap()
            .get(0);
        if blocked > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(blocked, 1, "the creation never waited on the other's row");
    creator.batch_execute("COMMIT").await.unwrap();
    let id = IdentityId::new("i1").unwrap();
    assert_eq!(
        racing.await.unwrap(),
        Err(StoreError::IdentityNotFound(id.clone()))
    );
    let stored = store.get_identity(&id).await.unwrap().unwrap();
    assert_eq!(stored.organization_id.as_deref(), Some("acme"));
}

/// Writes an address row directly, as an upgrade left it.
async fn insert_address(schema: &TempSchema, address: &str, identity_id: &str, held_in: &str) {
    schema
        .client()
        .await
        .execute(
            "INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id) \
             VALUES ('sms', $1, $2, $3)",
            &[&address, &identity_id, &held_in],
        )
        .await
        .unwrap();
}

/// Python: `test_a_replacement_removes_a_row_of_another_scope_for_a_kept_address`:
/// a row of another scope for an address it keeps goes, not only stops resolving.
#[tokio::test]
async fn a_replacement_removes_a_row_of_another_scope_for_a_kept_address() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let held = identity("i1", Some("acme"), Some("+1555"));
    store.create_identity(held.clone()).await.unwrap();
    insert_address(&schema, "+1555", "i1", "").await;
    store.create_identity(held.clone()).await.unwrap();
    let scopes: Vec<String> = schema
        .client()
        .await
        .query(
            "SELECT organization_id FROM identity_addresses WHERE identity_id = 'i1'",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(scopes, ["acme"]);
    assert_eq!(resolved(&store, "+1555", "acme").await, Some(held));
}

/// Python: `test_an_unlink_removes_the_identity_s_rows_in_every_scope`: a link
/// revoked is revoked, a row an upgrade left in another scope with it, so that no
/// later rewrite can restore it (RFC §5.6).
#[tokio::test]
async fn an_unlink_removes_the_identity_s_rows_in_every_scope() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    store
        .create_identity(identity("i1", Some("acme"), Some("+1")))
        .await
        .unwrap();
    insert_address(&schema, "+1", "i1", "").await;
    store
        .unlink_address(
            &IdentityId::new("i1").unwrap(),
            &ChannelType::SMS,
            "+1",
            roomkit::store::UnlinkAddressOptions {
                organization_id: Some("acme".to_owned()),
            },
        )
        .await
        .unwrap();
    let left: i64 = schema
        .client()
        .await
        .query_one("SELECT count(*) FROM identity_addresses", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(left, 0);
}

/// Python: `test_a_row_of_another_scope_than_its_identity_resolves_for_nobody`
/// (RFC §17.2, HITL 0f96a597): an identity is read in its own organization only,
/// whatever a row left by an upgrade or written directly says.
#[tokio::test]
async fn a_row_of_another_scope_than_its_identity_resolves_for_nobody() {
    for (held_in, stored) in [("", Some("acme")), ("globex", Some("acme")), ("acme", None)] {
        let Some((schema, store)) = fresh().await else {
            return;
        };
        store
            .create_identity(identity("i1", stored, None))
            .await
            .unwrap();
        insert_address(&schema, "+1555", "i1", held_in).await;
        for scope in [None, Some("acme"), Some("globex")] {
            let found = store
                .resolve_identity(
                    &ChannelType::SMS,
                    "+1555",
                    ResolveIdentityOptions {
                        organization_id: scope.map(str::to_owned),
                    },
                )
                .await
                .unwrap();
            assert_eq!(found, None, "row in {held_in:?}, read in {scope:?}");
        }
    }
}

/// Python: `test_a_row_no_read_serves_leaves_its_address_free` (RFC §5.6): an address
/// is refused for what another identity of the organization holds. A row an older
/// release left, whose identity is of another organization, resolves for nobody: the
/// organization's own identity takes the address.
#[tokio::test]
async fn a_row_no_read_serves_leaves_its_address_free() {
    for (held_in, stored) in [("", Some("acme")), ("globex", Some("acme")), ("acme", None)] {
        for taking in ["create_identity", "link_address"] {
            let Some((schema, store)) = fresh().await else {
                return;
            };
            store
                .create_identity(identity("i1", stored, None))
                .await
                .unwrap();
            insert_address(&schema, "+1555", "i1", held_in).await;
            let scope = (!held_in.is_empty()).then_some(held_in);
            if taking == "create_identity" {
                store
                    .create_identity(identity("dana", scope, Some("+1555")))
                    .await
                    .unwrap();
            } else {
                store
                    .create_identity(identity("dana", scope, None))
                    .await
                    .unwrap();
                store
                    .link_address(
                        &IdentityId::new("dana").unwrap(),
                        &ChannelType::SMS,
                        "+1555",
                        LinkAddressOptions {
                            organization_id: scope.map(str::to_owned),
                        },
                    )
                    .await
                    .unwrap();
            }
            let found = resolved(&store, "+1555", held_in).await.unwrap();
            assert_eq!(found.id.as_str(), "dana", "row in {held_in:?}, {taking}");
        }
    }
}

/// Python: `test_a_creation_waiting_on_a_new_identity_s_address_is_refused` (RFC §5.6):
/// the holder's organization is read once the first commits. Its identity is new to
/// the waiting statement's snapshot, and is no row to take.
#[tokio::test]
async fn a_creation_waiting_on_a_new_identity_s_address_is_refused() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let creator = schema.client().await;
    creator
        .batch_execute(
            "BEGIN; \
             INSERT INTO identities (id, organization_id) VALUES ('i1', 'acme'); \
             INSERT INTO identity_addresses (channel_type, address, identity_id, organization_id) \
             VALUES ('sms', '+1555', 'i1', 'acme')",
        )
        .await
        .unwrap();
    let racing = {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .create_identity(identity("i2", Some("acme"), Some("+1555")))
                .await
        })
    };
    let pid: i32 = creator
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let watcher = schema.client().await;
    let mut blocked = 0_i64;
    for _ in 0..400 {
        blocked = watcher
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid))",
                &[&pid],
            )
            .await
            .unwrap()
            .get(0);
        if blocked > 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(blocked, 1, "the creation never waited on the other's row");
    creator.batch_execute("COMMIT").await.unwrap();
    assert_eq!(
        racing.await.unwrap(),
        Err(StoreError::AddressLinked {
            channel_type: ChannelType::SMS
        })
    );
    let found = resolved(&store, "+1555", "acme").await.unwrap();
    assert_eq!(found.id.as_str(), "i1");
}

/// Python: `test_two_identities_taking_a_row_no_read_serves_leave_one_winner` (RFC
/// §5.6): the first holds the address from then on, and the second is refused.
#[tokio::test]
async fn two_identities_taking_a_row_no_read_serves_leave_one_winner() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    store
        .create_identity(identity("i1", Some("globex"), None))
        .await
        .unwrap();
    for number in 0..8 {
        let address = format!("+1{number}");
        insert_address(&schema, &address, "i1", "acme").await;
        let take = |name: String| {
            let (store, address) = (store.clone(), address.clone());
            tokio::spawn(async move {
                store
                    .create_identity(identity(&name, Some("acme"), Some(&address)))
                    .await
            })
        };
        let (one, two) = (take(format!("a{number}")), take(format!("b{number}")));
        let taken = [one.await.unwrap(), two.await.unwrap()];
        let won: Vec<&Identity> = taken
            .iter()
            .filter_map(|taken| taken.as_ref().ok())
            .collect();
        assert_eq!(won.len(), 1, "{taken:?}");
        assert!(
            taken
                .iter()
                .any(|taken| { matches!(taken, Err(StoreError::AddressLinked { .. })) }),
            "{taken:?}"
        );
        let found = resolved(&store, &address, "acme").await.unwrap();
        assert_eq!(found.id, won[0].id);
    }
}

/// Python: `test_two_replacements_swapping_addresses_are_refused_without_a_deadlock`:
/// each new address is filed before the old ones go, in one order whoever files
/// them, so neither waits on what the other holds, and each reads the refusal,
/// whether or not it keeps its own.
#[tokio::test]
async fn two_replacements_swapping_addresses_are_refused_without_a_deadlock() {
    let Some((_schema, store)) = fresh().await else {
        return;
    };
    for keeping in [false, true] {
        for number in 0..8 {
            let (one, two) = (
                format!("i{keeping}{number}a"),
                format!("i{keeping}{number}b"),
            );
            let (first, second) = (
                format!("+1{keeping}{number}"),
                format!("+2{keeping}{number}"),
            );
            for (name, address) in [(&one, &first), (&two, &second)] {
                store
                    .create_identity(identity(name, None, Some(address)))
                    .await
                    .unwrap();
            }
            // Its own first: filed in that order, each would hold its own row
            // while it waited on the other's.
            let taking = |name: &str, own: &str, other: &str| {
                let mut taker = identity(name, None, keeping.then_some(own));
                taker.link(ChannelType::SMS, other);
                taker
            };
            let (taken_by_one, taken_by_two) = tokio::join!(
                store.create_identity(taking(&one, &first, &second)),
                store.create_identity(taking(&two, &second, &first)),
            );
            for outcome in [taken_by_one, taken_by_two] {
                assert_eq!(
                    outcome,
                    Err(StoreError::AddressLinked {
                        channel_type: ChannelType::SMS,
                    }),
                    "keeping: {keeping}"
                );
            }
        }
    }
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

/// Python: `test_an_add_waiting_on_a_commit_holds_nothing_the_commit_needs` (RMK-1365):
/// a commit holds the room, then its sequence. An add takes them in that order too,
/// so that it waits on the room with the sequence free: in the other order, each
/// waits on what the other holds, and the server kills one.
#[tokio::test]
async fn an_add_waiting_on_a_commit_holds_nothing_the_commit_needs() {
    for auto_indexed in [false, true] {
        let Some((schema, store)) = fresh().await else {
            return;
        };
        let room = room_id("r1");
        store.create_room(Room::new(room.clone())).await.unwrap();
        store
            .add_event_auto_index(&room, event("r1", "first"))
            .await
            .unwrap();
        let committer = schema.client().await;
        committer
            .batch_execute("BEGIN; SELECT 1 FROM rooms WHERE id = 'r1' FOR UPDATE")
            .await
            .unwrap();
        let adding = {
            let (store, room) = (store.clone(), room.clone());
            tokio::spawn(async move {
                if auto_indexed {
                    store.add_event_auto_index(&room, event("r1", "x")).await
                } else {
                    let mut own = event("r1", "x");
                    own.index = 7;
                    store.add_event(own).await
                }
            })
        };
        until_blocked_by(&schema, &committer).await;
        committer
            .batch_execute(
                "UPDATE event_sequences SET next_index = next_index + 1 WHERE room_id = 'r1'; \
                 COMMIT",
            )
            .await
            .unwrap();
        adding.await.unwrap().unwrap();
    }
}

/// Python: `test_an_add_holds_its_room_so_that_no_stream_of_them_starves_a_commit`
/// (HITL 0affbb3b): an add waiting on the sequence holds the room in a mode the next
/// add cannot share, and that a foreign key can: `FOR NO KEY UPDATE`.
#[tokio::test]
async fn an_add_holds_its_room_so_that_no_stream_of_them_starves_a_commit() {
    for auto_indexed in [false, true] {
        let Some((schema, store)) = fresh().await else {
            return;
        };
        let room = room_id("r1");
        store.create_room(Room::new(room.clone())).await.unwrap();
        store
            .add_event_auto_index(&room, event("r1", "first"))
            .await
            .unwrap();
        let holder = schema.client().await;
        holder
            .batch_execute("BEGIN; SELECT 1 FROM event_sequences WHERE room_id = 'r1' FOR UPDATE")
            .await
            .unwrap();
        let adding = {
            let (store, room) = (store.clone(), room.clone());
            tokio::spawn(async move {
                if auto_indexed {
                    store.add_event_auto_index(&room, event("r1", "x")).await
                } else {
                    let mut own = event("r1", "x");
                    own.index = 7;
                    store.add_event(own).await
                }
            })
        };
        until_blocked_by(&schema, &holder).await;
        let other = schema.client().await;
        other
            .batch_execute("SELECT 1 FROM rooms WHERE id = 'r1' FOR KEY SHARE NOWAIT")
            .await
            .unwrap();
        let refused = other
            .batch_execute("SELECT 1 FROM rooms WHERE id = 'r1' FOR NO KEY UPDATE NOWAIT")
            .await
            .unwrap_err();
        assert_eq!(
            refused.code(),
            Some(&tokio_postgres::error::SqlState::LOCK_NOT_AVAILABLE)
        );
        holder.batch_execute("COMMIT").await.unwrap();
        adding.await.unwrap().unwrap();
    }
}

/// Python: `test_an_add_to_a_missing_room_stays_the_foreign_key_s_refusal`: holding
/// the room first finds no row and refuses nothing itself: the refusal is still the
/// write's (the shared suite's `an_event_added_to_a_missing_room_is_refused`), and
/// stores nothing.
#[tokio::test]
async fn an_add_to_a_missing_room_stays_the_foreign_key_s_refusal() {
    let Some((schema, store)) = fresh().await else {
        return;
    };
    let room = room_id("gone");
    store.add_event(event("gone", "x")).await.unwrap_err();
    store
        .add_event_auto_index(&room, event("gone", "x"))
        .await
        .unwrap_err();
    let left: i64 = schema
        .client()
        .await
        .query_one("SELECT count(*) FROM event_sequences", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(left, 0);
}

/// `message` as a delete leaves it, its edits to be marked.
fn marked_deleted(message: &RoomEvent) -> RoomEvent {
    let mut target = message.clone();
    target.metadata.insert("deleted".to_owned(), json!(true));
    target
}

/// An edit of `message`, for its room's next commit.
fn edit_of(message: &RoomEvent) -> RoomEvent {
    let mut edit = event(message.room_id.as_str(), "edit");
    edit.r#type = EventType::Edit;
    edit.content = EventContent::Edit(EditContent::new(
        message.id.clone(),
        TextContent::new("edited"),
    ));
    edit
}

/// Python: `test_a_write_of_a_message_and_its_edits_waits_on_the_room_holding_none_of_them`
/// (RMK-1394): a room's deletion holds the room, then takes its events in the order it
/// finds them, a message edited twice after its first edit. A write of a message and
/// its edits waits on the room first, so that it holds nothing the deletion needs: in
/// the other order each waits on what the other holds, and the server kills one.
#[tokio::test]
async fn a_write_of_a_message_and_its_edits_waits_on_the_room_holding_none_of_them() {
    for deleting in [true, false] {
        let Some((schema, store)) = fresh().await else {
            return;
        };
        let room = room_id("r1");
        store.create_room(Room::new(room.clone())).await.unwrap();
        let message = store
            .commit_event(&room, event("r1", "x"), CommitEventOptions::default())
            .await
            .unwrap();
        let deleter = schema.client().await;
        deleter
            .batch_execute("BEGIN; SELECT 1 FROM rooms WHERE id = 'r1' FOR UPDATE")
            .await
            .unwrap();
        let writing = {
            let (store, room) = (store.clone(), room.clone());
            tokio::spawn(async move {
                if deleting {
                    store
                        .delete_event(&room, &message.id, DeleteEventOptions::default())
                        .await
                        .map(|deleted| deleted.len())
                } else {
                    store
                        .write_change_target(&room, marked_deleted(&message))
                        .await
                        .map(|()| 1)
                }
            })
        };
        until_blocked_by(&schema, &deleter).await;
        deleter
            .batch_execute("DELETE FROM rooms WHERE id = 'r1'; COMMIT")
            .await
            .unwrap();
        let written = tokio::time::timeout(std::time::Duration::from_secs(2), writing)
            .await
            .unwrap()
            .unwrap();
        if deleting {
            assert_eq!(written.unwrap(), 0);
        } else {
            assert!(
                matches!(written, Err(StoreError::EventNotFound(_))),
                "{written:?}"
            );
        }
    }
}

/// Python: `test_a_room_deleted_under_a_message_and_its_edits_never_deadlocks` (RMK-1394):
/// unmodified calls, a message edited twice, its writer racing the room's deletion, on a
/// table small enough for the deletion to read it in heap order, where the first edit
/// comes before the message. RMK-1394 measured 135 and 288 of 3200 killed before.
#[tokio::test]
async fn a_room_deleted_under_a_message_and_its_edits_never_deadlocks() {
    for deleting in [true, false] {
        let Some((schema, _)) = fresh().await else {
            return;
        };
        let store = schema.store();
        store
            .init(InitOptions {
                min_size: 1,
                max_size: 40,
            })
            .await
            .unwrap();
        schema
            .client()
            .await
            .batch_execute("ANALYZE events")
            .await
            .unwrap();
        let mut failures = Vec::new();
        for batch in 0_u64..80 {
            let mut rooms = tokio::task::JoinSet::new();
            for number in batch * 40..batch * 40 + 40 {
                let store = store.clone();
                rooms.spawn(async move {
                    let room = room_id(&format!("r{number}"));
                    store.create_room(Room::new(room.clone())).await.unwrap();
                    let message = store
                        .commit_event(
                            &room,
                            event(room.as_str(), "x"),
                            CommitEventOptions::default(),
                        )
                        .await
                        .unwrap();
                    for _ in 0..2 {
                        let options = CommitEventOptions {
                            target: Some(message.clone()),
                            ..CommitEventOptions::default()
                        };
                        store
                            .commit_event(&room, edit_of(&message), options)
                            .await
                            .unwrap();
                    }
                    let deleted_later = async {
                        tokio::time::sleep(std::time::Duration::from_millis(number % 5)).await;
                        store.delete_room(&room).await.map(|_| ())
                    };
                    let written = async {
                        if deleting {
                            store
                                .delete_event(&room, &message.id, DeleteEventOptions::default())
                                .await
                                .map(|_| ())
                        } else {
                            match store
                                .write_change_target(&room, marked_deleted(&message))
                                .await
                            {
                                Err(StoreError::EventNotFound(_)) => Ok(()),
                                other => other,
                            }
                        }
                    };
                    let (written, deleted) = tokio::join!(written, deleted_later);
                    [written, deleted]
                });
            }
            while let Some(pair) = rooms.join_next().await {
                for outcome in pair.unwrap() {
                    if let Err(failure) = outcome {
                        failures.push(full_text(&failure));
                    }
                }
            }
        }
        assert!(
            failures.is_empty(),
            "{} of 6400 calls killed: {:?}",
            failures.len(),
            failures.first()
        );
    }
}

/// Python: `test_an_add_holds_a_room_whose_id_came_back_before_its_sequence` (RMK-1394):
/// an add waits on a room being deleted, its id created again meanwhile with a sequence:
/// its wait finds no row. Had it gone on to the sequence holding nothing, it would then
/// wait on the room through the event's foreign key, and a deletion holding the new room
/// would wait on that sequence: the server kills one. The trigger stops the add at its
/// insert, the moment that deletion needs.
#[tokio::test]
async fn an_add_holds_a_room_whose_id_came_back_before_its_sequence() {
    // This test's alone: "RMK1394".
    const KEY: i64 = 0x0052_4d4b_3133_3934;
    for auto_indexed in [false, true] {
        let Some((schema, store)) = fresh().await else {
            return;
        };
        let room = room_id("r1");
        store.create_room(Room::new(room.clone())).await.unwrap();
        store
            .add_event_auto_index(&room, event("r1", "first"))
            .await
            .unwrap();
        let outside = schema.client().await;
        outside
            .batch_execute(&format!(
                "CREATE FUNCTION rk_test_stop_insert() RETURNS trigger AS $$ \
                 BEGIN PERFORM pg_advisory_xact_lock({KEY}); RETURN NEW; END $$ \
                 LANGUAGE plpgsql; \
                 CREATE TRIGGER rk_test_stop_insert BEFORE INSERT ON events FOR EACH ROW \
                 WHEN (NEW.source_participant_id = 'stopped') \
                 EXECUTE FUNCTION rk_test_stop_insert(); \
                 SELECT pg_advisory_lock({KEY})"
            ))
            .await
            .unwrap();
        let deleter = schema.client().await;
        deleter
            .batch_execute("BEGIN; DELETE FROM rooms WHERE id = 'r1'")
            .await
            .unwrap();
        let mut stopped = event("r1", "stopped");
        stopped.index = 7;
        stopped.source.participant_id = Some(ParticipantId::new("stopped").unwrap());
        let adding = {
            let (store, room, stopped) = (store.clone(), room.clone(), stopped.clone());
            tokio::spawn(async move {
                if auto_indexed {
                    store.add_event_auto_index(&room, stopped).await
                } else {
                    store.add_event(stopped).await
                }
            })
        };
        until_blocked_by(&schema, &deleter).await;
        deleter
            .batch_execute(
                "INSERT INTO rooms (id) VALUES ('r1'); \
                 INSERT INTO event_sequences (room_id, next_index) VALUES ('r1', 5); COMMIT",
            )
            .await
            .unwrap();
        until_a_session(
            &schema,
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
        )
        .await;
        let deleting = {
            let (store, room) = (store.clone(), room.clone());
            tokio::spawn(async move { store.delete_room(&room).await })
        };
        until_a_session(
            &schema,
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() \
             AND wait_event_type = 'Lock' AND query LIKE 'DELETE FROM rooms%'",
        )
        .await;
        outside
            .batch_execute(&format!("SELECT pg_advisory_unlock({KEY})"))
            .await
            .unwrap();
        let both = async { (adding.await.unwrap(), deleting.await.unwrap()) };
        let (added, removed) = tokio::time::timeout(std::time::Duration::from_secs(3), both)
            .await
            .unwrap();
        assert_eq!(added.unwrap().id, stopped.id);
        assert!(removed.unwrap());
    }
}

/// Python: `test_a_room_deleted_and_created_again_under_its_adds_never_deadlocks`
/// (RMK-1394): adds at full pace on one room id, which a loop deletes and creates
/// again: an add is refused, or stored, and never killed (measured 6 kills in 3 s
/// before). Three seconds here, Python's stress run eight.
#[tokio::test]
async fn a_room_deleted_and_created_again_under_its_adds_never_deadlocks() {
    let Some((schema, _)) = fresh().await else {
        return;
    };
    let store = schema.store();
    store
        .init(InitOptions {
            min_size: 1,
            max_size: 12,
        })
        .await
        .unwrap();
    let room = room_id("r1");
    store.create_room(Room::new(room.clone())).await.unwrap();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut work = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let (store, room, stop) = (store.clone(), room.clone(), std::sync::Arc::clone(&stop));
        work.spawn(async move {
            let mut killed = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                match store.add_event_auto_index(&room, event("r1", "x")).await {
                    Ok(_) | Err(StoreError::RoomNotFound(_)) => {}
                    Err(other) => killed.push(full_text(&other)),
                }
            }
            killed
        });
    }
    {
        let (store, room, stop) = (store.clone(), room.clone(), std::sync::Arc::clone(&stop));
        work.spawn(async move {
            let mut killed = Vec::new();
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                if let Err(failure) = store.delete_room(&room).await {
                    killed.push(full_text(&failure));
                }
                if let Err(failure) = store.create_room(Room::new(room.clone())).await {
                    killed.push(full_text(&failure));
                }
            }
            killed
        });
    }
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut killed = Vec::new();
    while let Some(done) = work.join_next().await {
        killed.extend(done.unwrap());
    }
    assert!(
        killed.is_empty(),
        "{} calls killed: {:?}",
        killed.len(),
        killed.first()
    );
}

/// Python: `test_a_stream_of_adds_starves_nothing_on_its_room` (HITL 0affbb3b): four
/// sessions adding to one room without a pause, and what takes the room's row `FOR
/// UPDATE` still gets its turn. An add holds the room while it queues on the sequence,
/// and a lock the next add could share would never free it.
#[tokio::test]
async fn a_stream_of_adds_starves_nothing_on_its_room() {
    for waiting in ["commit_event", "update_room_if"] {
        let Some((schema, _)) = fresh().await else {
            return;
        };
        let store = schema.store();
        store
            .init(InitOptions {
                min_size: 1,
                max_size: 8,
            })
            .await
            .unwrap();
        let room = room_id("r1");
        let created = store.create_room(Room::new(room.clone())).await.unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut adders = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let (store, room, stop) = (store.clone(), room.clone(), std::sync::Arc::clone(&stop));
            adders.spawn(async move {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    store
                        .add_event_auto_index(&room, event("r1", "x"))
                        .await
                        .unwrap();
                }
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let limit = std::time::Duration::from_secs(2);
        let finished = if waiting == "commit_event" {
            tokio::time::timeout(
                limit,
                store.commit_event(&room, event("r1", "y"), CommitEventOptions::default()),
            )
            .await
            .is_ok()
        } else {
            tokio::time::timeout(
                limit,
                store.update_room_if(created, RoomStatus::Active, None, None),
            )
            .await
            .is_ok()
        };
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        while let Some(added) = adders.join_next().await {
            added.unwrap();
        }
        assert!(finished, "{waiting} waited on the adds");
    }
}

/// Python: `test_the_writers_of_one_room_at_once_never_deadlock` (RMK-1365): 40
/// commits, 40 automatic adds and 40 adds at their own index, on one room, over 40
/// connections: none fails. The own indexes stay far apart, so that a reserved index
/// never meets one.
#[tokio::test]
async fn the_writers_of_one_room_at_once_never_deadlock() {
    let Some((schema, _)) = fresh().await else {
        return;
    };
    let store = schema.store();
    store
        .init(InitOptions {
            min_size: 1,
            max_size: 40,
        })
        .await
        .unwrap();
    let room = room_id("r1");
    store.create_room(Room::new(room.clone())).await.unwrap();
    let mut failures = Vec::new();
    for pass in 0_u64..8 {
        let mut writers = tokio::task::JoinSet::new();
        for own in 0_u64..40 {
            let (first, second, third) = (store.clone(), store.clone(), store.clone());
            let (one, two) = (room.clone(), room.clone());
            writers.spawn(async move {
                first
                    .commit_event(&one, event("r1", "x"), CommitEventOptions::default())
                    .await
            });
            writers.spawn(async move { second.add_event_auto_index(&two, event("r1", "x")).await });
            writers.spawn(async move {
                let mut held = event("r1", "x");
                held.index = (pass * 40 + own + 1) * 1_000_000;
                third.add_event(held).await
            });
        }
        while let Some(written) = writers.join_next().await {
            if let Err(failure) = written.unwrap() {
                failures.push(full_text(&failure));
            }
        }
    }
    assert_eq!(failures.len(), 0, "of 960 calls: {:?}", failures.first());
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

/// Python: `test_an_event_added_beside_the_kit_holds_its_next_delivery_and_loses_none`
/// (RMK-1394): `add_event_auto_index` beside a kit's commits on one room. Each index it
/// takes is a hole of the delivery cursor no lane runs: the kit's next delivery waits
/// out the gap timeout, then skips the hole (RFC §13.5). Every message the kit took is
/// delivered once, in index order, and only added indexes are skipped.
#[tokio::test]
async fn an_event_added_beside_the_kit_holds_its_next_delivery_and_loses_none() {
    let Some((_schema, store)) = fresh().await else {
        return;
    };
    let kit = RoomKit::new(RoomKitOptions {
        delivery_gap_timeout: std::time::Duration::from_millis(50),
        ..RoomKitOptions::with_store(store.clone())
    });
    let out = RecordingTransport::new(
        ChannelId::new("out").unwrap(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    let source = RecordingTransport::new(
        ChannelId::new("src").unwrap(),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(source).unwrap();
    kit.register_channel(out.clone()).unwrap();
    let room = room_id("r1");
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    for id in ["src", "out"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(id).unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    let tap = EventTap::on(&kit, &["delivery_skipped"]);
    let mut sending = tokio::task::JoinSet::new();
    let mut adding = tokio::task::JoinSet::new();
    for number in 0..16 {
        let kit = kit.clone();
        sending.spawn(async move {
            let message = InboundMessage::new(
                ChannelId::new("src").unwrap(),
                "u1",
                TextContent::new(format!("m{number}")),
            );
            let result = kit
                .process_inbound(message, ProcessInboundOptions::default())
                .await
                .unwrap();
            result.event.unwrap().index
        });
        let (store, room) = (store.clone(), room.clone());
        adding.spawn(async move {
            store
                .add_event_auto_index(&room, event("r1", "added"))
                .await
                .unwrap()
                .index
        });
    }
    let mut sent = sending.join_all().await;
    let added = adding.join_all().await;
    kit.close().await.unwrap();
    sent.sort_unstable();
    let delivered: Vec<u64> = out
        .deliveries()
        .iter()
        .map(|delivery| delivery.event.index)
        .collect();
    assert_eq!(delivered, sent);
    let skipped: Vec<u64> = tap
        .events()
        .iter()
        .flat_map(|skip| {
            let from = skip.data["from_index"].as_u64().unwrap();
            let to = skip.data["to_index"].as_u64().unwrap();
            from..=to
        })
        .collect();
    assert_ne!(skipped, [] as [u64; 0]);
    assert!(
        skipped.iter().all(|index| added.contains(index)),
        "{skipped:?} of {added:?}"
    );
}
