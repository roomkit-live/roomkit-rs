//! The shared schema (step 4, decision 2): a file the Python
//! implementation's `SQLiteStore` wrote is read and rewritten by
//! [`SqliteStore`] with nothing lost, and what Python reads beside the
//! records is kept right.
//!
//! The file is `tests/fixtures/python-v12.db`, written by
//! `tests/fixtures/make_fixture.py` (`make sqlite-fixture`), at the roomkit
//! commit `tests/fixtures/ROOMKIT_COMMIT` names. Each test opens a copy.
//! The other direction, Python reading a file this crate wrote, is for
//! roomkit to test with a fixture of its own.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelId, ChannelType, DeleteContent, EventContent, EventType, EventUpdates, IdentityId,
    InboundMessage, RoomId, RoomUpdates, Timestamp,
};
use roomkit::testing::RecordingTransport;
use roomkit::{ConversationStore, RoomFilter, RoomKit, SequentialEnvironment};
use roomkit_sqlite::{OpenError, SqliteStore};
use rusqlite::Connection;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::support::TempFile;

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn fixture() -> PathBuf {
    fixtures().join("python-v12.db")
}

/// A copy of the fixture, which a test may write.
fn copy() -> TempFile {
    let file = TempFile::new("cross-read");
    std::fs::copy(fixture(), file.path()).unwrap();
    file
}

fn room(id: &str) -> RoomId {
    RoomId::new(id).unwrap()
}

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

/// A kit on the copy `file`, its clock fixed at the last instant the
/// fixture's `room-full` was written: its timers, armed when Python wrote
/// it, never fall due under a test, whenever the test runs (RFC §5.1).
async fn fixture_kit(file: &TempFile) -> RoomKit<SqliteStore> {
    let store = SqliteStore::open(file.path()).await.unwrap();
    let room = store.get_room(&room("room-full")).await.unwrap().unwrap();
    let written = room
        .timers
        .last_activity_at
        .map_or(room.updated_at, |activity| activity.max(room.updated_at));
    let kit = RoomKit::with_store(store);
    kit.set_environment(SequentialEnvironment::new(written));
    kit
}

/// The fixture is the one `make sqlite-fixture` wrote, at a roomkit commit.
#[test]
fn the_fixture_is_the_one_its_generator_wrote() {
    let commit = std::fs::read_to_string(fixtures().join("ROOMKIT_COMMIT")).unwrap();
    let recorded = std::fs::read_to_string(fixtures().join("FIXTURE_SHA256")).unwrap();
    let digest = hex(&Sha256::digest(std::fs::read(fixture()).unwrap()));
    assert_eq!(
        (
            commit.trim().len() == 40 && commit.trim().bytes().all(|byte| byte.is_ascii_hexdigit()),
            digest.as_str()
        ),
        (true, recorded.trim()),
        "the fixture differs from its generation: run `make sqlite-fixture`"
    );
}

/// Every record row of the file, by table and key: its JSON, and its
/// indexed columns; then the tables the records derive and those this
/// crate leaves alone, whole.
fn rows(path: &Path) -> BTreeMap<String, BTreeMap<String, Value>> {
    let connection = Connection::open(path).unwrap();
    let queries = [
        (
            "rooms",
            "SELECT id, json_object('data', json(data), 'organization_id', organization_id, 'status', status, 'delivered_index', delivered_index, 'created_ts', created_ts) FROM rooms",
        ),
        (
            "events",
            "SELECT id, json_object('data', json(data), 'room_id', room_id, 'idx', idx, 'type', type, 'visibility', visibility, 'source_channel_id', source_channel_id, 'source_channel_type', source_channel_type, 'participant_id', participant_id, 'correlation_id', correlation_id, 'parent_event_id', parent_event_id, 'idempotency_key', idempotency_key, 'created_ts', created_ts, 'responds_to', responds_to) FROM events",
        ),
        (
            "bindings",
            "SELECT room_id || '/' || channel_id, json_object('data', json(data), 'participant_id', participant_id, 'channel_type', channel_type) FROM bindings",
        ),
        (
            "participants",
            "SELECT room_id || '/' || id, json_object('data', json(data)) FROM participants",
        ),
        (
            "identities",
            "SELECT id, json_object('data', json(data)) FROM identities",
        ),
        (
            "tasks",
            "SELECT id, json_object('data', json(data), 'room_id', room_id, 'status', status) FROM tasks",
        ),
        (
            "observations",
            "SELECT id, json_object('data', json(data), 'room_id', room_id) FROM observations",
        ),
        (
            "private_state",
            "SELECT room_id || '/' || key, json_object('value', json(value)) FROM private_state",
        ),
        (
            "identity_addresses",
            "SELECT channel_type || '/' || address || '/' || organization_id, json_object('identity_id', identity_id) FROM identity_addresses",
        ),
        (
            "idempotency",
            "SELECT room_id || '/' || channel_id || '/' || key, json('{}') FROM idempotency",
        ),
        (
            "event_sequences",
            "SELECT room_id, json_object('next_index', next_index) FROM event_sequences",
        ),
        (
            "events_fts",
            "SELECT event_id, json_object('text', text, 'room_id', room_id) FROM events_fts",
        ),
        (
            "read_markers",
            "SELECT room_id || '/' || channel_id, json_object('event_index', event_index) FROM read_markers",
        ),
        (
            "verifications",
            "SELECT identity_id || '/' || organization_id, json(data) FROM verifications",
        ),
        (
            "verification_requests",
            "SELECT id, json(data) FROM verification_requests",
        ),
        (
            "voice_deliveries",
            "SELECT room_id || '/' || key_hash, json(data) FROM voice_deliveries",
        ),
    ];
    queries
        .into_iter()
        .map(|(table, sql)| {
            let mut statement = connection.prepare(sql).unwrap();
            let rows = statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .unwrap()
                .map(|row| {
                    let (key, value) = row.unwrap();
                    (key, serde_json::from_str(&value).unwrap())
                })
                .collect();
            (table.to_owned(), rows)
        })
        .collect()
}

/// What `after` lost or changed of `before`, member by member, and what it
/// added: an instant compares as the instant it writes, whatever its form,
/// and a number as its value.
fn differences(
    path: &str,
    before: &Value,
    after: &Value,
    lost: &mut Vec<String>,
    added: &mut BTreeSet<String>,
) {
    match (before, after) {
        (Value::Object(before), Value::Object(after)) => {
            for (member, value) in before {
                let path = format!("{path}.{member}");
                match after.get(member) {
                    Some(rewritten) => differences(&path, value, rewritten, lost, added),
                    None => lost.push(format!("{path} lost: {value}")),
                }
            }
            for member in after.keys().filter(|member| !before.contains_key(*member)) {
                added.insert(format!("{path}.{member}"));
            }
        }
        (Value::Array(before), Value::Array(after)) if before.len() == after.len() => {
            for (value, rewritten) in before.iter().zip(after) {
                differences(&format!("{path}[]"), value, rewritten, lost, added);
            }
        }
        (Value::Number(before), Value::Number(after)) => {
            let (before, after) = (before.as_f64().unwrap(), after.as_f64().unwrap());
            if (before - after).abs() > 1e-6 {
                lost.push(format!("{path} changed: {before} to {after}"));
            }
        }
        (Value::String(before), Value::String(after)) if before != after => {
            let instants = (before.parse::<Timestamp>(), after.parse::<Timestamp>());
            if !matches!(instants, (Ok(before), Ok(after)) if before == after) {
                lost.push(format!("{path} changed: {before:?} to {after:?}"));
            }
        }
        _ if before != after => lost.push(format!("{path} changed: {before} to {after}")),
        _ => {}
    }
}

/// Rewrites every record of the file through the store, each as it reads
/// it.
async fn rewrite_everything(store: &SqliteStore) {
    let full = room("room-full");
    for room in store.list_rooms(&RoomFilter::default()).await.unwrap() {
        store
            .update_room(&room.id, RoomUpdates::default())
            .await
            .unwrap();
    }
    for binding in store.list_bindings(&full).await.unwrap() {
        store.store_binding(binding).await.unwrap();
    }
    for participant in store.list_participants(&full).await.unwrap() {
        store.update_participant(participant).await.unwrap();
    }
    for event in store.list_events(&full, true).await.unwrap() {
        store
            .update_event(&event.id, EventUpdates::default())
            .await
            .unwrap();
    }
    let identity = IdentityId::new("id-jean").unwrap();
    store
        .link_address(&identity, ChannelType::SMS, "+15551234567", Some("org-1"))
        .await
        .unwrap();
    let (tasks, observations) = (
        store.list_tasks(&full).await.unwrap(),
        store.list_observations(&full).await.unwrap(),
    );
    store
        .persist_side_effects(&full, tasks, observations)
        .await
        .unwrap();
    let register = store
        .get_private_state(&full, "author_register")
        .await
        .unwrap();
    store
        .set_private_state(&full, "author_register", register)
        .await
        .unwrap();
}

/// Every record Python wrote is read, and rewriting each loses none of its
/// members, changes no indexed column, and leaves every other table as it
/// was. A rewrite adds only the members of the RFC Python does not model
/// (docs/rfc-gaps.md, "Found by step 4's shared SQLite file").
#[tokio::test]
async fn a_rewrite_keeps_everything_python_wrote() {
    let file = copy();
    let before = rows(file.path());
    let store = SqliteStore::open(file.path()).await.unwrap();
    rewrite_everything(&store).await;
    let after = rows(file.path());
    let (mut lost, mut added) = (Vec::new(), BTreeSet::new());
    for (table, rows) in &before {
        for (key, value) in rows {
            match after.get(table).and_then(|rows| rows.get(key)) {
                Some(rewritten) => differences(
                    &format!("{table}/{key}"),
                    value,
                    rewritten,
                    &mut lost,
                    &mut added,
                ),
                None => lost.push(format!("{table}/{key} lost")),
            }
        }
    }
    let added: BTreeSet<String> = added
        .into_iter()
        .map(|path| {
            path.split_once('.')
                .map(|(_, member)| member.to_owned())
                .unwrap_or(path)
        })
        .collect();
    let row_counts = |rows: &BTreeMap<String, BTreeMap<String, Value>>| {
        rows.iter()
            .map(|(table, rows)| (table.clone(), rows.len()))
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(
        (lost, added, row_counts(&after)),
        (
            Vec::<String>::new(),
            [
                "data.capabilities.rate_limit",
                "data.consumption",
                "data.rate_limit.burst",
            ]
            .map(str::to_owned)
            .into(),
            row_counts(&before)
        )
    );
}

/// Every record reads as the store's model: the rooms, their bindings of
/// the types Python writes and the RFC does not list, the timeline with its
/// refused events and a hard delete's gap, the people, the identity with an
/// address of a type no implementation lists, the side effects and the
/// register of authors.
#[tokio::test]
async fn every_record_python_wrote_reads() {
    let file = copy();
    let store = SqliteStore::open(file.path()).await.unwrap();
    let full = room("room-full");
    let rooms: Vec<String> = store
        .list_rooms(&RoomFilter::default())
        .await
        .unwrap()
        .into_iter()
        .map(|room| room.id.to_string())
        .collect();
    let types: Vec<String> = store
        .list_bindings(&full)
        .await
        .unwrap()
        .into_iter()
        .map(|binding| binding.channel_type.to_string())
        .collect();
    let timeline = store.list_events(&full, true).await.unwrap();
    let indexes: Vec<u64> = timeline.iter().map(|event| event.index).collect();
    let slack = ChannelType::new("slack").unwrap();
    let resolved = store
        .resolve_identity(slack, "U024BE7LH", Some("org-1"))
        .await
        .unwrap()
        .map(|identity| identity.id.to_string());
    let register = store
        .load_context(&full, 5)
        .await
        .unwrap()
        .unwrap()
        .author_register
        .is_some();
    assert_eq!(
        (
            rooms,
            types,
            indexes,
            store.list_participants(&full).await.unwrap().len(),
            resolved,
            store.list_tasks(&full).await.unwrap().len(),
            store.list_observations(&full).await.unwrap().len(),
            register
        ),
        (
            vec!["room-closed".to_owned(), "room-full".to_owned()],
            ["websocket", "sms", "ai", "discord", "buzz"]
                .map(str::to_owned)
                .to_vec(),
            (0..=19).filter(|index| *index != 10).collect(),
            1,
            Some("id-jean".to_owned()),
            1,
            1,
            true
        )
    );
}

/// RFC §7.5 rule 7, gap G-049: a channel the Python kit detached stays
/// detached for roomkit-rs: its next message is stored `BLOCKED`
/// `source_unbound`, the channel left unbound, and a channel bound still
/// takes its message, through the public API.
#[tokio::test]
async fn a_channel_python_detached_stays_detached() {
    let file = copy();
    let kit = fixture_kit(&file).await;
    for id in ["ws-gone", "ws-user"] {
        kit.register_channel(RecordingTransport::new(channel(id), ChannelType::WEBSOCKET))
            .unwrap();
    }
    let full = room("room-full");
    let mut results = Vec::new();
    for id in ["ws-gone", "ws-user"] {
        let message = InboundMessage::new(channel(id), "user-1", EventContent::text("Still here"));
        let result = kit.process_inbound_in(&full, message).await.unwrap();
        results.push((result.blocked, result.reason));
    }
    let bound: Vec<String> = kit
        .store()
        .list_bindings(&full)
        .await
        .unwrap()
        .into_iter()
        .map(|binding| binding.channel_id.to_string())
        .filter(|id| id.starts_with("ws-"))
        .collect();
    kit.shutdown().await;
    assert_eq!(
        (results, bound),
        (
            vec![(true, Some("source_unbound".to_owned())), (false, None)],
            vec!["ws-user".to_owned()]
        )
    );
}

/// The rows Python reads beside the timeline, for a message roomkit-rs
/// commits, edits to deleted and then removes: its idempotency key, its
/// text in the full-text index, which a deleted message leaves (RFC
/// §10.3), and the room's next index.
#[tokio::test]
async fn what_python_reads_beside_the_timeline_is_kept_right() {
    let file = copy();
    let kit = fixture_kit(&file).await;
    kit.register_channel(RecordingTransport::new(
        channel("ws-user"),
        ChannelType::WEBSOCKET,
    ))
    .unwrap();
    let full = room("room-full");
    let mut message = InboundMessage::new(
        channel("ws-user"),
        "user-1",
        EventContent::text("searchable words"),
    );
    message.idempotency_key = Some("rust-key".to_owned());
    let event = kit
        .process_inbound_in(&full, message)
        .await
        .unwrap()
        .event
        .unwrap();
    let derived = |path: &Path| {
        let connection = Connection::open(path).unwrap();
        let count = |sql: &str| -> i64 {
            connection
                .query_row(sql, [event.id.as_str()], |row| row.get(0))
                .unwrap()
        };
        (
            count("SELECT COUNT(*) FROM idempotency WHERE key = 'rust-key' AND ?1 = ?1"),
            count(
                "SELECT COUNT(*) FROM events_fts WHERE event_id = ?1 AND text = 'searchable words'",
            ),
            count("SELECT next_index FROM event_sequences WHERE room_id = 'room-full' AND ?1 = ?1"),
        )
    };
    let committed = derived(file.path());
    let mut deleted = event.metadata.clone();
    deleted.insert("deleted".to_owned(), true.into());
    let mut marking = EventUpdates::default();
    marking.metadata = Some(deleted);
    kit.store().update_event(&event.id, marking).await.unwrap();
    let marked = derived(file.path());
    kit.store()
        .delete_event(&full, &event.id, true)
        .await
        .unwrap();
    let removed = derived(file.path());
    kit.shutdown().await;
    assert_eq!(
        (committed, marked, removed),
        ((1, 1, 21), (1, 0, 21), (0, 0, 21))
    );
}

/// The delivery cursor is its column's, which Python's advance writes
/// alone (RFC §13.5).
#[tokio::test]
async fn the_delivery_cursor_is_its_column_s() {
    let file = copy();
    Connection::open(file.path())
        .unwrap()
        .execute(
            "UPDATE rooms SET delivered_index = 17 WHERE id = 'room-full'",
            [],
        )
        .unwrap();
    let store = SqliteStore::open(file.path()).await.unwrap();
    let full = room("room-full");
    assert_eq!(
        (
            store.get_delivered_index(&full).await,
            store
                .get_room(&full)
                .await
                .unwrap()
                .map(|room| room.delivered_index)
        ),
        (Ok(17), Some(17))
    );
}

/// A schema entry: its type, its name, its table and its statement.
type Entry = (String, String, String, Option<String>);

/// The schema of `path`: each table's and index's statement, and the
/// version.
fn schema(path: &Path) -> (Vec<Entry>, i64) {
    let connection = Connection::open(path).unwrap();
    let mut statement = connection
        .prepare("SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name")
        .unwrap();
    let entries = statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let version = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    (entries, version)
}

/// A new file holds the schema Python creates, statement for statement, at
/// version 12.
#[tokio::test]
async fn a_new_file_is_created_as_python_creates_it() {
    let file = TempFile::new("created");
    drop(SqliteStore::open(file.path()).await.unwrap());
    assert_eq!(schema(file.path()), schema(&fixture()));
}

/// A file of a newer schema is refused and left as it is; one of an older
/// schema is refused too, for Python to migrate.
#[tokio::test]
async fn a_file_of_another_schema_version_is_refused() {
    let mut refused = Vec::new();
    for version in [13, 11] {
        let file = copy();
        Connection::open(file.path())
            .unwrap()
            .pragma_update(None, "user_version", version)
            .unwrap();
        let written = std::fs::read(file.path()).unwrap();
        let opened = SqliteStore::open(file.path()).await.map(drop);
        let left = std::fs::read(file.path()).unwrap() == written;
        refused.push((
            matches!(
                opened,
                Err(OpenError::NewerSchema {
                    found: 13,
                    supported: 12
                } | OpenError::OlderSchema {
                    found: 11,
                    supported: 12
                })
            ),
            left,
        ));
    }
    assert_eq!(refused, [(true, true), (true, true)]);
}

/// A file that is no database fails with SQLite's error as the cause, read
/// through the source chain: the SQLite library's type is no part of the
/// error's API.
#[tokio::test]
async fn a_file_that_is_no_database_fails_with_sqlite_s_error_as_its_cause() {
    let file = TempFile::new("not-a-database");
    std::fs::write(file.path(), [b'x'; 4096]).unwrap();
    let opened = SqliteStore::open(file.path()).await.map(drop);
    let cause = match &opened {
        Err(error @ OpenError::Database(_)) => std::error::Error::source(error)
            .and_then(|cause| cause.downcast_ref::<rusqlite::Error>())
            .and_then(rusqlite::Error::sqlite_error_code),
        _ => None,
    };
    assert_eq!(cause, Some(rusqlite::ErrorCode::NotADatabase));
}

/// RFC §10.3: an edit holds a version of the text its target said. A
/// delete roomkit-rs commits marks the target's edits deleted, as the
/// Python implementation's store does and its histories read, so a Python
/// process sharing the file withholds them too.
#[tokio::test]
async fn a_delete_marks_its_target_s_edits_as_python_reads_them() {
    let file = copy();
    let kit = fixture_kit(&file).await;
    kit.register_channel(RecordingTransport::new(
        channel("ws-user"),
        ChannelType::WEBSOCKET,
    ))
    .unwrap();
    let full = room("room-full");
    let timeline = kit.store().list_events(&full, true).await.unwrap();
    let edited = timeline
        .iter()
        .find(|event| event.index == 3)
        .unwrap()
        .id
        .clone();
    let mut delete = InboundMessage::new(
        channel("ws-user"),
        "user-1",
        EventContent::Delete(DeleteContent::new(edited.clone())),
    );
    delete.event_type = EventType::Delete;
    let result = kit.process_inbound_in(&full, delete).await.unwrap();
    kit.shutdown().await;
    let marks: Vec<Option<bool>> = {
        let connection = Connection::open(file.path()).unwrap();
        let mut statement = connection
            .prepare(
                "SELECT json_extract(data, '$.metadata.deleted') FROM events
                 WHERE id = ?1 OR json_extract(data, '$.content.target_event_id') = ?1
                     AND type = 'edit'
                 ORDER BY idx",
            )
            .unwrap();
        statement
            .query_map([edited.as_str()], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    };
    assert_eq!(
        (result.blocked, marks),
        (false, vec![Some(true), Some(true)])
    );
}

/// A file in a directory that does not exist yet is created with the
/// directory, as the Python implementation creates it.
#[tokio::test]
async fn a_file_s_missing_directory_is_created() {
    let directory = TempFile::new("missing");
    let path = directory.path().with_extension("").join("nested/rooms.db");
    let opened = SqliteStore::open(&path).await.map(drop);
    let created = path.exists();
    std::fs::remove_dir_all(directory.path().with_extension("")).ok();
    assert_eq!((opened.is_ok(), created), (true, true));
}

/// The Python implementation takes any key in an identity's addresses; a
/// key that is no channel type's name for roomkit-rs (`Slack`) is a member
/// it does not model. The identity reads and resolves on its other
/// addresses, the key unread, and a rewrite keeps it as Python wrote it.
#[tokio::test]
async fn an_identity_with_a_key_rust_does_not_read_resolves_and_keeps_it() {
    let file = copy();
    let store = SqliteStore::open(file.path()).await.unwrap();
    let lena = IdentityId::new("id-lena").unwrap();
    let resolved = store
        .resolve_identity(ChannelType::SMS, "+15559876543", Some("org-1"))
        .await
        .map(|identity| identity.map(|identity| identity.id.to_string()));
    let read = store
        .get_identity(&lena)
        .await
        .map(|identity| identity.map(|identity| identity.channel_addresses.len()));
    store
        .link_address(&lena, ChannelType::EMAIL, "lena@example.com", Some("org-1"))
        .await
        .unwrap();
    let kept: Value = {
        let connection = Connection::open(file.path()).unwrap();
        connection
            .query_row(
                "SELECT json_extract(data, '$.channel_addresses') FROM identities WHERE id = 'id-lena'",
                [],
                |row| row.get::<_, String>(0),
            )
            .map(|text| serde_json::from_str(&text).unwrap())
            .unwrap()
    };
    assert_eq!(
        (resolved, read, kept),
        (
            Ok(Some("id-lena".to_owned())),
            Ok(Some(1)),
            serde_json::json!({
                "sms": ["+15559876543"],
                "Slack": ["U07LENA"],
                "email": ["lena@example.com"],
            })
        )
    );
}
