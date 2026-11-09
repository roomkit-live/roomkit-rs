//! Index and key allocation on independent connections to one database.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use std::sync::Arc;

use roomkit::model::{ChannelId, ChannelType, EventSource, Room, RoomEvent, RoomId, TextContent};
use roomkit::store::CommitEventOptions;
use roomkit::{ConversationStore, StoreError};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

use crate::support::TempFile;

fn event(room: &RoomId, key: Option<&str>) -> RoomEvent {
    let mut event = RoomEvent::new(
        room.clone(),
        EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
        TextContent::new("message"),
    );
    event.idempotency_key = key.map(str::to_owned);
    event
}

/// Opens two stores, not two clones of one connection owner.
async fn connections(file: &TempFile) -> (SqliteStore, SqliteStore, RoomId) {
    let open = || {
        SqliteStore::new(SqliteStoreOptions {
            path: file.path().into(),
        })
    };
    let (first, second) = (open(), open());
    let room = RoomId::new("r1").unwrap();
    first.create_room(Room::new(room.clone())).await.unwrap();
    assert!(second.room_exists(&room).await.unwrap());
    (first, second, room)
}

// Python: tests/test_store_sqlite.py::TestPersistence::test_two_connections_allocate_one_monotonic_sequence
#[tokio::test]
async fn two_connections_allocate_one_monotonic_sequence() {
    let file = TempFile::new("commit-sequence");
    let (first, second, room) = connections(&file).await;
    let start = Arc::new(Barrier::new(21));
    let mut writes = JoinSet::new();
    for i in 0..20 {
        let store = if i % 2 == 0 {
            first.clone()
        } else {
            second.clone()
        };
        let (start, room) = (Arc::clone(&start), room.clone());
        writes.spawn(async move {
            start.wait().await;
            store
                .commit_event(&room, event(&room, None), CommitEventOptions::default())
                .await
        });
    }
    start.wait().await;
    let mut indexes = Vec::new();
    while let Some(written) = writes.join_next().await {
        indexes.push(written.unwrap().unwrap().index);
    }
    indexes.sort_unstable();
    assert_eq!(indexes, (0..20).collect::<Vec<_>>());
    let stored = second.get_room(&room).await.unwrap().unwrap();
    assert_eq!((stored.event_count, stored.latest_index), (20, Some(19)));
    first.close().await;
    second.close().await;
}

// Python: tests/test_store_sqlite.py::TestPersistence::test_duplicate_idempotency_key_is_rejected_across_connections
#[tokio::test]
async fn duplicate_idempotency_key_is_rejected_across_connections() {
    let file = TempFile::new("commit-key");
    let (first, second, room) = connections(&file).await;
    let (one, two) = tokio::join!(
        first.commit_event(
            &room,
            event(&room, Some("k")),
            CommitEventOptions::default()
        ),
        second.commit_event(
            &room,
            event(&room, Some("k")),
            CommitEventOptions::default()
        ),
    );
    let (winner, refused) = match (one, two) {
        (Ok(winner), Err(refused)) | (Err(refused), Ok(winner)) => (winner, refused),
        other => panic!("expected exactly one commit, got {other:?}"),
    };
    assert!(matches!(refused, StoreError::IdempotencyKeyTaken { .. }));
    let held = second
        .get_event_by_idempotency_key(&room, "k", None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((held.id, held.index), (winner.id, 0));
    let next = first
        .commit_event(&room, event(&room, None), CommitEventOptions::default())
        .await
        .unwrap();
    let stored = second.get_room(&room).await.unwrap().unwrap();
    assert_eq!(
        (next.index, stored.event_count, stored.latest_index),
        (1, 2, Some(1))
    );
    first.close().await;
    second.close().await;
}
