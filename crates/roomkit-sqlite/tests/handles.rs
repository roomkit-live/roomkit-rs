//! The file a kit's store holds is let go at the kit's close, as the
//! Python implementation's kit closes its store (decided 2026-10-31),
//! whether a hook holds the kit strongly, a cycle, or weakly
//! (`RoomKit::downgrade`, step 4, milestone 4); a kit read after it
//! answers that the kit is closed.
//!
//! The handles are read from `/proc/self/fd`, so the test runs on Linux.

#![cfg(target_os = "linux")]
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;
#[path = "../../roomkit/tests/support/warnings.rs"]
mod warnings;

use std::path::Path;
use std::time::Duration;

use roomkit::model::{ChannelId, ChannelType, Room, RoomEvent, RoomId};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, GetBindingOptions, GetRoomOptions,
    GetTimelineOptions, HookExecution, HookOptions, HookTrigger, ListBindingsOptions, RoomKit,
    RoomKitError, RoomKitOptions, StoreError,
};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};

use crate::support::TempFile;
use crate::warnings::Warnings;

/// The handles this process holds open on `path`'s database, its journal
/// files included.
fn open_handles(path: &Path) -> usize {
    let database = path.to_string_lossy().into_owned();
    std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(|entry| std::fs::read_link(entry.ok()?.path()).ok())
        .filter(|target| target.to_string_lossy().starts_with(&database))
        .count()
}

/// How a hook holds its kit.
#[derive(Clone, Copy, Debug)]
enum Held {
    Strongly,
    Weakly,
}

/// Whether the kit's `ON_ROOM_CREATED` observer attached a channel to a new
/// room through a kit it holds `held` (RFC Appendix B.1), and the handles
/// left on the kit's file once the kit was shut down, before it is dropped.
async fn left_after_close(held: Held) -> (bool, usize) {
    let file = TempFile::new("handles");
    let kit = RoomKit::new(RoomKitOptions::with_store(SqliteStore::new(
        SqliteStoreOptions {
            path: file.path().into(),
        },
    )));
    let websocket = ChannelId::new("websocket").unwrap();
    kit.register_channel(RecordingTransport::new(
        websocket.clone(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    let (strong, weak) = match held {
        Held::Strongly => (Some(kit.clone()), None),
        Held::Weakly => (None, Some(kit.downgrade())),
    };
    kit.hook(
        HookTrigger::OnRoomCreated,
        move |event: RoomEvent, _| {
            let kit = strong
                .clone()
                .or_else(|| weak.as_ref().and_then(roomkit::WeakRoomKit::upgrade));
            async move {
                if let Some(kit) = kit {
                    kit.attach_channel(
                        &event.room_id,
                        &ChannelId::new("websocket").unwrap(),
                        AttachChannelOptions::default(),
                    )
                    .await
                    .unwrap();
                }
            }
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "attach_agent".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let room = RoomId::new("room_1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    roomkit::testing::idle(&kit).await;
    let bindings = kit.store().list_bindings(&room).await.unwrap();
    let attached = bindings
        .iter()
        .any(|binding| binding.channel_id == websocket);
    kit.close().await.unwrap();
    (attached, open_handles(file.path()))
}

/// The kit's close closes its store, and the store its file, even when a
/// hook holding the kit strongly keeps it from being dropped.
#[tokio::test]
async fn the_kit_s_close_closes_its_file_whatever_holds_the_kit() {
    let (attached_strongly, strongly) = left_after_close(Held::Strongly).await;
    let (attached_weakly, weakly) = left_after_close(Held::Weakly).await;
    assert_eq!(
        (attached_strongly, strongly, attached_weakly, weakly),
        (true, 0, true, 0)
    );
}

/// After the kit's close, its reads answer that the kit is closed, and the
/// store, which the host kept a clone of, that it is closed: what the
/// Python implementation answers (`tasks/probes/step5-lot5/py_kit_closes_store.out`).
#[tokio::test]
async fn a_kit_read_after_its_close_answers_that_the_kit_is_closed() {
    let file = TempFile::new("closed-reads");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let kit = RoomKit::new(RoomKitOptions::with_store(store.clone()));
    let room = RoomId::new("room_1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.close().await.unwrap();
    let kit_closed = |read: Result<(), RoomKitError>| matches!(read, Err(RoomKitError::KitClosed));
    let reads = [
        kit_closed(
            kit.get_room(&room, GetRoomOptions::default())
                .await
                .map(drop),
        ),
        kit_closed(
            kit.get_timeline(&room, GetTimelineOptions::default())
                .await
                .map(drop),
        ),
        kit_closed(
            kit.list_bindings(&room, ListBindingsOptions::default())
                .await
                .map(drop),
        ),
        kit_closed(
            kit.get_binding(
                &room,
                &ChannelId::new("websocket").unwrap(),
                GetBindingOptions::default(),
            )
            .await
            .map(drop),
        ),
    ];
    let stored = match store.get_room(&room).await {
        Err(StoreError::Unavailable(cause)) => cause.to_string(),
        other => format!("{other:?}"),
    };
    assert_eq!(
        (reads, stored.as_str()),
        ([true, true, true, true], "SQLiteStore is closed")
    );
}

// Python: tests/test_kit_close.py::test_a_write_held_past_the_unwind_does_not_hold_the_close_of_the_kit_s_sqlite_file
/// A write waiting for the file's write lock, which another process holds,
/// holds the store's writer past its caller's cut: the kit's close gives it a
/// moment, names it, and returns (RFC §12.10.4); the file closes once the
/// write ends. Python's call is cut by the kit's close past its grace, 50 ms
/// there; here the test cuts it, the kit's grace being 5 s.
#[tokio::test]
async fn a_write_held_past_the_unwind_does_not_hold_the_close_of_the_kit_s_sqlite_file() {
    let file = TempFile::new("held-write");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let kit = RoomKit::new(RoomKitOptions::with_store(store.clone()));
    kit.create_room(CreateRoomOptions {
        room_id: Some(RoomId::new("room_1").unwrap()),
        ..Default::default()
    })
    .await
    .unwrap();
    // Another process's write in progress: the store's write waits for the
    // file's lock, its busy timeout 5 s.
    let other = rusqlite::Connection::open(file.path()).unwrap();
    other.execute_batch("BEGIN IMMEDIATE").unwrap();
    let writing = tokio::spawn(async move {
        store
            .create_room(Room::new(RoomId::new("room_2").unwrap()))
            .await
    });
    tokio::task::yield_now().await;
    // The write's caller is cut once its write is under way, as the kit's
    // close cuts a call: the write runs on in SQLite.
    writing.abort();
    let (warnings, _capture) = Warnings::capture();
    let closed = tokio::time::timeout(Duration::from_secs(2), kit.close()).await;
    drop(other);
    closed
        .expect("the kit's close waited for the write")
        .unwrap();
    assert_eq!(
        warnings
            .containing("SQLiteStore closed with an operation still running")
            .len(),
        1
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while open_handles(file.path()) > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the file stays open"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}
