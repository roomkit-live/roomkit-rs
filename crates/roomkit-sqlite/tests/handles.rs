//! The file a kit's store holds is let go once the host drops the kit
//! (step 4, milestone 4): a hook that holds the kit holds it weakly
//! (`RoomKit::downgrade`), so that the kit, its hooks and its store make
//! no cycle.
//!
//! The handles are read from `/proc/self/fd`, so the test runs on Linux.

#![cfg(target_os = "linux")]
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use std::path::Path;

use roomkit::model::{ChannelId, ChannelType, RoomEvent, RoomId};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, ConversationStore, CreateRoomOptions, HookExecution, HookOptions,
    HookTrigger, RoomKit, RoomKitOptions,
};
use roomkit_sqlite::SqliteStore;

use crate::support::TempFile;

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
/// left on the kit's file once the kit was shut down and dropped.
async fn left_after_drop(held: Held) -> (bool, usize) {
    let file = TempFile::new("handles");
    let kit = RoomKit::new(RoomKitOptions::with_store(
        SqliteStore::open(file.path()).await.unwrap(),
    ));
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
    drop(kit);
    (attached, open_handles(file.path()))
}

/// A hook holding its kit weakly lets the kit go, and the store its file;
/// one holding it strongly keeps both, a cycle the host cannot break.
#[tokio::test]
async fn a_hook_holding_its_kit_weakly_leaves_the_file_closed_once_the_kit_is_dropped() {
    let (attached_strongly, strongly) = left_after_drop(Held::Strongly).await;
    let (attached_weakly, weakly) = left_after_drop(Held::Weakly).await;
    assert_eq!(
        (attached_strongly, strongly > 0, attached_weakly, weakly),
        (true, true, true, 0)
    );
}
