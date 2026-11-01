//! A proactive delivery's key outlives the process on a durable store, as
//! RFC §22.1 asks: a replay after a restart is that publication.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use roomkit::model::{ChannelId, ChannelType, RoomId};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{AttachChannelOptions, CreateRoomOptions, DeliverOptions, RoomKit, RoomKitOptions};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};

use crate::support::TempFile;

/// A kit over the file `file`, its transport `text` registered.
fn kit_over(file: &TempFile) -> RoomKit<SqliteStore> {
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    let kit = RoomKit::new(RoomKitOptions::with_store(store));
    let transport = RecordingTransport::new(
        ChannelId::new("text").unwrap(),
        ChannelType::WEBSOCKET,
        RecordingTransportOptions::default(),
    );
    kit.register_channel(transport).unwrap();
    kit
}

fn keyed(key: &str) -> DeliverOptions {
    DeliverOptions {
        idempotency_key: Some(key.to_owned()),
        ..DeliverOptions::default()
    }
}

// Python: tests/test_proactive_delivery.py::test_keys_survive_sqlite_restart_and_are_scoped_to_room
#[tokio::test]
async fn keys_survive_sqlite_restart_and_are_scoped_to_room() {
    let file = TempFile::new("publications");
    let (r1, r2) = (RoomId::new("r1").unwrap(), RoomId::new("r2").unwrap());
    let text = ChannelId::new("text").unwrap();
    let kit = kit_over(&file);
    let create = |room: &RoomId| CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create(&r1)).await.unwrap();
    kit.attach_channel(&r1, &text, AttachChannelOptions::default())
        .await
        .unwrap();
    let first = kit.deliver(&r1, "done", keyed("key")).await.unwrap();
    kit.close().await.unwrap();
    drop(kit);

    let kit = kit_over(&file);
    let replay = kit.deliver(&r1, "done", keyed("key")).await.unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.event_id, first.event_id);
    kit.create_room(create(&r2)).await.unwrap();
    kit.attach_channel(&r2, &text, AttachChannelOptions::default())
        .await
        .unwrap();
    let other = kit.deliver(&r2, "done", keyed("key")).await.unwrap();
    assert!(!other.duplicate);
    assert_ne!(other.event_id, first.event_id);
    kit.close().await.unwrap();
}
