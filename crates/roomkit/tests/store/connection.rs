//! Python: `tests/test_store_memory.py::TestConnectionTenure`.

use roomkit::model::{ChannelBinding, ChannelId, ChannelType, Room, RoomId};
use roomkit::{ConversationStore, InMemoryStore, StoreError};

#[tokio::test]
async fn nested_scopes_return_the_same_room_and_bindings_as_direct_calls() {
    let store = InMemoryStore::new();
    let room = RoomId::new("r1").unwrap();
    let binding = ChannelBinding::new(
        ChannelId::new("ch1").unwrap(),
        room.clone(),
        ChannelType::SMS,
    );
    store.create_room(Room::new(room.clone())).await.unwrap();
    store.add_binding(binding).await.unwrap();
    let scoped = store
        .connection(async {
            let found = store.get_room(&room).await?;
            let bindings = store.connection(store.list_bindings(&room)).await?;
            Ok::<_, StoreError>((found, bindings))
        })
        .await
        .unwrap();
    assert!(scoped.0.is_some());
    assert_eq!(scoped.1.len(), 1);
    assert_eq!(
        scoped,
        (
            store.get_room(&room).await.unwrap(),
            store.list_bindings(&room).await.unwrap()
        )
    );
}

#[tokio::test]
async fn the_callers_error_preserves_an_already_completed_write() {
    let store = InMemoryStore::new();
    let room = RoomId::new("written").unwrap();
    let error = store
        .connection(async {
            store.create_room(Room::new(room.clone())).await?;
            Err::<(), Box<dyn std::error::Error + Send + Sync>>("application failure".into())
        })
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "application failure");
    assert!(store.get_room(&room).await.unwrap().is_some());
}

#[tokio::test]
async fn the_default_context_loader_enters_the_backends_connection_block() {
    use std::sync::atomic::Ordering;
    let store = super::connection_store::ScopedReads::default();
    let room = RoomId::new("r1").unwrap();
    store.create_room(Room::new(room.clone())).await.unwrap();
    let binding = ChannelBinding::new(
        ChannelId::new("ch1").unwrap(),
        room.clone(),
        ChannelType::SMS,
    );
    store.add_binding(binding.clone()).await.unwrap();
    let (found, bindings, participants) = store.load_room_context(&room).await.unwrap();
    assert_eq!(found, store.get_room(&room).await.unwrap());
    assert_eq!(bindings, vec![binding]);
    assert_eq!(participants, Vec::new());
    assert_eq!(store.entries.load(Ordering::Relaxed), 1);
    let missing = store
        .load_room_context(&RoomId::new("missing").unwrap())
        .await
        .unwrap();
    assert_eq!(missing, (None, Vec::new(), Vec::new()));
    assert_eq!(store.entries.load(Ordering::Relaxed), 2);
}
