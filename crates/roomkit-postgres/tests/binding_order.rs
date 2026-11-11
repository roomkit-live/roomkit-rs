//! Binding order is persistent, independent of ids, timestamps and updates.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use roomkit::ConversationStore;
use roomkit::model::{ChannelBinding, ChannelId, ChannelType, Room, RoomId};
use roomkit_postgres::PostgresStore;

use support::{SMALL_POOL, TempSchema, dsn};

fn room() -> RoomId {
    RoomId::new("r1").unwrap()
}

fn binding(name: &str, at: &str) -> ChannelBinding {
    let mut binding = ChannelBinding::new(ChannelId::new(name).unwrap(), room(), ChannelType::SMS);
    binding.attached_at = at.parse().unwrap();
    binding
}

async fn check_order(store: &PostgresStore, expected: &[&str]) {
    let bindings = store.list_bindings(&room()).await.unwrap();
    let (_, contextual, _) = store.load_room_context(&room()).await.unwrap();
    for read in [bindings, contextual] {
        let ids: Vec<_> = read.iter().map(|b| b.channel_id.as_str()).collect();
        assert_eq!(ids, expected);
    }
}

/// Python: tests/test_store_binding_contract.py::test_binding_order_survives_updates_and_reattach.
#[tokio::test]
async fn binding_order_survives_updates_and_reattach() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    store.create_room(Room::new(room())).await.unwrap();
    let first = binding("z", "2026-01-01T00:00:00Z");
    let second = binding("a", "2026-01-01T00:00:00Z");
    store.add_binding(first.clone()).await.unwrap();
    store.add_binding(second).await.unwrap();
    check_order(&store, &["z", "a"]).await;
    let mut changed = first.clone();
    changed.muted = true;
    store.update_binding(changed.clone()).await.unwrap();
    check_order(&store, &["z", "a"]).await;
    store.add_binding(changed).await.unwrap();
    check_order(&store, &["z", "a"]).await;
    store
        .remove_binding(&room(), &first.channel_id)
        .await
        .unwrap();
    store.add_binding(first).await.unwrap();
    check_order(&store, &["a", "z"]).await;
    store.close().await;
    let reopened = schema.initialized_store().await;
    check_order(&reopened, &["a", "z"]).await;
    reopened.close().await;
}

/// Python: tests/test_store_postgres.py::test_binding_order_is_restored_when_a_legacy_schema_is_upgraded.
#[tokio::test]
async fn binding_order_is_restored_when_a_legacy_schema_is_upgraded() {
    if dsn().is_none() {
        return;
    }
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    store.create_room(Room::new(room())).await.unwrap();
    let bindings = [
        binding("c", "2026-01-01T00:00:01Z"),
        binding("z", "2026-01-01T00:00:00Z"),
        binding("a", "2026-01-01T00:00:00Z"),
    ];
    for binding in &bindings {
        store.add_binding(binding.clone()).await.unwrap();
    }
    schema
        .client()
        .await
        .batch_execute("ALTER TABLE bindings DROP COLUMN attachment_order")
        .await
        .unwrap();
    store.init(SMALL_POOL).await.unwrap();
    store.init(SMALL_POOL).await.unwrap();
    check_order(&store, &["a", "z", "c"]).await;
    for binding in bindings {
        assert_eq!(
            store
                .get_binding(&room(), &binding.channel_id)
                .await
                .unwrap(),
            Some(binding)
        );
    }
    store
        .add_binding(binding("b", "2026-01-01T00:00:00Z"))
        .await
        .unwrap();
    store.close().await;
    let reopened = schema.initialized_store().await;
    check_order(&reopened, &["a", "z", "c", "b"]).await;
    reopened.close().await;
}
