//! Replays at a commit boundary the shared scenario format cannot hold.

use super::*;
use pretty_assertions::assert_eq;
use roomkit::model::{EditContent, EditSource, EventType};

fn keyed() -> InboundMessage {
    let mut message = InboundMessage::new(channel("sms"), "user", TextContent::new("keyed"));
    message.idempotency_key = Some("k".to_owned());
    message
}

fn options() -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    }
}

// Python: tests/test_idempotent_redelivery.py::TestAStoreThatCannotResolveTheKey::test_falls_back_to_the_blocked_result
#[tokio::test]
async fn a_store_that_cannot_resolve_the_key_returns_the_blocked_duplicate() {
    let (kit, ws) = gated_room().await;
    kit.store().go.notify_one();
    let first = kit.process_inbound(keyed(), options()).await.unwrap();
    kit.store()
        .cannot_resolve_keys
        .store(true, Ordering::SeqCst);
    let replay = kit.process_inbound(keyed(), options()).await.unwrap();
    assert!(!first.blocked);
    assert!(replay.blocked && replay.duplicate);
    assert_eq!(replay.reason.as_deref(), Some("duplicate"));
    assert!(replay.event.is_none());
    assert_eq!(ws.texts(), ["keyed"]);
    kit.close().await.unwrap();
}

// Python: tests/test_framework.py::TestTimeouts::test_slow_store_commit_is_not_reported_blocked
#[tokio::test(start_paused = true)]
async fn a_slow_store_commit_outlives_the_process_budget_and_keeps_its_key() {
    let (kit, ws) = gated_room().await;
    let call = {
        let kit = kit.clone();
        tokio::spawn(async move { kit.process_inbound(keyed(), options()).await })
    };
    kit.store().committing.notified().await;
    tokio::time::advance(Duration::from_secs(120)).await;
    assert!(
        !call.is_finished(),
        "the process budget must not cancel a commit"
    );
    kit.store().go.notify_one();
    let committed = call.await.unwrap().unwrap();
    let replay = kit.process_inbound(keyed(), options()).await.unwrap();
    assert!(!committed.blocked && !replay.blocked && replay.duplicate);
    assert_eq!(replay.event.unwrap().id, committed.event.unwrap().id);
    assert_eq!(ws.texts(), ["keyed"]);
    kit.close().await.unwrap();
}

// Python: tests/test_commit_atomicity.py and tests/test_idempotent_redelivery.py:
// a caller gone during the commit can retry to learn the one committed event.
#[tokio::test]
async fn a_caller_gone_during_commit_retries_without_a_second_delivery() {
    let (kit, ws) = gated_room().await;
    let before = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    drop_during_the_commit(&kit, kit.process_inbound(keyed(), options())).await;
    let replay = kit.process_inbound(keyed(), options()).await.unwrap();
    let after = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    assert!(!replay.blocked && replay.duplicate);
    assert_eq!(replay.event.unwrap().index, before.event_count);
    assert_eq!(
        (after.event_count, after.latest_index),
        (before.event_count + 1, Some(before.event_count))
    );
    assert_eq!(ws.texts(), ["keyed"]);
    kit.close().await.unwrap();
}

// Python: tests/test_commit_atomicity.py::test_an_edit_whose_commit_fails_leaves_its_target_as_it_was
#[tokio::test]
async fn an_edit_whose_commit_fails_leaves_its_target_as_it_was() {
    let (kit, _) = gated_room().await;
    let first = InboundMessage::new(channel("sms"), "user", TextContent::new("First"));
    let original = kit
        .process_inbound(first, options())
        .await
        .unwrap()
        .event
        .unwrap();
    let before = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    kit.store().fails.store(true, Ordering::SeqCst);
    kit.store().go.notify_one();
    let mut content = EditContent::new(original.id.clone(), TextContent::new("changed"));
    content.edit_source = Some(EditSource::Sender);
    let mut edit = InboundMessage::new(channel("sms"), "user", content);
    edit.event_type = EventType::Edit;
    let failed = kit.process_inbound(edit, options()).await;
    assert!(matches!(failed, Err(RoomKitError::Value(_))));
    let stored = kit.store().get_event(&original.id).await.unwrap().unwrap();
    let after = kit.store().get_room(&room_id()).await.unwrap().unwrap();
    assert_eq!(
        (stored.content, stored.metadata),
        (original.content, original.metadata)
    );
    assert_eq!(
        (after.event_count, after.latest_index),
        (before.event_count, before.latest_index)
    );
    kit.close().await.unwrap();
}
