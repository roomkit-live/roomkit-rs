//! Exercise real Redis write refusal without interfering with any other inbox.
use std::time::Duration;

use redis::AsyncCommands;
use redis::streams::StreamRangeReply;
use uuid::Uuid;

use super::*;
use crate::turns::RedisTurnBackendOptions;

fn backend() -> RedisTurnBackend {
    let url = std::env::var("REDIS_URL").expect("Set REDIS_URL to the test service");
    RedisTurnBackend::new(
        Some(&url),
        RedisTurnBackendOptions {
            key_prefix: format!("test:roomkit:outbox:{}", Uuid::new_v4()),
            ..RedisTurnBackendOptions::default()
        },
    )
    .unwrap()
}

fn envelope(kind: &str) -> Message {
    Message::new(kind, "sender", kind.to_owned(), "turn", "ref")
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn a_retried_answer_outlives_its_caller_and_precedes_later_frames() {
    let backend = backend();
    let mut conn = backend.connection().await.unwrap();
    let key = backend.key("inbox:peer");
    conn.set_ex::<_, _, ()>(&key, "not a stream", 60)
        .await
        .unwrap();
    drop(backend.enqueue("peer", &envelope("answer")).unwrap());
    let later = backend.enqueue("peer", &envelope("frame")).unwrap();

    // XADD fails on this string, but Redis still executes PEXPIRE in the transaction.
    // Wait for this evidence before repairing the destination.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let ttl: i64 = conn.pttl(&key).await.unwrap();
            if ttl <= i64::try_from(backend.ttl_ms()).unwrap() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    backend
        .post("independent", &envelope("control"))
        .await
        .unwrap();
    conn.del::<_, ()>(&key).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), later)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let messages: StreamRangeReply = conn.xrange_all(&key).await.unwrap();
    let bodies: Vec<String> = messages
        .ids
        .iter()
        .map(|msg| msg.get("body").unwrap())
        .collect();
    assert_eq!(bodies, ["answer", "frame"]);
    backend.drain_outbox(Duration::from_secs(1)).await;
    conn.del::<_, ()>((&key, backend.key("inbox:independent")))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn a_failed_frame_returns_once_and_shutdown_bounds_retriable_work() {
    let backend = backend();
    let mut conn = backend.connection().await.unwrap();
    let key = backend.key("inbox:peer");
    conn.set_ex::<_, _, ()>(&key, "wrong type", 60)
        .await
        .unwrap();
    let refused = backend.post("peer", &envelope("frame")).await.unwrap_err();
    assert!(matches!(refused, RoomKitError::TurnLink {ref reason,..} if reason=="frame_lost"));
    let work = backend.enqueue("peer", &envelope("work")).unwrap();
    backend.drain_outbox(Duration::from_millis(20)).await;
    assert!(work.await.is_err());
    assert!(matches!(backend.post("peer", &envelope("answer")).await,
        Err(RoomKitError::TurnLink {ref reason,..}) if reason=="backend_closed"));
    conn.del::<_, ()>(&key).await.unwrap();
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn a_publication_precedes_the_next_question_from_the_same_host() {
    let backend = backend();
    let peer = "owner";
    let port = backend.remember_owner(peer);
    let publication = serde_json::from_value(serde_json::json!({
        "type": "room_publication", "channel_id": "ai", "room_id": "room",
        "turn_id": "turn", "executor_id": "worker",
        "frame": {"type":"framework_event","event_type":"ai_response","data":{}}
    }))
    .unwrap();
    port.publish(publication);
    backend.post(peer, &envelope("owner")).await.unwrap();
    let mut conn = backend.connection().await.unwrap();
    let key = backend.key("inbox:owner");
    let messages: StreamRangeReply = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let rows: StreamRangeReply = conn.xrange_all(&key).await.unwrap();
            if rows.ids.len() == 2 {
                break rows;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let kinds: Vec<String> = messages
        .ids
        .iter()
        .map(|m| m.get("kind").unwrap())
        .collect();
    conn.del::<_, ()>(key).await.unwrap();
    assert_eq!(kinds, ["work", "owner"]);
}

#[tokio::test]
#[ignore = "requires REDIS_URL"]
async fn a_refusal_to_one_peer_does_not_stall_another_peers_turn() {
    let backend = backend();
    let mut conn = backend.connection().await.unwrap();
    let unavailable = backend.key("inbox:unavailable");
    conn.set_ex::<_, _, ()>(&unavailable, "not a stream", 300)
        .await
        .unwrap();
    let mut turn = backend.route_turn("active", "healthy", false).unwrap();
    let question = Message::new("request", "unavailable", String::new(), "absent", "q");
    let frame = Message::new("frame", "healthy", "heartbeat".to_owned(), "active", "");
    for incoming in [&question, &question, &frame] {
        backend.write(backend.node_id(), incoming).await.unwrap();
    }
    backend.listen().await;
    let routed = tokio::time::timeout(Duration::from_secs(2), turn.inbox.recv()).await;
    conn.del::<_, ()>(&unavailable).await.unwrap();
    backend.close().await.unwrap();
    let entries: StreamRangeReply = conn.xrange_all(&unavailable).await.unwrap();
    conn.del::<_, ()>(&unavailable).await.unwrap();
    assert_eq!(
        routed
            .expect("an unreachable peer must not block the inbox")
            .unwrap()
            .body,
        "heartbeat"
    );
    assert_eq!(entries.ids.len(), 2);
    assert!(
        entries
            .ids
            .iter()
            .all(|entry| entry.get::<String>("body").as_deref() == Some("refused"))
    );
}
