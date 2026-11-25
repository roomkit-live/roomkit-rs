//! A broker that refuses one inbox, without interfering with any other.
use std::task::{Context, Waker};
use std::time::Duration;

use super::*;
use crate::turns::RoomOwner;
use crate::turns::distributed::test_support::Services;

fn backend() -> (Services, DistributedTurnBackend) {
    let services = Services::default();
    let backend = services.default_node();
    (services, backend)
}

fn envelope(kind: &str) -> Message {
    Message::new(kind, "sender", kind.to_owned(), "turn", "ref")
}

#[tokio::test]
async fn a_retried_answer_outlives_its_caller_and_precedes_later_frames() {
    let (services, backend) = backend();
    services.broker.refuse("peer");
    drop(backend.enqueue("peer", &envelope("answer")).unwrap());
    let later = backend.enqueue("peer", &envelope("frame")).unwrap();

    // The answer was tried and refused: wait for this evidence before
    // repairing the destination.
    services.broker.refused_once("peer").await;
    backend
        .post("independent", &envelope("control"))
        .await
        .unwrap();
    services.broker.accept("peer");
    tokio::time::timeout(Duration::from_secs(5), later)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let bodies: Vec<String> = services
        .broker
        .posted("peer")
        .into_iter()
        .map(|message| message.body)
        .collect();
    assert_eq!(bodies, ["answer", "frame"]);
    backend.drain_outbox(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn a_failed_frame_returns_once_and_shutdown_bounds_retriable_work() {
    let (services, backend) = backend();
    services.broker.refuse("peer");
    let refused = backend.post("peer", &envelope("frame")).await.unwrap_err();
    assert!(matches!(refused, RoomKitError::TurnLink {ref reason,..} if reason=="frame_lost"));
    let work = backend.enqueue("peer", &envelope("work")).unwrap();
    backend.drain_outbox(Duration::from_millis(20)).await;
    assert!(work.await.is_err());
    assert!(matches!(backend.post("peer", &envelope("answer")).await,
        Err(RoomKitError::TurnLink {ref reason,..}) if reason=="backend_closed"));
}

#[tokio::test]
async fn a_publication_precedes_the_next_question_from_the_same_host() {
    let (services, backend) = backend();
    let peer = "owner";
    let port = backend.remember_owner("turn", peer);
    let publication = serde_json::from_value(serde_json::json!({
        "type": "room_publication", "channel_id": "ai", "room_id": "room",
        "turn_id": "turn", "executor_id": "worker",
        "frame": {"type":"framework_event","event_type":"ai_response","data":{}}
    }))
    .unwrap();
    // As a turn's outlet sends it: polled once, the caller never yielding.
    let mut sent = port.room_publication(publication);
    let enqueued = sent.as_mut().poll(&mut Context::from_waker(Waker::noop()));
    assert!(enqueued.is_ready(), "the publication waited to be enqueued");
    backend.post(peer, &envelope("owner")).await.unwrap();
    let messages = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let messages = services.broker.posted(peer);
            if messages.len() == 2 {
                break messages;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let kinds: Vec<String> = messages.into_iter().map(|message| message.kind).collect();
    assert_eq!(kinds, ["work", "owner"]);
}

#[tokio::test]
async fn a_refusal_to_one_peer_does_not_stall_another_peers_turn() {
    let (services, backend) = backend();
    services.broker.refuse("unavailable");
    let mut turn = backend.route_turn("active", "healthy", false).unwrap();
    let question = Message::new("request", "unavailable", String::new(), "absent", "q");
    let frame = Message::new("frame", "healthy", "heartbeat".to_owned(), "active", "");
    for incoming in [&question, &question, &frame] {
        backend.write(backend.node_id(), incoming).await.unwrap();
    }
    backend.listen().await;
    let routed = tokio::time::timeout(Duration::from_secs(2), turn.inbox.recv()).await;
    services.broker.accept("unavailable");
    backend.close().await.unwrap();
    let entries = services.broker.posted("unavailable");
    assert_eq!(
        routed
            .expect("an unreachable peer must not block the inbox")
            .unwrap()
            .body,
        "heartbeat"
    );
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|entry| entry.body == "refused"));
}
