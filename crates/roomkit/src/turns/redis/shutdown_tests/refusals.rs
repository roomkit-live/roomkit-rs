//! Peer-visible results when a request cannot be handled.
use super::*;

#[tokio::test]
async fn a_channel_call_to_an_absent_worker_returns_its_terminal_reason() {
    let Some((worker, owner)) = pair() else {
        return;
    };
    worker.attend().await.unwrap();
    let raw = json!({"message":{"type":"channel_call", "call_id":"q", "channel_id":"ai", "room_id":"r", "verb":"ping"}}).to_string();
    let response = owner
        .ask(worker.node_id(), "channel", raw, "missing", LIMIT)
        .await
        .unwrap();
    let turn = owner
        .ask(
            worker.node_id(),
            "request",
            request("declined"),
            "missing",
            LIMIT,
        )
        .await
        .unwrap();
    worker.close().await.unwrap();
    owner.close().await.unwrap();
    let result: Value = serde_json::from_str(&response).expect("a channel_result from the worker");
    assert_eq!(result["type"], "channel_result");
    assert_eq!(result["error"]["attributes"]["reason"], "no_executor");
    assert_eq!(
        result["error"]["kind"],
        "roomkit.core.exceptions:TurnLinkError"
    );
    assert!(result["executor_id"].is_null());
    assert_eq!(turn, "refused");
}

#[tokio::test]
async fn a_captured_host_reports_the_closing_owners_refusal() {
    use crate::turns::wire::{OwnerMessage, OwnerMessageEnvelope};
    let Some((owner, worker)) = pair() else {
        return;
    };
    owner.attend().await.unwrap();
    // The call names its turn: its owner is the one that turn came from.
    let port = worker.remember_owner("old", owner.node_id());
    let OwnerMessageEnvelope {
        message: OwnerMessage::RoomCall(call),
    } = serde_json::from_str(&room_call("late")).unwrap()
    else {
        panic!("expected a room call")
    };
    // Hold the close open just long enough for the captured host to receive its refusal.
    let release = Arc::new(Notify::new());
    assert!(owner.dispatch({
        let release = Arc::clone(&release);
        async move {
            release.notified().await;
        }
    }));
    let mut draining = owner.0.draining.subscribe();
    let after_cut = async {
        drop(draining.wait_for(|closing| *closing).await.unwrap());
        // As a turn's host reads its owner's answer: a refusal is its error.
        let reason = match crate::turns::RoomOwner::room_call(&port, call).await {
            Ok(reply) => crate::turns::ErrorClasses::default()
                .rebuild(&reply.error.expect("the closing owner's refusal")),
            Err(error) => error,
        };
        release.notify_one();
        reason
    };
    let (closed, failure) = tokio::join!(owner.close(), after_cut);
    closed.unwrap();
    worker.close().await.unwrap();
    assert!(
        matches!(failure, crate::RoomKitError::TurnLink { ref reason, .. } if reason == "backend_closed"),
        "{failure:?}"
    );
}

#[tokio::test]
async fn unreadable_channel_and_room_calls_return_a_validation_error() {
    let Some((receiver, sender)) = pair() else {
        return;
    };
    let executor = TurnExecutor::new(
        |_| std::future::ready(Ok(None::<EchoAgent>)),
        TurnExecutorOptions {
            executor_id: Some("worker".into()),
            hosts: Some(["agent".to_string()].into()),
            ..TurnExecutorOptions::default()
        },
    )
    .unwrap();
    receiver.serve(executor.clone()).await.unwrap();
    let bad = |kind| {
        sender.ask(
            receiver.node_id(),
            kind,
            r#"{"message":{}}"#.into(),
            "worker",
            LIMIT,
        )
    };
    let (channel, room) = tokio::join!(bad("channel"), bad("owner"));
    receiver.close().await.unwrap();
    executor.close().await;
    sender.close().await.unwrap();
    for (kind, raw) in [("channel_result", channel), ("host_result", room)] {
        let result: Value =
            serde_json::from_str(&raw.expect("a result from the live peer")).unwrap();
        assert_eq!(result["type"], kind);
        assert_eq!(result["error"]["kind"], "builtins:ValueError", "{kind}");
        assert_ne!(result["error"]["message"].as_str().unwrap(), "");
        if kind == "channel_result" {
            assert_eq!(result["executor_id"], "worker");
        } else {
            assert_eq!(result["call_id"], "");
        }
    }
}
