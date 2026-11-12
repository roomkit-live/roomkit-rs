//! A lazy turn cannot resurrect the room after detachment (RFC §6.4).

use std::future::poll_fn;
use std::pin::Pin;

use futures_core::Stream;
use roomkit::Channel;
use roomkit::model::RoomId;
use serde_json::json;

use super::{Agent, PATIENCE, channel, turn};

// Python: tests/test_channels/test_acp_backend.py::test_a_turn_started_before_a_detach_never_prompts_after_it
// TurnStream is the lazy boundary; outside-kit on_event buffering is HITL c2e5eb96.
#[tokio::test]
async fn test_a_turn_started_before_a_detach_never_prompts_after_it() {
    let (channel, mut agents, _) = channel();
    let room = RoomId::new("r1").unwrap();
    let (mut stream, record) = turn(&channel, "r1");
    channel.on_room_detached(room.clone()).await.unwrap();
    assert!(
        poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
            .await
            .is_none()
    );
    assert_eq!(record.to_metadata()["acp"]["interrupted"], true);
    assert!(agents.try_recv().is_err());
    assert!(channel.session_id(&room).is_none());
    assert_eq!(channel.active_turns(), 0);
    channel.close().await;
}

// A stale unstarted stream has opened nothing; its rejection must preserve a fresh session.
#[tokio::test]
async fn an_unstarted_detached_turn_does_not_close_a_reattached_session() {
    let (channel, mut agents, _) = channel();
    let room = RoomId::new("r1").unwrap();
    let (mut stale, record) = turn(&channel, "r1");
    channel.on_room_detached(room.clone()).await.unwrap();
    let fresh = tokio::spawn({
        let channel = channel.clone();
        let room = room.clone();
        async move {
            channel
                .set_config_option(&room, "model", json!("sonnet"))
                .await
        }
    });
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let initialize = agent.read().await;
    agent
        .answer(
            &initialize,
            json!({"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"close":{}}}}),
        )
        .await;
    let opening = agent.read().await;
    assert_eq!(opening["method"], "session/new");
    agent.answer(&opening, json!({"sessionId":"fresh"})).await;
    let setting = agent.read().await;
    assert_eq!(setting["method"], "session/set_config_option");
    agent.answer(&setting, json!({"configOptions":[]})).await;
    fresh.await.unwrap().unwrap();
    tokio::time::timeout(PATIENCE, async {
        assert!(
            poll_fn(|cx| Pin::new(&mut stale).poll_next(cx))
                .await
                .is_none()
        );
    })
    .await
    .unwrap();
    assert_eq!(record.to_metadata()["acp"]["interrupted"], true);
    assert_eq!(channel.session_id(&room).as_deref(), Some("fresh"));
    assert_eq!(
        channel.room_state(&room).unwrap().get("config"),
        Some(&json!({"model":"sonnet"}))
    );
    let closing = tokio::spawn(async move { channel.close().await });
    let close = agent.read().await;
    assert_eq!(close["method"], "session/close");
    agent.answer(&close, json!({})).await;
    closing.await.unwrap();
}
