//! Opening and closing contracts whose lock and diagnostic evidence is native.

use std::future::{Future, poll_fn};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::Poll;

use roomkit::Channel;
use roomkit::model::RoomId;
use serde_json::json;
use tokio::io::AsyncWriteExt as _;
use tokio::sync::OwnedMutexGuard;

use super::{Agent, PATIENCE, channel};
use crate::AcpError;

#[path = "../../../../roomkit/tests/support/warnings.rs"]
pub(super) mod warnings;

/// Register the future's waiter without giving a timer the ordering decision.
pub(super) async fn pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_a_waiter_on_a_retired_room_lock_does_not_race_a_new_caller
#[tokio::test]
async fn test_a_waiter_on_a_retired_room_lock_does_not_race_a_new_caller() {
    let (channel, _agents, _) = channel();
    let room = RoomId::new("r1").unwrap();
    let held = channel.shared.holding_room(&room).await;
    let old = Arc::clone(OwnedMutexGuard::mutex(&held));
    let mut closing = pin!(channel.close_session(&room));
    pending(closing.as_mut()).await;
    let mut queued = pin!(channel.shared.holding_room(&room));
    pending(queued.as_mut()).await;

    drop(held);
    assert!(!tokio::time::timeout(PATIENCE, closing).await.unwrap());
    let fresh = channel.shared.holding_room(&room).await;
    assert!(!Arc::ptr_eq(&old, OwnedMutexGuard::mutex(&fresh)));
    // The old waiter must retry on the fresh lock, which is held now.
    pending(queued.as_mut()).await;
    let current = Arc::clone(OwnedMutexGuard::mutex(&fresh));
    drop(fresh);
    let acquired = tokio::time::timeout(PATIENCE, queued).await.unwrap();
    assert!(Arc::ptr_eq(&current, OwnedMutexGuard::mutex(&acquired)));
    drop(acquired);
    channel.close().await;
}

// Python: tests/test_channels/test_acp_session_setup.py::TestARefusedValueFailsClosed::test_a_session_the_agent_cannot_close_is_logged
#[tokio::test]
async fn test_a_session_the_agent_cannot_close_is_logged() {
    let (channel, mut agents, _) = channel();
    let room = RoomId::new("r1").unwrap();
    let (logs, _capture) = warnings::Warnings::capture();
    let agent = tokio::spawn(async move {
        let mut agent = Agent::new(agents.recv().await.unwrap());
        let initialize = agent.read().await;
        assert_eq!(initialize["method"], "initialize");
        agent
            .answer(
                &initialize,
                json!({"protocolVersion":1,"agentCapabilities":{}}),
            )
            .await;
        let opening = agent.read().await;
        assert_eq!(opening["method"], "session/new");
        agent.answer(&opening, json!({"sessionId":"s1"})).await;
        let setting = agent.read().await;
        assert_eq!(setting["method"], "session/set_config_option");
        let refusal = json!({"jsonrpc":"2.0", "id":setting["id"],
            "error":{"code":-32602,"message":"model refused"}});
        agent
            .writer
            .write_all(format!("{refusal}\n").as_bytes())
            .await
            .unwrap();
        agent
    });
    let result = channel
        .set_config_option(&room, "model", json!("sonnet"))
        .await;
    assert!(matches!(result, Err(AcpError::SetupRefused { .. })));
    let mut agent = agent.await.unwrap();
    let logged = logs.containing("ACP session stays open");
    assert_eq!(logged.len(), 1);
    assert!(logged[0].contains("WARN") && logged[0].contains("session=\"s1\""));
    assert!(logged[0].contains("agent does not announce session/close"));
    assert!(channel.session_id(&room).is_none());
    assert_eq!(
        channel.config_options(&room),
        Vec::<serde_json::Value>::new()
    );
    channel.close().await;
    // With the client pipe closed, there can be no hidden close or prompt request.
    assert!(
        tokio::time::timeout(PATIENCE, agent.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
}
