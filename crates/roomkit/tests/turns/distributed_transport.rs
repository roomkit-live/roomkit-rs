//! An application directly uses the distributed transport through the public turn contract.
use std::sync::Arc;
use std::time::Duration;

use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::turns::wire::{Control, Frame};
use roomkit::turns::{
    DistributedTurnBackend, DistributedTurnBackendOptions, InMemoryTurnBroker,
    InMemoryTurnRegistry, TurnBackendHandle, TurnRequest,
};
use roomkit::{RoomKitError, TurnExecutor, TurnExecutorOptions};
use serde_json::Value;

fn request() -> TurnRequest {
    let fixture = include_str!("../fixtures/turns/python-case1.jsonl");
    let row: Value = serde_json::from_str(fixture.lines().next().unwrap()).unwrap();
    serde_json::from_value(row["message"].clone()).unwrap()
}

/// The nodes of one deployment, on a broker and a registry of this process.
fn deployment() -> impl Fn() -> TurnBackendHandle {
    let broker = Arc::new(InMemoryTurnBroker::new());
    let registry = Arc::new(InMemoryTurnRegistry::new());
    move || {
        TurnBackendHandle::new(Arc::new(DistributedTurnBackend::new(
            Arc::<InMemoryTurnBroker>::clone(&broker),
            Arc::<InMemoryTurnRegistry>::clone(&registry),
            DistributedTurnBackendOptions::default(),
        )))
    }
}

async fn submit_and_read(owner: &TurnBackendHandle, worker_node: &TurnBackendHandle) {
    let worker = TurnExecutor::new(
        |id| {
            std::future::ready(Ok(Some(EchoAgent::new(
                id,
                "agent reply",
                EchoAgentOptions::default(),
            ))))
        },
        TurnExecutorOptions {
            hosts: Some(["ai".into()].into()),
            executor_id: Some("worker".into()),
            ..Default::default()
        },
    )
    .unwrap();
    worker_node.serve(Arc::new(worker.clone())).await.unwrap();
    let request = request();
    assert!(
        owner
            .executors_now()
            .unwrap()
            .await
            .unwrap()
            .iter()
            .any(|worker| worker.executor_id == "worker")
    );
    assert!(owner.claim_presence("ai", "r1", Some(true)).await.unwrap());
    assert!(
        !worker_node
            .claim_presence("ai", "r1", Some(true))
            .await
            .unwrap()
    );
    let call = roomkit::turns::ChannelCall::new(
        request.channel_id.clone(),
        request.room_id.clone(),
        "unknown",
    );
    let answer = owner
        .control(roomkit::turns::ChannelControl::ChannelCall(Box::new(
            call.clone(),
        )))
        .await
        .unwrap()
        .unwrap();
    assert!(call.handed());
    assert_eq!(answer.executor_id.as_deref(), Some("worker"));
    assert!(answer.error.is_some());
    let original = request.clone();
    let link = owner.submit(request).await.unwrap();
    assert!(original.handed());
    assert_eq!(link.request().turn_id, original.turn_id);
    assert!(link.taken());
    assert!(
        matches!(link.receive().await.unwrap(), (1, Frame::Accepted {executor_id}) if executor_id == "worker")
    );
    link.send(Control::Heartbeat).await.unwrap();
    assert!(
        matches!(link.receive().await.unwrap(), (2, Frame::Output { output, .. }) if output.responded)
    );
    assert!(matches!(link.receive().await.unwrap(), (3, Frame::End)));
    link.release();
    for _ in 0..2 {
        assert!(
            matches!(link.receive().await, Err(RoomKitError::TurnLink { reason, .. }) if reason == "backend_closed")
        );
    }
    worker.close().await;
}

#[tokio::test]
async fn the_public_backend_carries_an_admitted_turn_locally_and_across_nodes() {
    tokio::time::timeout(Duration::from_secs(15), async {
        for remote in [false, true] {
            let node = deployment();
            let owner = node();
            let worker = if remote { node() } else { owner.clone() };
            let missing = request();
            assert!(matches!(owner.submit(missing.clone()).await, Err(RoomKitError::TurnLink {reason, ..}) if reason == "no_executor"));
            assert!(!missing.handed());
            submit_and_read(&owner, &worker).await;
            worker.close().await.unwrap();
            owner.close().await.unwrap();
            assert!(owner.claim_presence("ai", "r1", None).await.is_err());
            let closed = request();
            assert!(matches!(owner.submit(closed.clone()).await, Err(RoomKitError::TurnLink {reason,..}) if reason == "backend_closed"));
            assert!(!closed.handed());
        }
    }).await.unwrap();
}

mod configured;
mod owner;
