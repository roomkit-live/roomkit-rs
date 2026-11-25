//! Work outliving its link reaches the first attending owner.
use super::{deployment, request};
use crate::executor_link::reach_owner::worker;
use roomkit::RoomKitError;
use roomkit::turns::wire::{Frame, HostResult};
use roomkit::turns::{RoomCall, RoomOwner, RoomPublication, TurnFuture};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

struct Recorder(&'static str, mpsc::UnboundedSender<RoomPublication>);
impl RoomOwner for Recorder {
    fn room_call(&self, message: RoomCall) -> TurnFuture<'_, Result<HostResult, RoomKitError>> {
        Box::pin(async move {
            if message.verb == "refuse" {
                return Err(RoomKitError::Value("refused by the owner".into()));
            }
            Ok(HostResult {
                call_id: message.call_id,
                result: json!({"owner":self.0,"turn":message.turn_id}),
                error: None,
                record_changes: None,
            })
        })
    }
    fn room_publication(
        &self,
        message: RoomPublication,
    ) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            self.1.send(message).unwrap();
            Ok(())
        })
    }
}

#[tokio::test]
async fn a_retained_worker_host_reaches_the_first_attending_owner_through_the_public_backend() {
    let node = deployment();
    let owner = node();
    let worker_node = node();
    let (events, mut received) = mpsc::unbounded_channel();
    let (other, mut ignored) = mpsc::unbounded_channel();
    let (worker, mut captured) = worker();
    tokio::time::timeout(Duration::from_secs(12), async {
        owner.attend(Arc::new(Recorder("first", events))).await.unwrap();
        owner.attend(Arc::new(Recorder("other", other))).await.unwrap();
        worker_node.serve(Arc::new(worker.clone())).await.unwrap();
        let link = owner.submit(request()).await.unwrap();
        while !matches!(link.receive().await.unwrap().1, Frame::End) {}
        link.release();
        let host = captured.recv().await.unwrap();
        host.emit_turn_event("ai_response", roomkit::model::Metadata::new());
        let response = host.call("probe", None).await.unwrap();
        assert_eq!(response, json!({"owner":"first","turn":link.request().turn_id}));
        let event = received.recv().await.unwrap();
        assert_eq!(event.turn_id, link.request().turn_id);
        assert!(matches!(Frame::from(*event.frame), Frame::FrameworkEvent { event_type,.. } if event_type=="ai_response"));
        assert!(ignored.try_recv().is_err());
        assert!(matches!(host.call("refuse", None).await, Err(RoomKitError::Value(s)) if s == "refused by the owner"));
        worker_node.close().await.unwrap();
        owner.close().await.unwrap();
        worker.close().await;
    }).await.unwrap();
}

/// Work a turn began keeps its way to its owner once its executor is gone,
/// whether its turn was submitted on the executor's node or on another: the
/// port the backend gave the executor carries the verb, as any backend's
/// does (RFC §10.6.3).
#[tokio::test]
async fn a_host_reaches_its_owner_through_the_public_backend_once_its_executor_is_gone() {
    let mut reached = Vec::new();
    tokio::time::timeout(Duration::from_secs(20), async {
        for remote in [false, true] {
            let node = deployment();
            let owner = node();
            let worker_node = if remote { node() } else { owner.clone() };
            let (events, _received) = mpsc::unbounded_channel();
            let recorder = Arc::new(Recorder("owner", events));
            owner.attend(recorder).await.unwrap();
            let (executor, mut captured) = worker();
            worker_node.serve(Arc::new(executor.clone())).await.unwrap();
            let link = owner.submit(request()).await.unwrap();
            while !matches!(link.receive().await.unwrap().1, Frame::End) {}
            link.release();
            let host = captured.recv().await.unwrap();
            // The executor goes: closed, replaced on its node, then dropped,
            // which the end of what its channels captured tells.
            executor.close().await;
            let (next, _hosts) = worker();
            worker_node.serve(Arc::new(next.clone())).await.unwrap();
            drop(executor);
            let gone = tokio::time::timeout(Duration::from_secs(3), captured.recv()).await;
            assert!(matches!(gone, Ok(None)), "the executor is still held");
            let answer = host.call("probe", None).await;
            reached.push((host.reachable(), answer.map(|reply| reply["owner"].clone())));
            next.close().await;
            worker_node.close().await.unwrap();
            owner.close().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(
        reached,
        [(true, Ok(json!("owner"))), (true, Ok(json!("owner")))],
        "submitted on the executor's node, then on another"
    );
}
