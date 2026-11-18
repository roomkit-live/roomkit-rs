//! Work outliving its link reaches the first attending owner.
use super::{backend, request};
use crate::executor_link::reach_owner::worker;
use roomkit::RoomKitError;
use roomkit::turns::wire::{Frame, HostResult};
use roomkit::turns::{RoomCall, RoomOwner, RoomPublication, TurnFuture};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use uuid::Uuid;

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
#[ignore = "requires REDIS_URL"]
async fn a_retained_worker_host_reaches_the_first_attending_owner_through_the_public_redis_backend()
{
    let prefix = format!("test:roomkit:public-owner:{}", Uuid::new_v4());
    let owner = backend(&prefix);
    let worker_node = backend(&prefix);
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
