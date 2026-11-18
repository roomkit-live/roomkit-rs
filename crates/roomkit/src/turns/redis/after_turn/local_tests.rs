//! A turn's late calls and publications meet the local owner in its own task context.
use crate::turns::wire::{Frame, HostResult};
use crate::turns::{RoomCall, RoomOwner, RoomPublication, TurnBackend, TurnContext, TurnFuture};
use crate::{RedisTurnBackend, RedisTurnBackendOptions, RoomKitError, current_turn_id};
use serde_json::Value;
use std::sync::Arc;

struct Owner;

impl RoomOwner for Owner {
    fn room_call(&self, call: RoomCall) -> TurnFuture<'_, Result<HostResult, RoomKitError>> {
        Box::pin(async move {
            Ok(HostResult {
                call_id: call.call_id,
                result: current_turn_id().map_or(Value::Null, Value::String),
                error: None,
                record_changes: None,
            })
        })
    }
    fn room_publication(&self, _: RoomPublication) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            let error = current_turn_id().map_or_else(
                || "publication rejected".into(),
                |turn| format!("wrong context {turn}"),
            );
            Err(RoomKitError::Value(error))
        })
    }
}

#[tokio::test]
async fn a_local_owner_receives_late_work_apart_from_the_worker_and_returns_its_error() {
    let Ok(url) = std::env::var("REDIS_URL") else {
        return;
    };
    let backend = RedisTurnBackend::new(
        Some(&url),
        RedisTurnBackendOptions {
            key_prefix: format!("test:owner-context:{}", uuid::Uuid::new_v4()),
            ..Default::default()
        },
    )
    .unwrap();
    TurnBackend::attend(&backend, Arc::new(Owner))
        .await
        .unwrap();
    let port = backend.local_owner();
    let call = RoomCall::new(
        "agent".parse().unwrap(),
        "room".parse().unwrap(),
        "worker",
        "earlier",
        "probe",
    );
    let publication = RoomPublication::new(
        call.channel_id.clone(),
        call.room_id.clone(),
        "worker",
        "earlier",
        Frame::FrameworkEvent {
            event_type: "ai_response".into(),
            room_id: None,
            channel_id: None,
            event_id: None,
            data: crate::model::Metadata::new(),
        }
        .try_into()
        .unwrap(),
    );
    TurnContext::default()
        .in_turn(Arc::from("worker-context"))
        .scope(async {
            assert_eq!(current_turn_id().as_deref(), Some("worker-context"));
            let response = port.room_call(call).await.unwrap();
            let published = port.room_publication(publication).await;
            assert_eq!(
                (response.result, published),
                (
                    Value::Null,
                    Err(RoomKitError::Value("publication rejected".into()))
                )
            );
            assert_eq!(current_turn_id().as_deref(), Some("worker-context"));
        })
        .await;
    backend.close().await.unwrap();
}
