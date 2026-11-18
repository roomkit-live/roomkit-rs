//! The backend lends its workers a port even before an owner attends.
use super::State;
use super::placement::crossed;
use crate::RoomKitError;
use crate::turns::wire::HostResult;
use crate::turns::{RoomCall, RoomOwner, RoomPublication, TurnFuture, lock};
use std::sync::{Arc, Mutex, Weak};
use tokio::task::JoinSet;

pub(super) struct OwnerPort(pub(super) Weak<Mutex<State>>);

impl OwnerPort {
    fn owner(&self) -> Option<Arc<dyn RoomOwner>> {
        let state = self.0.upgrade()?;
        lock(&state).owner.clone()
    }
}

impl RoomOwner for OwnerPort {
    fn room_call(&self, message: RoomCall) -> TurnFuture<'_, Result<HostResult, RoomKitError>> {
        Box::pin(local_call(self.owner(), message))
    }
    fn room_publication(
        &self,
        message: RoomPublication,
    ) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(local_publication(self.owner(), message))
    }
}

pub(in crate::turns) async fn local_call(
    owner: Option<Arc<dyn RoomOwner>>,
    message: RoomCall,
) -> Result<HostResult, RoomKitError> {
    let Some(owner) = owner else {
        return Ok(HostResult {
            call_id: message.call_id,
            result: serde_json::Value::Null,
            error: Some(crate::turns::error::one(
                "builtins:RuntimeError",
                "No room owner is reached outside a turn on this backend".into(),
            )),
            record_changes: None,
        });
    };
    let message = crossed(&message)?;
    let response = apart(async move { owner.room_call(message).await }).await?;
    crossed(&response)
}

pub(in crate::turns) async fn local_publication(
    owner: Option<Arc<dyn RoomOwner>>,
    message: RoomPublication,
) -> Result<(), RoomKitError> {
    if let Some(owner) = owner {
        let message = crossed(&message)?;
        apart(async move { owner.room_publication(message).await }).await?;
    }
    Ok(())
}

/// The owner's callbacks run with no worker task locals. Cancelling the caller cuts the callback.
async fn apart<T: Send + 'static>(
    work: impl Future<Output = Result<T, RoomKitError>> + Send + 'static,
) -> Result<T, RoomKitError> {
    let mut task = JoinSet::new();
    task.spawn(work);
    task.join_next()
        .await
        .ok_or(RoomKitError::KitClosed)?
        .map_err(|error| RoomKitError::Runtime(error.to_string()))?
}
