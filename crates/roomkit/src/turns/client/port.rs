//! A weak registration lets a transport keep its owner port without retaining the client.
use super::{State, TurnClient};
use crate::RoomKitError;
use crate::turns::wire::{HostResult, OwnerMessage};
use crate::turns::{RoomCall, RoomOwner, RoomPublication, ServedExecutor, TurnFuture};
use std::sync::{Arc, Weak};
pub(super) struct ClientOwner(pub(super) Weak<State>);
impl RoomOwner for ClientOwner {
    fn executor(&self) -> Option<Arc<dyn ServedExecutor>> {
        self.0.upgrade()?.services.executor.clone()
    }
    fn room_call(&self, message: RoomCall) -> TurnFuture<'_, Result<HostResult, RoomKitError>> {
        Box::pin(async move {
            if let Some(state) = self.0.upgrade().filter(|s| !s.turns.is_closed()) {
                let receiver = Arc::clone(&state);
                let call =
                    Box::pin(
                        async move { receiver.receive(OwnerMessage::RoomCall(message)).await },
                    );
                return state
                    .calls
                    .run(state.runtime().as_ref(), || state.turns.is_closed(), call)
                    .await?
                    .ok_or_else(|| RoomKitError::Runtime("Missing room call result".into()));
            }
            Ok(HostResult {
                call_id: message.call_id,
                result: serde_json::Value::Null,
                error: Some(crate::turns::error::one(
                    "RuntimeError",
                    "The room owner has closed".into(),
                )),
                record_changes: None,
            })
        })
    }
    fn room_publication(
        &self,
        message: RoomPublication,
    ) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            if let Some(state) = self.0.upgrade().filter(|s| !s.turns.is_closed()) {
                state.receive(OwnerMessage::RoomPublication(message)).await;
            }
            Ok(())
        })
    }
}

impl RoomOwner for TurnClient {
    fn executor(&self) -> Option<Arc<dyn ServedExecutor>> {
        self.state.services.executor.clone()
    }
    fn room_call(&self, message: RoomCall) -> TurnFuture<'_, Result<HostResult, RoomKitError>> {
        Box::pin(async move {
            ClientOwner(Arc::downgrade(&self.state))
                .room_call(message)
                .await
        })
    }
    fn room_publication(
        &self,
        message: RoomPublication,
    ) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            ClientOwner(Arc::downgrade(&self.state))
                .room_publication(message)
                .await
        })
    }
}
