//! An executor's route to the owner that submitted a turn (RFC §10.6.3, §10.6.5).
//! The last 10,000 received turns retain their routes, as in Python's Redis Routes.
mod owners;
pub(super) use owners::TurnOwners;
use std::sync::{Arc, Weak};
use std::time::Duration;
use uuid::Uuid;

use super::stream::Message;
use super::{DistributedTurnBackend, Node};
use crate::RoomKitError;
use crate::turns::wire::{OwnerMessage, OwnerMessageEnvelope, RoomCall, RoomCallResult};
use crate::turns::{RoomOwner, RoomPublication, TurnFuture};
use crate::turns::{in_memory, lock};

#[derive(Debug)]
pub(crate) struct PeerOwner {
    node: Weak<Node>,
    turn_id: Option<Arc<str>>,
}

impl DistributedTurnBackend {
    pub(super) fn local_owner(&self) -> PeerOwner {
        PeerOwner {
            node: Arc::downgrade(&self.0),
            turn_id: None,
        }
    }

    pub(super) fn remember_owner(&self, id: &str, peer: &str) -> PeerOwner {
        lock(&self.0.routes).turn_owners.remember(id, peer);
        self.local_owner().of(id)
    }
}

impl PeerOwner {
    fn of(&self, turn: &str) -> Self {
        Self {
            node: Weak::clone(&self.node),
            turn_id: Some(Arc::from(turn)),
        }
    }

    fn peer(&self, backend: &DistributedTurnBackend) -> Option<Arc<str>> {
        lock(&backend.0.routes)
            .turn_owners
            .peer(self.turn_id.as_deref()?)
    }

    async fn call_result(&self, call: RoomCall) -> Result<RoomCallResult, RoomKitError> {
        let backend = DistributedTurnBackend(self.node.upgrade().ok_or(RoomKitError::KitClosed)?);
        if *backend.0.stop.borrow() {
            return Err(super::routes::closed());
        }
        let call_id = call.call_id.clone();
        let result = if let Some(peer) = self.peer(&backend) {
            let envelope = OwnerMessageEnvelope {
                message: OwnerMessage::RoomCall(call),
            };
            let body = serde_json::to_string(&envelope).map_err(|error| invalid(&error))?;
            let exchange = backend.exchange(&peer, "owner", body, "");
            tokio::pin!(exchange);
            let interval = backend
                .timeouts()
                .heartbeat_interval
                .max(Duration::from_millis(1));
            let answer = loop {
                tokio::select! {
                    result = &mut exchange => break result?,
                    () = tokio::time::sleep(interval) => {}
                }
                if !super::registry::owner_alive(&backend, &peer)
                    .await
                    .unwrap_or(false)
                {
                    return Err(RoomKitError::TurnLink {
                        reason: "executor_lost".into(),
                        message: Some("The other side stopped beating".into()),
                    });
                }
            };
            serde_json::from_str::<RoomCallResult>(&answer).map_err(|error| invalid(&error))?
        } else {
            let owner = lock(&backend.0.owner).clone();
            in_memory::local_call(owner, call).await?
        };
        // A refusal can precede decoding the call and therefore carry an empty call id.
        // The Redis question reference already correlates the answer with this caller.
        if result.call_id != call_id && !(result.call_id.is_empty() && result.error.is_some()) {
            return Err(RoomKitError::TurnLink {
                reason: "frame_invalid".into(),
                message: Some("the room owner answered a different call".into()),
            });
        }
        Ok(result)
    }

    /// Enqueues at once for an owner on another node: a room call that
    /// follows must follow this work in the owner's Redis stream, even when
    /// the caller never yields between them.
    fn publication(
        &self,
        message: RoomPublication,
    ) -> Result<Option<TurnFuture<'static, Result<(), RoomKitError>>>, RoomKitError> {
        let backend = DistributedTurnBackend(self.node.upgrade().ok_or(RoomKitError::KitClosed)?);
        if *backend.0.stop.borrow() {
            return Err(super::routes::closed());
        }
        if let Some(peer) = self.peer(&backend) {
            let envelope = OwnerMessageEnvelope {
                message: OwnerMessage::RoomPublication(message),
            };
            let body = serde_json::to_string(&envelope).map_err(|error| invalid(&error))?;
            let reference = Uuid::new_v4().simple().to_string();
            let message = Message::new("work", backend.node_id(), body, "", &reference);
            backend.post_unconfirmed(&peer, &message)?;
            Ok(None)
        } else {
            let owner = lock(&backend.0.owner).clone();
            Ok(Some(Box::pin(in_memory::local_publication(owner, message))))
        }
    }
}

impl RoomOwner for PeerOwner {
    fn room_call(&self, message: RoomCall) -> TurnFuture<'_, Result<RoomCallResult, RoomKitError>> {
        Box::pin(async move { self.of(&message.turn_id).call_result(message).await })
    }

    fn room_publication(
        &self,
        message: RoomPublication,
    ) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            let route = self.of(&message.turn_id);
            if let Some(local) = route.publication(message)? {
                local.await?;
            }
            Ok(())
        })
    }
}

fn invalid(error: &serde_json::Error) -> RoomKitError {
    RoomKitError::TurnLink {
        reason: "frame_invalid".into(),
        message: Some(error.to_string()),
    }
}

#[cfg(test)]
mod local_tests;
