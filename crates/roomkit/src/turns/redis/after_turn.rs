//! An executor's route to the owner that submitted a turn (RFC §10.6.3, §10.6.5).
//! The last 10,000 received turns retain their routes, as in Python's Redis Routes.
use serde_json::Value;
use std::sync::{Arc, Weak};
use std::time::Duration;
use uuid::Uuid;

use super::stream::Message;
use super::{Node, RedisTurnBackend};
use crate::RoomKitError;
use crate::turns::error::rebuild_kit_error;
use crate::turns::lock;
use crate::turns::wire::{OwnerMessage, OwnerMessageEnvelope, RoomCall, RoomCallResult};

#[derive(Debug)]
pub(crate) struct PeerOwner {
    node: Weak<Node>,
    peer: Weak<str>,
}

impl RedisTurnBackend {
    pub(super) fn remember_owner(&self, peer: &str) -> PeerOwner {
        let peer: Arc<str> = Arc::from(peer);
        let port = PeerOwner {
            node: Arc::downgrade(&self.0),
            peer: Arc::downgrade(&peer),
        };
        let mut routes = lock(&self.0.routes);
        routes.turn_owners.push_back(peer);
        if routes.turn_owners.len() > 10_000 {
            routes.turn_owners.pop_front();
        }
        port
    }
}

impl PeerOwner {
    pub(crate) async fn call(&self, call: RoomCall) -> Result<Value, RoomKitError> {
        let backend = RedisTurnBackend(self.node.upgrade().ok_or(RoomKitError::KitClosed)?);
        if *backend.0.stop.borrow() {
            return Err(super::routes::closed());
        }
        let call_id = call.call_id.clone();
        let envelope = OwnerMessageEnvelope {
            message: OwnerMessage::RoomCall(call),
        };
        let result = if let Some(peer) = self.peer.upgrade() {
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
            let owner = lock(&backend.0.owner).clone().ok_or_else(no_owner)?;
            owner(envelope).await.ok_or_else(no_owner)?
        };
        if result.call_id != call_id {
            return Err(RoomKitError::TurnLink {
                reason: "frame_invalid".into(),
                message: Some("the room owner answered a different call".into()),
            });
        }
        match result.error {
            Some(error) => Err(rebuild_kit_error(&error)),
            None => Ok(result.result),
        }
    }

    /// Enqueue immediately: a subsequent room call must follow this work
    /// in the owner's Redis stream even if the caller never yields between them.
    pub(crate) fn publish(&self, message: OwnerMessage) {
        let Some(node) = self.node.upgrade() else {
            return;
        };
        let backend = RedisTurnBackend(node);
        if *backend.0.stop.borrow() {
            return;
        }
        let envelope = OwnerMessageEnvelope { message };
        if let Some(peer) = self.peer.upgrade() {
            let body = match serde_json::to_string(&envelope) {
                Ok(body) => body,
                Err(error) => {
                    tracing::debug!(%error, "failed to encode a room publication");
                    return;
                }
            };
            let reference = Uuid::new_v4().simple().to_string();
            let message = Message::new("work", backend.node_id(), body, "", &reference);
            if let Err(error) = backend.post_work(&peer, &message) {
                tracing::debug!(%error, "a publication after a turn did not reach its owner");
            }
        } else if let Some(owner) = lock(&backend.0.owner).clone() {
            tokio::spawn(owner(envelope));
        }
    }
}

fn invalid(error: &serde_json::Error) -> RoomKitError {
    RoomKitError::TurnLink {
        reason: "frame_invalid".into(),
        message: Some(error.to_string()),
    }
}

fn no_owner() -> RoomKitError {
    RoomKitError::Runtime("No room owner is reached outside a turn on this backend".into())
}
