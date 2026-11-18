//! The realtime outlet of an executor's text channels (RFC §10.6.3).
use super::{TurnWiring, WiringEnv};
use crate::model::RoomId;
use crate::realtime::{EphemeralCallback, EphemeralEvent, EphemeralEventType, RealtimeBackend};
use crate::{ChannelError, RoomKitError};
use std::sync::Arc;

/// Routes a channel's publication to its turn owner or to the host's fallback.
#[derive(Clone)]
pub struct TurnRealtime(Arc<dyn WiringEnv>);

impl TurnWiring {
    /// The realtime outlet this wiring gives its channels.
    pub fn realtime(&self) -> TurnRealtime {
        TurnRealtime(Arc::clone(&self.env))
    }
}

impl TurnRealtime {
    /// Publish to a named topic, preserving the original event.
    ///
    /// # Errors
    /// Propagates the fallback's publication error.
    pub async fn publish(&self, channel: &str, event: EphemeralEvent) -> Result<(), ChannelError> {
        if let Some(host) = crate::turn::turn_in_scope() {
            host.publish_scoped(Some(channel), event);
        } else if event.r#type != EphemeralEventType::TextDelta
            && let Some(backend) = self.0.realtime()
        {
            backend.publish(channel, event).await?;
        }
        Ok(())
    }

    /// Publish using the fallback's room-specific operation and its current creation mark.
    ///
    /// # Errors
    /// Propagates the fallback's publication error.
    pub async fn publish_to_room(
        &self,
        room_id: &RoomId,
        event: EphemeralEvent,
    ) -> Result<(), ChannelError> {
        if let Some(host) = crate::turn::turn_in_scope() {
            host.publish_scoped(Some(&format!("room:{room_id}")), event);
            Ok(())
        } else {
            self.outside_room(room_id, event).await
        }
    }

    pub(super) async fn outside_room(
        &self,
        room: &RoomId,
        mut event: EphemeralEvent,
    ) -> Result<(), ChannelError> {
        if event.r#type != EphemeralEventType::TextDelta
            && let Some(backend) = self.0.realtime()
        {
            if event.room_created_at.is_none() {
                event.room_created_at = self.0.room_mark(room).await;
            }
            backend.publish_to_room(room, event).await?;
        }
        Ok(())
    }

    fn backend(&self) -> Result<Arc<dyn RealtimeBackend>, RoomKitError> {
        self.0.realtime().ok_or_else(|| {
            RoomKitError::Runtime("This executor has no realtime backend to subscribe on".into())
        })
    }

    /// Subscribe on the host backend selected at the time of the call.
    ///
    /// # Errors
    /// Fails when this executor has no realtime backend.
    pub async fn subscribe(
        &self,
        channel: &str,
        callback: EphemeralCallback,
    ) -> Result<String, RoomKitError> {
        Ok(self.backend()?.subscribe(channel, callback).await)
    }

    /// Subscribe to a room's notifications.
    ///
    /// # Errors
    /// Fails when this executor has no realtime backend.
    pub async fn subscribe_to_room(
        &self,
        room_id: &RoomId,
        callback: EphemeralCallback,
    ) -> Result<String, RoomKitError> {
        self.subscribe(&format!("room:{room_id}"), callback).await
    }

    /// Remove a subscription from the currently selected host backend.
    ///
    /// # Errors
    /// Fails when this executor has no realtime backend.
    pub async fn unsubscribe(&self, subscription_id: &str) -> Result<bool, RoomKitError> {
        Ok(self.backend()?.unsubscribe(subscription_id).await)
    }
}

impl std::fmt::Debug for TurnRealtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TurnRealtime").finish_non_exhaustive()
    }
}
