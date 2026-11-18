//! Framework events emitted through an executor's wiring (RFC §10.6.3).
use super::{TurnFuture, TurnWiring};
use crate::model::{ChannelId, EventId, Metadata, RoomId};
use crate::{FrameworkEvent, RoomKitError};
use std::sync::Arc;

/// Optional keyword arguments to [`TurnWiring::emit`]. The fallback receives
/// omitted data as `None` and an explicitly empty mapping as `Some`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct EmitOptions {
    /// Room the fact concerns.
    pub room_id: Option<RoomId>,
    /// Channel the fact concerns.
    pub channel_id: Option<ChannelId>,
    /// Event the fact concerns.
    pub event_id: Option<EventId>,
    /// Additional fields provided by the caller.
    pub data: Option<Metadata>,
}

/// A framework event callback returned by [`super::WiringEnv::emitter`].
pub type FrameworkEmitter =
    Arc<dyn Fn(String, EmitOptions) -> TurnFuture<'static, Result<(), RoomKitError>> + Send + Sync>;

impl EmitOptions {
    pub(super) fn event(self, event_type: String) -> FrameworkEvent {
        let mut event = FrameworkEvent::new(event_type);
        event.room_id = self.room_id;
        event.channel_id = self.channel_id;
        event.event_id = self.event_id;
        event.data = self.data.unwrap_or_default();
        event
    }
}

impl TurnWiring {
    /// Emit a framework event to the current turn's owner, or the environment's callback.
    ///
    /// # Errors
    /// Returns the application callback's failure.
    pub async fn emit(&self, event_type: &str, options: EmitOptions) -> Result<(), RoomKitError> {
        let fallback = self.env.emitter();
        if let Some(host) = crate::turn::turn_in_scope() {
            host.emit_framework_event(options.event(event_type.to_owned()));
        } else if let Some(emit) = fallback {
            emit(event_type.to_owned(), options).await?;
        }
        Ok(())
    }

    pub(crate) async fn notify(
        &self,
        event_type: &str,
        room: &RoomId,
        channel: &ChannelId,
        data: Metadata,
    ) {
        let options = EmitOptions {
            room_id: Some(room.clone()),
            channel_id: Some(channel.clone()),
            data: Some(data),
            ..Default::default()
        };
        if let Err(error) = self.emit(event_type, options).await {
            tracing::debug!(%room, %channel, %event_type, %error, "could not emit a channel notification");
        }
    }
}
