//! The optional services a standalone turn owner supplies to its workers.
use roomkit_core::model::{ChannelId, Metadata, RoomEvent, RoomId};
use serde_json::Value;

use crate::realtime::EphemeralEvent;
use crate::turns::TurnFuture;
use crate::turns::wire::{OwnerTool, RoomCall, ToolCall, TurnRequest};
use crate::{ChannelError, RoomKitError};

/// What a standalone owner runs for the worker's turn host (RFC §10.6.5).
pub trait OwnerHost: Send + Sync + 'static {
    /// Run a verb while the turn is active, with the submitted request.
    fn call<'a>(
        &'a self,
        request: &'a TurnRequest,
        verb: &'a str,
        arguments: Metadata,
    ) -> TurnFuture<'a, Result<Value, ChannelError>>;
    /// Run a verb from work retained after its turn.
    fn call_between_turns<'a>(
        &'a self,
        message: &'a RoomCall,
    ) -> TurnFuture<'a, Result<Value, ChannelError>> {
        Box::pin(async move {
            Err(RoomKitError::Value(format!(
                "No verb {} is served between turns",
                crate::error::repr(&message.verb)
            ))
            .into())
        })
    }
    /// Read private history older than the first event carried in the request.
    fn channel_rows<'a>(
        &'a self,
        _room_id: &'a RoomId,
        _channel_id: &'a ChannelId,
        _first: Option<u64>,
    ) -> TurnFuture<'a, Result<Vec<RoomEvent>, ChannelError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

/// The owner-served tools a worker may request while its turn runs (RFC §10.6.5).
pub trait OwnerTools: Send + Sync + 'static {
    /// Tools available to the named channel in this room.
    fn declared(&self, channel_id: &ChannelId, room_id: &RoomId) -> Vec<OwnerTool>;
    /// Invoke the tool under the incoming call's execution context.
    fn serve<'a>(
        &'a self,
        request: &'a TurnRequest,
        call: ToolCall,
    ) -> TurnFuture<'a, Result<Value, ChannelError>>;
}

/// The standalone owner's destinations for publications from workers (RFC §10.6.3).
pub trait OwnerEvents: Send + Sync + 'static {
    /// Publish an ephemeral on its room topic.
    fn publish<'a>(
        &'a self,
        channel: &'a str,
        event: EphemeralEvent,
    ) -> TurnFuture<'a, Result<(), RoomKitError>>;
    /// Emit a framework event for the room, channel and event it concerns.
    fn emit<'a>(
        &'a self,
        event_type: &'a str,
        room_id: Option<&'a str>,
        channel_id: Option<&'a str>,
        event_id: Option<&'a str>,
        data: Metadata,
    ) -> TurnFuture<'a, Result<(), RoomKitError>>;
    /// Whether the current binding permits publication to the entire room.
    fn reaches_room<'a>(
        &'a self,
        _room_id: &'a RoomId,
        _channel_id: &'a ChannelId,
    ) -> TurnFuture<'a, Result<bool, RoomKitError>> {
        Box::pin(async { Ok(false) })
    }
}
