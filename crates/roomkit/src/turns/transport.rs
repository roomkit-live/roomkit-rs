//! The public contract of a turn transport (RFC §10.6).
use super::{
    ChannelControl, ChannelResult, ExecutorInfo, RoomOwner, ServedExecutor, TurnFuture, TurnLink,
    TurnRequest, TurnTimeouts,
};
use crate::RoomKitError;
use std::sync::Arc;

/// Carries turns and channel controls between room owners and executors.
pub trait TurnBackend: std::any::Any + Send + Sync {
    /// Deliver a request, marking it handed before waiting for the executor's answer.
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>>;
    /// Register an executor until it or the backend closes.
    fn serve(&self, executor: Arc<dyn ServedExecutor>) -> TurnFuture<'_, Result<(), RoomKitError>>;
    /// Carry a channel notification or a call to the executor holding its state.
    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>>;
    /// Connect the room owner's port for work that outlives a turn.
    fn attend(&self, _owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async { Ok(()) })
    }
    /// Override the handle's process-local claims when the transport shares them across owners.
    /// Returning `None` selects the state held by [`super::TurnBackendHandle`].
    fn claim_presence<'a>(
        &'a self,
        _channel_id: &'a str,
        _room_id: &'a str,
        _online: Option<bool>,
    ) -> Option<TurnFuture<'a, Result<bool, RoomKitError>>> {
        None
    }
    /// Supply a catalogue read, excluding the owner's own executor. Return
    /// `None` when this transport does not list workers. An empty catalogue
    /// is `Some` with an empty list. Perform I/O only when the future is polled.
    fn executors(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        None
    }

    /// Refresh a cached catalogue before a turn would be skipped (§19.7.5).
    /// The default reads the same catalogue as `executors`.
    fn executors_now(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        self.executors()
    }
    /// Bounds agreed by the participants, if supplied by this transport.
    fn timeouts(&self) -> Option<TurnTimeouts> {
        None
    }
    /// Release the transport and fail its outstanding turns.
    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>>;
}
