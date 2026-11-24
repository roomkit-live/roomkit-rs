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
    /// Deliver a request to the one executor a caller names, Python's
    /// `submit_to`: by its `executor_id`, and by its `node` when given (`None`:
    /// the one executor of that id that would take the turn). It takes the
    /// turn as it takes one `submit` places and from then holds the room's
    /// state for the channel; no other executor is tried (RFC §10.6.7). The
    /// default refuses: this transport submits to no executor a caller names.
    ///
    /// `no_executor` says the executor did not take the turn (not alive,
    /// closed, not meeting what the room needs, or one of several processes'
    /// executors that would take it under that id, no node named): nothing was
    /// handed, and the request is not marked so. Any other reason comes after
    /// the hand-over, the turn's outcome unknown: it is never handed to another
    /// executor.
    fn submit_to(
        &self,
        request: TurnRequest,
        executor: ExecutorInfo,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async move {
            Err(super::not_taken(
                &executor,
                request.channel_id.as_str(),
                &request.room_hooks,
                request.context.room.organization_id.as_deref(),
            ))
        })
    }
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

    /// Have `listener` run when this process serves an executor or one it
    /// serves closes; changes elsewhere are read at the owner's next look.
    /// The kit gives its own, to publish its agents' presence at once (RFC
    /// §10.6.7). The default keeps none, and the owner learns at its next
    /// look: a backend keeps them in an [`ExecutorWatchers`](super::ExecutorWatchers),
    /// and one that wraps another hands it the listener.
    fn watch_executors(&self, _listener: super::ExecutorsListener) {}

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
