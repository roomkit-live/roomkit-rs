//! Hook handlers: the host code behind a hook registration (RFC §9).

use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{ErrorRecord, HookResult, RoomContext, RoomEvent};
use roomkit_core::rules::hook_chain::HookOutcome;

use crate::channel::BoxFuture;

/// What a hook's handler failed with. A SYNC hook that fails lets the event
/// through, unless the hook or its trigger fails closed (RFC §9.3).
pub type HookError = Box<dyn Error + Send + Sync>;

/// The host code of a hook: given the payload and the room, it decides.
///
/// Any `Fn(RoomEvent, RoomContext) -> impl Future<Output = Result<HookResult,
/// HookError>>` is a handler. An observer's result is discarded.
pub trait HookHandler: Send + Sync + 'static {
    /// Runs the hook on `event`.
    fn call(&self, event: RoomEvent, context: RoomContext) -> BoxFuture<'static, HookReturn>;
}

/// What a hook handler returns.
pub type HookReturn = Result<HookResult, HookError>;

impl<F, Fut> HookHandler for F
where
    F: Fn(RoomEvent, RoomContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = HookReturn> + Send + 'static,
{
    fn call(&self, event: RoomEvent, context: RoomContext) -> BoxFuture<'static, HookReturn> {
        Box::pin(self(event, context))
    }
}

/// An observer turned into a handler that always allows: what an observer
/// returns is discarded anyway (RFC §9.3).
pub(crate) struct Observer<F>(pub(crate) F);

impl<F, Fut> HookHandler for Observer<F>
where
    F: Fn(RoomEvent, RoomContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    fn call(&self, event: RoomEvent, context: RoomContext) -> BoxFuture<'static, HookReturn> {
        let observing = (self.0)(event, context);
        Box::pin(async move {
            observing.await;
            Ok(HookResult::allow())
        })
    }
}

/// Runs `handler` within `timeout` and reads what it produced as the core
/// takes it.
pub(crate) async fn run(
    handler: Arc<dyn HookHandler>,
    event: RoomEvent,
    context: RoomContext,
    timeout: Duration,
) -> HookOutcome<RoomEvent> {
    match tokio::time::timeout(timeout, handler.call(event, context)).await {
        Ok(Ok(result)) => HookOutcome::Returned(result),
        Ok(Err(error)) => HookOutcome::Raised(ErrorRecord::new("HookError", error.to_string())),
        Err(_elapsed) => HookOutcome::TimedOut,
    }
}
