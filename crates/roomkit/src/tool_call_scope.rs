//! The tool call a handler runs within, which a task it spawns with
//! [`keep_context`](crate::keep_context) carries (RFC §21.4): Python's
//! `ContextVar` of the current tool call, which `asyncio.create_task`
//! copies. The call is an AI channel's (`roomkit_ai::current_tool_call`);
//! the kit only carries it, as it carries its own context.

use std::any::Any;
use std::future::Future;
use std::sync::Arc;

/// The call a handler runs within, as its channel's crate scopes it.
type Call = Arc<dyn Any + Send + Sync>;

tokio::task_local! {
    static TOOL_CALL: Call;
}

/// The tool call the current task runs within, as its channel scoped it;
/// `None` outside one. For `roomkit_ai::current_tool_call`.
#[doc(hidden)]
pub fn current_tool_call_scope() -> Option<Arc<dyn Any + Send + Sync>> {
    TOOL_CALL.try_with(Arc::clone).ok()
}

/// Runs `work` within the tool call `call`. For `roomkit_ai`'s handlers.
#[doc(hidden)]
pub async fn within_tool_call_scope<F: Future>(
    call: Arc<dyn Any + Send + Sync>,
    work: F,
) -> F::Output {
    TOOL_CALL.scope(call, work).await
}

/// `future` within the tool call its caller runs within, if any: what
/// [`keep_context`](crate::keep_context) carries into a spawned task.
pub(crate) fn within_current_tool_call<F>(
    future: F,
) -> impl Future<Output = F::Output> + Send + 'static
where
    F: Future + Send + 'static,
{
    let call = current_tool_call_scope();
    async move {
        match call {
            Some(call) => TOOL_CALL.scope(call, future).await,
            None => future.await,
        }
    }
}
