//! The tool call a handler runs within, which a task it spawns with
//! [`keep_context`](crate::keep_context) carries (RFC §21.4): Python's
//! `ContextVar` of the current tool call, which `asyncio.create_task`
//! copies. The call is an AI channel's (`roomkit_ai::current_tool_call`);
//! the kit only carries it, as it carries its own context.

use std::any::Any;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

/// The call a handler runs within, as its channel's crate scopes it.
type Call = Arc<dyn Any + Send + Sync>;

tokio::task_local! {
    static TOOL_CALL: Call;
    static CLOSING_CALLERS: Arc<CallerScopes>;
}

/// The tool call the current task runs within, as its channel scoped it;
/// `None` outside one. For `roomkit_ai::current_tool_call`.
#[doc(hidden)]
pub fn current_tool_call_scope() -> Option<Arc<dyn Any + Send + Sync>> {
    TOOL_CALL.try_with(Arc::clone).ok()
}

/// The current tool scope and the callers whose shutdown this task runs.
/// Internal channel teardown reads their identities without installing any
/// caller's tool, actor or turn context on the shutdown task (RFC §12.10.4).
#[doc(hidden)]
pub fn current_tool_call_scopes() -> Vec<Arc<dyn Any + Send + Sync>> {
    let mut scopes: Vec<_> = current_tool_call_scope().into_iter().collect();
    if let Ok(callers) = CLOSING_CALLERS.try_with(|callers| callers.snapshot()) {
        scopes.extend(callers);
    }
    scopes
}

/// Every caller of the kit's one shutdown, including callers joining later.
#[derive(Default)]
pub(crate) struct CallerScopes(Mutex<Vec<Call>>);

impl std::fmt::Debug for CallerScopes {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CallerScopes")
            .field("count", &self.snapshot().len())
            .finish()
    }
}

impl CallerScopes {
    pub(crate) fn join(&self) {
        let current = current_tool_call_scopes();
        let mut scopes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        for scope in current {
            if !scopes.iter().any(|held| Arc::ptr_eq(held, &scope)) {
                scopes.push(scope);
            }
        }
    }

    fn snapshot(&self) -> Vec<Call> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn scope<F: Future>(self: Arc<Self>, work: F) -> impl Future<Output = F::Output> {
        CLOSING_CALLERS.scope(self, work)
    }
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
    // Observers also enter here directly, without keep_context's boxed
    // input. Keep their large futures out of each carrier's initial state.
    let future = Box::pin(future);
    let call = current_tool_call_scope();
    let callers = CLOSING_CALLERS.try_with(Arc::clone).ok();
    async move {
        let work = async move {
            match call {
                Some(call) => TOOL_CALL.scope(call, future).await,
                None => future.await,
            }
        };
        match callers {
            Some(callers) => callers.scope(work).await,
            None => work.await,
        }
    }
}
