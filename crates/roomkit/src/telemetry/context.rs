//! The span the current code runs under, Python's `ContextVar` of
//! `roomkit.telemetry.context`: a tokio task-local the kit scopes around
//! the work of each span it opens, which [`keep_context`](crate::keep_context)
//! carries into a task the work spawns, as `asyncio.create_task` copies the
//! context. A task spawned plainly runs under no span.

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use super::TelemetryProvider;

/// The current span: its id, the provider that started it, and the
/// provider's own context for it.
#[derive(Clone)]
struct Current {
    provider: Arc<dyn TelemetryProvider>,
    span_id: Option<String>,
    #[cfg_attr(
        not(feature = "opentelemetry"),
        expect(
            dead_code,
            reason = "only OpenTelemetryProvider reads its context back"
        )
    )]
    context: Option<Arc<dyn Any + Send + Sync>>,
}

impl fmt::Debug for Current {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Current")
            .field("span_id", &self.span_id)
            .finish_non_exhaustive()
    }
}

tokio::task_local! {
    static CURRENT: Current;
}

/// Python `get_current_span()`: the id of the span the current code runs
/// under, which a span it starts names as its parent; `None` outside the
/// kit's work.
#[must_use]
pub fn get_current_span() -> Option<String> {
    CURRENT
        .try_with(|current| current.span_id.clone())
        .ok()
        .flatten()
}

/// The provider of the span the current code runs under: the kit's, for
/// a channel the kit calls (an AI channel's generation). Hidden: Python's
/// channels hold the kit's provider in an attribute the kit sets.
#[doc(hidden)]
#[must_use]
pub fn current_telemetry() -> Option<Arc<dyn TelemetryProvider>> {
    CURRENT
        .try_with(|current| Arc::clone(&current.provider))
        .ok()
}

/// The provider's own context of the current span, Python's
/// `get_current_telemetry_ctx()`.
#[cfg(feature = "opentelemetry")]
pub(crate) fn current_span_context() -> Option<Arc<dyn Any + Send + Sync>> {
    CURRENT
        .try_with(|current| current.context.clone())
        .ok()
        .flatten()
}

/// Runs `work` under the span `span_id` of `provider`, Python's
/// `restored_span`: [`get_current_span`] reads it there. Hidden: for the
/// crates beside this one.
#[doc(hidden)]
pub fn within_span<F: Future>(
    provider: Arc<dyn TelemetryProvider>,
    span_id: String,
    work: F,
) -> impl Future<Output = F::Output> {
    within_optional_span(provider, Some(span_id), work)
}

/// Runs `work` under the span `span_id` of another process, `context` what
/// [`TelemetryProvider::extract_context`] read of it: Python's
/// `restored_span(span_id, telemetry_ctx=...)`. Hidden: for the turn
/// executors to come.
#[doc(hidden)]
pub fn within_extracted_span<F: Future>(
    provider: Arc<dyn TelemetryProvider>,
    span_id: String,
    context: Option<Arc<dyn Any + Send + Sync>>,
    work: F,
) -> impl Future<Output = F::Output> {
    let current = Current {
        provider,
        span_id: Some(span_id),
        context,
    };
    CURRENT.scope(current, work)
}

/// `work` with `provider` the current one, under `span_id` if any.
pub(crate) fn within_optional_span<F: Future>(
    provider: Arc<dyn TelemetryProvider>,
    span_id: Option<String>,
    work: F,
) -> impl Future<Output = F::Output> {
    let context = span_id
        .as_deref()
        .and_then(|span_id| provider.get_span_context(span_id));
    let current = Current {
        provider,
        span_id,
        context,
    };
    CURRENT.scope(current, work)
}

/// `future` under the current span, if any: what
/// [`keep_context`](crate::keep_context) and the kit's execution context
/// carry into a spawned task. It reads the span when called, in the task
/// that spawns.
pub(crate) fn within_current_span<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let current = CURRENT.try_with(Current::clone).ok();
    async move {
        match current {
            Some(current) => CURRENT.scope(current, future).await,
            None => future.await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::MockTelemetryProvider;

    // Python: tests/test_telemetry.py::TestTelemetryContextPropagation::test_telemetry_context_none_by_default
    #[tokio::test]
    async fn no_span_is_current_outside_the_kit_s_work() {
        assert_eq!(get_current_span(), None);
        assert!(current_telemetry().is_none());
    }

    // Python: tests/test_telemetry.py::TestTelemetryContextPropagation::test_telemetry_context_set_and_retrieved
    #[tokio::test]
    async fn a_span_is_current_within_its_work_and_a_kept_task_s() {
        let provider: Arc<dyn TelemetryProvider> = Arc::new(MockTelemetryProvider::default());
        let read = within_span(Arc::clone(&provider), "s1".to_owned(), async {
            let spawned = tokio::spawn(crate::keep_context(async { get_current_span() }));
            let plain = tokio::spawn(async { get_current_span() });
            (
                get_current_span(),
                spawned.await.unwrap(),
                plain.await.unwrap(),
            )
        })
        .await;
        assert_eq!(read, (Some("s1".to_owned()), Some("s1".to_owned()), None));
        assert_eq!(get_current_span(), None, "the scope ends with its work");
    }
}
