//! The span the current code runs under, Python's `ContextVar` of
//! `roomkit.telemetry.context`: a tokio task-local the kit scopes around
//! the work of each span it opens, which [`keep_context`](crate::keep_context)
//! carries into a task the work spawns, as `asyncio.create_task` copies the
//! context. A task spawned plainly runs under no span.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::Arc;

use super::TelemetryProvider;
use super::open::{KitTelemetry, Late};

/// The current span: its id, the provider that started it, and the
/// provider's own context for it.
#[derive(Clone)]
struct Current {
    telemetry: KitTelemetry,
    span_id: Option<String>,
    context: Option<Arc<dyn Any + Send + Sync>>,
    /// The span of the call this work is that opens once the call goes in:
    /// from then on, the current one.
    late: Option<Arc<Late>>,
}

impl Current {
    /// The current span: the late one once it opened.
    fn span_id(&self) -> Option<String> {
        self.late
            .as_ref()
            .and_then(|late| late.id())
            .map(str::to_owned)
            .or_else(|| self.span_id.clone())
    }

    /// The provider's own context of the current span.
    #[cfg(feature = "opentelemetry")]
    fn span_context(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        match self.late.as_deref().filter(|late| late.id().is_some()) {
            Some(late) => late.context(),
            None => self.context.clone(),
        }
    }
}

impl fmt::Debug for Current {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Current")
            .field("span_id", &self.span_id())
            .finish_non_exhaustive()
    }
}

tokio::task_local! {
    // `None` where a task spawned under no span runs: every scope below is
    // this one type, with no future around the work, which an `async` block
    // would hold twice, doubling at each scope a call goes through.
    static CURRENT: Option<Current>;
}

/// What is current here, if anything.
fn read<T>(with: impl FnOnce(&Current) -> Option<T>) -> Option<T> {
    CURRENT
        .try_with(|current| current.as_ref().and_then(with))
        .ok()
        .flatten()
}

/// Python `get_current_span()`: the id of the span the current code runs
/// under, which a span it starts names as its parent; `None` outside the
/// kit's work.
#[must_use]
pub fn get_current_span() -> Option<String> {
    read(Current::span_id)
}

/// The provider of the span the current code runs under: the kit's, for
/// a channel the kit calls (an AI channel's generation). Hidden: Python's
/// channels hold the kit's provider in an attribute the kit sets.
#[doc(hidden)]
#[must_use]
pub fn current_telemetry() -> Option<Arc<dyn TelemetryProvider>> {
    read(|current| Some(Arc::clone(&current.telemetry.provider)))
}

/// The telemetry of the kit whose call this is.
pub(crate) fn current_kit_telemetry() -> Option<KitTelemetry> {
    read(|current| Some(current.telemetry.clone()))
}

/// Whether the kit whose call this is records what can hold personal data
/// on its spans. Hidden: for the crates beside this one.
#[doc(hidden)]
#[must_use]
pub fn current_records_sensitive_data() -> bool {
    read(|current| Some(current.telemetry.sensitive)).unwrap_or(false)
}

/// The provider's own context of the current span, Python's
/// `get_current_telemetry_ctx()`.
#[cfg(feature = "opentelemetry")]
pub(crate) fn current_span_context() -> Option<Arc<dyn Any + Send + Sync>> {
    read(Current::span_context)
}

/// What names the current span to the executor of a turn: its id, with what
/// names it to another process, for an executor elsewhere to continue its
/// trace (RFC §10.6.9), Python's `_trace`; `None` under no span.
pub(crate) fn current_trace() -> Option<BTreeMap<String, String>> {
    read(|current| {
        let span_id = current.span_id().filter(|span_id| !span_id.is_empty())?;
        let mut carrier = current.telemetry.provider.inject_context(&span_id);
        carrier.insert("span_id".to_owned(), span_id);
        Some(carrier)
    })
}

/// `work`, a turn's, continuing the span `trace`, its request's, names,
/// whichever process runs it and whatever its context holds: what it opens
/// sits under that span, in the owner's trace (RFC §10.6.9, §15.7), Python's
/// `_run_work`. Under the kit's `telemetry`.
pub(crate) fn within_turn_trace<F: Future>(
    telemetry: Option<KitTelemetry>,
    trace: Option<&BTreeMap<String, String>>,
    work: F,
) -> impl Future<Output = F::Output> + use<F> {
    let held = CURRENT.try_with(Option::clone).ok().flatten();
    let named = trace.and_then(|trace| Some((trace, trace.get("span_id")?)));
    let current = match (named, telemetry) {
        (Some((trace, span_id)), Some(telemetry))
            if !span_id.is_empty() && get_current_span().as_ref() != Some(span_id) =>
        {
            let context = telemetry.provider.extract_context(trace);
            Some(Current {
                telemetry,
                span_id: Some(span_id.clone()),
                context,
                late: None,
            })
        }
        _ => held,
    };
    CURRENT.scope(current, work)
}

/// `work` under no span and no telemetry: a task of another process's.
#[cfg(test)]
pub(crate) fn within_no_span<F: Future>(work: F) -> impl Future<Output = F::Output> {
    CURRENT.scope(None, work)
}

/// The late span of the call the current work is, if it has one: for the run
/// that adopts the call, which is another's work.
pub(crate) fn current_late_span() -> Option<Arc<Late>> {
    read(|current| current.late.clone())
}

/// The call the current work is goes in: its late span, if it has one and
/// nothing ended it, opens.
pub(crate) fn current_call_goes_in() {
    if let Some(late) = current_late_span() {
        late.open();
    }
}

/// `work` under `late`, which is the current span once it opens, and until
/// then under whatever is current.
pub(super) fn within_late_span<F: Future>(
    telemetry: &KitTelemetry,
    late: Arc<Late>,
    work: F,
) -> impl Future<Output = F::Output> + use<F> {
    let current = Current {
        telemetry: telemetry.clone(),
        span_id: get_current_span(),
        context: read(|current| current.context.clone()),
        late: Some(late),
    };
    CURRENT.scope(Some(current), work)
}

/// `work` with `telemetry` the current one, under `span_id` if any.
pub(crate) fn within_optional_span<F: Future>(
    telemetry: &KitTelemetry,
    span_id: Option<String>,
    work: F,
) -> impl Future<Output = F::Output> + use<F> {
    let context = span_id
        .as_deref()
        .and_then(|span_id| telemetry.provider.get_span_context(span_id));
    let current = Current {
        telemetry: telemetry.clone(),
        span_id,
        context,
        late: None,
    };
    CURRENT.scope(Some(current), work)
}

/// `future` under the current span, if any: what
/// [`keep_context`](crate::keep_context) and the kit's execution context
/// carry into a spawned task. It reads the span when called, in the task
/// that spawns.
pub(crate) fn within_current_span<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let current = CURRENT.try_with(Option::clone).ok().flatten();
    CURRENT.scope(current, future)
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
        let telemetry = KitTelemetry {
            provider: Arc::new(MockTelemetryProvider::default()),
            sensitive: false,
            suppressed: Arc::default(),
        };
        let read = within_optional_span(&telemetry, Some("s1".to_owned()), async {
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
