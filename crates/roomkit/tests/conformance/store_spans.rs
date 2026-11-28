//! The run's store tracing its delivery cursor (roomkit-specs
//! `conformance/README.md`, "Setup": `kit.store_spans`).
//!
//! Once declared, each read and each write of a room's delivery cursor
//! (RFC §13.5) records a `store.query` span, `store.<operation>`, to the
//! telemetry provider the kit handed its store, under the span current when
//! the operation is called, as a store that traces its queries does; the
//! operation itself is the store's own.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit::telemetry::{
    EndSpanOptions, SpanKind, StartSpanOptions, TelemetryProvider, get_current_span,
};

/// Whether the store traces its cursor, and the provider the kit handed it.
#[derive(Default)]
pub(crate) struct CursorSpans {
    declared: AtomicBool,
    provider: Mutex<Option<Arc<dyn TelemetryProvider>>>,
}

impl CursorSpans {
    /// The setup's `kit.store_spans`: from now on the cursor is traced.
    pub(crate) fn declare(&self) {
        self.declared.store(true, Ordering::SeqCst);
    }

    /// Keeps the provider the kit hands its store.
    pub(crate) fn set_provider(&self, provider: Arc<dyn TelemetryProvider>) {
        *self.provider.lock().unwrap_or_else(PoisonError::into_inner) = Some(provider);
    }

    /// `work`, the cursor operation `operation`, under a span of its own
    /// when the store traces its cursor.
    pub(crate) async fn spanned<T>(&self, operation: &str, work: impl Future<Output = T>) -> T {
        let provider = self
            .provider
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(provider) = provider.filter(|_| self.declared.load(Ordering::SeqCst)) else {
            return work.await;
        };
        let options = StartSpanOptions {
            parent_id: get_current_span(),
            ..StartSpanOptions::default()
        };
        let span =
            provider.start_span(SpanKind::StoreQuery, &format!("store.{operation}"), options);
        let outcome = work.await;
        provider.end_span(&span, EndSpanOptions::default());
        outcome
    }
}
