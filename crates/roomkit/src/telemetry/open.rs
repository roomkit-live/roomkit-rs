//! A span the kit opened: Python's pair of `start_span` and `end_span`
//! around a piece of its work, with the span made current for that work.

use std::future::Future;
use std::sync::Arc;

use roomkit_core::model::Metadata;

use super::context::{get_current_span, within_span};
use super::{EndSpanOptions, SpanKind, StartSpanOptions, TelemetryProvider};

/// A span the kit started, a child of the current span unless it names
/// another parent, ended once. Dropped unended, the work it measured was
/// cancelled, and it ends `cancelled`; or the work panicked, and it ends
/// `error`.
#[derive(Debug)]
pub(crate) struct OpenSpan {
    provider: Arc<dyn TelemetryProvider>,
    id: String,
    ended: bool,
}

impl OpenSpan {
    pub(crate) fn start(
        provider: &Arc<dyn TelemetryProvider>,
        kind: SpanKind,
        name: &str,
        mut options: StartSpanOptions,
    ) -> Self {
        if options.parent_id.is_none() {
            options.parent_id = get_current_span();
        }
        let id = provider.start_span(kind, name, options);
        Self {
            provider: Arc::clone(provider),
            id,
            ended: false,
        }
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    /// `work` with this span current.
    pub(crate) fn within<F: Future>(&self, work: F) -> impl Future<Output = F::Output> + use<F> {
        within_span(Arc::clone(&self.provider), self.id.clone(), work)
    }

    /// Ends it `ok`, `attributes` added.
    pub(crate) fn end(mut self, attributes: Metadata) {
        self.finish(EndSpanOptions {
            attributes,
            ..EndSpanOptions::default()
        });
    }

    /// Ends it `cancelled`, `attributes` added.
    pub(crate) fn cancel(mut self, attributes: Metadata) {
        self.finish(EndSpanOptions {
            status: "cancelled".to_owned(),
            attributes,
            ..EndSpanOptions::default()
        });
    }

    /// Ends it `error`, `message` what failed, `attributes` added.
    pub(crate) fn fail(mut self, message: impl Into<String>, attributes: Metadata) {
        self.finish(EndSpanOptions {
            status: "error".to_owned(),
            error_message: Some(message.into()),
            attributes,
        });
    }

    fn finish(&mut self, options: EndSpanOptions) {
        if !self.ended {
            self.ended = true;
            self.provider.end_span(&self.id, options);
        }
    }
}

impl Drop for OpenSpan {
    fn drop(&mut self) {
        let options = if std::thread::panicking() {
            EndSpanOptions {
                status: "error".to_owned(),
                error_message: Some("panicked".to_owned()),
                ..EndSpanOptions::default()
            }
        } else {
            EndSpanOptions {
                status: "cancelled".to_owned(),
                ..EndSpanOptions::default()
            }
        };
        self.finish(options);
    }
}

/// `work` run with `provider` the kit's telemetry, its current span
/// `span_id`, else whatever is current: every call the driver makes runs so,
/// for a channel to report to its kit's provider.
pub(crate) fn within_kit<F: Future>(
    provider: &Arc<dyn TelemetryProvider>,
    span_id: Option<String>,
    work: F,
) -> impl Future<Output = F::Output> + use<F> {
    let span_id = span_id.or_else(get_current_span);
    super::context::within_optional_span(Arc::clone(provider), span_id, work)
}
