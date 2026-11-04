//! A span the kit opened: Python's pair of `start_span` and `end_span`
//! around a piece of its work, with the span made current for that work.

use std::any::Any;
use std::collections::BTreeSet;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use roomkit_core::model::{ChannelId, Metadata, RoomId};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::HookTrigger;
use serde_json::Value;

use super::context::{get_current_span, within_late_span, within_optional_span};
use super::{Attr, EndSpanOptions, SpanKind, StartSpanOptions, TelemetryProvider};

/// The kit's telemetry: its provider, and whether its spans carry what can
/// hold personal data (`TelemetryConfig::record_sensitive_data`).
#[derive(Clone, Debug)]
pub(crate) struct KitTelemetry {
    pub(crate) provider: Arc<dyn TelemetryProvider>,
    pub(crate) sensitive: bool,
    /// The hook triggers whose hooks open no span.
    pub(crate) suppressed: Arc<BTreeSet<String>>,
}

impl KitTelemetry {
    /// A span's `sender_id`, an address: none unless the kit records what
    /// can hold personal data (Python's `sensitive`).
    pub(crate) fn sender_attributes(&self, sender_id: &str) -> Metadata {
        let mut attributes = Metadata::new();
        if self.sensitive {
            attributes.insert("sender_id".to_owned(), Value::from(sender_id));
        }
        attributes
    }

    /// The span of a message finding its room, Python's `framework.route`.
    pub(crate) fn route_span(
        &self,
        parent_id: Option<String>,
        channel_id: &ChannelId,
        sender_id: &str,
    ) -> OpenSpan {
        let attributes = self.sender_attributes(sender_id);
        OpenSpan::start(
            self,
            SpanKind::InboundPipeline,
            "framework.route",
            StartSpanOptions {
                parent_id,
                channel_id: Some(channel_id.to_string()),
                attributes,
                ..StartSpanOptions::default()
            },
        )
    }

    /// The span of the hook `name` on `trigger` in `room_id`, Python's
    /// `hook.sync.<name>` and `hook.async.<name>`; `None` for a trigger the
    /// telemetry's config suppresses.
    pub(crate) fn hook_span(
        &self,
        kind: SpanKind,
        name: &str,
        trigger: HookTrigger,
        room_id: &RoomId,
    ) -> Option<OpenSpan> {
        let trigger = serde_json::to_value(trigger)
            .ok()
            .and_then(|value| value.as_str().map(str::to_owned))
            .unwrap_or_default();
        if self.suppressed.contains(&trigger) {
            return None;
        }
        let name = if name.is_empty() { "unnamed" } else { name };
        let mut attributes = Metadata::new();
        attributes.insert(Attr::HOOK_NAME.to_owned(), Value::from(name));
        attributes.insert(Attr::HOOK_TRIGGER.to_owned(), Value::from(trigger));
        let options = StartSpanOptions {
            room_id: Some(room_id.to_string()),
            attributes,
            ..StartSpanOptions::default()
        };
        Some(OpenSpan::start(
            self,
            kind,
            &format!("{kind}.{name}"),
            options,
        ))
    }
}

/// Ends a sync hook's span as its `outcome` reads: `ok` with the action it
/// returned; `error` with what it raised, `timeout`, or
/// `invalid return type`.
pub(crate) fn end_hook_span<P>(span: Option<OpenSpan>, outcome: &HookOutcome<P>) {
    let Some(span) = span else {
        return;
    };
    match outcome {
        HookOutcome::Returned(result) => {
            let mut attributes = Metadata::new();
            let action = serde_json::to_value(result.action).unwrap_or_default();
            attributes.insert(Attr::HOOK_RESULT.to_owned(), action);
            span.end(attributes);
        }
        HookOutcome::Raised(error) => {
            span.fail_error(&error.error_type, &error.message, Metadata::new());
        }
        HookOutcome::TimedOut => span.fail("timeout", Metadata::new()),
        HookOutcome::Invalid { .. } => span.fail("invalid return type", Metadata::new()),
    }
}

/// Ends an async hook's span as its `outcome` reads, as Python's, which never
/// reads what an async hook returns: `ok`; `error` with `timeout`, `capped`
/// for a loop its cascade's cap stopped, or `failed`, never what it raised.
pub(crate) fn end_observer_span<P>(span: Option<OpenSpan>, outcome: &HookOutcome<P>) {
    let Some(span) = span else {
        return;
    };
    match outcome {
        HookOutcome::Returned(_) | HookOutcome::Invalid { .. } => span.end(Metadata::new()),
        HookOutcome::TimedOut => span.fail("timeout", Metadata::new()),
        HookOutcome::Raised(error) if error.error_type == "ReentryLoopCapError" => {
            span.fail("capped", Metadata::new());
        }
        HookOutcome::Raised(_) => span.fail("failed", Metadata::new()),
    }
}

/// A span the kit started, a child of the current span unless it names
/// another parent, ended once. Dropped unended, the work it measured was
/// cancelled, and it ends `cancelled`; or the work panicked, and it ends
/// `error`.
#[derive(Debug)]
pub(crate) struct OpenSpan {
    telemetry: KitTelemetry,
    id: String,
    ended: bool,
}

impl OpenSpan {
    pub(crate) fn start(
        telemetry: &KitTelemetry,
        kind: SpanKind,
        name: &str,
        mut options: StartSpanOptions,
    ) -> Self {
        if options.parent_id.is_none() {
            options.parent_id = get_current_span();
        }
        let id = telemetry.provider.start_span(kind, name, options);
        Self {
            telemetry: telemetry.clone(),
            id,
            ended: false,
        }
    }

    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    /// `work` with this span current.
    pub(crate) fn within<F: Future>(&self, work: F) -> impl Future<Output = F::Output> + use<F> {
        within_optional_span(&self.telemetry, Some(self.id.clone()), work)
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

    /// Ends it `error` for an error of the class `class` that says `text`:
    /// the class alone, unless the kit records its text, which can quote a
    /// message or an address (Python's `span_error`).
    pub(crate) fn fail_error(
        self,
        class: impl Into<String>,
        text: impl Into<String>,
        attributes: Metadata,
    ) {
        let text = text.into();
        let message = if self.telemetry.sensitive && !text.is_empty() {
            text
        } else {
            class.into()
        };
        self.fail(message, attributes);
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
            self.telemetry.provider.end_span(&self.id, options);
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

/// The span of a call that opens only once the call goes in, when it does:
/// a call refused before that, as Python's `post()` refuses what cannot be
/// injected, leaves no span at all. Its work runs under whatever is current
/// until it opens, and under it from then on. It ends with its value, as
/// [`OpenSpan`] does when dropped unended.
#[derive(Debug)]
pub(crate) struct LateSpan(Arc<Late>);

/// What a [`LateSpan`] and the work under it share.
#[derive(Debug)]
pub(crate) struct Late {
    telemetry: KitTelemetry,
    kind: SpanKind,
    name: &'static str,
    /// What it opens with, until it opened or its call ended.
    options: Mutex<Option<StartSpanOptions>>,
    span: Mutex<Option<OpenSpan>>,
    id: OnceLock<String>,
    /// The provider's own context of the span, read as it opens: a provider
    /// forgets a span it ended, and what the work opens later stays in its
    /// trace.
    context: OnceLock<Option<Arc<dyn Any + Send + Sync>>>,
}

/// `mutex`'s value, a panic that poisoned it past.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Late {
    /// The span's id, once it opened.
    pub(crate) fn id(&self) -> Option<&str> {
        self.id.get().map(String::as_str)
    }

    /// Opens the span, the first time and unless its call ended.
    pub(crate) fn open(&self) {
        let Some(options) = lock(&self.options).take() else {
            return;
        };
        let span = OpenSpan::start(&self.telemetry, self.kind, self.name, options);
        let context = self.telemetry.provider.get_span_context(span.id());
        if self.id.set(span.id().to_owned()).is_ok() {
            drop(self.context.set(context));
            *lock(&self.span) = Some(span);
        }
    }

    /// The provider's own context of the span, once it opened.
    #[cfg(feature = "opentelemetry")]
    pub(super) fn context(&self) -> Option<Arc<dyn Any + Send + Sync>> {
        self.context.get().cloned().flatten()
    }

    /// The span, if it opened, for its end; none opens from here on.
    fn ending(&self) -> Option<OpenSpan> {
        lock(&self.options).take();
        lock(&self.span).take()
    }
}

impl LateSpan {
    /// The span `name` of `kind`, not open yet, a child of the current span
    /// when `options` name no other parent.
    pub(crate) fn new(
        telemetry: &KitTelemetry,
        kind: SpanKind,
        name: &'static str,
        mut options: StartSpanOptions,
    ) -> Self {
        if options.parent_id.is_none() {
            options.parent_id = get_current_span();
        }
        Self(Arc::new(Late {
            telemetry: telemetry.clone(),
            kind,
            name,
            options: Mutex::new(Some(options)),
            span: Mutex::new(None),
            id: OnceLock::new(),
            context: OnceLock::new(),
        }))
    }

    /// `work` under this span once it opens.
    pub(crate) fn within<F: Future>(&self, work: F) -> impl Future<Output = F::Output> + use<F> {
        within_late_span(&self.0.telemetry, Arc::clone(&self.0), work)
    }

    /// Ends it `ok`, `attributes` added, if it opened.
    pub(crate) fn end(&self, attributes: Metadata) {
        if let Some(span) = self.0.ending() {
            span.end(attributes);
        }
    }

    /// Ends it as [`OpenSpan::fail_error`] does, if it opened.
    pub(crate) fn fail_error(
        &self,
        class: impl Into<String>,
        text: impl Into<String>,
        attributes: Metadata,
    ) {
        if let Some(span) = self.0.ending() {
            span.fail_error(class, text, attributes);
        }
    }
}

impl Drop for LateSpan {
    fn drop(&mut self) {
        drop(self.0.ending());
    }
}

/// `work` run under the kit's `telemetry`, its current span `span_id`, else
/// whatever is current: every call the driver makes runs so, for a channel
/// to report to its kit's provider.
pub(crate) fn within_kit<F: Future>(
    telemetry: &KitTelemetry,
    span_id: Option<String>,
    work: F,
) -> impl Future<Output = F::Output> + use<F> {
    let span_id = span_id.or_else(get_current_span);
    within_optional_span(telemetry, span_id, work)
}
