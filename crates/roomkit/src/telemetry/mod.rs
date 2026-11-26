//! Telemetry: the spans and metrics of the kit's work, Python's
//! `roomkit.telemetry` (RMK-1213).
//!
//! The kit reports to the provider of
//! [`RoomKitOptions::telemetry`](crate::RoomKitOptions::telemetry), a
//! [`NoopTelemetryProvider`] by default. [`MockTelemetryProvider`] records
//! what it is told for a test and [`ConsoleTelemetryProvider`] logs it.
//! The `roomkit-providers-opentelemetry` crate of the roomkit-providers
//! repository hands it to an OpenTelemetry SDK tracer provider, whose
//! exporter the application chooses: a provider written outside this crate
//! starts its spans under [`get_current_telemetry_ctx`].

mod config;
mod console;
mod context;
mod kinds;
mod mock;
mod noop;
mod open;
mod provider;
#[cfg(test)]
mod tests;

pub use config::TelemetryConfig;
pub use console::{ConsoleTelemetryProvider, ConsoleTelemetryProviderOptions};
pub(crate) use context::within_current_span;
#[cfg(test)]
pub(crate) use context::within_no_span;
pub(crate) use context::{
    current_call_goes_in, current_kit_telemetry, current_late_span, current_trace,
    within_turn_trace,
};
#[doc(hidden)]
pub use context::{current_records_sensitive_data, current_telemetry};
pub use context::{get_current_span, get_current_telemetry_ctx};
pub use kinds::{Attr, SpanKind};
pub use mock::MockTelemetryProvider;
pub use noop::NoopTelemetryProvider;
pub(crate) use open::{
    KitTelemetry, Late, LateSpan, OpenSpan, end_hook_span, end_observer_span, within_kit,
};
pub use provider::{
    EndSpanOptions, RecordMetricOptions, Span, SpanGuard, StartSpanOptions, TelemetryProvider,
};
