//! Telemetry: the spans and metrics of the kit's work, Python's
//! `roomkit.telemetry` (RMK-1213).
//!
//! The kit reports to the provider of
//! [`RoomKitOptions::telemetry`](crate::RoomKitOptions::telemetry), a
//! [`NoopTelemetryProvider`] by default. [`MockTelemetryProvider`] records
//! what it is told for a test, [`ConsoleTelemetryProvider`] logs it, and,
//! with the cargo feature `opentelemetry` (off by default, as Python's
//! extra), `OpenTelemetryProvider` hands it to an OpenTelemetry SDK
//! tracer provider, whose exporter the application chooses.

mod config;
mod console;
mod context;
mod kinds;
mod mock;
mod noop;
mod open;
#[cfg(feature = "opentelemetry")]
mod otel;
#[cfg(test)]
#[cfg(feature = "opentelemetry")]
mod otel_tests;
mod provider;
#[cfg(test)]
mod tests;

pub use config::TelemetryConfig;
pub use console::{ConsoleTelemetryProvider, ConsoleTelemetryProviderOptions};
pub use context::get_current_span;
pub(crate) use context::within_current_span;
#[cfg(test)]
pub(crate) use context::within_no_span;
pub(crate) use context::{
    current_call_goes_in, current_kit_telemetry, current_late_span, current_trace,
    within_turn_trace,
};
#[doc(hidden)]
pub use context::{current_records_sensitive_data, current_telemetry};
pub use kinds::{Attr, SpanKind};
pub use mock::MockTelemetryProvider;
pub use noop::NoopTelemetryProvider;
pub(crate) use open::{
    KitTelemetry, Late, LateSpan, OpenSpan, end_hook_span, end_observer_span, within_kit,
};
#[cfg(feature = "opentelemetry")]
pub use otel::{OpenTelemetryProvider, OpenTelemetryProviderOptions};
pub use provider::{
    EndSpanOptions, RecordMetricOptions, Span, SpanGuard, StartSpanOptions, TelemetryProvider,
};
