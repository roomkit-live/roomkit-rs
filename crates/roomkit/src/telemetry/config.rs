//! Python's `TelemetryConfig`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use super::{SpanKind, TelemetryProvider};

/// Python `TelemetryConfig`: the kit's telemetry
/// ([`RoomKitOptions::telemetry`](crate::RoomKitOptions::telemetry)). A
/// provider alone converts into one (`Some(provider.into())`), as Python
/// takes a provider where it takes a config.
#[derive(Clone)]
pub struct TelemetryConfig {
    /// The provider, a [`NoopTelemetryProvider`](super::NoopTelemetryProvider)
    /// when `None`.
    pub provider: Option<Arc<dyn TelemetryProvider>>,
    /// The fraction of spans to record, 1.0 by default. Python keeps it and
    /// reads it nowhere; so does the kit.
    pub sample_rate: f64,
    /// The span kinds to record, `None` for all. Python keeps it and reads
    /// it nowhere; so does the kit.
    pub enabled_spans: Option<BTreeSet<SpanKind>>,
    /// Tags every span of a provider that keeps them carries
    /// (`roomkit.<key>` for OpenTelemetry).
    pub metadata: BTreeMap<String, String>,
    /// Whether spans carry what can hold personal data: a sender's address
    /// (`sender_id`), and the text of an error, which then reads its class's
    /// name alone. Off by default: an exporter often stands outside the
    /// application's production data (HITL a5171c2d).
    pub record_sensitive_data: bool,
    /// The hook triggers whose hooks open no span: the audio levels by
    /// default, which fire many times a second.
    pub suppressed_hook_triggers: BTreeSet<String>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            provider: None,
            sample_rate: 1.0,
            enabled_spans: None,
            metadata: BTreeMap::new(),
            record_sensitive_data: false,
            suppressed_hook_triggers: [
                "on_input_audio_level",
                "on_output_audio_level",
                "on_vad_audio_level",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        }
    }
}

impl fmt::Debug for TelemetryConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelemetryConfig")
            .field(
                "provider",
                &self
                    .provider
                    .as_ref()
                    .map(|provider| provider.name().to_owned()),
            )
            .field("sample_rate", &self.sample_rate)
            .field("enabled_spans", &self.enabled_spans)
            .field("metadata", &self.metadata)
            .field("record_sensitive_data", &self.record_sensitive_data)
            .field("suppressed_hook_triggers", &self.suppressed_hook_triggers)
            .finish()
    }
}

impl From<Arc<dyn TelemetryProvider>> for TelemetryConfig {
    fn from(provider: Arc<dyn TelemetryProvider>) -> Self {
        Self {
            provider: Some(provider),
            ..Self::default()
        }
    }
}

impl<P: TelemetryProvider> From<Arc<P>> for TelemetryConfig {
    fn from(provider: Arc<P>) -> Self {
        let provider: Arc<dyn TelemetryProvider> = provider;
        provider.into()
    }
}
