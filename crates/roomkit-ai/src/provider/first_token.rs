//! A generation's time to first token, the metric `roomkit.llm.ttfb_ms`:
//! Python's `AIProvider._record_ttfb` and `FirstToken`, which a provider's
//! read of its answer holds.

use roomkit::model::Metadata;
use roomkit::telemetry::{Attr, RecordMetricOptions, current_telemetry};
use serde_json::Value;
use tokio::time::Instant;

/// A generation's time to first token, recorded once to the kit whose call
/// this is: at its first text or reasoning, never at a call fragment.
/// Started when it is made, just before the request. Hidden: Python's is no
/// public name either.
#[doc(hidden)]
#[derive(Debug)]
pub struct FirstToken {
    provider: String,
    model: String,
    started: Option<Instant>,
}

impl FirstToken {
    /// Starts the measure for the generation of `model` by `provider`.
    #[must_use]
    pub fn start(provider: &str, model: &str) -> Self {
        Self {
            provider: provider.to_owned(),
            model: model.to_owned(),
            started: Some(Instant::now()),
        }
    }

    /// Output reached the generation's reader; the first call records it.
    pub fn seen(&mut self) {
        let Some(started) = self.started.take() else {
            return;
        };
        let Some(telemetry) = current_telemetry() else {
            return;
        };
        let elapsed = started.elapsed().as_secs_f64() * 1000.0;
        let mut attributes = Metadata::new();
        attributes.insert(
            Attr::PROVIDER.to_owned(),
            Value::from(self.provider.as_str()),
        );
        attributes.insert(Attr::MODEL.to_owned(), Value::from(self.model.as_str()));
        let options = RecordMetricOptions {
            unit: "ms".to_owned(),
            attributes,
        };
        telemetry.record_metric("roomkit.llm.ttfb_ms", elapsed, options);
    }
}
