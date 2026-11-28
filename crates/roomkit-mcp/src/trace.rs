//! The host's trace context on MCP requests (RFC §21.2, RK-21.2-057): a
//! provider given `propagate_trace_context` writes the W3C context of the
//! host's current span into each request's `_meta`, unless the request's
//! caller wrote a trace context of its own, which goes whole in its place.
//! The host's current span is the kit's, named to another process by its
//! telemetry provider, as a turn's trace is (RFC §10.6.9).

use roomkit::model::Metadata;
use roomkit::telemetry::{current_telemetry, get_current_span};
use serde_json::Value;

/// The `_meta` keys a W3C trace context travels under.
const TRACE_KEYS: [&str; 3] = ["traceparent", "tracestate", "baggage"];

/// The host's current span's trace context, as the kit's telemetry
/// provider names it to another process; empty under no span, or with a
/// provider that names none. Read in the caller's task, where the span is
/// current.
pub(crate) fn host_context() -> Metadata {
    let (Some(telemetry), Some(span)) = (current_telemetry(), get_current_span()) else {
        return Metadata::new();
    };
    telemetry
        .inject_context(&span)
        .into_iter()
        .filter(|(key, _)| TRACE_KEYS.contains(&key.as_str()))
        .map(|(key, value)| (key, Value::String(value)))
        .collect()
}

/// `params` with `host`'s trace context in their `_meta`, unless their
/// caller wrote trace keys of its own there, which stay alone: never the
/// caller's `traceparent` beside the host's `tracestate`.
pub(crate) fn stamped(params: Option<Metadata>, host: &Metadata) -> Option<Metadata> {
    if host.is_empty() {
        return params;
    }
    let mut params = params.unwrap_or_default();
    let meta = params
        .entry("_meta")
        .or_insert_with(|| Value::Object(Metadata::new()));
    let Some(meta) = meta.as_object_mut() else {
        return Some(params);
    };
    if !TRACE_KEYS.iter().any(|key| meta.contains_key(*key)) {
        meta.extend(host.clone());
    }
    Some(params)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn metadata(value: &Value) -> Metadata {
        value.as_object().cloned().unwrap()
    }

    // Python: tests/test_mcp_wire_reading.py::test_a_provider_that_propagates_the_trace_context_sends_the_host_s
    #[test]
    fn a_caller_s_own_trace_context_goes_whole() {
        let host = metadata(&json!({"traceparent":"00-host-01","baggage":"tenant=t1"}));
        let own = metadata(&json!({"_meta":{"traceparent":"00-caller-01"},"uri":"x"}));
        assert_eq!(stamped(Some(own.clone()), &host), Some(own));
        assert_eq!(
            stamped(None, &host),
            Some(metadata(
                &json!({"_meta":{"traceparent":"00-host-01","baggage":"tenant=t1"}})
            ))
        );
        assert_eq!(stamped(None, &Metadata::new()), None);
    }
}
