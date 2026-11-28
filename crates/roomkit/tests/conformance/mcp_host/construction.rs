//! Constructor selection and mutation of the original public header handle.

use std::sync::Arc;
use std::time::Duration;

use roomkit_mcp::{FromUrlOptions, McpError, McpHeaders, McpToolProvider, McpToolProviderOptions};
use serde_json::Value;

/// The provider public construction builds on `url`, or its refusal.
pub(super) fn construct(url: &str, spec: &Value) -> Result<McpToolProvider, McpError> {
    let headers = spec
        .get("headers")
        .map(|value| McpHeaders::new(entries(value)));
    let selected = spec.get("tool_filter").cloned();
    let options = McpToolProviderOptions {
        transport: spec["transport"]
            .as_str()
            .unwrap_or("streamable_http")
            .to_owned(),
        headers: headers.clone(),
        discover: spec["discover"].as_bool().unwrap_or(true),
        label: spec["label"].as_str().map(str::to_owned),
        connect_timeout: spec["connect_timeout_ms"]
            .as_u64()
            .map(Duration::from_millis),
        tool_filter: selected.map(|names| {
            let filter: Arc<dyn Fn(&str) -> bool + Send + Sync> = Arc::new(move |name: &str| {
                names.as_array().unwrap().iter().any(|kept| kept == name)
            });
            filter
        }),
        ..Default::default()
    };
    let provider = if spec["constructor"] == "direct" {
        McpToolProvider::new(Some(url), options)
    } else {
        McpToolProvider::from_url(
            url,
            FromUrlOptions {
                transport: options.transport,
                headers: options.headers,
                discover: options.discover,
                label: options.label,
                connect_timeout: options
                    .connect_timeout
                    .unwrap_or_else(|| FromUrlOptions::default().connect_timeout),
                tool_filter: options.tool_filter,
                protocol: options.protocol,
                propagate_trace_context: options.propagate_trace_context,
            },
        )
    }?;
    if let Some(replacement) = spec.get("headers_after_construction") {
        headers.as_ref().unwrap().replace(entries(replacement));
    }
    Ok(provider)
}

fn entries(value: &Value) -> Vec<(String, String)> {
    value
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_owned()))
        .collect()
}
