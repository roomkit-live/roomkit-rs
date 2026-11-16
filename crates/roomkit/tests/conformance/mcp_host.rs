//! Public MCP host operations against a bounded, scripted loopback peer.

#[path = "mcp_host/peer.rs"]
mod peer;
#[path = "mcp_host/shape.rs"]
mod shape;

use std::sync::Arc;
use std::time::Duration;

use roomkit_mcp::{FromUrlOptions, McpError, McpToolProvider, ReadResourceOptions, RequestOptions};
use serde_json::{Value, json};

use crate::calls::Unreadable;
pub(crate) use shape::validate;

pub(crate) async fn session(spec: &Value) -> Result<Value, Unreadable> {
    let mut problems = Vec::new();
    validate(spec, "mcp_session", &mut problems);
    if !problems.is_empty() {
        return Err(problems.join("; "));
    }
    let peer = peer::Peer::start(spec["peer"].clone()).await?;
    let selected = spec.get("tool_filter").cloned();
    let discover = spec["discover"].as_bool().unwrap_or(true);
    let mut options = FromUrlOptions {
        discover,
        ..Default::default()
    };
    if let Some(names) = selected {
        options.tool_filter = Some(Arc::new(move |name: &str| {
            names
                .as_array()
                .is_some_and(|names| names.iter().any(|n| n == name))
        }));
    }
    let provider =
        McpToolProvider::from_url(&peer.url, options).map_err(|error| error.to_string())?;
    let mut results = Vec::new();
    for operation in spec["operations"].as_array().unwrap() {
        peer.discovery(operation["method"] == "connect" && discover);
        results.push(observed(&provider, operation).await);
    }
    provider.close().await;
    let requests = peer.finish().await?;
    Ok(json!({
        "results_json": canonical(&Value::Array(results.clone())),
        "requests_json": canonical(&requests),
        "results": results, "requests": requests,
    }))
}

async fn observed(provider: &McpToolProvider, op: &Value) -> Value {
    match operation(provider, op).await {
        Ok(mut value) => {
            if let Some(mutation) = op.get("mutate") {
                let keys = mutation["path"].as_array().unwrap();
                let mut target = &mut value;
                for key in &keys[..keys.len() - 1] {
                    target = &mut target[key.as_str().unwrap()];
                }
                target[keys.last().unwrap().as_str().unwrap()] = mutation["value"].clone();
            }
            json!({"result": value})
        }
        Err(McpError::NotConnected) => json!({"error":"not_connected"}),
        Err(McpError::CallTimeout { .. }) => json!({"error":"timeout"}),
        Err(McpError::Validation { .. }) => json!({"error":"validation_error"}),
        Err(McpError::Call { message }) => json!({"error":"mcp_error","message":message}),
        Err(error) => json!({"error":"unexpected","message":error.to_string()}),
    }
}

async fn operation(provider: &McpToolProvider, op: &Value) -> Result<Value, McpError> {
    let timeout = op["timeout_ms"].as_u64().map(Duration::from_millis);
    Ok(match op["method"].as_str().unwrap() {
        "connect" => {
            provider.connect().await?;
            Value::Null
        }
        "close" => {
            provider.close().await;
            Value::Null
        }
        "connected" => json!(provider.connected()),
        "tool_names" => json!(provider.tool_names()?),
        "tool_meta" => json!(provider.tool_meta()?),
        "server_info" => json!(provider.server_info()?),
        "server_extensions" => json!(provider.server_extensions()?),
        "request" => json!(
            provider
                .request(
                    op["name"].as_str().unwrap(),
                    op["params"].as_object(),
                    RequestOptions {
                        timeout: timeout.unwrap_or_else(|| RequestOptions::default().timeout)
                    },
                )
                .await?
        ),
        "read_resource" => json!(
            provider
                .read_resource(
                    op["uri"].as_str().unwrap(),
                    ReadResourceOptions {
                        timeout: timeout.unwrap_or_else(|| ReadResourceOptions::default().timeout)
                    },
                )
                .await?
        ),
        _ => unreachable!("shape validated before operations"),
    })
}

fn canonical(value: &Value) -> String {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(map) => {
                let ordered: std::collections::BTreeMap<_, _> =
                    map.iter().map(|(k, v)| (k.clone(), sorted(v))).collect();
                Value::Object(ordered.into_iter().collect())
            }
            Value::Array(items) => Value::Array(items.iter().map(sorted).collect()),
            value => value.clone(),
        }
    }
    sorted(value).to_string()
}
