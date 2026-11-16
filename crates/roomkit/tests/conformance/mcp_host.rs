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
    let results = observations(&provider, &peer, spec, discover).await;
    provider.close().await;
    let requests = peer.finish().await?;
    let results = results?;
    Ok(json!({
        "results_json": canonical(&Value::Array(results.clone())),
        "requests_json": canonical(&requests),
        "results": results, "requests": requests,
    }))
}

async fn observations(
    provider: &McpToolProvider,
    peer: &peer::Peer,
    spec: &Value,
    discover: bool,
) -> Result<Vec<Value>, Unreadable> {
    let mut results = Vec::new();
    for operation in spec["operations"].as_array().unwrap() {
        peer.discovery(operation["method"] == "connect" && discover);
        results.push(observed(provider, operation).await?);
    }
    Ok(results)
}

async fn observed(provider: &McpToolProvider, op: &Value) -> Result<Value, Unreadable> {
    Ok(match operation(provider, op).await {
        Ok(mut value) => {
            if let Some(mutation) = op.get("mutate") {
                mutate(&mut value, mutation)?;
            }
            json!({"result": value})
        }
        Err(McpError::NotConnected) => json!({"error":"not_connected"}),
        Err(McpError::CallTimeout { .. }) => json!({"error":"timeout"}),
        Err(McpError::Validation { .. }) => json!({"error":"validation_error"}),
        Err(McpError::Call { message }) => json!({"error":"mcp_error","message":message}),
        Err(error) => return Err(format!("unexpected MCP host error: {error}")),
    })
}

fn mutate(value: &mut Value, mutation: &Value) -> Result<(), Unreadable> {
    let keys = mutation["path"].as_array().unwrap();
    let mut target = value;
    for key in &keys[..keys.len() - 1] {
        target = target
            .get_mut(key.as_str().unwrap())
            .ok_or("snapshot mutation parent is absent")?;
    }
    target
        .as_object_mut()
        .ok_or("snapshot mutation parent is not an object")?
        .insert(
            keys.last().unwrap().as_str().unwrap().into(),
            mutation["value"].clone(),
        );
    Ok(())
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
        "get_tools_as_dicts" => json!(provider.get_tools_as_dicts()?),
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
            value @ (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)) => {
                value.clone()
            }
        }
    }
    sorted(value).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_unexpected_connection_error_is_a_fixture_failure() {
        let spec = json!({
            "discover":false,
            "peer":{"server_info":{"name":42,"version":"1"},"replies":[]},
            "operations":[{"method":"connect"}]
        });
        assert!(session(&spec).await.is_err());
    }

    #[tokio::test]
    async fn a_snapshot_mutation_cannot_create_a_missing_parent() {
        for path in [json!(["absent", "child"]), json!(["name", "child"])] {
            let spec = json!({
                "discover":false,"peer":{"replies":[]},
                "operations":[{"method":"connect"},{"method":"server_info",
                    "mutate":{"path":path,"value":true}}]
            });
            assert!(session(&spec).await.is_err());
        }
    }
}
