//! Public MCP host operations against a bounded, scripted loopback peer.

#[path = "mcp_host/construction.rs"]
mod construction;
#[path = "mcp_host/peer.rs"]
mod peer;
#[path = "mcp_host/shape.rs"]
mod shape;

use std::time::Duration;

use roomkit_mcp::{
    CallToolOptions, McpError, McpToolProvider, ReadResourceOptions, RequestOptions,
};
use serde_json::{Value, json};

use crate::calls::{Called, Unreadable};
use crate::errors::named;
pub(crate) use shape::{arguments, validate};

pub(crate) async fn session(spec: &Value) -> Result<Called, Unreadable> {
    let mut problems = Vec::new();
    validate(spec, "mcp_session", &mut problems);
    if !problems.is_empty() {
        return Err(problems.join("; "));
    }
    let peer = peer::Peer::start(spec["peer"].clone(), spec["transport"] == "sse").await?;
    let discover = spec["discover"].as_bool().unwrap_or(true);
    // A URL of the scenario's own, in place of the peer's (README, "MCP
    // host sessions"): the peer still runs, and is never reached from it.
    let url = spec["url"].as_str().unwrap_or(&peer.url).to_owned();
    let provider = match construction::construct(&url, spec) {
        Ok(provider) => provider,
        // Public construction refused its arguments: the call's
        // invalid_argument, and no operation runs.
        Err(McpError::Configuration { .. }) => {
            peer.finish().await?;
            return Ok(Err(named("invalid_argument")));
        }
        Err(error) => return Err(format!("unexpected MCP construction error: {error}")),
    };
    let results = observations(&provider, &peer, spec, discover).await;
    provider.close().await;
    peer.settle_closed().await?;
    let http = peer.http(spec);
    let posted = peer.posted();
    let requests = peer.finish().await?;
    let results = results?;
    let mut observed = json!({
        "results_json": canonical(&Value::Array(results.clone())),
        "requests_json": canonical(&requests),
        "results": results, "requests": requests,
    });
    if let Some(http) = http {
        observed["http_json"] = json!(canonical(&http));
        observed["http"] = http;
    }
    if let Some(posted) = posted {
        observed["messages"] = posted;
    }
    Ok(Ok(observed))
}

async fn observations(
    provider: &McpToolProvider,
    peer: &peer::Peer,
    spec: &Value,
    discover: bool,
) -> Result<Vec<Value>, Unreadable> {
    let mut results = Vec::new();
    for operation in spec["operations"].as_array().unwrap() {
        peer.operation(
            operation["method"] == "connect" && discover,
            operation["method"] == "call_tool",
        );
        results.push(observed(provider, operation).await?);
        if !provider.connected() {
            peer.settle_closed().await?;
        }
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
        Err(McpError::AlreadyConnected) => json!({"error":"already_connected"}),
        Err(
            McpError::Http {
                reason: "connection timed out",
                ..
            }
            | McpError::CallTimeout { .. },
        ) => json!({"error":"timeout"}),
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
        "instructions" => json!(provider.instructions()?),
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
        "call_tool" => json!(
            provider
                .call_tool(
                    op["name"].as_str().unwrap(),
                    op["arguments"].as_object().cloned().unwrap_or_default(),
                    CallToolOptions {
                        timeout: timeout.unwrap_or_else(|| CallToolOptions::default().timeout)
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

    // Python: tests/test_mcp_discovery.py::test_a_tool_the_sdk_cannot_read_leaves_the_others_headers
    /// A tool annotated with `x-mcp-header` whose input schema the SDK does
    /// not read (no root `type`) is listed, and the tools beside it on the
    /// page still mirror their arguments into headers, which the 2026-07-28
    /// peer checks.
    #[tokio::test]
    async fn a_tool_the_sdk_cannot_read_leaves_the_others_headers() {
        let envelope = json!({"resultType":"complete","ttlMs":0,"cacheScope":"private"});
        let mut listing = json!({"tools":[
            {"name":"routed","inputSchema":{"type":"object",
                "properties":{"region":{"type":"string","x-mcp-header":"Region"}}}},
            {"name":"odd","inputSchema":{"properties":{"zone":{"type":"string","x-mcp-header":"Zone"}}}},
        ]});
        let mut called = json!({"content":[{"type":"text","text":"eu"}],"isError":false});
        for result in [&mut listing, &mut called] {
            result
                .as_object_mut()
                .unwrap()
                .extend(envelope.as_object().unwrap().clone());
        }
        let spec = json!({
            "peer":{"era":"2026-07-28","replies":[
                {"method":"tools/list","result":listing},
                {"method":"tools/call","params":{"name":"routed","arguments":{"region":"eu"}},
                    "result":called},
            ]},
            "operations":[{"method":"connect"},{"method":"tool_names"},
                {"method":"call_tool","name":"routed","arguments":{"region":"eu"}},{"method":"close"}],
        });
        let observed = session(&spec).await.unwrap().unwrap();
        assert_eq!(
            observed["results"],
            json!([{"result":null},{"result":["routed","odd"]},{"result":"eu"},{"result":null}])
        );
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
