//! Public stdio witnesses complement the shared HTTP/SSE host scenarios.
use super::support::fake;
use pretty_assertions::assert_eq;
use roomkit::model::Metadata;
use roomkit_ai::ToolError;
use roomkit_mcp::{
    AsToolHandlerOptions, CallToolOptions, McpError, ReadResourceOptions, RequestOptions,
};
use serde_json::{Value, json};
use std::time::Duration;

// Python: tests/test_mcp_listing.py::test_from_command_connects_without_listing
// Python: tests/test_mcp_listing.py::test_without_discovery_the_gated_handler_serves_no_name
#[tokio::test]
async fn test_from_command_connects_without_listing() {
    let provider = fake("host").discover(false).connect().await.unwrap();
    assert_eq!(provider.tool_names().unwrap(), Vec::<String>::new());
    assert!(provider.tool_meta().unwrap().is_empty());
    let state = provider
        .request("host/state", None, RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(state["listings"], 0);
    let handler = provider
        .as_tool_handler(AsToolHandlerOptions::default())
        .unwrap();
    assert!(matches!(
        handler.call("board".into(), Metadata::new()).await,
        Err(ToolError::UnservedToolCall(_))
    ));
    assert_eq!(
        provider
            .call_tool(
                "echo",
                json!({"text":"hi"}).as_object().unwrap().clone(),
                CallToolOptions::default()
            )
            .await
            .unwrap(),
        "hi"
    );
    provider.close().await;
}

// Python: tests/test_mcp_host_access.py::test_the_tool_meta_comes_from_the_listing_made_at_connection
// Python: tests/test_mcp_server_instructions.py::test_the_provider_keeps_what_the_server_published
#[tokio::test]
async fn test_the_provider_keeps_what_the_server_published() {
    let provider = fake("host").connect().await.unwrap();
    assert_eq!(
        provider.instructions().unwrap().as_deref(),
        Some("Use the host.")
    );
    assert_eq!(
        provider.server_info().unwrap(),
        json!({"name":"host","version":"1","vendor":{"keep":7}})
            .as_object()
            .unwrap()
            .clone()
    );
    assert_eq!(
        provider
            .server_extensions()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["vendor/good"]
    );
    assert_eq!(
        provider.tool_meta().unwrap()["board"]["ui"]["resourceUri"],
        "ui://board/view.html"
    );
    assert_eq!(
        provider
            .request("host/state", None, RequestOptions::default())
            .await
            .unwrap()["listings"],
        1
    );
    provider.close().await;
    assert!(matches!(
        provider.server_info(),
        Err(McpError::NotConnected)
    ));
    assert!(matches!(
        provider.server_extensions(),
        Err(McpError::NotConnected)
    ));
    assert!(matches!(provider.tool_meta(), Err(McpError::NotConnected)));
}

// Python: tests/test_mcp_host_access.py::test_a_resource_is_read_as_the_server_returns_it
// Python: tests/test_mcp_skill_wire.py::TestAgainstARealServer::test_an_unknown_skill_is_the_servers_error
#[tokio::test]
async fn test_a_resource_is_read_as_the_server_returns_it() {
    let provider = fake("host").discover(false).connect().await.unwrap();
    let raw = provider
        .request("vendor/read", None, RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(
        raw,
        json!({"contents":[],"vendor":{"keep":7},"inner":{"optional":null}})
            .as_object()
            .unwrap()
            .clone()
    );
    for _ in 0..2 {
        let read = provider
            .read_resource("resource://body", ReadResourceOptions::default())
            .await
            .unwrap();
        assert_eq!(read, json!({"contents":[{"uri":"resource://body","text":"Résumé","vendor":7,"optional":null}],
            "vendor":8,"optional":null,"ttlMs":60000}).as_object().unwrap().clone());
    }
    assert_eq!(
        provider
            .request("host/state", None, RequestOptions::default())
            .await
            .unwrap()["reads"],
        2
    );
    let error = provider
        .read_resource("resource://missing", ReadResourceOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(error, McpError::Call { message } if message == "unknown resource"));
    provider.close().await;
}

#[tokio::test]
async fn a_timed_out_request_does_not_capture_a_late_response() {
    let provider = fake("host").discover(false).connect().await.unwrap();
    let error = provider
        .request(
            "host/hold",
            None,
            RequestOptions {
                timeout: Duration::from_millis(40),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(error, McpError::CallTimeout { .. }));
    assert_eq!(
        provider
            .request("host/state", None, RequestOptions::default())
            .await
            .unwrap()["held"],
        1
    );
    assert_eq!(
        provider
            .request("host/release", None, RequestOptions::default())
            .await
            .unwrap(),
        json!({"released":true}).as_object().unwrap().clone()
    );
    provider.close().await;
}

#[tokio::test]
async fn a_server_ping_with_the_same_id_cannot_consume_the_host_response() {
    let provider = fake("host").discover(false).connect().await.unwrap();
    let response = provider
        .request("host/ping", None, RequestOptions::default())
        .await
        .unwrap();
    assert_eq!(response.get("after_ping"), Some(&Value::Bool(true)));
    assert_eq!(
        provider.server_info().unwrap().get("name"),
        Some(&json!("host"))
    );
    provider.close().await;
}

#[tokio::test]
async fn host_metadata_reaches_stdio_without_sdk_overwrites() {
    let provider = fake("host").discover(false).connect().await.unwrap();
    for meta in [
        Value::Null,
        json!("opaque"),
        json!({}),
        json!({"progressToken":"host","io.modelcontextprotocol/protocolVersion":"caller","custom":null}),
    ] {
        let params = json!({"_meta":meta}).as_object().unwrap().clone();
        let result = provider
            .request("host/echo", Some(&params), RequestOptions::default())
            .await
            .unwrap();
        assert_eq!(result.get("params"), Some(&Value::Object(params)));
    }
    provider.close().await;
}

#[tokio::test]
async fn a_protocol_python_does_not_support_is_refused_and_closed() {
    let provider = fake("host-modern").discover(false).provider();
    let error = provider.connect().await.unwrap_err();
    assert!(
        matches!(error, McpError::Handshake { message, .. } if message == "unsupported MCP protocol version")
    );
    assert!(!provider.connected());
}

#[tokio::test]
async fn closing_settles_a_pending_host_request() {
    let provider = fake("host").discover(false).connect().await.unwrap();
    let other = provider.clone();
    let pending = tokio::spawn(async move {
        other
            .request("host/hold", None, RequestOptions::default())
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state = provider
                .request("host/state", None, RequestOptions::default())
                .await
                .unwrap();
            if state["held"] == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    provider.close().await;
    assert!(
        tokio::time::timeout(Duration::from_secs(2), pending)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(!provider.connected());
}

#[tokio::test]
async fn both_host_calls_default_to_thirty_seconds() {
    for resource in [false, true] {
        let provider = fake("host").discover(false).connect().await.unwrap();
        let other = provider.clone();
        let pending = tokio::spawn(async move {
            if resource {
                other
                    .read_resource("resource://hold", ReadResourceOptions::default())
                    .await
            } else {
                other
                    .request("host/hold", None, RequestOptions::default())
                    .await
            }
        });
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state = provider
                    .request("host/state", None, RequestOptions::default())
                    .await
                    .unwrap();
                if state["held"] == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(29)).await;
        assert!(!pending.is_finished());
        tokio::time::advance(Duration::from_secs(1)).await;
        let error = pending.await.unwrap().unwrap_err();
        tokio::time::resume();
        assert!(
            matches!(error, McpError::CallTimeout { after, .. } if after == Duration::from_secs(30))
        );
        provider.close().await;
    }
}
