//! The public skill-server protocol delegates to a real MCP connection.

use pretty_assertions::assert_eq;
use roomkit::model::Metadata;
use roomkit_ai::{McpSkillServer, McpSkillServerError};
use roomkit_mcp::McpError;
use serde_json::json;

use super::support::fake;

// Python: tests/test_mcp_skill_wire.py::TestAgainstARealServer::test_a_server_that_declares_nothing_has_no_extensions (accessor only)
#[tokio::test]
async fn a_server_without_extensions_returns_an_empty_snapshot() {
    let provider = fake("names").discover(false).connect().await.unwrap();
    let server: &dyn McpSkillServer = &provider;
    assert!(server.server_extensions().unwrap().is_empty());
    provider.close().await;
}

#[tokio::test]
async fn the_protocol_preserves_snapshots_parameters_and_complete_resource_results() {
    let provider = fake("host").discover(false).connect().await.unwrap();
    let server: &dyn McpSkillServer = &provider;
    let mut extensions = server.server_extensions().unwrap();
    extensions
        .get_mut("vendor/good")
        .unwrap()
        .insert("enabled".into(), json!(false));
    assert_eq!(
        server.server_extensions().unwrap()["vendor/good"]["enabled"],
        true
    );
    assert_eq!(
        server.request("host/state", None).await.unwrap()["listings"],
        0
    );
    assert_eq!(
        server.request("host/echo", None).await.unwrap()["params"],
        json!({})
    );
    let params: Metadata = json!({"_meta":{"keep":7},"nested":{"value":null}})
        .as_object()
        .unwrap()
        .clone();
    assert_eq!(
        server.request("host/echo", Some(&params)).await.unwrap()["params"],
        json!(params)
    );
    assert_eq!(
        server.read_resource("resource://body").await.unwrap(),
        json!({"contents":[{"uri":"resource://body","text":"Résumé","vendor":7,"optional":null}],
            "vendor":8,"optional":null,"ttlMs":60000})
    );
    provider.close().await;
}

fn not_connected(error: McpSkillServerError) {
    assert!(matches!(
        error.downcast::<McpError>(),
        Ok(error) if matches!(*error, McpError::NotConnected)
    ));
}

// Python: tests/test_mcp_skill_wire.py::TestAgainstARealServer::test_the_extensions_are_read_only_while_connected
#[tokio::test]
async fn disconnected_errors_keep_the_original_mcp_error_type() {
    let provider = fake("host").discover(false).provider();
    let server: &dyn McpSkillServer = &provider;
    not_connected(server.server_extensions().unwrap_err());
    not_connected(server.request("host/state", None).await.unwrap_err());
    not_connected(server.read_resource("resource://body").await.unwrap_err());
    provider.connect().await.unwrap();
    provider.close().await;
    not_connected(server.server_extensions().unwrap_err());
    not_connected(server.request("host/state", None).await.unwrap_err());
    not_connected(server.read_resource("resource://body").await.unwrap_err());
}

#[tokio::test]
async fn validation_and_server_refusals_keep_their_original_mcp_error_type() {
    let provider = fake("host").discover(false).connect().await.unwrap();
    let server: &dyn McpSkillServer = &provider;
    let validation = server.read_resource("").await.unwrap_err();
    assert!(matches!(
        validation.downcast_ref::<McpError>(),
        Some(McpError::Validation { .. })
    ));
    let refused = server.request("skills/get", None).await.unwrap_err();
    assert!(
        matches!(refused.downcast_ref::<McpError>(), Some(McpError::Call {message}) if message == "unknown resource")
    );
    let refused = server.read_resource("resource://absent").await.unwrap_err();
    assert!(
        matches!(refused.downcast_ref::<McpError>(), Some(McpError::Call {message}) if message == "unknown resource")
    );
    provider.close().await;
}
