//! Python: `tests/test_mcp_tool_provider.py::test_get_tools_as_dicts`.

use roomkit_mcp::McpError;
use serde_json::json;

use super::support::fake;

#[tokio::test]
async fn tool_dictionaries_keep_model_fields_and_own_their_nested_values() {
    let provider = fake("normal").tools(&["echo"]).connect().await.unwrap();
    let expected = json!([{
        "name":"echo", "description":"Answers its text.",
        "parameters":{"type":"object","properties":{"text":{"type":"string"}}},
        "tags":[], "defer_loading":false, "changes_state":false,
    }]);
    let mut snapshot = provider.get_tools_as_dicts().unwrap();
    assert_eq!(json!(snapshot), expected);
    snapshot[0]["parameters"]["properties"]["text"]["type"] = json!("integer");
    snapshot[0].insert("tags".into(), json!(["changed"]));
    snapshot[0].shift_remove("name");
    assert_eq!(json!(provider.get_tools_as_dicts().unwrap()), expected);
    assert_eq!(provider.get_tools().unwrap()[0].name, "echo");
    provider.close().await;
}

// Python: tests/test_mcp_tool_provider.py::test_not_connected_guard (dictionary slice).
#[tokio::test]
async fn tool_dictionaries_require_a_connection_and_do_not_start_discovery() {
    let provider = fake("host").discover(false).provider();
    assert!(matches!(
        provider.get_tools_as_dicts(),
        Err(McpError::NotConnected)
    ));
    provider.connect().await.unwrap();
    assert_eq!(provider.get_tools_as_dicts().unwrap(), []);
    provider.close().await;
    assert!(matches!(
        provider.get_tools_as_dicts(),
        Err(McpError::NotConnected)
    ));
}
