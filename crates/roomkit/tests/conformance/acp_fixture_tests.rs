//! Store-independent runner checks run once on the in-memory kit.

use serde_json::json;

// Python: tests/conformance/test_acp_mcp_fixture.py::test_expected_callback_error_does_not_hide_exhaustion
#[tokio::test]
async fn test_expected_callback_error_does_not_hide_exhaustion() {
    for mode in ["sync", "async"] {
        let scenario = json!({
            "setup":{
                "channels":[
                    {"id":"ws","type":"websocket","category":"transport"},
                    {"id":"agent","type":"ai","acp":{"cwd":"/srv/project","agent":{},
                        "mcp_servers":{"mode":mode,"responses":[]}}}
                ],
                "rooms":[{"id":"room","bindings":[{"channel_id":"ws"},{"channel_id":"agent"}]}]
            },
            "steps":[{"call":"process_inbound","room_id":"room","message":{
                "channel_id":"ws","sender_id":"alice","content":{"type":"text","body":"Hello"}},
                "expect":{"result":{"error":{"$contains":"callback script exhausted"}}}}],
            "expect":{"acp_mcp_requests":{"agent":["room"]}}
        });
        let problems = crate::harness::run(&scenario).await;
        assert!(
            problems
                .iter()
                .any(|problem| problem.contains("acp.mcp_servers: callback script exhausted")),
            "{problems:?}"
        );
    }
}

// Python: tests/conformance/test_acp_fixture.py::test_permission_fixture_rejects_malformed_steps
#[test]
fn test_permission_fixture_rejects_malformed_steps() {
    for steps in [
        json!(null),
        json!({}),
        json!([null]),
        json!([{}]),
        json!([{"request":{}}]),
        json!([{"request":{"toolCall":{},"options":[]}}]),
        json!([{"request":{"toolCall":{"toolCallId":"t"},"options":[{}]}}]),
        json!([{"request":{"toolCall":{"toolCallId":"t"},"options":[]},"updates":null}]),
    ] {
        let scenario = json!({"setup":{"channels":[{"id":"agent","type":"ai",
            "acp":{"cwd":"/srv/project","agent":{"prompts":[{"permission_steps":steps}]}}}]}});
        assert!(
            crate::shape::unknown_members(&scenario)
                .iter()
                .any(|error| error.contains("permission_steps"))
        );
    }
}
