use serde_json::json;

use super::parse;

// Python: tests/conformance/test_acp_mcp_fixture.py::test_malformed_mcp_sources_are_rejected
#[test]
fn test_malformed_mcp_sources_are_rejected() {
    for value in [
        json!(null),
        json!({}),
        json!({"mode":"later","responses":[]}),
        json!({"mode":"async","responses":[{"servers":[],"error":"no"}]}),
        json!({"mode":"sync","responses":[{"servers":[],"gate":"hold"}]}),
        json!({"mode":"async","responses":[{"servers":[],"gates":"hold"}]}),
        json!([{"name":"tools","command":"tool","args":[],"env":{},"unknown":true}]),
        json!([{"type":"stdio","name":"tools","command":"tool","args":[],"env":[]}]),
        json!([{"type":"http","name":"tools","url":"https://example.test","headers":[1]}]),
    ] {
        assert!(parse(&value).is_err(), "{value}");
    }
}

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
