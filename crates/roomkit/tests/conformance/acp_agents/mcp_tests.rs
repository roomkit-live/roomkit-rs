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
