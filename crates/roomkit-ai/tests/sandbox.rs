//! The sandbox carrier and host contract, without a concrete execution engine.
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#[path = "sandbox/boundaries.rs"]
mod boundaries;
#[path = "sandbox/contracts.rs"]
mod contracts;
#[path = "sandbox/lifetime.rs"]
mod lifetime;
#[path = "sandbox/search.rs"]
mod search;
#[path = "sandbox/support.rs"]
mod support;
use roomkit_ai::sandbox::{SANDBOX_TOOL_PREFIX, SANDBOX_TOOL_SCHEMAS};
use roomkit_ai::{AiChannelOptions, SandboxResult};
use serde_json::json;

#[test]
fn success_is_computed_after_mutation_and_serialized() {
    assert!(AiChannelOptions::default().sandbox.is_none());
    let mut result = SandboxResult::new(0);
    assert!(result.success());
    assert_eq!(
        serde_json::to_value(&result).unwrap(),
        json!({
            "exit_code": 0, "output": "", "error": "", "metadata": {}, "success": true
        })
    );
    result.exit_code = -2;
    assert!(!result.success());
    assert_eq!(serde_json::to_value(&result).unwrap()["success"], false);
    let parsed: SandboxResult =
        serde_json::from_value(json!({"exit_code": 1,"success": true})).unwrap();
    assert!(!parsed.success());
    assert!(serde_json::from_value::<SandboxResult>(json!({})).is_err());
    assert_eq!(SANDBOX_TOOL_PREFIX, "sandbox_");
    assert_eq!(SANDBOX_TOOL_SCHEMAS.len(), 10);
}
