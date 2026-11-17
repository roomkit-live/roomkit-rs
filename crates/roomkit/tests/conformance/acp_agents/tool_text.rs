//! ACP tool publication text is an explicit boolean fixture option.

use crate::calls::Unreadable;
use serde_json::Value;

pub(crate) fn publishes(acp: &Value) -> Result<bool, Unreadable> {
    acp.get("publish_tool_call_text")
        .map(|value| value.as_bool().ok_or_else(|| "expected a boolean".into()))
        .transpose()
        .map(|value| value.unwrap_or(false))
}

pub(crate) fn validate(acp: &Value, path: &str, found: &mut Vec<String>) {
    if let Err(error) = publishes(acp) {
        found.push(format!("{path}.publish_tool_call_text: {error}"));
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn test_tool_text_fixture_rejects_non_boolean_controls() {
        for value in [json!(null), json!("false"), json!(1), json!([]), json!({})] {
            let mut found = Vec::new();
            super::validate(&json!({"publish_tool_call_text":value}), "acp", &mut found);
            assert_eq!(found.len(), 1);
        }
        assert!(!super::publishes(&json!({})).unwrap());
        assert!(!super::publishes(&json!({"publish_tool_call_text":false})).unwrap());
        assert!(super::publishes(&json!({"publish_tool_call_text":true})).unwrap());
    }
}
