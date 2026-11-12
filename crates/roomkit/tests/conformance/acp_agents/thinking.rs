//! The thought-publication option and the gate after a prompt's updates.

use serde_json::Value;

use crate::calls::Unreadable;

pub(crate) fn publishes(acp: &Value) -> Result<bool, Unreadable> {
    acp.get("publish_thinking_text")
        .map(|value| value.as_bool().ok_or_else(|| "expected a boolean".into()))
        .transpose()
        .map(|value| value.unwrap_or(false))
}

pub(crate) fn validate(acp: &Value, path: &str, found: &mut Vec<String>) {
    if let Err(error) = publishes(acp) {
        found.push(format!("{path}.publish_thinking_text: {error}"));
    }
    for (index, prompt) in acp["agent"]["prompts"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
    {
        if let Some(gate) = prompt.get("after_updates_gate")
            && !gate.is_string()
        {
            found.push(format!(
                "{path}.agent.prompts[{index}].after_updates_gate: expected a gate name"
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    // Python: tests/conformance/test_acp_fixture.py::test_thinking_fixture_rejects_malformed_controls
    #[test]
    fn test_thinking_fixture_rejects_malformed_controls() {
        for value in [
            json!({"publish_thinking_text":"false"}),
            json!({"publish_thinking_text":null}),
            json!({"agent":{"prompts":[{"after_updates_gate":true}]}}),
            json!({"agent":{"prompts":[{"after_updates_gate":null}]}}),
        ] {
            let mut found = Vec::new();
            super::validate(&value, "acp", &mut found);
            assert!(!found.is_empty(), "{value}");
        }
    }
}
