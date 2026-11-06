//! How a turn ended, as its record names it (RFC §6.4): Python's
//! `recorded_turn_end`.

use serde_json::Value;

use crate::model::Metadata;

/// How the turn whose record is `record` ended, as its record names it
/// (RFC §6.4), Python's `recorded_turn_end`: an AI channel's
/// `loop_end_reason`; an ACP agent's stop reason when it named one other
/// than `end_turn`, else `interrupted` when its prompt never returned or
/// failed after, else `completed` once its prompt returned. None when the
/// record names no end: an ACP turn never prompted names none.
pub fn recorded_turn_end(record: &Metadata) -> Option<Value> {
    if let Some(reason) = record.get("loop_end_reason").filter(|end| !end.is_null()) {
        return Some(reason.clone());
    }
    let acp = record.get("acp")?.as_object()?;
    if let Some(reason) = acp.get("stop_reason").filter(|reason| truthy(reason)) {
        return Some(reason.clone());
    }
    if acp.get("interrupted").is_some_and(truthy) {
        return Some(Value::from("interrupted"));
    }
    acp.get("prompt_returned")
        .is_some_and(truthy)
        .then(|| Value::from("completed"))
}

/// Whether `value` reads as true, as Python's `bool()` reads it.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn end_of(record: Value) -> Option<Value> {
        let Value::Object(record) = record else {
            panic!("a record is an object");
        };
        recorded_turn_end(&record)
    }

    // Python: tests/test_inbound_turns_record.py::test_an_acp_record_names_its_end
    #[test]
    fn an_acp_record_names_its_end() {
        assert_eq!(end_of(json!({ "acp": {} })), None);
        assert_eq!(
            end_of(json!({ "acp": { "prompt_returned": true } })),
            Some(json!("completed"))
        );
        assert_eq!(
            end_of(json!({ "acp": { "interrupted": true } })),
            Some(json!("interrupted"))
        );
        // Its stop reason first: a drain that failed after the prompt
        // returned does not hide why the agent stopped.
        assert_eq!(
            end_of(
                json!({ "acp": { "prompt_returned": true, "stop_reason": "max_tokens", "interrupted": true } })
            ),
            Some(json!("max_tokens"))
        );
    }

    #[test]
    fn an_ai_channels_end_comes_first() {
        assert_eq!(
            end_of(json!({ "loop_end_reason": "max_rounds", "acp": { "prompt_returned": true } })),
            Some(json!("max_rounds"))
        );
        assert_eq!(end_of(json!({})), None);
    }
}
