//! Decode the room owner's tool answer for the waiting worker.
use crate::turn::OwnerToolAnswer;
use crate::turns::wire::{ToolOutcome, WireError, WireResult};
use serde_json::Value;

/// The answer of an owner's tool as the model's call reads it: a served
/// call's result, its `value` as the codec writes it; a refusal's, a
/// failure's or an unserved call's error.
pub(super) fn tool_answer(
    outcome: ToolOutcome,
    result: Option<Value>,
    error: Option<&WireError>,
) -> OwnerToolAnswer {
    let message = || error.map(|error| error.message.clone()).unwrap_or_default();
    match (outcome, result) {
        (ToolOutcome::Served, Some(Value::Object(record))) => match WireResult::read(record) {
            Ok(WireResult::Parts { parts }) => OwnerToolAnswer::Parts(parts),
            Ok(WireResult::Value { value }) => OwnerToolAnswer::Served(value),
            Err(error) => OwnerToolAnswer::Failed {
                kind: error.kind().into(),
                message: error.to_string(),
            },
        },
        (ToolOutcome::Served, Some(_)) => OwnerToolAnswer::Failed {
            kind: "ValidationError".into(),
            message: "tool_result.result must be an object".into(),
        },
        (ToolOutcome::Refused, _) => OwnerToolAnswer::Refused(message()),
        (ToolOutcome::Unserved, _) => OwnerToolAnswer::Unserved(message()),
        (ToolOutcome::Served | ToolOutcome::Failed, _) => match error {
            Some(error) => OwnerToolAnswer::Failed {
                kind: error.kind.clone(),
                message: error.message.clone(),
            },
            None => OwnerToolAnswer::Failed {
                kind: "RuntimeError".to_owned(),
                message: "The room owner answered a call without its result".to_owned(),
            },
        },
    }
}

#[cfg(test)]
mod result_shapes {
    use super::{OwnerToolAnswer, ToolOutcome, tool_answer};
    use serde_json::json;

    #[test]
    fn malformed_owner_tool_results_fail_the_call_without_returning_a_value() {
        for (payload, expected_kind) in [
            (json!({}), "KeyError"),
            (
                json!({"parts":[{"type":"text"}], "value":"fallback"}),
                "ValidationError",
            ),
            (json!({"parts":null}), "TypeError"),
            (json!({"parts":[3]}), "AttributeError"),
        ] {
            let answer = tool_answer(ToolOutcome::Served, Some(payload), None);
            assert!(
                matches!(answer, OwnerToolAnswer::Failed { kind, .. } if kind == expected_kind)
            );
        }
    }

    #[test]
    fn an_owner_can_return_the_same_array_as_content_parts_or_as_an_ordinary_value() {
        let parts = vec![
            json!({"type":"text","text":"one"}),
            json!({"type":"text","text":"two"}),
        ];
        let ordinary = tool_answer(ToolOutcome::Served, Some(json!({"value":parts})), None);
        let typed = tool_answer(ToolOutcome::Served, Some(json!({"parts":parts})), None);
        assert_eq!(ordinary, OwnerToolAnswer::Served(json!(parts)));
        assert_eq!(typed, OwnerToolAnswer::Parts(parts));
    }
}
