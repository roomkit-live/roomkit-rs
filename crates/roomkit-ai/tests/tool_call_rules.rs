//! The public text-provider rules for partial tool calls (RFC §6.4).

use roomkit_ai::{
    AiToolCall, FinishReason, call_cut, call_garbled, call_partial, cut_call_error, is_truncation,
    partial_call_error, unreadable_arguments, unreadable_call_error,
};
use serde_json::{Value, json};

#[test]
fn provider_inputs_follow_the_python_call_rules() {
    // Rows record raw input, reason, last, unreadable, partial, cut and garbled.
    type Case = (Value, Option<FinishReason>, bool, bool, bool, bool, bool);
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("tool_call_rules/python_cases.json")).unwrap();
    for (raw, reason, last, unreadable, partial, cut, garbled) in cases {
        assert_eq!(
            (
                unreadable_arguments(&raw),
                call_partial(&raw, reason, roomkit_ai::CallPartialOptions { last }),
                call_cut(&raw, reason, roomkit_ai::CallCutOptions { last }),
                call_garbled(&raw, reason, roomkit_ai::CallGarbledOptions { last })
            ),
            (unreadable, partial, cut, garbled),
            "{raw:?}, {reason:?}, last={last}"
        );
    }
}

#[test]
fn nonfinite_text_is_kept_verbatim_by_the_wire_reader() {
    for value in ["NaN", "Infinity", "-Infinity", "1e900", "-1e900"] {
        let raw = format!(r#"{{"record":[{{"amount":{value}}}]}}"#);
        let call = AiToolCall::read("id", "find", &raw, Some(FinishReason::Stop), true);
        assert_eq!(call.arguments["raw"], raw);
        assert!(call.partial && call.garbled);
    }
}

#[test]
fn stopping_reasons_and_model_errors_are_available_outside_the_channel() {
    assert!(is_truncation(Some(FinishReason::Length)));
    for reason in [
        None,
        Some(FinishReason::ContentFilter),
        Some(FinishReason::Stop),
    ] {
        assert!(!is_truncation(reason));
    }
    let cut = cut_call_error("find");
    let unreadable = unreadable_call_error("find");
    assert_eq!(json!(cut)["error"], "Tool call cut off");
    assert_eq!(json!(unreadable)["error"], "Tool call arguments unreadable");
    assert_eq!(cut["tool"], "find");
    assert_eq!(unreadable["tool"], "find");
    assert_eq!(partial_call_error("find", false), cut);
    assert_eq!(partial_call_error("find", true), unreadable);
    assert!(cut["hint"].as_str().unwrap().contains("shorter arguments"));
    assert!(
        unreadable["hint"]
            .as_str()
            .unwrap()
            .contains("valid JSON object")
    );
}
