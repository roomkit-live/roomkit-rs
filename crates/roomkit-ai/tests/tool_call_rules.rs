//! The public text-provider rules for partial tool calls (RFC §6.4).

use roomkit_ai::{
    AiToolCall, call_cut, call_garbled, call_partial, cut_call_error, is_truncation,
    partial_call_error, unreadable_arguments, unreadable_call_error,
};
use serde_json::{Value, json};

#[test]
fn provider_inputs_follow_the_python_call_rules() {
    // Rows record raw input, reason, last, unreadable, partial, cut and garbled.
    type Case = (Value, Option<String>, bool, bool, bool, bool, bool);
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("tool_call_rules/python_cases.json")).unwrap();
    for (raw, finish, last, unreadable, partial, cut, garbled) in cases {
        let reason = finish.as_deref();
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
        let call = AiToolCall::read("id", "find", &raw, Some("stop"), true);
        assert_eq!(call.arguments["raw"], raw);
        assert!(call.partial && call.garbled);
    }
}

#[test]
fn stopping_reasons_and_model_errors_are_available_outside_the_channel() {
    for reason in [
        "length",
        "MAX_TOKENS",
        "model_context_window_exceeded",
        "MODEL_LENGTH",
    ] {
        assert!(is_truncation(Some(reason)));
    }
    for reason in [None, Some("content_filter"), Some("stop")] {
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
