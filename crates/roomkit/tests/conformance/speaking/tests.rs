//! The same malformed controls as the Python fixture tests.
use serde_json::{Value, json};

// Python: tests/conformance/test_speaking_fixture.py::test_invalid_policy_is_refused_before_product_execution
#[test]
fn invalid_policy_is_refused_before_product_execution() {
    let values: Vec<Value> = serde_json::from_str(r#"[null, [], {}, {"decisions": []}, {"decisions": [null]}, {"decisions": [{"decision": null}]}, {"decisions": [{"decision": {"mode": "maybe"}}]}, {"decisions": [{"decision": {"mode": "speak", "final": true}}]}, {"decisions": [{"decision": {"mode": "silent", "notes": "not an array"}}]}, {"decisions": [{"decision": {"mode": "silent", "judgments": null}}]}, {"decisions": [{"decision": {"mode": "silent", "judgments": {"p": true}}}]}, {"decisions": [{"fail": null}]}, {"decisions": [{"fail": "failed", "decision": {"mode": "silent"}}]}, {"decisions": [{"fail": "failed", "gate": null}]}, {"decisions": [{"decision": {"mode": "silent", "misspelled": 1}}]}, {"decisions": [{"fail": "failed", "misspelled": 1}]}, {"decisions": [{"fail": "failed"}], "misspelled": 1}]"#).unwrap();
    for value in values {
        let scenario = json!({"setup":{"channels":[{"id":"ai","type":"ai","speak_policy":value}]}});
        assert!(
            crate::shape::unknown_members(&scenario)
                .iter()
                .any(|problem| problem.contains("speak_policy")),
            "{value}"
        );
        assert!(super::shape::script(&value, false).is_err());
    }
}

// Python: tests/conformance/test_thinking_fixture.py::test_invalid_thinker_is_refused_before_product_execution
#[test]
fn invalid_thinker_is_refused_before_product_execution() {
    let values: Vec<Value> = serde_json::from_str(r#"[null, [], {}, {"thoughts": []}, {"thoughts": [null]}, {"thoughts": [{"thought": null}]}, {"thoughts": [{"thought": {"text": 1}}]}, {"thoughts": [{"thought": {"urgent": 1}}]}, {"thoughts": [{"thought": {"want_to_say": "words"}}]}, {"thoughts": [{"thought": {"want_to_say": [null]}}]}, {"thoughts": [{"thought": {"misspelled": 1}}]}, {"thoughts": [{"thought": {}, "fail": "failed"}]}, {"thoughts": [{"fail": null}]}, {"thoughts": [{"thought": {}, "gate": null}]}, {"thoughts": [{"thought": {}, "misspelled": 1}]}, {"thoughts": [{"thought": {}}], "misspelled": 1}]"#).unwrap();
    for value in values {
        let scenario = json!({"setup":{"channels":[{"id":"ai","type":"ai","thinker":value}]}});
        assert!(
            crate::shape::unknown_members(&scenario)
                .iter()
                .any(|problem| problem.contains("thinker")),
            "{value}"
        );
        assert!(super::shape::script(&value, true).is_err());
    }
}

// Python: tests/conformance/test_speaking_fixture.py::test_invalid_decision_patch_is_refused_before_a_hook_can_swallow_it
#[test]
fn invalid_decision_patch_is_refused_before_a_hook_can_swallow_it() {
    for patch in [
        Value::Null,
        json!({}),
        json!({"decision":null}),
        json!({"decision":{"mode":"maybe"}}),
        json!({"misspelled":1}),
    ] {
        let scenario = json!({"setup":{"hooks":[{"name":"decision","trigger":"on_speak_decision","reactions":[{"result":{"action":"modify","event":patch}}]}]}});
        assert_ne!(
            crate::shape::unknown_members(&scenario),
            Vec::<String>::new()
        );
    }
}
