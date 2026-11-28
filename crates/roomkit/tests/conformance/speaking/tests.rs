//! The same malformed controls as the Python fixture tests.
use serde_json::{Value, json};

// Python: tests/conformance/test_speaking_fixture.py::test_invalid_policy_is_refused_before_product_execution
#[test]
fn invalid_policy_is_refused_before_product_execution() {
    let values: Vec<Value> = serde_json::from_str(r#"[null, [], {}, {"decisions": []}, {"decisions": [null]}, {"decisions": [{"decision": null}]}, {"decisions": [{"decision": {"mode": "maybe"}}]}, {"decisions": [{"decision": {"mode": "speak", "final": true}}]}, {"decisions": [{"decision": {"mode": "silent", "notes": "not an array"}}]}, {"decisions": [{"decision": {"mode": "silent", "judgments": null}}]}, {"decisions": [{"decision": {"mode": "silent", "judgments": {"p": true}}}]}, {"decisions": [{"fail": null}]}, {"decisions": [{"fail": "failed", "decision": {"mode": "silent"}}]}, {"decisions": [{"fail": "failed", "gate": null}]}, {"decisions": [{"decision": {"mode": "silent", "misspelled": 1}}]}, {"decisions": [{"fail": "failed", "misspelled": 1}]}, {"decisions": [{"fail": "failed"}], "misspelled": 1}, {"classifier": {"calls": []}, "agent_name": "Nova"}, {"classifier": {"calls": [{"answers": []}]}, "agent_name": "Nova"}, {"classifier": {"calls": [{"answers": {"hush": true}}]}, "agent_name": "Nova"}, {"classifier": {"calls": [{"answers": {}, "fail": "down"}]}, "agent_name": "Nova"}, {"classifier": {"calls": [{"answers": {}, "misspelled": 1}]}, "agent_name": "Nova"}, {"classifier": {"calls": [{"answers": {}}]}}, {"classifier": {"calls": [{"answers": {}}]}, "agent_name": "Nova", "decisions": [{"decision": {"mode": "speak"}}]}, {"agent_name": "Nova", "decisions": [{"decision": {"mode": "speak"}}]}, {"classifier": {"calls": [{"answers": {}}]}, "agent_name": "Nova", "questions": {"hush": {"instructions": 1}}}, {"classifier": {"calls": [{"answers": {}}]}, "agent_name": "Nova", "questions": {"hush": {"instructions": "?", "options": {}, "levels": []}}}, {"classifier": {"calls": [{"answers": {}}]}, "agent_name": "Nova", "languages": {"French": 1}}, {"classifier": {"calls": [{"answers": {}}]}, "agent_name": "Nova", "composition": {"decision": {"mode": "maybe"}}}, {"classifier": {"calls": [{"answers": {}}]}, "agent_name": "Nova", "composition": {"decision": {"mode": "speak"}, "misspelled": 1}}, {"answer_only": {"people": []}, "decisions": [{"decision": {"mode": "speak"}}]}, {"answer_only": ["alice"], "decisions": [{"decision": {"mode": "speak"}}]}]"#).unwrap();
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
    let values: Vec<Value> = serde_json::from_str(r#"[null, [], {}, {"thoughts": []}, {"thoughts": [null]}, {"thoughts": [{"thought": null}]}, {"thoughts": [{"thought": {"text": 1}}]}, {"thoughts": [{"thought": {"urgent": 1}}]}, {"thoughts": [{"thought": {"want_to_say": "words"}}]}, {"thoughts": [{"thought": {"want_to_say": [null]}}]}, {"thoughts": [{"thought": {"misspelled": 1}}]}, {"thoughts": [{"thought": {}, "fail": "failed"}]}, {"thoughts": [{"fail": null}]}, {"thoughts": [{"thought": {}, "gate": null}]}, {"thoughts": [{"thought": {}, "misspelled": 1}]}, {"thoughts": [{"thought": {}}], "misspelled": 1}, {"llm": null}, {"llm": {}}, {"llm": {"model": {"rounds": []}, "timeout_ms": "1"}}, {"llm": {"model": {"rounds": []}, "max_tokens": 1.5}}, {"llm": {"model": {"rounds": []}, "misspelled": 1}}, {"llm": {"model": {"rounds": []}}, "thoughts": [{"thought": {}}]}]"#).unwrap();
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

// Python: tests/conformance/test_speaking_fixture.py::test_a_scripted_answer_the_question_cannot_take_is_a_problem_not_a_fallback
#[test]
fn a_scripted_answer_the_question_cannot_take_is_a_problem_not_a_fallback() {
    let questions: roomkit_ai::classifiers::Questions = serde_json::from_value(json!({
        "hush": {"instructions": "Quiet?", "yes": null, "no": null},
        "level": {"instructions": "How?", "levels": ["a", "b"]},
        "pick": {"instructions": "Which?", "options": {"a": "", "b": ""}},
    }))
    .unwrap();
    for answers in [
        json!({"hush": "yes"}),
        json!({"level": "high"}),
        json!({"pick": "c"}),
        json!({"pick": 1}),
        json!({"husk": 0.9}),
    ] {
        assert_eq!(
            super::classifier::answer_problems(&answers, &questions).len(),
            1,
            "{answers}"
        );
    }
    let fitting = json!({"hush": 0.9, "level": 1, "pick": "b"});
    assert!(super::classifier::answer_problems(&fitting, &questions).is_empty());
}
