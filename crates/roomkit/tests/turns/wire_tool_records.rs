//! Validate the same tool record in a numbered update and in a stream item.
use roomkit::turns::wire::{Control, Frame, Item, WireResult};
use serde_json::{Value, json};

fn tool_ended_frames(mut end: Value) -> [Value; 2] {
    let update = json!({"type":"tool_ended", "start":1, "end":end});
    end["kind"] = json!("tool_end");
    [update, json!({"type":"item", "item": end})]
}

#[test]
fn an_old_tool_end_omits_intent_and_keeps_required_nullable_fields() {
    let complete = json!({"tool_name":"lookup","tool_id":"call","arguments":{},
        "result":{"value":"ok"},"status":"completed","duration_ms":0,
        "error":null,"structured_content":null,"outcome":null,"refused_but_ran":false});
    for body in tool_ended_frames(complete.clone()) {
        serde_json::from_value::<Frame>(body).unwrap();
    }
    for field in [
        "arguments",
        "duration_ms",
        "error",
        "structured_content",
        "outcome",
        "refused_but_ran",
    ] {
        let mut partial = complete.clone();
        partial.as_object_mut().unwrap().shift_remove(field);
        for body in tool_ended_frames(partial) {
            let error = serde_json::from_value::<Frame>(body).unwrap_err();
            assert!(
                error.to_string().contains(field),
                "missing {field}: {error}"
            );
        }
    }
}

#[test]
fn a_tool_start_can_omit_intent_but_preserves_the_recorded_arguments_and_gate() {
    let start = json!({"kind":"tool_start","tool_name":"lookup","tool_id":"call","arguments":{},"ran_with":null});
    serde_json::from_value::<Item>(start.clone()).unwrap();
    for field in ["arguments", "ran_with"] {
        let mut partial = start.clone();
        partial.as_object_mut().unwrap().shift_remove(field);
        assert!(
            serde_json::from_value::<Item>(partial).is_err(),
            "missing {field}"
        );
    }
}

#[test]
fn result_parts_use_the_python_model_fields_and_take_precedence_over_value() {
    let value = json!([{"type":"text","whatever":5}]);
    assert_eq!(
        serde_json::from_value::<WireResult>(json!({"value":value})).unwrap(),
        WireResult::Value { value }
    );

    let parts = json!([{"type":"text","text":"ok","extra":0}, {"url":"asset","extra":1}]);
    let result = serde_json::from_value::<WireResult>(json!({"parts":parts,"value":3})).unwrap();
    assert_eq!(
        serde_json::to_value(result).unwrap(),
        json!({"parts":[
            {"type":"text","text":"ok"},{"type":"image","url":"asset","mime_type":null}
        ]})
    );
    for bad in [
        json!({"type":"text"}),
        json!({"type":"other","url":"asset"}),
        json!({"type":"image","url":9}),
    ] {
        assert!(serde_json::from_value::<WireResult>(json!({"parts":[bad],"value":3})).is_err());
    }
    for body in [json!({"parts":null,"value":3}), json!({})] {
        assert!(serde_json::from_value::<WireResult>(body).is_err());
    }
    assert_eq!(
        serde_json::to_value(serde_json::from_value::<WireResult>(json!({"value":null})).unwrap())
            .unwrap(),
        json!({"value":null})
    );
}

#[test]
fn an_owner_tool_result_may_omit_its_payload_or_carry_an_object() {
    let mut reply = json!({"type":"tool_result","call_id":"c","outcome":"served"});
    for accepted in [Value::Null, json!({}), json!({"value":17})] {
        reply["result"] = accepted;
        serde_json::from_value::<Control>(reply.clone()).unwrap();
    }
    reply.as_object_mut().unwrap().shift_remove("result");
    serde_json::from_value::<Control>(reply.clone()).unwrap();
    reply["result"] = json!(17);
    assert!(serde_json::from_value::<Control>(reply).is_err());
}
