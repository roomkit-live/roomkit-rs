//! The Python protocol at the API pin and the JSON carried by public Rust links.
use roomkit::turns::wire::{
    Control, ControlEnvelope, Frame, FrameEnvelope, InputOutcome, Item, OwnerMessageEnvelope,
    PublicationFrame, ToolEnd, ToolOutcome, WireLoopEnd, WireResult,
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::BTreeSet;

const PYTHON: &str = include_str!("../../../../api/python/turn_protocol.json");
const RPC: &str = include_str!("../fixtures/turns/python-rpc.jsonl");
const TURN: &str = include_str!("../fixtures/turns/python-case1.jsonl");

fn roundtrip<T: DeserializeOwned + Serialize>(input: &Value) -> Value {
    let value: T = serde_json::from_value(input.clone()).unwrap();
    serde_json::to_value(value).unwrap()
}

fn names(value: &Value) -> BTreeSet<String> {
    value.as_object().unwrap().keys().cloned().collect()
}

fn check_record(shape: &Value, before: &Value, after: &Value) -> String {
    let tag = shape["tag"].as_str().unwrap();
    let value = before[tag].as_str().unwrap();
    assert_eq!(after[tag], value);
    let fields = shape["variants"][value].as_array().unwrap();
    for key in names(after) {
        assert!(
            key == tag || fields.contains(&Value::from(key.as_str())),
            "{value}.{key}"
        );
    }
    for key in names(before) {
        assert!(after.get(&key).is_some(), "{value}.{key} was lost");
    }
    value.to_owned()
}

fn check_envelope<T: DeserializeOwned + Serialize>(
    shape: &Value,
    record: &Value,
    before: &Value,
) -> String {
    let after = roundtrip::<T>(before);
    let keys: BTreeSet<_> = shape["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(names(&after), keys);
    assert_eq!(after["turn_id"], before["turn_id"]);
    assert_eq!(after["seq"], before["seq"]);
    check_record(record, &before["message"], &after["message"])
}

#[test]
fn the_public_link_carries_every_pinned_message_and_only_publication_frames_outside_turns() {
    let pin: Value = serde_json::from_str(PYTHON).unwrap();
    let forms = &pin["wire_shapes"];
    let frame = &forms["roomkit.turns.wire.Frame"];
    let control = &forms["roomkit.turns.wire.Control"];
    let owner = &forms["roomkit.turns.wire.OwnerMessage"];
    let publication = &forms["roomkit.turns.wire.RoomPublication.frame"];
    let (mut frames, mut controls, mut owners) =
        (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
    for line in RPC.lines().chain(TURN.lines()) {
        let row: Value = serde_json::from_str(line).unwrap();
        let envelope = &row["message"];
        let message = &envelope["message"];
        match row["direction"].as_str().unwrap() {
            "frame" => {
                let tag = check_envelope::<FrameEnvelope>(
                    &forms["roomkit.turns.wire.FrameEnvelope"],
                    frame,
                    envelope,
                );
                let accepted = serde_json::from_value::<PublicationFrame>(message.clone());
                assert_eq!(
                    accepted.is_ok(),
                    publication["variants"].get(&tag).is_some(),
                    "{tag}"
                );
                if let Ok(published) = accepted {
                    check_record(
                        publication,
                        message,
                        &serde_json::to_value(published).unwrap(),
                    );
                }
                frames.insert(tag);
            }
            "control" => {
                controls.insert(check_envelope::<ControlEnvelope>(
                    &forms["roomkit.turns.wire.ControlEnvelope"],
                    control,
                    envelope,
                ));
            }
            "owner" => {
                owners.insert(check_record(
                    owner,
                    message,
                    &roundtrip::<OwnerMessageEnvelope>(envelope)["message"],
                ));
            }
            _ => {}
        }
    }
    assert_eq!(frames, names(&frame["variants"]));
    assert_eq!(controls, names(&control["variants"]));
    assert_eq!(owners, names(&owner["variants"]));
    for invalid in [json!({"type":"unknown"}), json!({"kind":"heartbeat"})] {
        assert!(serde_json::from_value::<Frame>(invalid.clone()).is_err());
        assert!(serde_json::from_value::<Control>(invalid).is_err());
    }
}

fn check_literals<T: DeserializeOwned + Serialize>(shape: &Value) {
    for value in shape["values"].as_array().unwrap() {
        assert_eq!(roundtrip::<T>(value), *value);
    }
    assert!(serde_json::from_value::<T>(json!("unknown")).is_err());
}

#[test]
fn stream_markers_outcomes_and_result_keys_match_the_python_codecs() {
    let pin: Value = serde_json::from_str(PYTHON).unwrap();
    let forms = &pin["wire_shapes"];
    let shape = &forms["roomkit.turns._codec.encode_item"];
    let mut kinds = BTreeSet::new();
    for line in RPC.lines().chain(TURN.lines()) {
        let row: Value = serde_json::from_str(line).unwrap();
        if let Some(item) = row["message"]["message"].get("item") {
            kinds.insert(check_record(shape, item, &roundtrip::<Item>(item)));
        }
    }
    for item in [
        json!({"kind":"thinking", "thinking":"thought"}),
        json!({"kind":"segment_break"}),
        json!({"kind":"instruction_read", "text":"instruction", "runtime_record":"runtime"}),
    ] {
        kinds.insert(check_record(shape, &item, &roundtrip::<Item>(&item)));
    }
    assert_eq!(kinds, names(&shape["variants"]));
    assert!(serde_json::from_value::<Item>(json!({"kind":"unknown"})).is_err());
    check_literals::<InputOutcome>(&forms["roomkit.turns.wire.InputAnswer.outcome"]);
    check_literals::<ToolOutcome>(&forms["roomkit.turns.wire.ToolResult.outcome"]);

    let mut results = BTreeSet::new();
    for value in [
        json!({"value":[1,2]}),
        json!({"parts":[{"type":"text","text":"reply"}]}),
    ] {
        let encoded = roundtrip::<WireResult>(&value);
        assert_eq!(encoded, value);
        results.extend(names(&encoded));
    }
    let pinned: BTreeSet<_> = forms["roomkit.turns._codec.encode_result"]["keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|key| key.as_str().unwrap().to_owned())
        .collect();
    assert_eq!(results, pinned);
}

#[test]
fn the_early_tool_end_and_the_loop_summary_emit_the_pinned_codec_fields() {
    let pin: Value = serde_json::from_str(PYTHON).unwrap();
    let forms = &pin["wire_shapes"];
    let mut checked = BTreeSet::new();
    for line in RPC.lines().chain(TURN.lines()) {
        let row: Value = serde_json::from_str(line).unwrap();
        let frame = &row["message"]["message"];
        let (source, output) = if frame.get("type") == Some(&json!("tool_ended")) {
            let end = &frame["end"];
            if end.get("intent").is_none() {
                continue;
            }
            ("roomkit.turns._codec.encode_end", roundtrip::<ToolEnd>(end))
        } else if frame["item"]["kind"] == "loop_end" {
            (
                "roomkit.models.streaming.LoopEndMarker",
                roundtrip::<WireLoopEnd>(&frame["item"]),
            )
        } else {
            continue;
        };
        let fields: BTreeSet<_> = forms[source]["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| field.as_str().unwrap().to_owned())
            .collect();
        assert_eq!(names(&output), fields, "{source}");
        checked.insert(source);
    }
    assert_eq!(checked.len(), 2);
}
