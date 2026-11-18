//! Bidirectional text-turn RPC fixtures. The Python writer and reader use the
//! SDK's actual models. Rust rewrites peer-owned identifiers on the decoded
//! messages so the reverse fixture cannot be a copy of the Python one.

use std::path::PathBuf;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::turns::channel_calls::ChannelResult;
use crate::turns::memory::CarriedRequest;
use crate::turns::pipe::MAX_MESSAGE_BYTES;
use crate::turns::wire::{
    ChannelControl, ChannelControlEnvelope, Control, ControlEnvelope, Frame, FrameEnvelope,
    OwnerMessage, OwnerMessageEnvelope, RoomCallResult, TurnRequest,
};

const PYTHON: &str = include_str!("../../../tests/fixtures/turns/python-rpc.jsonl");

fn typed<T: DeserializeOwned + Serialize>(value: &Value, update: impl FnOnce(&mut T)) -> Value {
    let mut parsed: T = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(&parsed).unwrap(), *value);
    update(&mut parsed);
    serde_json::to_value(parsed).unwrap()
}

fn rewrite(family: &str, value: &Value) -> Value {
    match family {
        "request" => {
            let carried = CarriedRequest::from_wire(&value.to_string()).unwrap();
            assert_eq!(carried.history.len(), 1);
            assert_eq!(carried.channel_rows.len(), 1);
            assert_eq!(carried.event, carried.channel_rows[0]);
            let mut request: TurnRequest =
                serde_json::from_str(&carried.to_wire().unwrap()).unwrap();
            assert_eq!(serde_json::to_value(&request).unwrap(), *value);
            request.turn_id = "rust-turn".into();
            serde_json::to_value(request).unwrap()
        }
        "frame" => typed(value, |envelope: &mut FrameEnvelope| {
            envelope.turn_id = "rust-turn".into();
            if let Frame::HostCall { call_id, .. }
            | Frame::ToolCall { call_id, .. }
            | Frame::CallCancel { call_id } = &mut envelope.message
            {
                *call_id = "rust-call".into();
            }
        }),
        "control" => typed(value, |envelope: &mut ControlEnvelope| {
            envelope.turn_id = "rust-turn".into();
            if let Control::HostResult { call_id, .. } | Control::ToolResult { call_id, .. } =
                &mut envelope.message
            {
                *call_id = "rust-call".into();
            }
        }),
        "channel_control" => typed(value, |envelope: &mut ChannelControlEnvelope| {
            if let ChannelControl::ChannelCall(call) = &mut envelope.message {
                call.call_id = "rust-call".into();
            }
        }),
        "channel_result" => typed(value, |_: &mut ChannelResult| {}),
        "owner" => typed(
            value,
            |envelope: &mut OwnerMessageEnvelope| match &mut envelope.message {
                OwnerMessage::RoomCall(call) => {
                    call.call_id = "rust-call".into();
                    call.turn_id = "rust-turn".into();
                }
                OwnerMessage::RoomPublication { turn_id, .. } => *turn_id = "rust-turn".into(),
            },
        ),
        "host_result" => typed(value, |answer: &mut RoomCallResult| {
            answer.call_id = "rust-call".into();
        }),
        _ => panic!("unrecognized fixture family {family}"),
    }
}

#[test]
fn python_and_rust_exchange_requests_and_turn_rpcs() {
    let mut result = String::new();
    for line in PYTHON.lines() {
        let item: Value = serde_json::from_str(line).unwrap();
        let family = item["direction"].as_str().unwrap();
        let rewritten = rewrite(family, &item["message"]);
        result += &json!({"direction":family, "message":rewritten}).to_string();
        result.push('\n');
    }
    let fixture =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/turns/rust-rpc.jsonl");
    if std::env::var("ROOMKIT_WRITE_TURNS_FIXTURE").as_deref() == Ok("1") {
        std::fs::write(&fixture, &result).unwrap();
    }
    assert_eq!(std::fs::read_to_string(fixture).unwrap(), result);
}

#[test]
fn the_full_request_accepts_events_larger_than_its_metadata_bound() {
    let mut row: Value = serde_json::from_str(PYTHON.lines().next().unwrap()).unwrap();
    let request = &mut row["message"];
    let body = "e".repeat(MAX_MESSAGE_BYTES + 1);
    request["channel_rows"][0]["content"]["body"] = Value::from(body.as_str());
    let carried = CarriedRequest::from_wire(&request.to_string()).unwrap();
    let full: Value = serde_json::from_str(&carried.to_wire().unwrap()).unwrap();
    assert_eq!(
        full["channel_rows"][0]["content"]["body"].as_str(),
        Some(body.as_str())
    );
    assert!(CarriedRequest::from_wire("{}").is_err());
}

#[test]
fn a_room_publication_only_accepts_a_publication_frame() {
    let mut row: Value = PYTHON
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .find(|item| {
            item.pointer("/message/message/type") == Some(&Value::from("room_publication"))
        })
        .unwrap();
    row["message"]["message"]["frame"] = json!({"type":"end"});
    assert!(serde_json::from_value::<OwnerMessageEnvelope>(row["message"].take()).is_err());
}

#[test]
fn empty_pull_grants_and_incomplete_rpc_contexts_are_refused() {
    assert!(serde_json::from_value::<Control>(json!({"type":"pull","count":0})).is_err());
    assert!(serde_json::from_value::<Control>(json!({"type":"pull"})).is_ok());
    for message in [
        json!({"type":"host_call","call_id":"h","verb":"room_context","arguments":{}}),
        json!({"type":"tool_call","call_id":"t","name":"lookup","arguments":{},"context":{"tool_call_id":"lookup","channel_id":"agent"}}),
    ] {
        assert!(serde_json::from_value::<Frame>(message).is_err());
    }
}
