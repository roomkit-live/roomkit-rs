//! A transport can read the request and exchange the same numbered messages as Python.
use roomkit::turns::wire::{Control, ControlEnvelope, Frame, FrameEnvelope};
use roomkit::turns::{ChannelControl, ChannelResult, TurnRequest};
use serde_json::{Value, json};

#[test]
fn a_transport_round_trips_the_public_request_and_both_numbered_directions() {
    let fixture = include_str!("../fixtures/turns/python-case1.jsonl");
    let row: Value = serde_json::from_str(fixture.lines().next().unwrap()).unwrap();
    let request: TurnRequest = serde_json::from_value(row["message"].clone()).unwrap();
    assert_eq!(request.channel_id.as_str(), "ai");
    assert_eq!(request.context.room.id, request.room_id);
    request.hand_over();
    assert!(request.handed());
    assert!(
        !serde_json::from_value::<TurnRequest>(serde_json::to_value(&request).unwrap())
            .unwrap()
            .handed()
    );
    request.refused();
    assert!(!request.handed());
    let frame = FrameEnvelope {
        turn_id: request.turn_id.clone(),
        seq: 1,
        message: Frame::Accepted {
            executor_id: "worker".into(),
        },
    };
    let serialized = serde_json::to_value(&frame).unwrap();
    assert_eq!(
        serialized["message"],
        json!({"type":"accepted","executor_id":"worker"})
    );
    assert_eq!(
        serde_json::from_value::<FrameEnvelope>(serialized).unwrap(),
        frame
    );
    let control = ControlEnvelope {
        turn_id: request.turn_id,
        seq: 1,
        message: Control::Cancel {
            reason: "shutdown".into(),
        },
    };
    assert_eq!(
        serde_json::to_value(control).unwrap()["message"],
        json!({"type":"cancel","reason":"shutdown"})
    );
}

#[test]
fn a_channel_message_retains_its_transport_handover_only_within_the_owner() {
    let body = json!({"type":"channel_call","call_id":"call","channel_id":"ai","room_id":"room","verb":"status"});
    let control: ChannelControl = serde_json::from_value(body).unwrap();
    let ChannelControl::ChannelCall(call) = &control else {
        panic!("expected a channel call")
    };
    assert!(!call.handed());
    call.hand_over();
    assert!(call.handed());
    let raw = serde_json::to_value(&control).unwrap();
    assert!(raw.get("handed").is_none());
    let ChannelControl::ChannelCall(remote) = serde_json::from_value(raw).unwrap() else {
        panic!("expected the remote call")
    };
    assert!(!remote.handed());
    call.refused();
    assert!(!call.handed());

    let result = json!({"type":"channel_result","executor_id":"worker","result":7,"room_state":{"version":2},"error":null});
    let decoded: ChannelResult = serde_json::from_value(result.clone()).unwrap();
    assert_eq!(decoded.result, 7);
    assert_eq!(serde_json::to_value(decoded).unwrap(), result);
}

#[test]
fn owner_messages_keep_one_python_discriminator_inside_or_outside_the_envelope() {
    use roomkit::turns::wire::{OwnerMessage, OwnerMessageEnvelope};
    let fixture = include_str!("../fixtures/turns/python-rpc.jsonl");
    let mut seen = Vec::new();
    for row in fixture
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
    {
        if row["direction"] != "owner" {
            continue;
        }
        let message: OwnerMessageEnvelope = serde_json::from_value(row["message"].clone()).unwrap();
        let source = &row["message"]["message"];
        let (kind, inner) = match &message.message {
            OwnerMessage::RoomCall(call) => ("room_call", serde_json::to_string(call).unwrap()),
            OwnerMessage::RoomPublication(publication) => (
                "room_publication",
                serde_json::to_string(publication).unwrap(),
            ),
        };
        assert_eq!(source["type"], kind);
        assert_eq!(serde_json::to_string(&message.message).unwrap(), inner);
        let restored: OwnerMessage = serde_json::from_str(&inner).unwrap();
        assert_eq!(restored, message.message);
        seen.push(kind);
    }
    assert!(seen.contains(&"room_call"));
    assert!(seen.contains(&"room_publication"));
}

#[test]
fn a_channel_call_keeps_one_type_in_every_wire_form() {
    use roomkit::turns::ChannelCall;
    use roomkit::turns::wire::ChannelControlEnvelope;

    let expected = json!({"type":"channel_call","call_id":"call","channel_id":"ai","room_id":"room",
        "verb":"status","arguments":{"type":"snapshot","take":1},"organization_id":"org","room_state":{"version":2},"previous_turn":null});
    let call: ChannelCall = serde_json::from_value(expected.clone()).unwrap();
    let raw = serde_json::to_string(&call).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&raw).unwrap(), expected);
    let envelope = ChannelControlEnvelope {
        message: ChannelControl::ChannelCall(Box::new(call)),
    };
    let raw = serde_json::to_string(&envelope).unwrap();
    assert_eq!(raw.matches(r#""type":"channel_call""#).count(), 1);
    assert_eq!(
        serde_json::from_str::<ChannelControlEnvelope>(&raw).unwrap(),
        envelope
    );
    assert_eq!(
        serde_json::from_str::<Value>(&raw).unwrap()["message"],
        expected
    );
}

#[test]
fn an_input_request_keeps_one_type_in_every_wire_form() {
    use roomkit::turns::wire::InputRequest;

    let expected = json!({"type":"input_request","request_id":"pending","tool_name":"ask_user",
        "arguments":{"question":"Which room?"},"room_id":"room","channel_id":"ai",
        "tool_call_id":"call","actor_id":"alice","created_at":null});
    let request: InputRequest = serde_json::from_value(expected.clone()).unwrap();
    let raw = serde_json::to_string(&request).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&raw).unwrap(), expected);
    let envelope = FrameEnvelope {
        turn_id: "turn".into(),
        seq: 1,
        message: Frame::InputRequest {
            request: Box::new(request),
        },
    };
    let raw = serde_json::to_string(&envelope).unwrap();
    assert_eq!(raw.matches(r#""type":"input_request""#).count(), 1);
    assert_eq!(
        serde_json::from_str::<FrameEnvelope>(&raw).unwrap(),
        envelope
    );
    assert_eq!(
        serde_json::from_str::<Value>(&raw).unwrap()["message"],
        expected
    );
}

#[test]
fn both_call_kinds_generate_independent_ids_and_preserve_an_explicit_id() {
    use roomkit::turns::{ChannelCall, RoomCall};

    let channel = roomkit::model::ChannelId::new("agent").unwrap();
    let room = roomkit::model::RoomId::new("room").unwrap();
    let first = ChannelCall::new(channel.clone(), room.clone(), "freeze");
    let second = RoomCall::new(channel, room, "worker", "turn", "freeze");
    let mut raw = json!({"channel_id":"agent","room_id":"room","verb":"freeze"});
    let decoded: ChannelCall = serde_json::from_value(raw.clone()).unwrap();
    raw["turn_id"] = json!("turn");
    raw["executor_id"] = json!("worker");
    let recalled: RoomCall = serde_json::from_value(raw.clone()).unwrap();
    let ids = [
        &first.call_id,
        &second.call_id,
        &decoded.call_id,
        &recalled.call_id,
    ];
    for id in ids {
        let suffix = id.strip_prefix("call-").unwrap();
        assert_eq!(suffix.len(), 32);
        assert!(suffix.bytes().all(|b| b.is_ascii_hexdigit()));
    }
    assert_eq!(std::collections::BTreeSet::from(ids).len(), 4);
    assert_eq!(second.chain_depth, 0);
    assert!(second.arguments.is_empty());
    raw["call_id"] = json!("chosen");
    assert_eq!(
        serde_json::from_value::<RoomCall>(raw).unwrap().call_id,
        "chosen"
    );
}

#[test]
fn an_application_can_construct_the_public_models_with_the_python_defaults() {
    use roomkit::turns::{OwnerTool, PreviousTurn, RoomPublication};

    assert_eq!(
        serde_json::to_value(ChannelResult::new()).unwrap(),
        json!({
            "type":"channel_result", "result":null, "error":null, "executor_id":null, "room_state":null
        })
    );
    let tool = OwnerTool::new(json!({"name":"search"}));
    assert!(!tool.waits_by_design && !tool.ends_turn);
    assert_eq!(PreviousTurn::new("turn").executor_id, None);
    let frame = json!({"type":"framework_event","event_type":"seen","room_id":"r","channel_id":"c","event_id":null,"data":{}});
    let publication = RoomPublication::new(
        "c".parse().unwrap(),
        "r".parse().unwrap(),
        "worker",
        "turn",
        serde_json::from_value(frame.clone()).unwrap(),
    );
    assert_eq!(
        serde_json::to_value(publication).unwrap(),
        json!({"type":"room_publication","channel_id":"c","room_id":"r","executor_id":"worker","turn_id":"turn","frame":frame})
    );
}
