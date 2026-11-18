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
