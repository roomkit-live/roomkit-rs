//! A refused placement preserves the last turn (RFC §10.6.2).
//! Python: `tests/test_turns_channel_control.py::test_a_missing_hook_refusal_preserves_the_previous_turn`.

use crate::support;
use roomkit::model::ChannelType;
use roomkit::testing::TurnTap;
use roomkit::testing::{
    EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
};
use roomkit::{AttachChannelOptions, CreateRoomOptions, RoomKit, RoomKitOptions};
use roomkit::{DisableRoomHookOptions, EnableRoomHookOptions};
use serde_json::Value;

#[tokio::test]
async fn refused_missing_hook_does_not_become_previous() {
    let kit = kit().await;
    let tap = TurnTap::on(&kit);
    assert!(support::asks(&kit, "first").await.error.is_none());
    roomkit::testing::idle(&kit).await;
    kit.enable_room_hook(
        &support::room(),
        "missing",
        EnableRoomHookOptions::default(),
    )
    .await
    .unwrap();
    assert!(support::asks(&kit, "refused").await.error.is_some());
    assert!(
        kit.disable_room_hook(
            &support::room(),
            "missing",
            DisableRoomHookOptions::default()
        )
        .await
        .unwrap()
    );
    assert!(support::asks(&kit, "third").await.error.is_none());
    let requests: Vec<Value> = tap
        .messages()
        .into_iter()
        .filter(|m| m.family == "request")
        .map(|m| serde_json::from_str(&m.json).unwrap())
        .collect();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1]["previous_turn"]["turn_id"],
        requests[0]["turn_id"]
    );
    kit.close().await.unwrap();
}

async fn kit() -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        support::channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(EchoAgent::new(
        support::channel("ai"),
        "answer",
        EchoAgentOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(support::room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ai"] {
        kit.attach_channel(
            &support::room(),
            &support::channel(id),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    kit
}
