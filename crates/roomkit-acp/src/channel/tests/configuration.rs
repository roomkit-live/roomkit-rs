//! Native configuration checks that need the channel object or a held wire reply.

use roomkit::model::{ChannelId, Metadata, RoomId};
use roomkit::{
    AttachChannelOptions, Channel, ChannelCallOptions, CreateRoomOptions, GetRoomOptions, RoomKit,
    RoomKitOptions,
};
use serde_json::{Value, json};

use super::{Agent, PATIENCE, Piped, channel};
use crate::{AcpChannel, AcpChannelOptions, AcpError};

fn options(value: &str) -> Value {
    json!([{"id":"model","type":"select","name":"Model","currentValue":value,
        "options":[{"value":"opus","name":"Opus"},{"value":"sonnet","name":"Sonnet"}]}])
}

async fn open(agent: &mut Agent) {
    let request = agent.read().await;
    assert_eq!(request["method"], "initialize");
    agent
        .answer(
            &request,
            json!({"protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"close":{}}}}),
        )
        .await;
    let request = agent.read().await;
    assert_eq!(request["method"], "session/new");
    agent
        .answer(
            &request,
            json!({"sessionId":"s1","configOptions":options("opus")}),
        )
        .await;
}

async fn answer_setting(agent: &mut Agent, value: &str) {
    let request = agent.read().await;
    assert_eq!(request["method"], "session/set_config_option");
    assert_eq!(request["params"]["value"], value);
    agent
        .answer(&request, json!({"configOptions":options(value)}))
        .await;
}

async fn close(agent: &mut Agent) {
    let request = agent.read().await;
    assert_eq!(request["method"], "session/close");
    agent.answer(&request, json!({})).await;
}

// Python: tests/test_channels/test_acp_backend.py::test_a_value_set_on_the_channel_object_is_not_stored_by_a_read
#[tokio::test]
async fn test_a_value_set_on_the_channel_object_is_not_stored_by_a_read() {
    let (channel, mut agents, _) = channel();
    let agent = tokio::spawn(async move {
        let mut agent = Agent::new(agents.recv().await.unwrap());
        open(&mut agent).await;
        answer_setting(&mut agent, "sonnet").await;
        answer_setting(&mut agent, "opus").await;
        close(&mut agent).await;
    });
    let kit = RoomKit::new(RoomKitOptions::default());
    let id = ChannelId::new("claude").unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.register_channel(channel.clone()).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room, &id, AttachChannelOptions::default())
        .await
        .unwrap();
    let arguments = Metadata::from_iter([
        ("config_id".to_owned(), json!("model")),
        ("value".to_owned(), json!("sonnet")),
    ]);
    kit.channel_call(
        &id,
        &room,
        "set_config_option",
        ChannelCallOptions {
            arguments,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    channel
        .set_config_option(&room, "model", json!("opus"))
        .await
        .unwrap();
    let values = kit
        .channel_call(&id, &room, "session_config", ChannelCallOptions::default())
        .await
        .unwrap();
    assert_eq!(values, json!({"model":"opus"}));
    let stored = kit
        .get_room(&room, GetRoomOptions::default())
        .await
        .unwrap();
    assert_eq!(
        stored.metadata.get("roomkit:channel_state:claude"),
        Some(&json!({"config":{"model":"sonnet"}}))
    );
    kit.close().await.unwrap();
    tokio::time::timeout(PATIENCE, agent)
        .await
        .unwrap()
        .unwrap();
    assert!(channel.session_config(&room).is_empty());
}

// Python: tests/test_channels/test_acp_session_setup.py::TestSessionConfig::test_a_value_set_while_the_session_closes_is_not_kept
#[tokio::test]
async fn test_a_value_set_while_the_session_closes_is_not_kept() {
    let (channel, mut agents, _) = channel();
    let room = RoomId::new("r1").unwrap();
    let setting = tokio::spawn({
        let channel = channel.clone();
        let room = room.clone();
        async move {
            channel
                .set_config_option(&room, "model", json!("sonnet"))
                .await
        }
    });
    let mut agent = Agent::new(agents.recv().await.unwrap());
    open(&mut agent).await;
    answer_setting(&mut agent, "sonnet").await;
    setting.await.unwrap().unwrap();
    let setting = tokio::spawn({
        let channel = channel.clone();
        let room = room.clone();
        async move {
            channel
                .set_config_option(&room, "model", json!("opus"))
                .await
        }
    });
    let pending = agent.read().await;
    assert_eq!(pending["method"], "session/set_config_option");
    let closing = tokio::spawn({
        let channel = channel.clone();
        let room = room.clone();
        async move { channel.close_session(&room).await }
    });
    close(&mut agent).await;
    assert!(closing.await.unwrap());
    agent
        .answer(&pending, json!({"configOptions":options("opus")}))
        .await;
    assert_eq!(
        setting.await.unwrap().unwrap().get("model"),
        Some(&json!("opus"))
    );
    assert!(channel.session_config(&room).is_empty());
    assert!(channel.room_state(&room).unwrap().is_empty());
    assert!(channel.shared.rooms.lock().unwrap().is_empty());
    channel.close().await;
}

// Python: tests/test_channels/test_acp_session_setup.py::TestARefusedValueFailsClosed::test_a_lost_connection_is_not_reported_as_a_refusal
#[tokio::test]
async fn test_a_lost_connection_is_not_reported_as_a_refusal() {
    let (channel, mut agents, _) = channel();
    let room = RoomId::new("r1").unwrap();
    let setting = tokio::spawn({
        let channel = channel.clone();
        let room = room.clone();
        async move {
            channel
                .set_config_option(&room, "model", json!("sonnet"))
                .await
        }
    });
    let mut agent = Agent::new(agents.recv().await.unwrap());
    open(&mut agent).await;
    assert_eq!(agent.read().await["method"], "session/set_config_option");
    drop(agent);
    assert!(matches!(
        setting.await.unwrap(),
        Err(AcpError::ConnectionClosed)
    ));
    assert!(channel.session_id(&room).is_none());
    assert!(channel.room_state(&room).unwrap().is_empty());
    channel.close().await;
}

// Python: tests/test_channels/test_acp_session_setup.py::TestSessionConfig::test_values_acp_cannot_carry_are_refused
#[test]
fn test_constructor_refuses_values_acp_cannot_carry() {
    for value in [json!(3), Value::Null, json!([])] {
        let options = AcpChannelOptions::<Piped> {
            session_config: Some(Metadata::from_iter([("effort".to_owned(), value)])),
            ..Default::default()
        };
        assert!(matches!(
            AcpChannel::new(ChannelId::new("claude").unwrap(), "/srv", options),
            Err(AcpError::Type(_))
        ));
    }
}
