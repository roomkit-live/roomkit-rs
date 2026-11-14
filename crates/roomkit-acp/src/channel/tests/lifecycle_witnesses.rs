//! Direct public calls and repeated session retirement beyond the shared DSL.

mod concurrent;

use std::sync::Arc;

use roomkit::Channel;
use roomkit::model::{ChannelBinding, ChannelOutput, ChannelType, EventType, RoomId};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;

use super::{Agent, PATIENCE, Piped, channel, test_input};
use crate::AcpChannel;

async fn call(channel: &AcpChannel<Piped>, standalone: bool) -> ChannelOutput {
    let mut input = test_input("r1");
    if standalone {
        input.trigger.r#type = EventType::Instruction;
        input
            .trigger
            .metadata
            .insert("standalone".to_owned(), json!(true));
    }
    let binding = ChannelBinding::new(
        channel.channel_id(),
        input.context.room.id.clone(),
        ChannelType::AI,
    );
    channel
        .on_event(input.trigger, binding, input.context)
        .await
        .unwrap()
}

async fn initialize(agent: &mut Agent, closes: bool) {
    let request = agent.read().await;
    assert_eq!(request["method"], "initialize");
    let capabilities = if closes {
        json!({"sessionCapabilities":{"close":{}}})
    } else {
        json!({})
    };
    agent
        .answer(
            &request,
            json!({"protocolVersion":1,"agentCapabilities":capabilities}),
        )
        .await;
}

fn option(id: &str, value: &str) -> Value {
    json!({"id":id,"type":"select","name":id,"currentValue":value,
        "options":[{"value":value,"name":value}]})
}

async fn open(agent: &mut Agent, scope: &str, session: &str) {
    let request = agent.read().await;
    assert_eq!(request["method"], "session/new");
    assert_eq!(
        request["params"]["_meta"]["roomkit.live/sessionScope"],
        scope
    );
    agent
        .answer(
            &request,
            json!({"sessionId":session,
        "configOptions":[option("model","sonnet")]}),
        )
        .await;
}

async fn finish(agent: &mut Agent, session: &str) {
    let request = agent.read().await;
    assert_eq!(request["method"], "session/prompt");
    assert_eq!(request["params"]["sessionId"], session);
    agent
        .answer(&request, json!({"stopReason":"end_turn"}))
        .await;
}

async fn close(agent: &mut Agent, session: &str) {
    let request = agent.read().await;
    assert_eq!(request["method"], "session/close");
    assert_eq!(request["params"]["sessionId"], session);
    agent.answer(&request, json!({})).await;
}

fn assert_forgotten(channel: &AcpChannel<Piped>, room: &RoomId) {
    assert!(channel.session_id(room).is_none());
    assert_eq!(channel.config_options(room), Vec::<Value>::new());
    assert!(channel.session_config(room).is_empty());
    assert!(channel.room_state(room).unwrap().is_empty());
    assert_eq!(channel.info()["session_count"], 0);
    assert_eq!(channel.active_turns(), 0);
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_an_agent_without_session_close_is_never_asked_to_close
#[tokio::test]
async fn test_an_agent_without_session_close_is_never_asked_to_close() {
    let (channel, mut incoming, _) = channel();
    let room = RoomId::new("r1").unwrap();
    let (logs, _capture) = super::session_lifecycle::warnings::Warnings::capture();
    let agent = async {
        let mut agent = Agent::new(incoming.recv().await.unwrap());
        initialize(&mut agent, false).await;
        open(&mut agent, "room", "ordinary").await;
        finish(&mut agent, "ordinary").await;
        agent
    };
    let (output, mut agent) = tokio::join!(call(&channel, false), agent);
    assert_eq!(
        output.response_metadata.to_metadata()["acp"]["prompt_returned"],
        true
    );
    assert!(channel.close_session(&room).await);
    assert_forgotten(&channel, &room);
    let logged = logs.containing("ACP session stays open");
    assert_eq!(logged.len(), 1);
    assert!(logged[0].contains("session=\"ordinary\""));
    assert!(logged[0].contains("does not announce session/close"));
    channel.close().await;
    assert!(
        tokio::time::timeout(PATIENCE, agent.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_closing_a_session_frees_everything_keyed_by_it
#[tokio::test]
async fn test_closing_a_session_frees_everything_keyed_by_it() {
    let (channel, mut incoming, _) = channel();
    let room = RoomId::new("r1").unwrap();
    let remote = tokio::spawn(async move {
        let mut agent = Agent::new(incoming.recv().await.unwrap());
        initialize(&mut agent, true).await;
        for cycle in 0..3 {
            let session = format!("cycle-{cycle}");
            open(&mut agent, "room", &session).await;
            finish(&mut agent, &session).await;
            close(&mut agent, &session).await;
        }
        agent
    });
    for cycle in 0..3 {
        let output = call(&channel, false).await;
        assert_eq!(
            output.response_metadata.to_metadata()["acp"]["prompt_returned"],
            true
        );
        assert_eq!(
            channel.session_id(&room).as_deref(),
            Some(format!("cycle-{cycle}").as_str())
        );
        assert_eq!(channel.session_config(&room)["model"], "sonnet");
        let retired = Arc::downgrade(channel.shared.rooms.lock().unwrap().get(&room).unwrap());
        assert!(channel.close_session(&room).await);
        assert_forgotten(&channel, &room);
        assert!(
            retired.upgrade().is_none(),
            "the old room lock must be released"
        );
    }
    let mut agent = remote.await.unwrap();
    channel.close().await;
    assert!(
        tokio::time::timeout(PATIENCE, agent.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    );
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_default_permission_policy_rejects
#[tokio::test]
async fn test_default_permission_policy_rejects() {
    let (channel, mut incoming, _) = channel();
    let agent = async {
        let mut agent = Agent::new(incoming.recv().await.unwrap());
        initialize(&mut agent, true).await;
        open(&mut agent, "room", "ordinary").await;
        let prompt = agent.read().await;
        assert_eq!(prompt["method"], "session/prompt");
        let options: Vec<_> = ["allow_once", "reject_always", "reject_once"]
            .into_iter()
            .map(|kind| json!({"kind":kind,"name":kind,"optionId":kind.replace('_', "-")}))
            .collect();
        let permission = json!({"jsonrpc":"2.0","id":"permission", "method":"session/request_permission",
            "params":{"sessionId":"ordinary","toolCall":{"toolCallId":"read","title":"Read"},"options":options}});
        agent
            .writer
            .write_all(format!("{permission}\n").as_bytes())
            .await
            .unwrap();
        let answer = agent.read().await;
        assert_eq!(answer["id"], "permission");
        assert_eq!(
            answer["result"]["outcome"],
            json!({"outcome":"selected","optionId":"reject-once"})
        );
        agent
            .answer(&prompt, json!({"stopReason":"end_turn"}))
            .await;
        agent
    };
    let (output, mut agent) = tokio::join!(call(&channel, false), agent);
    assert_eq!(
        output.response_metadata.to_metadata()["acp"]["prompt_returned"],
        true
    );
    tokio::join!(channel.close(), close(&mut agent, "ordinary"));
    assert!(agent.lines.next_line().await.unwrap().is_none());
}
