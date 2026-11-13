//! Standalone ownership boundaries that the shared wire scenarios cannot order.

mod boundary;
mod readers;

use std::sync::Arc;

use roomkit::Channel;
use roomkit::model::{EventType, Metadata, ResponseMetadata, RoomId};
use serde_json::json;

use super::{Agent, Ask, PATIENCE, TurnStream, drained, test_input};
use crate::{AcpChannel, AcpTransport};
use boundary::Boundary;

fn stream<T: AcpTransport>(channel: &AcpChannel<T>) -> (TurnStream<T>, ResponseMetadata) {
    let mut input = test_input("r1");
    input.trigger.r#type = EventType::Instruction;
    input.text = roomkit_core::rules::transcript::mark_instruction("Summarize.");
    let record = crate::channel::turn::response_metadata(true);
    (
        TurnStream::new(
            Arc::clone(&channel.shared),
            Ask {
                standalone: true,
                room_id: RoomId::new("r1").unwrap(),
                event_id: "instruction".to_owned(),
                input,
                record: record.clone(),
                host: None,
                answers_room: true,
            },
        ),
        record,
    )
}

async fn initialized(agent: &mut Agent, closes: bool) {
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

async fn opened(agent: &mut Agent, scope: &str, id: &str) {
    let request = agent.read().await;
    assert_eq!(request["method"], "session/new");
    assert_eq!(
        request["params"]["_meta"]["roomkit.live/sessionScope"],
        scope
    );
    agent.answer(&request, json!({"sessionId":id})).await;
}

async fn closed(agent: &mut Agent, id: &str) {
    let request = agent.read().await;
    assert_eq!(request["method"], "session/close");
    assert_eq!(request["params"]["sessionId"], id);
    agent.answer(&request, json!({})).await;
}

// Python: tests/test_channels/test_acp_session_setup.py::TestACancelReachesATurnWhileItsSessionOpens::test_a_cancel_landing_as_the_opening_ends_closes_the_turn_session
#[tokio::test]
async fn test_a_cancel_landing_as_the_opening_ends_closes_the_turn_session() {
    for at in [Boundary::Opened, Boundary::Configured] {
        tokio::time::timeout(PATIENCE, cancel_at_return(at))
            .await
            .unwrap();
    }
}

async fn cancel_at_return(at: Boundary) {
    let setup = (at == Boundary::Configured)
        .then(|| Metadata::from_iter([("mode".to_owned(), json!("default"))]));
    let (channel, mut incoming, actions) = boundary::channel(setup);
    let room = RoomId::new("r1").unwrap();
    let cancelled = channel.clone();
    actions.at(
        at,
        Box::new(move || {
            Box::pin(async move {
                assert!(cancelled.cancel(&RoomId::new("r1").unwrap()).await.unwrap());
            })
        }),
    );
    let (mut stream, record) = stream(&channel);
    let agent = async {
        let mut agent = Agent::new(incoming.recv().await.unwrap());
        initialized(&mut agent, true).await;
        opened(&mut agent, "turn", "temporary").await;
        if at == Boundary::Configured {
            let setting = agent.read().await;
            assert_eq!(setting["method"], "session/set_config_option");
            agent.answer(&setting, json!({"configOptions":[]})).await;
        }
        closed(&mut agent, "temporary").await;
        agent
    };
    let ((texts, failed), mut agent) = tokio::join!(drained(&mut stream), agent);
    assert!(texts.is_empty() && !failed);
    assert_eq!(record.to_metadata()["acp"]["interrupted"], true);
    assert!(channel.session_id(&room).is_none());
    assert!(!channel.cancel(&room).await.unwrap());
    channel.close().await;
    assert!(agent.lines.next_line().await.unwrap().is_none());
}

// Python: tests/test_channels/test_acp_instruction.py::test_a_tool_setting_config_inside_a_first_standalone_turn_does_not_deadlock
#[tokio::test]
async fn test_a_tool_setting_config_inside_a_first_standalone_turn_does_not_deadlock() {
    tokio::time::timeout(PATIENCE, reentrant_setting())
        .await
        .unwrap();
}

async fn reentrant_setting() {
    let (channel, mut incoming, actions) = boundary::channel(None);
    let setting = channel.clone();
    actions.at(
        Boundary::Prompting,
        Box::new(move || {
            Box::pin(async move {
                setting
                    .set_config_option(&RoomId::new("r1").unwrap(), "model", json!("sonnet"))
                    .await
                    .unwrap();
            })
        }),
    );
    let (mut stream, record) = stream(&channel);
    let agent = async {
        let mut agent = Agent::new(incoming.recv().await.unwrap());
        initialized(&mut agent, true).await;
        opened(&mut agent, "turn", "temporary").await;
        opened(&mut agent, "room", "room").await;
        let setting = agent.read().await;
        assert_eq!(setting["method"], "session/set_config_option");
        assert_eq!(setting["params"]["sessionId"], "room");
        agent.answer(&setting, json!({"configOptions":[]})).await;
        let prompt = agent.read().await;
        assert_eq!(prompt["method"], "session/prompt");
        assert_eq!(prompt["params"]["sessionId"], "temporary");
        agent
            .answer(&prompt, json!({"stopReason":"end_turn"}))
            .await;
        closed(&mut agent, "temporary").await;
        agent
    };
    let ((_, failed), mut agent) = tokio::join!(drained(&mut stream), agent);
    assert!(!failed);
    assert!(
        !serde_json::to_string(&record.to_metadata())
            .unwrap()
            .contains("Summarize.")
    );
    assert_eq!(
        channel.session_id(&RoomId::new("r1").unwrap()).as_deref(),
        Some("room")
    );
    tokio::join!(channel.close(), closed(&mut agent, "room"));
}

// Python: tests/test_channels/test_acp_instruction.py::test_a_standalone_turn_on_an_agent_without_session_close_is_never_prompted_again
#[tokio::test]
async fn test_a_standalone_turn_on_an_agent_without_session_close_is_never_prompted_again() {
    use super::session_lifecycle::warnings::Warnings;
    let (logs, _capture) = Warnings::capture();
    let (channel, mut incoming, _) = super::channel();
    let (mut stream, _) = stream(&channel);
    let agent = async {
        let mut agent = Agent::new(incoming.recv().await.unwrap());
        initialized(&mut agent, false).await;
        opened(&mut agent, "turn", "temporary").await;
        let prompt = agent.read().await;
        assert_eq!(prompt["method"], "session/prompt");
        agent
            .answer(&prompt, json!({"stopReason":"end_turn"}))
            .await;
        agent
    };
    let ((_, failed), mut agent) = tokio::time::timeout(PATIENCE, async {
        tokio::join!(drained(&mut stream), agent)
    })
    .await
    .unwrap();
    assert!(!failed);
    assert!(channel.session_id(&RoomId::new("r1").unwrap()).is_none());
    assert_eq!(
        logs.containing("ACP standalone turn session stays open")
            .len(),
        1
    );
    channel.close().await;
    assert!(agent.lines.next_line().await.unwrap().is_none());
}
