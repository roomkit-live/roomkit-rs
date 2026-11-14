//! Poll the actual ACP work while session/new is unanswered. A background
//! owner call alone does not prove its executor reached the opening lock.

mod concurrent_close;

use std::pin::pin;

use roomkit::Channel;
use roomkit::model::{Metadata, RoomId};
use serde_json::{Value, json};

use super::session_lifecycle::pending;
use super::{Agent, PATIENCE, Piped, channel, drained, poll_once, turn};
use crate::AcpChannel;

fn options(model: &str) -> Value {
    json!([{"id":"model","type":"select","name":"Model","currentValue":model,
        "options":[{"value":"opus","name":"Opus"},{"value":"sonnet","name":"Sonnet"}]}])
}

/// Establish the connection first, so the second caller's first poll can
/// only wait for the first caller's room opening, not for the handshake.
async fn connected() -> (AcpChannel<Piped>, Agent) {
    let (channel, mut agents, _) = channel();
    let connecting = channel.shared.connector.connection();
    let agent = async {
        let mut agent = Agent::new(agents.recv().await.unwrap());
        let request = agent.read().await;
        assert_eq!(request["method"], "initialize");
        agent
            .answer(
                &request,
                json!({"protocolVersion":1,
            "agentCapabilities":{"sessionCapabilities":{"close":{}}}}),
            )
            .await;
        agent
    };
    let (connection, agent) = tokio::join!(connecting, agent);
    drop(connection.unwrap());
    (channel, agent)
}

async fn opened(agent: &mut Agent, request: &Value) {
    assert_eq!(request["method"], "session/new");
    agent
        .answer(
            request,
            json!({"sessionId":"s1","configOptions":options("opus")}),
        )
        .await;
}

async fn setting(agent: &mut Agent, model: &str) {
    let request = agent.read().await;
    assert_eq!(request["method"], "session/set_config_option");
    assert_eq!(request["params"]["sessionId"], "s1");
    assert_eq!(request["params"]["value"], model);
    agent
        .answer(&request, json!({"configOptions":options(model)}))
        .await;
}

async fn closing(agent: &mut Agent) {
    let request = agent.read().await;
    assert_eq!(request["method"], "session/close");
    assert_eq!(request["params"]["sessionId"], "s1");
    agent.answer(&request, json!({})).await;
}

async fn close(channel: &AcpChannel<Piped>, agent: &mut Agent) {
    tokio::join!(channel.close(), closing(agent));
}

/// Once the opening is answered, the prompt and the setter may arrive in
/// either order; both must use it, with no second session/new.
async fn prompt_and_setting(agent: &mut Agent, opening: &Value) {
    opened(agent, opening).await;
    let mut seen = Vec::new();
    for _ in 0..2 {
        let request = agent.read().await;
        assert_eq!(request["params"]["sessionId"], "s1");
        let method = request["method"].as_str().unwrap().to_owned();
        let answer = match method.as_str() {
            "session/prompt" => json!({"stopReason":"end_turn"}),
            "session/set_config_option" => {
                assert_eq!(request["params"]["value"], "sonnet");
                json!({"configOptions":options("sonnet")})
            }
            other => panic!("an opening was already in flight, got {other}"),
        };
        seen.push(method);
        agent.answer(&request, answer).await;
    }
    seen.sort();
    assert_eq!(seen, ["session/prompt", "session/set_config_option"]);
}

async fn one_setting_and_prompt(setter_first: bool) {
    let (channel, mut agent) = connected().await;
    let room = RoomId::new("r1").unwrap();
    let mut setter = pin!(channel.set_config_option(&room, "model", json!("sonnet")));
    let (mut stream, _) = turn(&channel, "r1");
    if setter_first {
        pending(setter.as_mut()).await;
    } else {
        poll_once(&mut stream).await;
    }
    let opening = agent.read().await;
    assert_eq!(opening["method"], "session/new");
    if setter_first {
        poll_once(&mut stream).await;
    } else {
        pending(setter.as_mut()).await;
    }
    let (values, (_, failed), ()) = tokio::join!(
        setter,
        drained(&mut stream),
        prompt_and_setting(&mut agent, &opening)
    );
    assert!(!failed);
    assert_eq!(
        values.unwrap(),
        Metadata::from_iter([("model".to_owned(), json!("sonnet"))])
    );
    assert_eq!(channel.session_id(&room).as_deref(), Some("s1"));
    close(&channel, &mut agent).await;
}

// Python: tests/test_channels/test_acp_session_setup.py::TestOneOpeningAtATime::test_a_host_call_and_a_first_prompt_open_one_session
#[tokio::test]
async fn test_a_host_call_and_a_first_prompt_open_one_session() {
    tokio::time::timeout(PATIENCE, one_setting_and_prompt(true))
        .await
        .unwrap();
}

// The reverse ordering of the same Python contract, before the opening completes.
#[tokio::test]
async fn test_a_first_prompt_and_a_host_call_open_one_session() {
    tokio::time::timeout(PATIENCE, one_setting_and_prompt(false))
        .await
        .unwrap();
}

// Python: tests/test_channels/test_acp_session_setup.py::TestOneOpeningAtATime
// The room opening contract with two setters; standalone remains a separate lot.
#[tokio::test]
async fn test_two_host_calls_open_one_room_session() {
    tokio::time::timeout(PATIENCE, async {
        let (channel, mut agent) = connected().await;
        let room = RoomId::new("r1").unwrap();
        let mut first = pin!(channel.set_config_option(&room, "model", json!("sonnet")));
        pending(first.as_mut()).await;
        let opening = agent.read().await;
        let mut second = pin!(channel.set_config_option(&room, "model", json!("opus")));
        pending(second.as_mut()).await;
        let answer = async {
            opened(&mut agent, &opening).await;
            setting(&mut agent, "sonnet").await;
            setting(&mut agent, "opus").await;
        };
        let (first, second, ()) = tokio::join!(first, second, answer);
        assert_eq!(first.unwrap().get("model"), Some(&json!("sonnet")));
        assert_eq!(second.unwrap().get("model"), Some(&json!("opus")));
        assert_eq!(
            channel.session_config(&room).get("model"),
            Some(&json!("opus"))
        );
        close(&channel, &mut agent).await;
    })
    .await
    .unwrap();
}

// Python: tests/test_channels/test_acp_session_setup.py::TestOneOpeningAtATime::test_close_session_waits_for_an_opening_then_closes_it
#[tokio::test]
async fn test_close_session_waits_for_an_opening_then_closes_it() {
    tokio::time::timeout(PATIENCE, async {
        let (channel, mut agent) = connected().await;
        let room = RoomId::new("r1").unwrap();
        let mut setter = pin!(channel.set_config_option(&room, "model", json!("sonnet")));
        pending(setter.as_mut()).await;
        let opening = agent.read().await;
        let mut closer = pin!(channel.close_session(&room));
        pending(closer.as_mut()).await;
        let answer = async {
            opened(&mut agent, &opening).await;
            setting(&mut agent, "sonnet").await;
            closing(&mut agent).await;
        };
        let (set, had_session, ()) = tokio::join!(setter, closer, answer);
        assert_eq!(set.unwrap().get("model"), Some(&json!("sonnet")));
        assert!(had_session);
        assert!(channel.session_id(&room).is_none());
        assert_eq!(channel.room_state(&room).unwrap(), Metadata::new());
        assert!(channel.shared.rooms.lock().unwrap().is_empty());
        channel.close().await;
    })
    .await
    .unwrap();
}
