//! Both actual setters wait for one opening while a standalone prompt is held.

use std::pin::pin;

use roomkit::Channel;
use roomkit::model::RoomId;
use serde_json::{Value, json};

use super::super::session_lifecycle::pending;
use super::{Agent, PATIENCE, call, channel, close, finish, initialize, open, option};

async fn settings(agent: &mut Agent, opening: &Value) {
    agent.answer(opening, json!({"sessionId":"room"})).await;
    let mut options = Vec::new();
    for (id, value) in [("model", "sonnet"), ("mode", "default")] {
        let request = agent.read().await;
        assert_eq!(request["method"], "session/set_config_option");
        assert_eq!(request["params"]["sessionId"], "room");
        assert_eq!(request["params"]["configId"], id);
        assert_eq!(request["params"]["value"], value);
        options.push(option(id, value));
        agent
            .answer(&request, json!({"configOptions":options}))
            .await;
    }
}

// Python: tests/test_channels/test_acp_session_setup.py::TestOneOpeningAtATime::test_two_host_calls_during_a_standalone_turn_open_one_session
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_two_host_calls_during_a_standalone_turn_open_one_session() {
    tokio::time::timeout(PATIENCE, Box::pin(check()))
        .await
        .unwrap();
}

async fn check() {
    let (channel, mut incoming, _) = channel();
    let room = RoomId::new("r1").unwrap();
    let running_channel = channel.clone();
    let running = tokio::spawn(async move { call(&running_channel, true).await });
    let mut agent = Agent::new(incoming.recv().await.unwrap());
    initialize(&mut agent, true).await;
    open(&mut agent, "turn", "temporary").await;
    let prompt = agent.read().await;
    assert_eq!(prompt["method"], "session/prompt");
    assert_eq!(prompt["params"]["sessionId"], "temporary");
    let mut model = pin!(channel.set_config_option(&room, "model", json!("sonnet")));
    pending(model.as_mut()).await;
    let opening = agent.read().await;
    assert_eq!(opening["method"], "session/new");
    assert_eq!(
        opening["params"]["_meta"]["roomkit.live/sessionScope"],
        "room"
    );
    let mut mode = pin!(channel.set_config_option(&room, "mode", json!("default")));
    pending(mode.as_mut()).await;
    let (model, mode, ()) = tokio::join!(model, mode, settings(&mut agent, &opening));
    assert_eq!(model.unwrap()["model"], "sonnet");
    assert_eq!(mode.unwrap()["mode"], "default");
    assert_eq!(
        json!(channel.session_config(&room)),
        json!({"model":"sonnet","mode":"default"})
    );
    assert!(
        !running.is_finished(),
        "both setters finish before the standalone prompt"
    );
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    close(&mut agent, "temporary").await;
    assert_eq!(
        running.await.unwrap().response_metadata.to_metadata()["acp"]["standalone"],
        true
    );
    assert_eq!(channel.session_id(&room).as_deref(), Some("room"));
    assert_eq!(channel.info()["session_count"], 1);
    let (output, ()) = tokio::join!(call(&channel, false), finish(&mut agent, "room"));
    assert_eq!(
        output.response_metadata.to_metadata()["acp"]["prompt_returned"],
        true
    );
    tokio::join!(channel.close(), close(&mut agent, "room"));
    assert!(agent.lines.next_line().await.unwrap().is_none());
}
