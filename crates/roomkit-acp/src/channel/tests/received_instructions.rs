//! A received prompt stays acknowledged when its reader stops before another poll.
use super::{
    AcpChannel, AcpChannelOptions, Agent, Ask, PATIENCE, Piped, TurnStream, drained, poll_once,
    test_input, turn, while_agent,
};
use futures_core::Stream;
use roomkit::model::{ChannelId, RoomId};
use roomkit::{Channel, ResponseItem};
use serde_json::json;
use std::{future::poll_fn, pin::Pin, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

// Python: tests/test_channels/test_acp_received_instructions.py::test_received_instructions_survive_abandonment_before_terminal_read
#[tokio::test]
async fn received_instructions_survive_abandonment_before_terminal_read() {
    tokio::time::timeout(PATIENCE, check_received())
        .await
        .unwrap();
}

async fn check_received() {
    let (agents, mut incoming) = mpsc::unbounded_channel();
    let channel = AcpChannel::new(
        ChannelId::new("claude").unwrap(),
        "/srv",
        AcpChannelOptions {
            transport: Some(Piped {
                agents,
                closed: Arc::default(),
                log: Arc::default(),
            }),
            instructions: Some("You are Dev.".to_owned()),
            ..Default::default()
        },
    )
    .unwrap();
    let (mut first, record) = turn(&channel, "room-1");
    poll_once(&mut first).await;
    let mut agent = Agent::new(incoming.recv().await.unwrap());
    let request = while_agent(&mut first, agent.prompted(true)).await;
    assert!(
        request["params"]["prompt"][0]["text"]
            .as_str()
            .unwrap()
            .contains("You are Dev.")
    );
    let update = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":request["params"]["sessionId"],"update":{
            "sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"partial"}}}});
    agent
        .writer
        .write_all(format!("{update}\n").as_bytes())
        .await
        .unwrap();
    let item = poll_fn(|cx| Pin::new(&mut first).poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item,ResponseItem::Text(text) if text=="partial"));
    agent
        .answer(&request, json!({"stopReason":"end_turn"}))
        .await;
    // This is set by the production prompt task after receiving its RPC result.
    // Do not poll the response stream again before dropping it.
    while record.to_metadata()["acp"].get("prompt_returned") != Some(&json!(true)) {
        tokio::task::yield_now().await;
    }
    drop(first);
    assert_eq!(
        channel
            .session_id(&RoomId::new("room-1").unwrap())
            .as_deref(),
        request["params"]["sessionId"].as_str()
    );
    let mut input = test_input("room-1");
    let old = test_input("room-1").trigger;
    input.trigger.index = 1;
    input.text = "next".to_owned();
    input.context.recent_events = vec![old, input.trigger.clone()];
    let next_record = roomkit::model::ResponseMetadata::new();
    next_record.insert("acp", json!({"protocol_version":1}));
    let mut next = TurnStream::new(
        Arc::clone(&channel.shared),
        Ask {
            room_id: RoomId::new("room-1").unwrap(),
            event_id: "next".to_owned(),
            input,
            record: next_record,
            host: None,
            answers_room: true,
        },
    );
    let second = while_agent(&mut next, agent.read()).await;
    assert_eq!(second["method"], "session/prompt");
    assert_eq!(
        second["params"]["sessionId"],
        request["params"]["sessionId"]
    );
    agent
        .answer(&second, json!({"stopReason":"end_turn"}))
        .await;
    assert!(!drained(&mut next).await.1);
    let closing = channel.close();
    let agent_close = async {
        let close = agent.read().await;
        assert_eq!(close["method"], "session/close");
        agent.answer(&close, json!({})).await;
    };
    tokio::join!(closing, agent_close);
    assert_eq!(
        second["params"]["prompt"][0]["text"], "next",
        "a normally returned RPC already acknowledged standing instructions"
    );
}
