//! A response reader can disappear during setup or after the RPC returned.

use std::future::poll_fn;
use std::pin::Pin;

use futures_core::Stream;
use roomkit::Channel;
use roomkit::model::{Metadata, RoomId};
use serde_json::json;
use tokio::io::AsyncWriteExt;

use super::super::{Agent, PATIENCE, drained, poll_once, turn, while_agent};
use super::{boundary, closed, initialized, opened, stream};

// Python: tests/test_channels/test_acp_instruction.py::test_a_standalone_turn_interrupted_during_setup_leaves_no_session
#[tokio::test]
async fn test_a_standalone_turn_interrupted_during_setup_leaves_no_session() {
    tokio::time::timeout(PATIENCE, abandon_setup())
        .await
        .unwrap();
}

async fn abandon_setup() {
    let setup = Metadata::from_iter([("mode".to_owned(), json!("default"))]);
    let (channel, mut incoming, _) = boundary::channel(Some(setup));
    let (mut stream, _) = stream(&channel);
    poll_once(&mut stream).await;
    let mut agent = Agent::new(incoming.recv().await.unwrap());
    let setting = while_agent(&mut stream, async {
        initialized(&mut agent, true).await;
        opened(&mut agent, "turn", "temporary").await;
        agent.read().await
    })
    .await;
    assert_eq!(setting["method"], "session/set_config_option");
    // The stream owns a session, but no prompt has been sent. Closing it
    // must release that session without sending a prompt cancellation.
    drop(stream);
    closed(&mut agent, "temporary").await;
    let room = RoomId::new("r1").unwrap();
    assert!(channel.session_id(&room).is_none());
    assert!(!channel.cancel(&room).await.unwrap());
    channel.close().await;
    assert!(agent.lines.next_line().await.unwrap().is_none());
}

// Python: tests/test_channels/test_acp_standalone_lifecycle.py::test_a_received_standalone_reply_is_released_when_its_reader_is_abandoned
#[tokio::test]
async fn test_a_received_standalone_reply_is_released_when_its_reader_is_abandoned() {
    tokio::time::timeout(PATIENCE, abandon_received())
        .await
        .unwrap();
}

async fn abandon_received() {
    let (channel, mut incoming, _) = super::super::channel();
    let (mut stream, record) = stream(&channel);
    poll_once(&mut stream).await;
    let mut agent = Agent::new(incoming.recv().await.unwrap());
    let prompt = while_agent(&mut stream, async {
        initialized(&mut agent, true).await;
        opened(&mut agent, "turn", "temporary").await;
        agent.read().await
    })
    .await;
    assert_eq!(prompt["method"], "session/prompt");
    let update = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"temporary","update":{"sessionUpdate":"agent_message_chunk",
        "content":{"type":"text","text":"partial"}}}});
    agent
        .writer
        .write_all(format!("{update}\n").as_bytes())
        .await
        .unwrap();
    let item = poll_fn(|cx| Pin::new(&mut stream).poll_next(cx))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item, roomkit::ResponseItem::Text(text) if text == "partial"));
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    while record.to_metadata()["acp"].get("prompt_returned") != Some(&json!(true)) {
        tokio::task::yield_now().await;
    }
    // No response-reader poll after the prompt's normal return.
    drop(stream);
    closed(&mut agent, "temporary").await;
    assert!(channel.session_id(&RoomId::new("r1").unwrap()).is_none());
    assert_eq!(record.to_metadata()["acp"]["standalone"], true);
    let (mut next, _) = turn(&channel, "r1");
    let prompt = while_agent(&mut next, agent.prompted(false)).await;
    assert_eq!(prompt["params"]["sessionId"], "session-r1");
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    assert!(!drained(&mut next).await.1);
    tokio::join!(channel.close(), closed(&mut agent, "session-r1"));
}
