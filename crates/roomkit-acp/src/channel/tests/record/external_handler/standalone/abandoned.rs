//! A handler keeps the ACP record when another task drops the public reader.

use super::*;
use crate::channel::tests::record::{abandoned::Abandon, room_with_transport};

// Python: test_acp_record.py::test_an_acp_callback_keeps_its_record_across_a_nested_local_turn[abandoned].
// Literal chain depth and realtime context clearing are separate contracts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_abandoned_reader_keeps_the_handlers_record_across_a_nested_scope() {
    tokio::time::timeout(PATIENCE, check()).await.unwrap();
}

async fn check() {
    let handler = Arc::new(Recording::default());
    let (channel, mut opened) = channel_with_handler(Arc::<Recording>::clone(&handler));
    let foreign = TurnRecord::default();
    foreign.current().insert("foreign", json!(true));
    let kit = room_with_transport(channel, Abandon::new(foreign.clone())).await;
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.clone().scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let _unanswered = start_tool(&mut agent).await;
    send_anchor(&mut agent).await;
    running.await.unwrap();
    handler.recorded.notified().await;
    close(&kit, &mut agent).await;
    let records = handler.seen.lock().unwrap().clone();
    assert_eq!(records.len(), 2, "before and after the nested scope");
    records[0].insert("identity_check", json!(true));
    assert_eq!(records[1].get("identity_check"), Some(json!(true)));
    assert_eq!(records[0].get("observed"), Some(json!(true)));
    assert_eq!(records[0].get("nested"), None);
    for (record, key) in [(parent, "parent"), (foreign, "foreign")] {
        assert_eq!(record.current().len(), 1);
        assert_eq!(record.current().get(key), Some(json!(true)));
    }
    assert!(current_response_metadata().is_none());
}

async fn send_anchor(agent: &mut Agent) {
    let text = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"session-r1","update":{"sessionUpdate":"agent_message_chunk",
            "content":{"type":"text","text":"Anchor"}}
    }});
    agent
        .writer
        .write_all(format!("{text}\n").as_bytes())
        .await
        .unwrap();
}
