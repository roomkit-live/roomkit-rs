//! A standalone channel's callbacks retain its own response record.

mod streamed;

use super::*;
use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelType, EventSource, ResponseMetadata, Room, RoomEvent, RoomId,
    TextContent,
};
use roomkit_ai::{OnToolCancelledOptions, current_response_metadata};

#[derive(Default)]
struct Recording {
    state: ExternalToolHandlerState,
    seen: Mutex<Vec<ResponseMetadata>>,
    recorded: tokio::sync::Notify,
}

impl Recording {
    async fn record(&self) {
        let record = current_response_metadata().expect("the standalone ACP turn's record");
        assert!(record.get("acp").is_some());
        assert_eq!(record.get("parent"), None);
        assert_eq!(record.get("foreign"), None);
        self.seen.lock().unwrap().push(record.clone());
        TurnRecord::default()
            .scope(async {
                current_response_metadata()
                    .unwrap()
                    .insert("nested", json!(true));
            })
            .await;
        current_response_metadata()
            .unwrap()
            .insert("observed", json!(true));
        assert_eq!(record.get("observed"), Some(json!(true)));
        assert_eq!(record.get("nested"), None);
        self.seen
            .lock()
            .unwrap()
            .push(current_response_metadata().unwrap());
        self.recorded.notify_one();
    }
}

impl ExternalToolHandler for Recording {
    fn state(&self) -> &ExternalToolHandlerState {
        &self.state
    }
    fn process_tool_call<'a>(
        &'a self,
        _: &'a str,
        _: &'a Metadata,
        _: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        Box::pin(async { Ok(ToolDecision::new(true)) })
    }
    fn on_tool_result<'a>(
        &'a self,
        _: &'a str,
        _: &'a Metadata,
        _: &'a str,
        _: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.record().await;
            Ok(())
        })
    }
    fn on_tool_cancelled<'a>(
        &'a self,
        _: &'a str,
        _: &'a Metadata,
        _: OnToolCancelledOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            self.record().await;
            Ok(())
        })
    }
}

// Python: tests/test_channels/test_acp_record.py::test_an_acp_callback_keeps_its_record_across_a_nested_local_turn
#[tokio::test]
async fn a_standalone_handler_keeps_its_record_across_a_nested_record_scope() {
    for ending in ["complete", "error", "cancelled"] {
        tokio::time::timeout(PATIENCE, check(ending)).await.unwrap();
    }
}

async fn check(ending: &str) {
    let handler = Arc::new(Recording::default());
    let (channel, mut opened) = channel_with_handler(Arc::<Recording>::clone(&handler));
    let channel = Arc::new(channel);
    let room = RoomId::new("r1").unwrap();
    let binding = ChannelBinding::new(channel.channel_id(), room.clone(), ChannelType::AI);
    let event = RoomEvent::new(
        room.clone(),
        EventSource::new(ChannelId::new("ws").unwrap(), ChannelType::WEBSOCKET),
        TextContent::new("read"),
    );
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let calling = Arc::clone(&channel);
    let running = tokio::spawn(parent.clone().scope(async move {
        calling
            .on_event(event, binding, RoomContext::new(Room::new(room)))
            .await
    }));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = start_tool(&mut agent).await;
    if ending == "complete" {
        let update = json!({"jsonrpc":"2.0","method":"session/update","params":{
            "sessionId":"session-r1","update":{"sessionUpdate":"tool_call_update",
                "toolCallId":"read","status":"completed","rawOutput":"read"}}});
        agent
            .writer
            .write_all(format!("{update}\n").as_bytes())
            .await
            .unwrap();
        agent
            .answer(&prompt, json!({"stopReason":"end_turn"}))
            .await;
    } else if ending == "error" {
        let failure = json!({"jsonrpc":"2.0","id":prompt["id"],
            "error":{"code":-32000,"message":"agent failed"}});
        agent
            .writer
            .write_all(format!("{failure}\n").as_bytes())
            .await
            .unwrap();
    } else {
        agent
            .answer(&prompt, json!({"stopReason":"cancelled"}))
            .await;
    }
    let result = running.await.unwrap();
    let records = handler.seen.lock().unwrap().clone();
    assert_eq!(
        records.len(),
        2,
        "{ending}: the callback completed in its record"
    );
    records[0].insert("identity_check", json!(true));
    assert_eq!(records[1].get("identity_check"), Some(json!(true)));
    if ending == "error" {
        assert!(result.is_err());
    } else {
        let output = result.unwrap();
        records[0].insert("shared", json!(true));
        assert_eq!(output.response_metadata.get("shared"), Some(json!(true)));
    }
    assert_eq!(parent.current().len(), 1);
    assert_eq!(parent.current().get("parent"), Some(json!(true)));
    assert!(current_response_metadata().is_none());
    let closing = channel.close();
    tokio::pin!(closing);
    loop {
        tokio::select! {
            () = &mut closing => break,
            line = agent.lines.next_line() => {
                let Some(line) = line.unwrap() else { closing.await; break; };
                let request: serde_json::Value = serde_json::from_str(&line).unwrap();
                if request.get("id").is_some() { agent.answer(&request, json!({})).await; }
            }
        }
    }
}
