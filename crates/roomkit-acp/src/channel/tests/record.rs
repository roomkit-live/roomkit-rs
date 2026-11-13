//! ACP observers keep their turn's record across nested scopes and termination.

mod abandoned;
mod external_handler;

use super::{Agent, PATIENCE, channel};
use roomkit::model::{
    ChannelId, ChannelType, InboundMessage, ResponseMetadata, RoomContext, RoomId, TextContent,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, CreateRoomOptions, HookExecution, HookRegistration, HookTrigger,
    InboundResult, ProcessInboundOptions, RoomKit, RoomKitOptions, ToolCallEvent, TurnRecord,
};
use roomkit_ai::{AiResponseEvent, current_response_metadata};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

type Observed = (&'static str, ResponseMetadata);

async fn room(channel: impl Channel) -> Arc<RoomKit> {
    room_with_transport(
        channel,
        RecordingTransport::new(
            ChannelId::new("ws").unwrap(),
            ChannelType::WEBSOCKET,
            RecordingTransportOptions::default(),
        ),
    )
    .await
}

async fn room_with_transport(channel: impl Channel, transport: impl Channel) -> Arc<RoomKit> {
    let kit = Arc::new(RoomKit::new(RoomKitOptions::default()));
    kit.register_channel(channel).unwrap();
    kit.register_channel(transport).unwrap();
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for name in ["ws", "claude"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(name).unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    kit
}

fn observe_tools(kit: &RoomKit, sent: mpsc::UnboundedSender<Observed>) {
    kit.turn_executor()
        .hooks()
        .register(HookRegistration::new(
            HookTrigger::OnToolCall,
            HookExecution::Async,
            move |_event: ToolCallEvent, _context: RoomContext| {
                let sent = sent.clone();
                async move {
                    let record = current_response_metadata().expect("ACP tool's record");
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
                    sent.send(("tool", record)).unwrap();
                }
            },
        ))
        .unwrap();
}

fn observe(kit: &RoomKit) -> mpsc::UnboundedReceiver<Observed> {
    let (sent, received) = mpsc::unbounded_channel();
    observe_tools(kit, sent.clone());
    kit.turn_executor()
        .hooks()
        .register(HookRegistration::new(
            HookTrigger::OnAiResponse,
            HookExecution::Async,
            move |_event: AiResponseEvent, _context: RoomContext| {
                let sent = sent.clone();
                async move {
                    sent.send((
                        "response",
                        current_response_metadata().expect("ACP response's record"),
                    ))
                    .unwrap();
                }
            },
        ))
        .unwrap();
    received
}

async fn start_tool(agent: &mut Agent) -> Value {
    let prompt = agent.prompted(true).await;
    let notification = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"session-r1","update":{
            "sessionUpdate":"tool_call","toolCallId":"read","title":"Read","status":"in_progress"
        }
    }});
    agent
        .writer
        .write_all(format!("{notification}\n").as_bytes())
        .await
        .unwrap();
    prompt
}

async fn answer(agent: &mut Agent, ending: &str) {
    let prompt = start_tool(agent).await;
    if ending == "error" {
        let failure = json!({"jsonrpc":"2.0","id":prompt["id"],"error":{"code":-32000,"message":"agent failed"}});
        agent
            .writer
            .write_all(format!("{failure}\n").as_bytes())
            .await
            .unwrap();
    } else {
        agent.answer(&prompt, json!({"stopReason":ending})).await;
    }
}

async fn close(kit: &RoomKit, agent: &mut Agent) {
    let closing = kit.close();
    tokio::pin!(closing);
    loop {
        tokio::select! {
            result = &mut closing => { result.unwrap(); return; }
            line = agent.lines.next_line() => {
                let Some(line) = line.unwrap() else {
                    closing.await.unwrap();
                    return;
                };
                let request: Value = serde_json::from_str(&line).unwrap();
                if request.get("id").is_some() {
                    agent.answer(&request, json!({})).await;
                }
            }
        }
    }
}

async fn send(kit: Arc<RoomKit>) -> InboundResult {
    let result = kit
        .process_inbound(
            InboundMessage::new(
                ChannelId::new("ws").unwrap(),
                "alice",
                TextContent::new("read"),
            ),
            ProcessInboundOptions {
                room_id: Some(RoomId::new("r1").unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let parent = current_response_metadata().unwrap().to_metadata();
    assert_eq!(parent.len(), 1);
    assert_eq!(parent.get("parent"), Some(&json!(true)));
    result
}

async fn check_record(ending: &'static str) {
    let (channel, mut agents, _) = channel();
    let kit = room(channel).await;
    let mut observed = observe(&kit);
    let parent = TurnRecord::default();
    parent.current().insert("parent", json!(true));
    let running = tokio::spawn(parent.clone().scope(send(Arc::clone(&kit))));
    let mut agent = Agent::new(agents.recv().await.unwrap());
    answer(&mut agent, ending).await;
    let result = tokio::time::timeout(PATIENCE, running)
        .await
        .unwrap()
        .unwrap();
    check_observed(&mut observed, ending != "error").await;
    assert_eq!(parent.current().get("observed"), None);
    assert!(current_response_metadata().is_none());
    assert_eq!(result.error.is_some(), ending == "error");
    close(&kit, &mut agent).await;
}

async fn check_observed(observed: &mut mpsc::UnboundedReceiver<Observed>, completed: bool) {
    let mut records = BTreeMap::new();
    for _ in 0..if completed { 2 } else { 1 } {
        let (kind, record) = tokio::time::timeout(PATIENCE, observed.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(records.insert(kind, record).is_none());
    }
    let record = &records["tool"];
    assert_eq!(record.get("observed"), Some(json!(true)));
    if completed {
        record.insert("late_identity_check", json!(true));
        assert_eq!(
            records["response"].get("late_identity_check"),
            Some(json!(true))
        );
    }
}

// Python: tests/test_channels/test_acp_record.py::test_an_acp_callback_keeps_its_record_across_a_nested_local_turn
// and test_the_acp_response_observer_reads_the_record_its_tool_hook_wrote.
#[tokio::test]
async fn an_acp_callback_keeps_its_record_across_a_nested_local_turn() {
    for ending in ["end_turn", "cancelled", "error"] {
        tokio::time::timeout(PATIENCE, check_record(ending))
            .await
            .unwrap();
    }
}
