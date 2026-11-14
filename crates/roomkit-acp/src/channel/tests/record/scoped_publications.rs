//! Python: `test_acp_publication_scope.py::test_a_callback_child_keeps_its_turns_narrow_answer_scope`.

use super::*;
use roomkit::model::Visibility;
use roomkit::realtime::EphemeralEventType;
use std::sync::Mutex;
use tokio::sync::Notify;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_a_callback_child_keeps_its_turns_narrow_answer_scope() {
    tokio::time::timeout(PATIENCE, check()).await.unwrap();
}

async fn check() {
    let (channel, mut agents, _) = channel();
    let kit = room(channel.clone()).await;
    let notices = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&notices);
    kit.subscribe_room(
        &RoomId::new("r1").unwrap(),
        move |event| {
            let seen = Arc::clone(&seen);
            async move {
                if event.r#type == EphemeralEventType::Custom {
                    seen.lock().unwrap().push(event);
                }
            }
        },
        roomkit::SubscribeRoomOptions::default(),
    )
    .await
    .unwrap();
    let changed = Arc::new(Notify::new());
    let done = Arc::clone(&changed);
    kit.turn_executor()
        .hooks()
        .register(HookRegistration::new(
            HookTrigger::OnToolCall,
            HookExecution::Async,
            move |_: ToolCallEvent, _: RoomContext| {
                let channel = channel.clone();
                let done = Arc::clone(&done);
                async move {
                    let values = tokio::spawn(roomkit::keep_context(async move {
                        channel
                            .set_config_option(
                                &RoomId::new("r1").unwrap(),
                                "model",
                                json!("sonnet"),
                            )
                            .await
                            .unwrap()
                    }))
                    .await
                    .unwrap();
                    assert_eq!(values["model"], "sonnet");
                    done.notify_one();
                }
            },
        ))
        .unwrap();
    let running = Arc::clone(&kit);
    let processing = tokio::spawn(async move {
        let mut message = InboundMessage::new(
            ChannelId::new("ws").unwrap(),
            "alice",
            TextContent::new("go"),
        );
        message.response_visibility = Some(Visibility::Intelligence);
        running
            .process_inbound(
                message,
                ProcessInboundOptions {
                    room_id: Some(RoomId::new("r1").unwrap()),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
    });
    let mut agent = Agent::new(agents.recv().await.unwrap());
    let prompt = start_tool(&mut agent).await;
    let end = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":"session-r1", "update":{"sessionUpdate":"tool_call_update","toolCallId":"read","status":"completed","rawOutput":"done"}
    }});
    agent
        .writer
        .write_all(format!("{end}\n").as_bytes())
        .await
        .unwrap();
    let setter = agent.read().await;
    assert_eq!(setter["method"], "session/set_config_option");
    agent.answer(&setter, json!({"configOptions":[{"id":"model","name":"Model","type":"select","currentValue":"sonnet","options":[{"name":"Sonnet","value":"sonnet"}]}]})).await;
    changed.notified().await;
    agent
        .answer(&prompt, json!({"stopReason":"end_turn"}))
        .await;
    assert!(processing.await.unwrap().error.is_none());
    close(&kit, &mut agent).await;
    assert!(
        notices.lock().unwrap().is_empty(),
        "{:?}",
        notices.lock().unwrap()
    );
}
