//! Permission and tool callbacks publish through their own room's host.

use super::*;
use roomkit::model::{InboundMessage, RoomId, TextContent};
use roomkit::orchestration::{RecentOptions, StatusLevel};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, PostStatusOptions, ProcessInboundOptions, RoomKit,
    current_turn_host,
};

#[derive(Default)]
struct Posting(PolicyExternalToolHandler);

impl ExternalToolHandler for Posting {
    fn state(&self) -> &ExternalToolHandlerState {
        self.0.state()
    }

    fn process_tool_call<'a>(
        &'a self,
        _: &'a str,
        _: &'a Metadata,
        options: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        Box::pin(async move {
            post("permission", options.room_id.unwrap().as_str()).await;
            Ok(ToolDecision::new(true))
        })
    }

    fn on_tool_result<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        result: &'a str,
        options: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        self.0.on_tool_result(name, input, result, options)
    }
}

async fn post(action: &str, room: &str) {
    current_turn_host()
        .expect("a callback's room host")
        .post_status(
            "claude",
            action,
            StatusLevel::Ok,
            PostStatusOptions {
                detail: room.to_owned(),
                ..PostStatusOptions::default()
            },
        )
        .await
        .unwrap();
}

async fn two_rooms() -> (
    Arc<RoomKit>,
    mpsc::UnboundedReceiver<tokio::io::DuplexStream>,
) {
    let (channel, opened) = channel_with_handler(Arc::new(Posting::default()));
    let kit = room(channel).await;
    let r2 = RoomId::new("r2").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(r2.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    for id in ["ws", "claude"] {
        kit.attach_channel(
            &r2,
            &ChannelId::new(id).unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    kit.turn_executor()
        .hooks()
        .register(HookRegistration::new(
            HookTrigger::OnToolCall,
            HookExecution::Async,
            |event: ToolCallEvent, _: RoomContext| async move {
                post("tool", event.room_id.unwrap().as_str()).await;
            },
        ))
        .unwrap();
    (kit, opened)
}

async fn send_room(kit: Arc<RoomKit>, room: &'static str) {
    let result = kit
        .process_inbound(
            InboundMessage::new(
                ChannelId::new("ws").unwrap(),
                "alice",
                TextContent::new("read"),
            ),
            ProcessInboundOptions {
                room_id: Some(RoomId::new(room).unwrap()),
                ..ProcessInboundOptions::default()
            },
        )
        .await
        .unwrap();
    assert!(result.error.is_none());
}

async fn permit_and_finish(agent: &mut Agent, prompt: &serde_json::Value) {
    let session = &prompt["params"]["sessionId"];
    let permission = json!({"jsonrpc":"2.0","id":"permission","method":"session/request_permission","params":{
        "sessionId":session,"toolCall":{"toolCallId":"read","title":"Read","rawInput":{}},
        "options":[{"optionId":"a","name":"Allow","kind":"allow_once"}]}});
    agent
        .writer
        .write_all(format!("{permission}\n").as_bytes())
        .await
        .unwrap();
    let reply = agent.read().await;
    assert_eq!(reply["id"], "permission");
    assert_eq!(reply["result"]["outcome"]["optionId"], "a");
    let update = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":session,"update":{"sessionUpdate":"tool_call_update", "toolCallId":"read",
            "status":"completed","rawOutput":"read"}}});
    agent
        .writer
        .write_all(format!("{update}\n").as_bytes())
        .await
        .unwrap();
    agent.answer(prompt, json!({"stopReason":"end_turn"})).await;
}

// Python: test_acp_backend::test_each_room_s_tool_hooks_and_permissions_see_that_room_s_host.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_each_room_s_tool_hooks_and_permissions_see_that_room_s_host() {
    tokio::time::timeout(PATIENCE, check()).await.unwrap();
}

async fn check() {
    let (kit, mut opened) = two_rooms().await;
    let first = tokio::spawn(send_room(Arc::clone(&kit), "r1"));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt = agent.prompted(true).await;
    permit_and_finish(&mut agent, &prompt).await;
    first.await.unwrap();
    let second = tokio::spawn(send_room(Arc::clone(&kit), "r2"));
    let prompt = agent.prompted(false).await;
    permit_and_finish(&mut agent, &prompt).await;
    second.await.unwrap();
    let mut posted = posted(&kit).await;
    posted.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    assert_eq!(
        posted,
        vec![
            ("permission".to_owned(), "r1".to_owned(), Some(json!("r1"))),
            ("permission".to_owned(), "r2".to_owned(), Some(json!("r2"))),
            ("tool".to_owned(), "r1".to_owned(), Some(json!("r1"))),
            ("tool".to_owned(), "r2".to_owned(), Some(json!("r2"))),
        ]
    );
    close(&kit, &mut agent).await;
}

async fn posted(kit: &RoomKit) -> Vec<(String, String, Option<serde_json::Value>)> {
    loop {
        let entries = kit
            .status_bus()
            .recent(RecentOptions {
                n: 20,
                ..RecentOptions::default()
            })
            .await;
        if entries.len() >= 4 {
            break entries
                .into_iter()
                .map(|entry| {
                    (
                        entry.action,
                        entry.detail,
                        entry.metadata.get("room_id").cloned(),
                    )
                })
                .collect::<Vec<_>>();
        }
        tokio::task::yield_now().await;
    }
}
