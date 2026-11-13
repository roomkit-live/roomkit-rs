//! Identical provider call ids in concurrent rooms have independent report witnesses.

use roomkit::model::{InboundMessage, RoomId, TextContent};
use roomkit::{AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions};
use tokio::sync::Notify;

use super::*;

#[derive(Default)]
struct Reports {
    policy: PolicyExternalToolHandler,
    first_reported: Notify,
    second_reporting: Notify,
}

impl ExternalToolHandler for Reports {
    fn state(&self) -> &ExternalToolHandlerState {
        self.policy.state()
    }
    fn process_tool_call<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        options: ProcessToolCallOptions,
    ) -> ExternalToolFuture<'a, ToolDecision> {
        self.policy.process_tool_call(name, input, options)
    }
    fn on_tool_result<'a>(
        &'a self,
        name: &'a str,
        input: &'a Metadata,
        result: &'a str,
        options: OnToolResultOptions,
    ) -> ExternalToolFuture<'a, ()> {
        Box::pin(async move {
            if options.room_id.as_ref().unwrap().as_str() == "r1" {
                self.policy
                    .on_tool_result(name, input, result, options)
                    .await?;
                self.first_reported.notify_one();
                self.second_reporting.notified().await;
            } else {
                self.second_reporting.notify_one();
            }
            Err(ToolError::Failed("audit sink down".to_owned()))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_handler_reports_with_the_same_id_have_separate_witnesses() {
    tokio::time::timeout(PATIENCE, concurrent_reports())
        .await
        .unwrap();
}

async fn concurrent_reports() {
    let handler = Arc::new(Reports::default());
    let (channel, mut opened) = channel_with_handler(Arc::<Reports>::clone(&handler));
    let kit = room(channel).await;
    let r2 = RoomId::new("r2").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(r2.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for channel in ["ws", "claude"] {
        kit.attach_channel(
            &r2,
            &ChannelId::new(channel).unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    let reports = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&reports);
    kit.turn_executor()
        .hooks()
        .register(HookRegistration::new(
            HookTrigger::OnToolCall,
            HookExecution::Async,
            move |event: ToolCallEvent, _: RoomContext| {
                observed.lock().unwrap().push(event);
                async {}
            },
        ))
        .unwrap();
    let first = tokio::spawn(send_room(Arc::clone(&kit), "r1"));
    let mut agent = Agent::new(opened.recv().await.unwrap());
    let prompt1 = agent.prompted(true).await;
    completed(&mut agent, "session-r1").await;
    handler.first_reported.notified().await;
    let second = tokio::spawn(send_room(Arc::clone(&kit), "r2"));
    let prompt2 = agent.prompted(false).await;
    completed(&mut agent, "session-r2").await;
    for prompt in [&prompt2, &prompt1] {
        agent.answer(prompt, json!({"stopReason":"end_turn"})).await;
    }
    for task in [first, second] {
        task.await.unwrap();
    }
    close(&kit, &mut agent).await;
    let reports = reports.lock().unwrap();
    assert_eq!(reports.len(), 2);
    let rooms: std::collections::BTreeSet<_> =
        reports.iter().map(|event| event.room_id.clone()).collect();
    assert_eq!(rooms.len(), 2);
    assert!(
        reports
            .iter()
            .all(|event| event.tool_call_id == "read" && event.result == Some(json!("original")))
    );
}

async fn send_room(kit: Arc<roomkit::RoomKit>, room: &str) {
    let result = kit
        .process_inbound(
            InboundMessage::new(
                ChannelId::new("ws").unwrap(),
                "alice",
                TextContent::new("read"),
            ),
            ProcessInboundOptions {
                room_id: Some(RoomId::new(room).unwrap()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(result.error.is_none());
}

async fn completed(agent: &mut Agent, session: &str) {
    let update = json!({"jsonrpc":"2.0","method":"session/update","params":{
        "sessionId":session,"update":{"sessionUpdate":"tool_call", "toolCallId":"read", "title":"Read",
            "status":"completed","rawOutput":"original"}}});
    agent
        .writer
        .write_all(format!("{update}\n").as_bytes())
        .await
        .unwrap();
}
