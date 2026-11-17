//! Public hooks observe refused and abandoned calls after catalogue changes.
use std::future::ready;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::model::hook::{AllowOptions, ModifyOptions};
use roomkit::model::{HookResult, InboundMessage, RoomContext, TextContent};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, CreateRoomOptions, HookExecution, HookOptions, HookTrigger,
    ProcessInboundOptions, RoomKit, RoomKitOptions, ToolCallEvent,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiGenerationEvent, AiProvider, AiResponse,
    ProviderError, ProviderStream,
};

use super::dispatch::{Case, Changing};
use super::support::{Model, bounded};

async fn room<C: Channel + 'static>(agent: C) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        "web".parse().unwrap(),
        roomkit::model::ChannelType::WEBHOOK,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(agent).unwrap();
    let room = "r".parse().unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    for name in ["web", "ai"] {
        kit.attach_channel(
            &"r".parse().unwrap(),
            &name.parse().unwrap(),
            AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    kit
}
fn observe(kit: &RoomKit) -> Arc<Mutex<Vec<ToolCallEvent>>> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&events);
    kit.hook(
        HookTrigger::OnToolCall,
        move |event: ToolCallEvent, _| {
            seen.lock().unwrap().push(event);
            ready(())
        },
        HookOptions {
            execution: HookExecution::Async,
            ..HookOptions::default()
        },
    )
    .unwrap();
    events
}
async fn send(kit: &RoomKit) {
    bounded(kit.process_inbound(
        InboundMessage::new("web".parse().unwrap(), "ada", TextContent::new("go")),
        ProcessInboundOptions {
            room_id: Some("r".parse().unwrap()),
            ..ProcessInboundOptions::default()
        },
    ))
    .await
    .unwrap();
    bounded(roomkit::testing::idle(kit)).await;
}
struct FailingOnGeneration {
    model: Model,
    sandbox: Arc<Changing>,
}
impl AiProvider for FailingOnGeneration {
    fn model_name(&self) -> &'static str {
        "live catalogue witness"
    }
    fn supports_structured_streaming(&self) -> bool {
        self.model.streaming
    }
    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.sandbox.force_failure.store(true, Ordering::SeqCst);
        self.model.generate(context).await
    }
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        self.sandbox.force_failure.store(true, Ordering::SeqCst);
        self.model.generate_structured_stream(context)
    }
}
#[tokio::test]
async fn withdrawal_refuses_before_the_live_catalogue_is_read() {
    for streaming in [false, true] {
        let case = Case::new(streaming, false, false);
        let model = Model {
            streaming,
            inner: ScriptedProvider::new([
                ScriptedRound::calling("", [("one", "sandbox_bash", "{}")]),
                ScriptedRound::answering("done"),
            ]),
        };
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            FailingOnGeneration {
                model: model.clone(),
                sandbox: Arc::clone(&case.sandbox),
            },
            AiChannelOptions {
                sandbox: Some(Arc::<Changing>::clone(&case.sandbox)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let kit = room(agent).await;
        let reports = observe(&kit);
        kit.hook(
            HookTrigger::BeforeAiGeneration,
            |mut event: AiGenerationEvent, _: RoomContext| {
                event.ai_context.tools.clear();
                ready(HookResult::modify(event, ModifyOptions::default()))
            },
            HookOptions::default(),
        )
        .unwrap();
        send(&kit).await;
        bounded(kit.close()).await.unwrap();
        assert!(case.sandbox.calls.lock().unwrap().is_empty());
        assert_eq!(model.inner.requests().len(), 2);
        let events = reports.lock().unwrap();
        assert_eq!(events.len(), 1);
        let event = events.first().unwrap();
        assert!(event.refused && !event.cancelled);
        assert_eq!(
            event.result,
            Some(serde_json::json!(
                "{\"error\": \"Tool 'sandbox_bash' is not available in this turn.\"}"
            ))
        );
    }
}
#[tokio::test]
async fn a_failed_gate_reports_each_announced_call_once_as_cancelled() {
    for streaming in [false, true] {
        let case = Case::new(streaming, true, true);
        let model = Model {
            streaming,
            inner: ScriptedProvider::new([ScriptedRound::calling(
                "",
                [
                    ("one", "sandbox_bash", r#"{"n":1}"#),
                    ("two", "sandbox_bash", r#"{"n":2}"#),
                ],
            )]),
        };
        let agent = AiChannel::new(
            "ai".parse().unwrap(),
            model.clone(),
            AiChannelOptions {
                sandbox: Some(Arc::<Changing>::clone(&case.sandbox)),
                ..AiChannelOptions::default()
            },
        )
        .unwrap();
        let kit = room(agent).await;
        let reports = observe(&kit);
        let judged = Arc::new(AtomicUsize::new(0));
        let counting = Arc::clone(&judged);
        kit.hook(
            HookTrigger::OnToolCall,
            move |_: ToolCallEvent, _| {
                counting.fetch_add(1, Ordering::SeqCst);
                ready(HookResult::allow(AllowOptions::default()))
            },
            HookOptions::default(),
        )
        .unwrap();
        kit.hook(
            HookTrigger::BeforeToolUse,
            |mut event: ToolCallEvent, _| {
                event.arguments.insert("n".into(), serde_json::json!(101));
                ready(HookResult::modify(event, ModifyOptions::default()))
            },
            HookOptions::default(),
        )
        .unwrap();
        send(&kit).await;
        bounded(kit.close()).await.unwrap();
        assert!(case.sandbox.entered.load(Ordering::SeqCst));
        assert!(case.sandbox.ended.load(Ordering::SeqCst));
        assert_eq!(case.sandbox.calls.lock().unwrap().len(), 1);
        assert_eq!(model.inner.requests().len(), 1);
        assert_eq!(judged.load(Ordering::SeqCst), 0);
        let events = reports.lock().unwrap();
        let mut ids = events
            .iter()
            .map(|event| event.tool_call_id.as_str())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, ["one", "two"]);
        assert!(events.iter().all(|event| event.cancelled && event.is_error));
        for event in events.iter() {
            assert_eq!(event.error_detail, None);
            let expected = if event.tool_call_id == "one" { 101 } else { 2 };
            assert_eq!(event.arguments.get("n"), Some(&serde_json::json!(expected)));
            assert_eq!(
                event.result,
                Some(serde_json::json!(
                    "{\"error\": \"Tool call cancelled\", \"tool\": \"sandbox_bash\", \"hint\": \"The turn ended before its result.\"}"
                ))
            );
        }
    }
}
