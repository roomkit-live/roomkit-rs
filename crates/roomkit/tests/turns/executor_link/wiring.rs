//! Public application wiring, including the independent worker's last room context.
mod application_channel;
mod ordinary_tools;
mod public_ports;
mod text_callbacks;
mod text_loaders;
mod tool_callbacks;
mod tool_methods;

mod observed;
use super::{Link, link};
use roomkit::model::{ChannelId, RoomContext, RoomEvent, RoomId};
use roomkit::testing::{EchoAgent, EchoAgentOptions};
use roomkit::turns::{ChannelControl, EmitOptions, TurnFuture, WiringEnv};
use roomkit::{Channel, RoomKit, RoomKitError, TurnExecutor, TurnExecutorOptions};
use roomkit_ai::testing::ScriptedProvider;
use roomkit_ai::{AiChannel, AiChannelOptions};
use std::sync::{Arc, Mutex};

fn worker() -> TurnExecutor {
    TurnExecutor::new(
        |id| async move {
            Ok(Some(EchoAgent::new(
                id,
                "done",
                EchoAgentOptions::default(),
            )))
        },
        TurnExecutorOptions::default(),
    )
    .unwrap()
}

async fn accepted(worker: &TurnExecutor, channel: &str, org: Option<&str>) -> RoomContext {
    let (mut link, mut frames, _controls) = link(false);
    let req = &mut Arc::get_mut(&mut link).unwrap().request;
    let id = ChannelId::new(channel).unwrap();
    req.turn_id = format!("turn-{channel}");
    req.channel_id = id.clone();
    req.binding.channel_id = id;
    req.context.room.organization_id = org.map(ToOwned::to_owned);
    req.context
        .room
        .metadata
        .insert("latest".into(), channel.into());
    let expected = req.context.clone();
    worker.take(Arc::<Link>::clone(&link)).unwrap();
    link.write.notify_one();
    while let Some(frame) = frames.recv().await {
        if matches!(frame, roomkit::turns::wire::Frame::End) {
            break;
        }
    }
    link.released.notified().await;
    expected
}

async fn detach(worker: &TurnExecutor, room: &RoomId, channel: &str) {
    worker
        .control(ChannelControl::RoomDetached {
            room_id: room.clone(),
            channel_id: ChannelId::new(channel).unwrap(),
            organization_id: None,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn wiring_retains_the_last_admitted_room_until_its_last_channel_detaches() {
    let worker = worker();
    let default = worker.wiring();
    let first = accepted(&worker, "one", None).await;
    let room = &first.room.id;
    assert_eq!(default.env.context(room, None).await.unwrap(), first);
    let second = accepted(&worker, "two", None).await;
    assert_eq!(default.env.context(room, None).await.unwrap(), second);
    accepted(&worker, "refused", Some("other")).await;
    assert_eq!(default.env.context(room, None).await.unwrap(), second);
    detach(&worker, room, "one").await;
    assert_eq!(default.env.context(room, None).await.unwrap(), second);
    detach(&worker, room, "two").await;
    assert!(matches!(
        default.env.context(room, None).await,
        Err(RoomKitError::RoomNotFound(_))
    ));
    worker.close().await;
}

struct Env {
    realtime: Option<Arc<dyn roomkit::realtime::RealtimeBackend>>,
    rows: Vec<RoomEvent>,
    tasks: Vec<roomkit::model::Metadata>,
    row_reads: Mutex<Vec<Option<u64>>>,
    context: RoomContext,
    events: Arc<Mutex<Vec<String>>>,
    reads: std::sync::atomic::AtomicUsize,
    fields: Arc<Mutex<Vec<EmitOptions>>>,
}

impl Env {
    fn new(context: RoomContext) -> Self {
        Self {
            context,
            realtime: None,
            events: Arc::default(),
            reads: std::sync::atomic::AtomicUsize::new(0),
            fields: Arc::default(),
            rows: Vec::new(),
            tasks: Vec::new(),
            row_reads: Mutex::default(),
        }
    }
}

impl WiringEnv for Env {
    fn can_publish_direct<'a>(
        &'a self,
        room: &'a RoomId,
        channel: &'a ChannelId,
        created_at: roomkit::model::Timestamp,
    ) -> TurnFuture<'a, bool> {
        Box::pin(async move {
            self.context.room.id == *room
                && self.context.room.created_at == created_at
                && self.context.bindings.iter().any(|b| {
                    b.channel_id == *channel
                        && b.can_write()
                        && b.visibility == roomkit::model::Visibility::All
                })
        })
    }
    fn realtime(&self) -> Option<Arc<dyn roomkit::realtime::RealtimeBackend>> {
        self.realtime.clone()
    }

    fn room_mark<'a>(&'a self, _: &'a RoomId) -> TurnFuture<'a, Option<roomkit::model::Timestamp>> {
        Box::pin(async { Some(self.context.room.created_at) })
    }

    fn context<'a>(
        &'a self,
        _: &'a RoomId,
        _: Option<&'a RoomContext>,
    ) -> TurnFuture<'a, Result<RoomContext, RoomKitError>> {
        Box::pin(async { Ok(self.context.clone()) })
    }
    fn tool_rows<'a>(
        &'a self,
        _: &'a RoomId,
        _: &'a ChannelId,
        _: u64,
    ) -> TurnFuture<'a, Result<Vec<RoomEvent>, RoomKitError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn history_rows<'a>(
        &'a self,
        _: &'a RoomId,
        _: &'a ChannelId,
        after: Option<u64>,
        limit: u64,
    ) -> TurnFuture<'a, Result<Vec<RoomEvent>, RoomKitError>> {
        Box::pin(async move {
            self.row_reads.lock().unwrap().push(after);
            Ok(self
                .rows
                .iter()
                .filter(|row| after.is_none_or(|i| row.index > i))
                .take(usize::try_from(limit).unwrap())
                .cloned()
                .collect())
        })
    }
    fn room_tasks<'a>(
        &'a self,
        _: &'a RoomId,
    ) -> TurnFuture<'a, Result<Vec<roomkit::model::Metadata>, RoomKitError>> {
        Box::pin(async { Ok(self.tasks.clone()) })
    }
    fn message_before<'a>(
        &'a self,
        _: &'a RoomId,
        _: u64,
    ) -> TurnFuture<'a, Result<Option<u64>, RoomKitError>> {
        Box::pin(async { Ok(None) })
    }
    fn emitter(&self) -> Option<roomkit::turns::FrameworkEmitter> {
        self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (events, fields) = (Arc::clone(&self.events), Arc::clone(&self.fields));
        Some(Arc::new(move |kind, options| {
            let (events, fields) = (Arc::clone(&events), Arc::clone(&fields));
            Box::pin(async move {
                if kind == "failed" {
                    return Err(RoomKitError::Runtime("sink refused".into()));
                }
                events.lock().unwrap().push(kind);
                fields.lock().unwrap().push(options);
                Ok(())
            })
        }))
    }
}

#[tokio::test]
async fn wiring_before_registration_retains_the_environment_selected_for_the_channel() {
    let (link, _, _) = link(false);
    let kit = RoomKit::new(roomkit::RoomKitOptions::default());
    let local = kit.turn_executor().wiring();
    let room = &link.request.room_id;
    kit.create_room(roomkit::CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    assert_eq!(&local.env.context(room, None).await.unwrap().room.id, room);
    let select = || Arc::new(Env::new(link.request.context.clone()));
    let first = select();
    let second = select();
    let channel = AiChannel::new(
        link.request.channel_id.clone(),
        ScriptedProvider::new([]),
        AiChannelOptions::default(),
    )
    .unwrap();
    kit.turn_executor().use_wiring(Arc::<Env>::clone(&first));
    kit.turn_executor().wire(&channel).unwrap();
    kit.turn_executor().use_wiring(Arc::<Env>::clone(&second));
    channel
        .turn_wiring()
        .unwrap()
        .emit("first", EmitOptions::default())
        .await
        .unwrap();
    kit.register_channel(channel.clone()).unwrap();
    channel
        .turn_wiring()
        .unwrap()
        .emit("second", EmitOptions::default())
        .await
        .unwrap();
    assert_eq!(*first.events.lock().unwrap(), ["first"]);
    assert_eq!(*second.events.lock().unwrap(), ["second"]);
    kit.close().await.unwrap();
}

#[tokio::test]
async fn wiring_an_unregistered_ai_channel_activates_its_external_tool_gate() {
    use roomkit::model::{HookResult, Metadata};
    use roomkit::{HookTrigger, ToolCallEvent};
    use roomkit_ai::{ExternalToolHandler, PolicyExternalToolHandler, ProcessToolCallOptions};

    let (link, _, _) = link(false);
    let worker = worker();
    let env = Arc::new(Env::new(link.request.context.clone()));
    worker.use_wiring(Arc::<Env>::clone(&env));
    worker
        .hook(
            HookTrigger::BeforeToolUse,
            |_: ToolCallEvent, ctx: RoomContext| async move {
                HookResult::block(
                    format!("approval in {}", ctx.room.id),
                    roomkit::model::hook::BlockOptions::default(),
                )
            },
            roomkit::turns::HookOptions::default(),
        )
        .unwrap();
    let handler = Arc::new(PolicyExternalToolHandler::default());
    let ai = AiChannel::new(
        link.request.channel_id.clone(),
        ScriptedProvider::new([]),
        AiChannelOptions {
            external_tool_handler: Some(Arc::<PolicyExternalToolHandler>::clone(&handler)),
            ..Default::default()
        },
    )
    .unwrap();
    let options = ProcessToolCallOptions {
        room_id: Some(link.request.room_id.clone()),
        ..Default::default()
    };
    assert!(
        handler
            .process_tool_call("search", &Metadata::new(), options.clone())
            .await
            .unwrap()
            .approved
    );
    worker.wire(&ai).unwrap();
    let decision = handler
        .process_tool_call("search", &Metadata::new(), options)
        .await
        .unwrap();
    assert!(!decision.approved);
    assert!(decision.reason.contains("approval in r1"));
    assert_eq!(*env.events.lock().unwrap(), ["before_tool_use"]);
    worker.close().await;
}

#[tokio::test]
async fn wiring_reads_own_history_in_pages_and_the_ai_reads_tasks_before_registration() {
    use roomkit::model::{EventId, EventType};
    use roomkit_ai::testing::ScriptedRound;
    let (link, _, _) = link(false);
    let request = &link.request;
    let mut env = Env::new(request.context.clone());
    env.rows = (0..501)
        .map(|index| {
            let mut row = request.event.clone();
            row.id = EventId::new(format!("instruction-{index}")).unwrap();
            row.index = index;
            row.source.channel_id = request.channel_id.clone();
            row.r#type = EventType::InstructionRead;
            row
        })
        .collect();
    let task =
        serde_json::json!({"agent":"helper","task":"prepare the wiring audit","status":"running"});
    env.tasks.push(task.as_object().unwrap().clone());
    let env = Arc::new(env);
    let worker = worker();
    worker.use_wiring(Arc::<Env>::clone(&env));
    let rows = worker
        .wiring()
        .tool_history(&request.channel_id, &request.room_id, None)
        .await
        .unwrap();
    assert_eq!(rows, env.rows);
    assert_eq!(*env.row_reads.lock().unwrap(), [None, Some(499)]);
    let provider = ScriptedProvider::new([ScriptedRound::writing(["done"])]);
    let ai = AiChannel::new(
        request.channel_id.clone(),
        provider.clone(),
        AiChannelOptions::default(),
    )
    .unwrap();
    worker.wire(&ai).unwrap();
    ai.on_event(
        request.event.clone(),
        request.binding.clone(),
        request.context.clone(),
    )
    .await
    .unwrap();
    let calls = provider.requests();
    assert_eq!(calls.len(), 1);
    assert!(format!("{:?}", calls.first().unwrap().messages).contains("prepare the wiring audit"));
    assert_eq!(
        *env.row_reads.lock().unwrap(),
        [None, Some(499), None, Some(499)]
    );
    worker.close().await;
}

#[tokio::test]
async fn wiring_before_registration_arms_human_input_and_propagates_a_closed_handler() {
    use roomkit::human_input::{
        CloseOptions, CreateOptions, PendingInputEvent, PendingInputStatus, WaitOptions,
    };
    use roomkit::model::{HookResult, Metadata, hook::BlockOptions};
    use roomkit::{HookTrigger, turns::HookOptions};
    use roomkit_ai::{HumanInputToolHandler, HumanInputToolHandlerOptions};

    let (link, _, _) = link(false);
    let request = &link.request;
    let worker = worker();
    let env = Arc::new(Env::new(request.context.clone()));
    worker.use_wiring(Arc::<Env>::clone(&env));
    worker
        .hook(
            HookTrigger::OnUserInputRequired,
            |_: PendingInputEvent, ctx: RoomContext| async move {
                assert_eq!(ctx.room.id.as_str(), "r1");
                HookResult::block("not allowed", BlockOptions::default())
            },
            HookOptions::default(),
        )
        .unwrap();
    let tool =
        HumanInputToolHandler::new(["Ask"], HumanInputToolHandlerOptions::default()).unwrap();
    let handler = tool.handler().clone();
    let ai = AiChannel::new(
        request.channel_id.clone(),
        ScriptedProvider::new([]),
        AiChannelOptions {
            human_input_handler: Some(tool),
            ..Default::default()
        },
    )
    .unwrap();
    worker.wire(&ai).unwrap();
    let pending = handler
        .create(
            "Ask",
            Metadata::new(),
            CreateOptions {
                channel_id: request.channel_id.to_string(),
                room_id: request.room_id.to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let answer = handler
        .wait(
            &pending.pending_id,
            WaitOptions {
                timeout: std::time::Duration::from_secs(2),
            },
        )
        .await;
    assert!(answer.is_err());
    assert_eq!(pending.status(), PendingInputStatus::Rejected);
    assert_eq!(*env.events.lock().unwrap(), ["user_input_required"]);
    handler.close(CloseOptions::default()).await;
    assert!(
        matches!(worker.wire(&ai),Err(RoomKitError::Runtime(message)) if message.contains("closed"))
    );
    worker.close().await;
}

#[tokio::test]
async fn wired_ai_generation_and_response_fire_without_a_turn_host() {
    use roomkit::model::{HookResult, hook::AllowOptions};
    use roomkit::turns::HookOptions;
    use roomkit::{HookExecution, HookTrigger};
    use roomkit_ai::{AiGenerationEvent, AiResponseEvent, testing::ScriptedRound};
    let (link, _, _) = link(false);
    let request = &link.request;
    let worker = worker();
    let env = Arc::new(Env::new(request.context.clone()));
    worker.use_wiring(Arc::<Env>::clone(&env));
    let (sent, mut seen) = tokio::sync::mpsc::unbounded_channel();
    let start = sent.clone();
    worker
        .hook(
            HookTrigger::BeforeAiGeneration,
            move |_: AiGenerationEvent, ctx: RoomContext| {
                let start = start.clone();
                async move {
                    assert!(roomkit::current_turn_host().is_none());
                    start.send(("before", ctx.room.id)).unwrap();
                    HookResult::allow(AllowOptions::default())
                }
            },
            HookOptions::default(),
        )
        .unwrap();
    worker
        .hook(
            HookTrigger::OnAiResponse,
            move |_: AiResponseEvent, ctx: RoomContext| {
                let sent = sent.clone();
                async move {
                    assert!(roomkit::current_turn_host().is_none());
                    sent.send(("after", ctx.room.id)).unwrap();
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    let ai = AiChannel::new(
        request.channel_id.clone(),
        ScriptedProvider::new([ScriptedRound::writing(["done"])]),
        AiChannelOptions::default(),
    )
    .unwrap();
    worker.wire(&ai).unwrap();
    ai.on_event(
        request.event.clone(),
        request.binding.clone(),
        request.context.clone(),
    )
    .await
    .unwrap();
    for expected in ["before", "after"] {
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(2), seen.recv())
                .await
                .unwrap(),
            Some((expected, request.room_id.clone()))
        );
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while env.events.lock().unwrap().len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        *env.events.lock().unwrap(),
        ["before_ai_generation", "ai_response"]
    );
    worker.close().await;
}

#[tokio::test]
async fn an_application_supplies_the_emitter_for_each_call_with_its_original_arguments() {
    let (link, _, _) = link(false);
    let env = Arc::new(Env::new(link.request.context.clone()));
    let worker = worker();
    worker.use_wiring(Arc::<Env>::clone(&env));
    let wiring = worker.wiring();
    wiring.emit("first", EmitOptions::default()).await.unwrap();
    let explicit = EmitOptions {
        room_id: Some(link.request.room_id.clone()),
        channel_id: Some(link.request.channel_id.clone()),
        event_id: Some(link.request.event.id.clone()),
        data: Some(roomkit::model::Metadata::new()),
    };
    wiring.emit("second", explicit.clone()).await.unwrap();
    assert!(
        matches!(wiring.emit("failed", EmitOptions::default()).await, Err(RoomKitError::Runtime(reason)) if reason == "sink refused")
    );
    assert_eq!(env.reads.load(std::sync::atomic::Ordering::SeqCst), 3);
    assert_eq!(
        *env.fields.lock().unwrap(),
        [EmitOptions::default(), explicit]
    );
    worker.close().await;
}
