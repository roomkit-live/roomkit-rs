//! Application calls into the same executor hook builders as its text channels.
use super::{Env, link, worker};
use roomkit::human_input::PendingInputEvent;
use roomkit::model::hook::{BlockOptions, ModifyOptions};
use roomkit::model::{ChannelId, ChannelType, HookResult, Metadata, RoomContext};
use roomkit::turns::HookOptions;
use roomkit::{HookExecution, HookTrigger};
use roomkit_ai::speaking::SpeakMode;
use roomkit_ai::{
    AiContext, AiGenerationEvent, AiResponseEvent, SpeakDecisionEvent, ThinkingEvent, Thought,
    ThoughtEvent, ToolRoundEvent, TurnWiringExt,
};
use std::sync::Arc;
use tokio::sync::{Notify, mpsc::unbounded_channel};

#[tokio::test]
async fn application_reads_the_generation_verdict_and_the_output_names_its_bound_channel() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let env = Arc::new(Env::new(req.context.clone()));
    let worker = worker();
    worker.use_wiring(Arc::<Env>::clone(&env));
    let emitter = ChannelId::new("callback-owner").unwrap();
    let callback = worker.wiring().build_before_generation_hook(&emitter);
    let mut event = AiGenerationEvent::new(AiContext::default(), req.channel_id.clone());
    let empty = callback(event.clone()).await.unwrap();
    assert!(empty.allowed && empty.event.is_none());
    assert!(env.events.lock().unwrap().is_empty());
    event.room_id = Some(req.context.room.id.clone());
    let allowed = callback(event.clone()).await.unwrap();
    assert!(allowed.allowed && allowed.event.is_some());
    let injection = roomkit::model::InjectedEvent::new(req.event.clone());
    worker
        .hook(
            HookTrigger::BeforeAiGeneration,
            move |_: AiGenerationEvent, _: RoomContext| {
                let mut rejected = HookResult::block("needs review", BlockOptions::default());
                rejected.injected_events.push(injection.clone());
                rejected.metadata.insert("tag".into(), "held".into());
                std::future::ready(rejected)
            },
            HookOptions {
                name: "policy".into(),
                ..Default::default()
            },
        )
        .unwrap();
    let denied = callback(event).await.unwrap();
    assert!(!denied.allowed);
    assert_eq!(denied.reason.as_deref(), Some("needs review"));
    assert_eq!(denied.blocked_by.as_deref(), Some("policy"));
    assert_eq!(denied.metadata["tag"], "held");
    assert_eq!(denied.injected_events, []);
    assert_eq!(
        env.fields
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .channel_id
            .as_ref(),
        Some(&emitter)
    );
}

#[tokio::test]
async fn a_round_updates_the_callers_tools_and_messages_and_a_block_makes_the_speaker_silent() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    worker.use_wiring(Arc::new(Env::new(req.context.clone())));
    worker
        .hook(
            HookTrigger::AfterToolRound,
            |mut e: ToolRoundEvent, _: RoomContext| async move {
                e.withdraw(["lookup"]);
                e.add_message("use the collected answer");
                HookResult::modify(e, ModifyOptions::default())
            },
            HookOptions::default(),
        )
        .unwrap();
    let room = &req.context.room.id;
    let mut round = ToolRoundEvent::new(
        req.channel_id.clone(),
        Some(room.clone()),
        0,
        vec![],
        vec![],
    );
    worker.wiring().build_after_tool_round_hook()(&mut round).await;
    assert!(round.withdrawn.contains("lookup"));
    assert_eq!(round.messages, ["use the collected answer"]);
    worker
        .hook(
            HookTrigger::OnSpeakDecision,
            |_: SpeakDecisionEvent, _: RoomContext| async {
                HookResult::block("quiet", BlockOptions::default())
            },
            HookOptions {
                name: "floor".into(),
                ..Default::default()
            },
        )
        .unwrap();
    let speak = worker.wiring().build_speak_decision_hook();
    let decision = SpeakDecisionEvent::new(
        room.as_str(),
        req.channel_id.as_str(),
        req.event.clone(),
        SpeakMode::Speak.into(),
    );
    let standing = speak(decision).await;
    assert_eq!(standing.mode, SpeakMode::Silent);
    assert_eq!(standing.reason, "blocked by floor");
}

#[tokio::test]
async fn reasoning_and_thought_are_observed_and_the_response_is_reported_after_its_observer() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let env = Arc::new(Env::new(req.context.clone()));
    let room = &req.context.room.id;
    let channel = &req.channel_id;
    let worker = worker();
    worker.use_wiring(Arc::<Env>::clone(&env));
    let (sent, mut observed) = unbounded_channel();
    let tx = sent.clone();
    worker
        .hook(
            HookTrigger::OnAiThinking,
            move |e: ThinkingEvent, _: RoomContext| {
                let tx = tx.clone();
                async move {
                    tx.send(format!("{} {}", e.round_index, e.thinking))
                        .unwrap();
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    worker
        .hook(
            HookTrigger::OnThought,
            move |e: ThoughtEvent, _: RoomContext| {
                let tx = sent.clone();
                async move {
                    tx.send(e.thought.text).unwrap();
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    let wiring = worker.wiring();
    wiring.build_thinking_hook(channel)(room.clone(), "reasoning".into(), 3).await;
    assert_eq!(observed.recv().await.as_deref(), Some("3 reasoning"));
    wiring.build_thought_hook()(ThoughtEvent::new(
        room.as_str(),
        channel.as_str(),
        Thought {
            text: "thought".into(),
            ..Default::default()
        },
    ))
    .await;
    assert_eq!(observed.recv().await.as_deref(), Some("thought"));
    let gate = Arc::new(Notify::new());
    let release = Arc::clone(&gate);
    worker
        .hook(
            HookTrigger::OnAiResponse,
            move |_: AiResponseEvent, _: RoomContext| {
                let release = Arc::clone(&release);
                async move {
                    release.notified().await;
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
    let response = AiResponseEvent {
        room_id: Some(room.clone()),
        latency_ms: 13,
        ..AiResponseEvent::new(channel.clone(), "done")
    };
    wiring.build_after_response_hook(channel)(response)
        .await
        .unwrap();
    assert!(env.events.lock().unwrap().is_empty());
    gate.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while env.events.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(env.events.lock().unwrap().as_slice(), ["ai_response"]);
    assert_eq!(
        env.fields.lock().unwrap()[0].data.as_ref().unwrap()["latency_ms"],
        13
    );
}

#[tokio::test]
async fn a_human_input_callback_returns_the_gate_and_reports_the_request_to_the_right_channel() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let env = Arc::new(Env::new(req.context.clone()));
    let worker = worker();
    worker.use_wiring(Arc::<Env>::clone(&env));
    worker
        .hook(
            HookTrigger::OnUserInputRequired,
            |_: PendingInputEvent, _: RoomContext| async {
                HookResult::block("not available", BlockOptions::default())
            },
            HookOptions::default(),
        )
        .unwrap();
    let callback = worker
        .wiring()
        .build_on_user_input_required_hook(&req.channel_id);
    let mut e = PendingInputEvent::new(
        "request",
        "ask",
        Metadata::new(),
        "",
        "call",
        "",
        ChannelType::AI,
    );
    assert!(callback(e.clone()).await.unwrap());
    assert!(env.events.lock().unwrap().is_empty());
    e.room_id = req.context.room.id.to_string();
    assert!(!callback(e).await.unwrap());
    let fields = env.fields.lock().unwrap();
    assert_eq!(fields[0].channel_id.as_ref(), Some(&req.channel_id));
    assert_eq!(fields[0].data.as_ref().unwrap()["reason"], "not available");
}

#[tokio::test]
async fn a_fail_closed_generation_hook_exposes_the_hook_and_its_error() {
    let (link, _, _) = link(false);
    let req = &link.request;
    let worker = worker();
    worker.use_wiring(Arc::new(Env::new(req.context.clone())));
    worker
        .hook(
            HookTrigger::BeforeAiGeneration,
            |_: AiGenerationEvent, _: RoomContext| async {
                Err::<HookResult<AiGenerationEvent>, roomkit::HookError>(
                    "temporarily offline".into(),
                )
            },
            HookOptions {
                name: "audit".into(),
                fail_closed: true,
                ..Default::default()
            },
        )
        .unwrap();
    let result = worker
        .wiring()
        .build_before_generation_hook(&req.channel_id)(AiGenerationEvent {
        room_id: Some(req.context.room.id.clone()),
        ..AiGenerationEvent::new(AiContext::default(), req.channel_id.clone())
    })
    .await
    .unwrap();
    assert!(!result.allowed && result.failed_closed);
    assert_eq!(result.reason.as_deref(), Some("hook_error:audit"));
    assert_eq!(result.blocked_by.as_deref(), Some("audit"));
    assert_eq!(result.hook_errors.len(), 1);
    assert_eq!(result.hook_errors[0]["hook"], "audit");
    assert_eq!(result.hook_errors[0]["error"], "temporarily offline");
}
