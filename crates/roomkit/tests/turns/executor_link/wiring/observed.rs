//! Turn callbacks on a manually wired AI, before any executor has taken a turn.
use super::{Env, link, worker};
use roomkit::model::{HookResult, RoomContext, hook::BlockOptions};
use roomkit::realtime::{
    EphemeralEventType as Kind, InMemoryRealtime, InMemoryRealtimeOptions, RealtimeBackend,
};
use roomkit::turns::HookOptions;
use roomkit::{Channel, HookExecution, HookTrigger, TurnExecutor, TurnPayload};
use roomkit_ai::speaking::SpeakingFuture;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AlwaysSpeak, PlanUpdatedEvent, SharedAiContext,
    SpeakDecisionEvent, Thinker, ThinkingEvent, Thought, ThoughtEvent,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

struct Listening;

impl Thinker for Listening {
    fn think<'a>(&'a self, _: &'a Thought, _: &'a SharedAiContext) -> SpeakingFuture<'a, Thought> {
        Box::pin(std::future::ready(Ok(Thought {
            text: "listening".into(),
            ..Thought::default()
        })))
    }
}

fn observe<P: TurnPayload>(
    worker: &TurnExecutor,
    sent: &UnboundedSender<String>,
    read: fn(P) -> String,
) {
    let sent = sent.clone();
    worker
        .hook(
            P::TRIGGER,
            move |event: P, ctx: RoomContext| {
                let sent = sent.clone();
                async move {
                    assert!(roomkit::current_turn_host().is_none());
                    assert_eq!(ctx.room.id.as_str(), "r1");
                    sent.send(read(event)).unwrap();
                }
            },
            HookOptions {
                execution: HookExecution::Async,
                ..Default::default()
            },
        )
        .unwrap();
}

#[tokio::test]
async fn a_wired_speaking_gate_can_silence_the_model_and_observe_its_thought() {
    let (link, _, _) = link(false);
    let r = &link.request;
    let worker = worker();
    worker.use_wiring(Arc::new(Env::new(r.context.clone())));
    worker
        .hook(
            HookTrigger::OnSpeakDecision,
            |_: SpeakDecisionEvent, _: RoomContext| async {
                HookResult::block("listen", BlockOptions::default())
            },
            HookOptions {
                name: "listening".into(),
                ..Default::default()
            },
        )
        .unwrap();
    let (sent, mut seen) = unbounded_channel();
    observe(&worker, &sent, |event: SpeakDecisionEvent| {
        event.decision.reason
    });
    observe(&worker, &sent, |event: ThoughtEvent| event.thought.text);
    let provider = ScriptedProvider::new([ScriptedRound::answering("unexpected")]);
    let ai = AiChannel::new(
        r.channel_id.clone(),
        provider.clone(),
        AiChannelOptions {
            speak_policy: Some(Arc::new(AlwaysSpeak)),
            thinker: Some(Arc::new(Listening)),
            think_wait: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .unwrap();
    worker.wire(&ai).unwrap();
    ai.on_event(r.event.clone(), r.binding.clone(), r.context.clone())
        .await
        .unwrap();
    assert_eq!(provider.requests().len(), 0);
    let mut observed = Vec::new();
    for _ in 0..2 {
        observed.push(
            tokio::time::timeout(Duration::from_secs(2), seen.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    observed.sort();
    assert_eq!(observed, ["blocked by listening", "listening"]);
    worker.close().await;
}

#[tokio::test]
async fn wired_thinking_and_plan_updates_reach_the_executor_without_a_turn_host() {
    let (link, _, _) = link(false);
    let r = &link.request;
    let worker = worker();
    let mut env = Env::new(r.context.clone());
    let backend = Arc::new(InMemoryRealtime::new(InMemoryRealtimeOptions::default()));
    let (published, mut events) = unbounded_channel();
    backend
        .subscribe_to_room(
            &r.room_id,
            Arc::new(move |event| {
                published.send(event).unwrap();
                Box::pin(std::future::ready(()))
            }),
        )
        .await;
    env.realtime = Some(Arc::<InMemoryRealtime>::clone(&backend));
    worker.use_wiring(Arc::new(env));
    let (sent, mut seen) = unbounded_channel();
    observe(&worker, &sent, |event: ThinkingEvent| event.thinking);
    observe(&worker, &sent, |event: PlanUpdatedEvent| {
        event.tasks[0].get()["title"].as_str().unwrap().to_owned()
    });
    let provider = ScriptedProvider::new([
        ScriptedRound::calling(
            "",
            [(
                "p1",
                "plan_tasks",
                r#"{"tasks":[{"title":"write plan","status":"pending"}]}"#,
            )],
        )
        .reasoning(["take stock"]),
        ScriptedRound::answering("done"),
    ]);
    let ai = AiChannel::new(
        r.channel_id.clone(),
        provider.clone(),
        AiChannelOptions {
            enable_planning: true,
            ..Default::default()
        },
    )
    .unwrap();
    worker.wire(&ai).unwrap();
    ai.on_event(r.event.clone(), r.binding.clone(), r.context.clone())
        .await
        .unwrap();
    assert_eq!(provider.requests().len(), 2);
    let mut observed = Vec::new();
    for _ in 0..2 {
        observed.push(
            tokio::time::timeout(Duration::from_secs(2), seen.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    observed.sort();
    assert_eq!(observed, ["take stock", "write plan"]);
    let expected = [
        Kind::ThinkingStart,
        Kind::ThinkingDelta,
        Kind::ThinkingEnd,
        Kind::Custom,
    ];
    let published = tokio::time::timeout(Duration::from_secs(2), async {
        let mut published = Vec::new();
        while published.len() < expected.len() {
            let event = events.recv().await.unwrap();
            if expected.contains(&event.r#type) {
                assert_eq!(event.room_created_at, Some(r.context.room.created_at));
                assert_eq!(event.channel_id.as_ref(), Some(&r.channel_id));
                assert!(!event.data.contains_key("thinking"));
                if event.r#type == Kind::Custom {
                    assert_eq!(event.data["type"], "plan_updated");
                }
                published.push(event.r#type);
            }
        }
        published
    })
    .await
    .unwrap();
    assert_eq!(published, expected);
    backend.close().await;
    worker.close().await;
}

#[tokio::test]
async fn an_unreadable_wired_room_honors_the_speaking_gates_failure_mode() {
    let (link, _, _) = link(false);
    let r = &link.request;
    for fail_closed in [false, true] {
        let worker = worker();
        // The separate executor has never received this room.
        let read = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let hook = Arc::clone(&read);
        worker
            .hook(
                HookTrigger::OnSpeakDecision,
                move |_: SpeakDecisionEvent, _: RoomContext| {
                    hook.store(true, std::sync::atomic::Ordering::Relaxed);
                    std::future::ready(HookResult::block("unreachable", BlockOptions::default()))
                },
                HookOptions {
                    fail_closed,
                    ..Default::default()
                },
            )
            .unwrap();
        let provider = ScriptedProvider::new([ScriptedRound::answering("speaking")]);
        let ai = AiChannel::new(
            r.channel_id.clone(),
            provider.clone(),
            AiChannelOptions {
                speak_policy: Some(Arc::new(AlwaysSpeak)),
                ..Default::default()
            },
        )
        .unwrap();
        worker.wire(&ai).unwrap();
        ai.on_event(r.event.clone(), r.binding.clone(), r.context.clone())
            .await
            .unwrap();
        assert_eq!(provider.requests().len(), usize::from(!fail_closed));
        assert!(!read.load(std::sync::atomic::Ordering::Relaxed));
        worker.close().await;
    }
}
