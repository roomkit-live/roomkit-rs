//! Per-tool observers spend delivery passes, never hook-body mutation
//! calls (RFC §8.3, §9.5.1).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{EventId, Metadata, RoomEvent, TextContent, Visibility};
use roomkit::{
    HookExecution, HookOptions, HookTrigger, RoomKit, RoomKitOptions, SendEventOptions,
    ToolCallEvent, UpdateEventOptions,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, ProviderError, ToolRoundEvent,
};
use tokio::sync::watch;

use super::support::{Room, channel, room_id, served};

const ROUNDS: usize = 25;
const BOUND: Duration = Duration::from_secs(5);

/// Its final generation keeps the turn alive until every observer has
/// updated and posted; no observer accidentally becomes a fresh arrival.
struct HeldAnswer {
    scripted: ScriptedProvider,
    observed: watch::Receiver<usize>,
}

impl AiProvider for HeldAnswer {
    fn model_name(&self) -> &'static str {
        "held-answer"
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        let answer = self.scripted.generate(context).await?;
        if answer.tool_calls.is_empty() {
            self.observed
                .clone()
                .wait_for(|count| *count == ROUNDS)
                .await
                .unwrap();
        }
        Ok(answer)
    }
}

#[derive(Clone)]
struct Audit {
    kit: RoomKit,
    spare: EventId,
    completed: watch::Sender<usize>,
    next_update: Arc<AtomicUsize>,
    posted: Arc<Mutex<Vec<RoomEvent>>>,
    updated: Arc<Mutex<Vec<Option<RoomEvent>>>>,
}

impl Audit {
    async fn run(self) {
        let n = self.next_update.fetch_add(1, Ordering::SeqCst);
        let metadata = Metadata::from_iter([("n".to_owned(), n.into())]);
        let updated = self
            .kit
            .update_event(
                &room_id(),
                &self.spare,
                UpdateEventOptions {
                    metadata: Some(metadata),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        self.updated.lock().unwrap().push(updated);
        let event = self
            .kit
            .send_event(
                &room_id(),
                &channel("sms"),
                TextContent::new("called"),
                private(),
            )
            .await
            .unwrap();
        self.posted.lock().unwrap().push(event);
        self.completed.send_modify(|count| *count += 1);
    }

    fn register(&self, trigger: HookTrigger) {
        let options = HookOptions {
            execution: HookExecution::Async,
            name: "audit".to_owned(),
            ..Default::default()
        };
        let audit = self.clone();
        if trigger == HookTrigger::AfterToolRound {
            self.kit
                .hook(
                    trigger,
                    move |_event: ToolRoundEvent, _| audit.clone().run(),
                    options,
                )
                .unwrap();
        } else {
            self.kit
                .hook(
                    trigger,
                    move |_event: ToolCallEvent, _| audit.clone().run(),
                    options,
                )
                .unwrap();
        }
    }
}

fn private() -> SendEventOptions {
    SendEventOptions {
        visibility: Visibility::channels([channel("sms")]).unwrap(),
        ..Default::default()
    }
}

// Python: tests/test_reentrance.py::TestACallFromAnAsyncHook::test_an_observer_of_a_trigger_per_tool_call_runs_as_the_turn_s_task
#[tokio::test]
async fn per_tool_observers_keep_mutations_free_while_their_posts_reach_the_cap() {
    for trigger in [
        HookTrigger::BeforeToolUse,
        HookTrigger::OnToolCall,
        HookTrigger::AfterToolRound,
    ] {
        let (completed, observed) = watch::channel(0);
        let rounds = (0..ROUNDS)
            .map(|n| {
                ScriptedRound::calling(
                    "",
                    [(
                        &*format!("t{n}"),
                        "lookup_order",
                        &*format!("{{\"order\":\"{n}\"}}"),
                    )],
                )
            })
            .chain([ScriptedRound::answering("Done.")]);
        let scripted = ScriptedProvider::new(rounds);
        let provider = HeldAnswer {
            scripted: scripted.clone(),
            observed,
        };
        let agent = AiChannel::new(
            channel("assistant"),
            provider,
            AiChannelOptions {
                tools: served().into(),
                ..Default::default()
            },
        )
        .unwrap();
        let room = Room::on_kit(
            RoomKit::new(RoomKitOptions {
                max_chain_depth: 2,
                ..Default::default()
            }),
            agent,
        )
        .await;
        let spare = room
            .kit
            .send_event(
                &room_id(),
                &channel("sms"),
                TextContent::new("card"),
                private(),
            )
            .await
            .unwrap();
        let audit = Audit {
            kit: room.kit.clone(),
            spare: spare.id,
            completed,
            next_update: Arc::default(),
            posted: Arc::default(),
            updated: Arc::default(),
        };
        audit.register(trigger);
        tokio::time::timeout(BOUND, room.says("Hello"))
            .await
            .unwrap();
        tokio::time::timeout(BOUND, roomkit::testing::idle(&room.kit))
            .await
            .unwrap();
        let mut posts = audit.posted.lock().unwrap().clone();
        posts.sort_by_key(|event| event.index);
        let reasons: Vec<_> = posts
            .iter()
            .map(|event| event.blocked_by.as_deref())
            .collect();
        let first_cap = reasons
            .iter()
            .position(|reason| *reason == Some("reentry_loop_cap"))
            .unwrap();
        assert!(first_cap > 0 && first_cap < ROUNDS, "{trigger:?}");
        assert_eq!(
            reasons,
            [
                vec![None; first_cap],
                vec![Some("reentry_loop_cap"); ROUNDS - first_cap]
            ]
            .concat(),
            "{trigger:?}"
        );
        assert_eq!(
            audit
                .updated
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.is_some())
                .count(),
            ROUNDS,
            "{trigger:?}"
        );
        assert_eq!(scripted.requests().len(), ROUNDS + 1);
        room.kit.close().await.unwrap();
    }
}
