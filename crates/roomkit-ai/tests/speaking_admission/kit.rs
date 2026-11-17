//! The actual generation hook and thinker share the context they selected.

use std::sync::Mutex;

use roomkit::model::hook::{AllowOptions, ModifyOptions};
use roomkit::model::{HookResult, InboundMessage};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, HookOptions, HookTrigger, ProcessInboundOptions,
    RoomKit, RoomKitOptions,
};
use roomkit_ai::speaking::{SpeakingFuture, Thinker};
use roomkit_ai::{
    Agent, AgentOptions, AiContext, AiGenerationEvent, AiMessage, AiRole, GenerationPurpose,
    SharedAiContext,
};

use super::*;

#[derive(Default)]
struct Editing {
    calls: Mutex<Vec<SharedAiContext>>,
}

impl Thinker for Editing {
    fn think<'a>(
        &'a self,
        _previous: &'a Thought,
        context: &'a SharedAiContext,
    ) -> SpeakingFuture<'a, Thought> {
        self.calls.lock().unwrap().push(context.clone());
        context.update(|context| {
            context.system_prompt = Some("Changed by the thinker.".into());
            context
                .messages
                .push(AiMessage::new(AiRole::User, "A thinker annotation."));
        });
        Box::pin(std::future::ready(Ok(Thought {
            text: "A quiet impression.".into(),
            ..Thought::default()
        })))
    }
}

pub(super) async fn setup(
    agent: bool,
    options: AiChannelOptions,
    model: MockAiProvider,
) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        id("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    if agent {
        kit.register_channel(
            Agent::new(
                id("ai"),
                model,
                AgentOptions {
                    ai: options,
                    ..AgentOptions::default()
                },
            )
            .unwrap(),
        )
        .unwrap();
    } else {
        kit.register_channel(AiChannel::new(id("ai"), model, options).unwrap())
            .unwrap();
    }
    let room = RoomId::new("r1").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..CreateRoomOptions::default()
    })
    .await
    .unwrap();
    for channel in ["sms", "ai"] {
        kit.attach_channel(&room, &id(channel), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}

// Python: tests/test_speaking_inter_api_alias.py::test_a_hook_and_the_thinker_keep_the_same_selected_context
#[tokio::test]
async fn a_hook_and_the_thinker_keep_the_same_selected_context() {
    for agent in [false, true] {
        for replace in [false, true] {
            let thinker = Arc::new(Editing::default());
            let model = model(false);
            let policy = MockSpeakPolicy::new(MockSpeakPolicyOptions {
                decisions: vec![SpeakMode::Silent.into()],
                ..MockSpeakPolicyOptions::default()
            });
            let kit = setup(
                agent,
                AiChannelOptions {
                    thinker: Some(Arc::<Editing>::clone(&thinker)),
                    speak_policy: Some(Arc::new(policy)),
                    ..AiChannelOptions::default()
                },
                model.clone(),
            )
            .await;
            let original = Arc::new(Mutex::new(Vec::<SharedAiContext>::new()));
            let selected = Arc::new(Mutex::new(Vec::<SharedAiContext>::new()));
            let (originals, kept) = (Arc::clone(&original), Arc::clone(&selected));
            kit.hook(
                HookTrigger::BeforeAiGeneration,
                move |mut event: AiGenerationEvent, _: RoomContext| {
                    assert_eq!(event.purpose, GenerationPurpose::Thought);
                    originals.lock().unwrap().push(event.ai_context.clone());
                    if replace {
                        event.ai_context = SharedAiContext::new(AiContext {
                            system_prompt: Some("Replacement.".into()),
                            ..event.ai_context.snapshot()
                        });
                    }
                    kept.lock().unwrap().push(event.ai_context.clone());
                    async move {
                        if replace {
                            HookResult::modify(event, ModifyOptions::default())
                        } else {
                            HookResult::allow(AllowOptions::default())
                        }
                    }
                },
                HookOptions::default(),
            )
            .unwrap();
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                kit.process_inbound(
                    InboundMessage::new(id("sms"), "alice", TextContent::new("Listen.")),
                    ProcessInboundOptions {
                        room_id: Some(RoomId::new("r1").unwrap()),
                        ..ProcessInboundOptions::default()
                    },
                ),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(result.error.is_none());
            assert_eq!(model.calls(), Vec::new());
            assert_selected_aliases(&original, &selected, &thinker, replace);
            tokio::time::timeout(Duration::from_secs(3), kit.close())
                .await
                .unwrap()
                .unwrap();
        }
    }
}

// Python: tests/test_turn_hooks.py::test_the_turn_triggers_are_the_rfcs
#[test]
fn the_turn_trigger_collection_includes_both_speaking_reports() {
    let names = roomkit::TURN_TRIGGERS
        .into_iter()
        .map(|trigger| serde_json::to_value(trigger).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        json!([
            "before_ai_generation",
            "on_ai_thinking",
            "on_ai_response",
            "before_tool_use",
            "after_tool_round",
            "on_tool_call",
            "on_user_input_required",
            "on_speak_decision",
            "on_thought",
            "on_plan_updated"
        ])
        .as_array()
        .unwrap()
        .clone()
    );
}

fn assert_selected_aliases(
    original: &Mutex<Vec<SharedAiContext>>,
    selected: &Mutex<Vec<SharedAiContext>>,
    thinker: &Editing,
    replace: bool,
) {
    let (original, selected, calls) = (
        original.lock().unwrap().clone(),
        selected.lock().unwrap().clone(),
        thinker.calls.lock().unwrap().clone(),
    );
    assert_eq!((original.len(), selected.len(), calls.len()), (1, 1, 1));
    let kept = selected.first().unwrap();
    assert!(kept.same_object(calls.first().unwrap()));
    assert_eq!(
        kept.snapshot().system_prompt.as_deref(),
        Some("Changed by the thinker.")
    );
    assert_eq!(
        kept.snapshot().messages.last().unwrap().text(),
        "A thinker annotation."
    );
    kept.update(|context| {
        context
            .messages
            .push(AiMessage::new(AiRole::User, "A later hook annotation."));
    });
    assert_eq!(
        calls
            .first()
            .unwrap()
            .snapshot()
            .messages
            .last()
            .unwrap()
            .text(),
        "A later hook annotation."
    );
    assert_eq!(original.first().unwrap().same_object(kept), !replace);
    if replace {
        let original = original.first().unwrap().snapshot();
        assert_ne!(
            original.system_prompt.as_deref(),
            Some("Changed by the thinker.")
        );
        assert!(
            !original
                .messages
                .iter()
                .any(|message| message.text() == "A thinker annotation.")
        );
    }
}

// Python: tests/test_turns_host.py::test_an_event_a_hook_sends_before_the_tool_loop_is_as_deep_as_the_response
#[tokio::test]
async fn an_early_speak_hook_keeps_the_turns_rights_and_response_depth() {
    for streaming in [false, true] {
        let kit = setup(
            true,
            AiChannelOptions {
                speak_policy: Some(Arc::new(roomkit_ai::AlwaysSpeak)),
                ..AiChannelOptions::default()
            },
            model(streaming),
        )
        .await;
        let sent = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&sent);
        kit.hook(
            HookTrigger::OnSpeakDecision,
            move |_: roomkit_ai::SpeakDecisionEvent, _: RoomContext| {
                let recorded = Arc::clone(&recorded);
                async move {
                    let host = roomkit::current_turn_host().unwrap();
                    assert!(host.open());
                    let event = host
                        .send_event(
                            &RoomId::new("r1").unwrap(),
                            &id("sms"),
                            TextContent::new("noted"),
                            roomkit::SendEventOptions {
                                addressed_to: Some(Vec::new()),
                                ..roomkit::SendEventOptions::default()
                            },
                        )
                        .await
                        .unwrap();
                    recorded.lock().unwrap().push(event);
                }
            },
            HookOptions {
                execution: roomkit::HookExecution::Async,
                ..HookOptions::default()
            },
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            kit.send_event(
                &RoomId::new("r1").unwrap(),
                &id("sms"),
                TextContent::new("go"),
                roomkit::SendEventOptions {
                    chain_depth: 3,
                    ..roomkit::SendEventOptions::default()
                },
            )
            .await
            .unwrap();
            roomkit::testing::idle(&kit).await;
            kit.close().await.unwrap();
        })
        .await
        .unwrap();
        let events = sent.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events.first().unwrap().chain_depth, 4);
    }
}
