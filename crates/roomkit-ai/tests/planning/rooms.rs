use super::*;
use roomkit::model::{EventType, HookResult, RoomContext};
use roomkit::{HookOptions, HookTrigger, SendEventOptions};
use roomkit_ai::AiGenerationEvent;
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn real_messages_and_generation_hooks_keep_room_plans_apart() {
    for streaming in [false, true] {
        let model = model(
            vec![
                plan(tasks("ALPHA_SECRET")),
                AiResponse::new("Done."),
                AiResponse::new("Done."),
                AiResponse::new("Done."),
            ],
            streaming,
        );
        let kit = setup(model.clone(), &["a", "b"], RoomKitOptions::default()).await;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let keeping = Arc::clone(&seen);
        kit.hook(
            HookTrigger::BeforeAiGeneration,
            move |event: AiGenerationEvent, _: RoomContext| {
                keeping.lock().unwrap().push(event.ai_context.snapshot());
                async { HookResult::allow(roomkit::model::hook::AllowOptions::default()) }
            },
            HookOptions::default(),
        )
        .unwrap();
        say(&kit, "a", "Plan.").await;
        say(&kit, "b", "Other room.").await;
        say(&kit, "a", "Continue.").await;
        let calls = model.calls();
        assert_eq!(calls.len(), 4);
        assert!(
            calls
                .iter()
                .all(|call| call.system_prompt.as_deref() == Some("Fixed."))
        );
        assert!(!format!("{:?}", calls.get(2).unwrap().messages).contains("ALPHA_SECRET"));
        assert!(input(calls.last().unwrap()).contains("## Current Task Plan"));
        assert!(input(calls.last().unwrap()).contains("ALPHA_SECRET"));
        let seen = seen.lock().unwrap().clone();
        assert_eq!(seen.len(), 3);
        assert!(!format!("{:?}", seen.get(1).unwrap().messages).contains("ALPHA_SECRET"));
        assert!(input(seen.last().unwrap()).contains("ALPHA_SECRET"));
        kit.close().await.unwrap();
    }
}

#[tokio::test]
async fn detach_forgets_plan_notes_and_preserves_the_tool_history() {
    for streaming in [false, true] {
        let model = model(
            vec![
                plan(tasks("Old plan")),
                AiResponse::new("Done."),
                AiResponse::new("Done."),
            ],
            streaming,
        );
        let kit = setup(model.clone(), &["r"], RoomKitOptions::default()).await;
        say(&kit, "r", "Plan.").await;
        kit.detach_channel(
            &"r".parse().unwrap(),
            &"agent".parse().unwrap(),
            roomkit::DetachChannelOptions::default(),
        )
        .await
        .unwrap();
        attach(&kit, "r", "agent").await;
        say(&kit, "r", "Again.").await;
        let calls = model.calls();
        let last = calls.last().unwrap();
        assert_eq!(input(last), "Again.");
        assert!(format!("{:?}", last.messages).contains("Old plan"));
        assert_eq!(last.system_prompt.as_deref(), Some("Fixed."));
        kit.close().await.unwrap();
    }
}

#[tokio::test]
async fn a_standalone_instruction_reads_no_plan_and_leaves_the_room_plan_intact() {
    for streaming in [false, true] {
        let model = model(
            vec![
                plan(tasks("Room-only plan")),
                AiResponse::new("Done."),
                AiResponse::new("Done."),
                AiResponse::new("Done."),
            ],
            streaming,
        );
        let kit = setup(model.clone(), &["r"], RoomKitOptions::default()).await;
        say(&kit, "r", "Plan.").await;
        kit.send_event(
            &"r".parse().unwrap(),
            &"ws".parse().unwrap(),
            TextContent::new("Alone."),
            SendEventOptions {
                event_type: EventType::Instruction,
                addressed_to: Some(vec!["agent".parse().unwrap()]),
                standalone: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(
            !format!("{:?}", model.calls().last().unwrap().messages).contains("Room-only plan")
        );
        say(&kit, "r", "Continue.").await;
        assert!(input(model.calls().last().unwrap()).contains("Room-only plan"));
        kit.close().await.unwrap();
    }
}

#[tokio::test]
async fn an_empty_plan_removes_the_note_without_erasing_the_previous_call() {
    for streaming in [false, true] {
        let model = model(
            vec![
                plan(tasks("Old plan")),
                AiResponse::new("Done."),
                plan(json!([])),
                AiResponse::new("Done."),
                AiResponse::new("Done."),
            ],
            streaming,
        );
        let kit = setup(model.clone(), &["r"], RoomKitOptions::default()).await;
        for body in ["Plan.", "Clear.", "Continue."] {
            say(&kit, "r", body).await;
        }
        let calls = model.calls();
        assert_eq!(input(calls.last().unwrap()), "Continue.");
        assert!(format!("{:?}", calls.last().unwrap().messages).contains("Old plan"));
        kit.close().await.unwrap();
    }
}
