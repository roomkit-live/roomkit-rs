use super::*;
use roomkit::model::RoomContext;
use roomkit::{HookOptions, HookTrigger};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiGenerationEvent, AiTool, ProviderError, ProviderStream, ToolHandler, ToolResult,
};
use std::sync::Arc;

#[derive(Clone)]
struct Model {
    inner: ScriptedProvider,
    streaming: bool,
}

impl AiProvider for Model {
    fn model_name(&self) -> &'static str {
        "planning-compaction"
    }
    fn supports_structured_streaming(&self) -> bool {
        self.streaming
    }
    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.inner.generate(context).await
    }
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        self.inner.generate_structured_stream(context)
    }
}

// Python: test_compaction_keeps_input.py creates its plan in a previous turn.
#[tokio::test]
async fn overflow_preserves_the_actual_plan_notes_and_replaces_an_older_long_result() {
    for streaming in [false, true] {
        compact(streaming).await;
    }
}

fn script() -> Vec<ScriptedRound> {
    let mut rounds = vec![
        ScriptedRound::calling(
            "",
            [(
                "p1",
                "plan_tasks",
                r#"{"tasks":[{"title":"Check SHIPPED-7731","status":"pending"}]}"#,
            )],
        ),
        ScriptedRound::answering("Done."),
    ];
    for n in 1..=6 {
        rounds.push(ScriptedRound::calling(
            "",
            [(
                format!("c{n}").as_str(),
                "lookup",
                format!("{{\"n\":{n}}}").as_str(),
            )],
        ));
    }
    rounds.push(
        ScriptedRound::answering("")
            .refusing(400, "prompt is too long")
            .with_context_overflow(true),
    );
    rounds.push(ScriptedRound::answering("Done."));
    rounds
}

#[expect(clippy::panic, reason = "test helper rejects a malformed model result")]
async fn compact(streaming: bool) {
    let rounds = script();
    let provider = Model {
        inner: ScriptedProvider::new(rounds),
        streaming,
    };
    let model = provider.inner.clone();
    let tool = AiTool {
        parameters: json!({"type":"object", "properties":{"n":{"type":"integer"}}}),
        ..AiTool::new("lookup", "Look up a quote").unwrap()
    };
    let channel = AiChannel::new(
        "agent".parse().unwrap(),
        provider,
        AiChannelOptions {
            enable_planning: true,
            system_prompt: Some("Fixed.".into()),
            tools: vec![tool.into()],
            tool_handler: Some(ToolHandler::new(|_, args| async move {
                Ok(ToolResult::Text(format!(
                    "quote {} {}",
                    args.get("n").unwrap(),
                    "x".repeat(3000)
                )))
            })),
            ..Default::default()
        },
    )
    .unwrap();
    let kit = setup_channel(channel, &["r"], RoomKitOptions::default()).await;
    let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
    let saving = Arc::clone(&seen);
    kit.hook(
        HookTrigger::BeforeAiGeneration,
        move |event: AiGenerationEvent, _: RoomContext| {
            saving.lock().unwrap().push(input(&event.ai_context));
            async {}
        },
        HookOptions::default(),
    )
    .unwrap();
    say(&kit, "r", "Make a plan.").await;
    say(
        &kit,
        "r",
        "Compare the three quotes and tell me which one to take.",
    )
    .await;
    let requests = model.requests();
    assert_eq!(requests.len(), 10);
    let before = requests.get(8).unwrap();
    let after = requests.last().unwrap();
    let actual = seen.lock().unwrap().last().unwrap().clone();
    assert!(actual.contains("Check SHIPPED-7731"));
    assert!(actual.contains("## Current Task Plan"));
    assert!(
        before
            .messages
            .iter()
            .any(|message| message.content.text() == actual)
    );
    // The kit retained turn one's history; Python's matching case joins its
    // summary before the current input and keeps that input/notes whole.
    let compacted = after.messages.first().unwrap().content.text();
    assert!(compacted.starts_with("[Context compacted"));
    assert!(compacted.contains("Make a plan."));
    assert!(compacted.ends_with(&actual));
    let saved = results(after)
        .into_iter()
        .find(|result| result.tool_call_id == "c1")
        .unwrap();
    let ToolResult::Text(text) = &saved.result else {
        panic!("text result required")
    };
    assert!(text.contains("Full output saved as 'evicted_c1'"));
    assert!(
        requests
            .iter()
            .all(|request| request.system_prompt.as_deref() == Some("Fixed."))
    );
    kit.close().await.unwrap();
}
