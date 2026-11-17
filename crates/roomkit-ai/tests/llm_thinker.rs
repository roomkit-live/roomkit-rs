//! The public thinker reads a bounded transcript and owns no provider lifecycle.
#![expect(clippy::unwrap_used, reason = "a broken test fixture should fail")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::{ChannelError, ClassedError};
use roomkit_ai::{
    AiContext, AiMessage, AiProvider, AiResponse, AiRole, LlmThinker, LlmThinkerOptions,
    ProviderError, SharedAiContext, Thinker, Thought,
};
use serde_json::{Value, json};
use tokio::sync::Notify;

#[derive(Clone)]
struct Model {
    calls: Arc<Mutex<Vec<AiContext>>>,
    answer: String,
    schema: bool,
    held: bool,
    entered: Arc<Notify>,
    release: Arc<Notify>,
    finished: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
}

impl Model {
    fn new(answer: impl Into<String>) -> Self {
        Self {
            calls: Arc::default(),
            answer: answer.into(),
            schema: true,
            held: false,
            entered: Arc::default(),
            release: Arc::default(),
            finished: Arc::default(),
            closed: Arc::default(),
        }
    }
    fn recorded(&self) -> Vec<AiContext> {
        self.calls.lock().unwrap().clone()
    }
}

struct Finished(Arc<AtomicBool>);
impl Drop for Finished {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl AiProvider for Model {
    fn model_name(&self) -> &'static str {
        "thought"
    }
    fn supports_response_schema(&self) -> bool {
        self.schema
    }
    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.calls.lock().unwrap().push(context.clone());
        let _finished = Finished(Arc::clone(&self.finished));
        self.entered.notify_one();
        if self.held {
            self.release.notified().await;
        }
        Ok(AiResponse::new(&self.answer))
    }
    fn close(&self) -> impl Future<Output = ()> + Send {
        self.closed.store(true, Ordering::Release);
        std::future::ready(())
    }
}

fn heard() -> Model {
    Model::new(r#"{"text":"heard","want_to_say":[],"urgent":false}"#)
}

fn class(error: &ChannelError) -> &str {
    error.downcast_ref::<ClassedError>().unwrap().class()
}

// Python: tests/test_thinker.py::test_llm_thinker_reads_the_agent_the_previous_thought_then_the_conversation
#[tokio::test]
async fn llm_thinker_reads_the_agent_the_previous_thought_then_the_conversation() {
    let model = Model::new(json!({"text":"They need the price.","want_to_say":[" 1,200 $ ","","a","b","c"],"urgent":true}).to_string());
    let thinker = LlmThinker::new(model.clone(), LlmThinkerOptions::default()).unwrap();
    let context = SharedAiContext::new(AiContext {
        system_prompt: Some("You are Nova, the team's assistant.".into()),
        messages: vec![
            AiMessage::new(AiRole::User, "Paul: what does the licence cost?"),
            AiMessage::new(AiRole::Assistant, "Let me check."),
            AiMessage::new(AiRole::Tool, "{}"),
        ],
        metadata: json!({"secret":"not forwarded"})
            .as_object()
            .unwrap()
            .clone(),
        ..AiContext::default()
    });
    let thought = thinker
        .think(
            &Thought {
                text: "Earlier.".into(),
                ..Thought::default()
            },
            &context,
        )
        .await
        .unwrap();
    assert_eq!(
        thought,
        Thought {
            text: "They need the price.".into(),
            want_to_say: vec!["1,200 $".into(), "a".into(), "b".into()],
            urgent: true
        }
    );
    let calls = model.recorded();
    let request = calls.first().unwrap();
    assert!(request.response_schema.is_some());
    assert!(
        request
            .system_prompt
            .as_deref()
            .unwrap()
            .contains("<agent>\nYou are Nova, the team's assistant.\n</agent>")
    );
    assert!(
        request
            .system_prompt
            .as_deref()
            .unwrap()
            .contains("3 sentences at most")
    );
    let text = request.messages.first().unwrap().text();
    assert_eq!(
        text.lines().skip(1).collect::<Vec<_>>(),
        [
            "The conversation, up to what was just said:",
            "“Paul: what does the licence cost?”",
            "You: “Let me check.”",
            "Your thought, now:"
        ]
    );
    assert!(text.lines().next().unwrap().contains("Earlier."));
    assert_eq!(request.max_tokens, Some(600));
    assert!(request.tools.is_empty() && request.metadata.is_empty() && request.room.is_none());
}

// Python: tests/test_thinker.py::test_llm_thinker_replaces_its_instructions_and_fails_on_no_json
#[tokio::test]
async fn llm_thinker_replaces_its_instructions_and_fails_on_no_json() {
    let model = Model::new("not json");
    let thinker = LlmThinker::new(
        model.clone(),
        LlmThinkerOptions {
            instructions: Some("Think briefly, {max} items at most.".into()),
            ..LlmThinkerOptions::default()
        },
    )
    .unwrap();
    let error = thinker
        .think(
            &Thought::default(),
            &SharedAiContext::new(AiContext::default()),
        )
        .await
        .unwrap_err();
    let error = error.downcast_ref::<ProviderError>().unwrap();
    assert!(error.message.contains("JSON"));
    assert!(std::error::Error::source(error).is_some());
    assert_eq!(
        model.recorded().first().unwrap().system_prompt.as_deref(),
        Some("Think briefly, 3 items at most.")
    );
}

// Python: tests/test_thinker.py::test_llm_thinker_refuses_a_provider_without_response_schema
#[test]
fn llm_thinker_refuses_a_provider_without_response_schema() {
    let error = LlmThinker::new(
        Model {
            schema: false,
            ..heard()
        },
        LlmThinkerOptions::default(),
    )
    .err()
    .unwrap();
    assert_eq!(class(&error), "ValueError");
    assert!(error.to_string().contains("response schema"));
}

// Python: tests/test_speaking_thinker_options.py::test_a_nonpositive_thinker_bound_never_enters_even_an_immediate_provider
#[tokio::test]
async fn a_nonpositive_thinker_bound_never_enters_even_an_immediate_provider() {
    for timeout in [0.0, -0.1, f64::NEG_INFINITY] {
        for held in [false, true] {
            let model = Model { held, ..heard() };
            let thinker = LlmThinker::new(
                model.clone(),
                LlmThinkerOptions {
                    timeout: Some(timeout),
                    ..LlmThinkerOptions::default()
                },
            )
            .unwrap();
            let error = thinker
                .think(
                    &Thought::default(),
                    &SharedAiContext::new(AiContext::default()),
                )
                .await
                .unwrap_err();
            assert_eq!(class(&error), "TimeoutError");
            assert_eq!(model.recorded(), Vec::new());
            assert!(!model.finished.load(Ordering::Acquire));
        }
    }
}

// Python: tests/test_speaking_thinker_options.py::test_a_positive_thinker_bound_cancels_and_drains_the_provider_call
// Python: tests/test_speaking_thinker_options.py::test_a_nan_thinker_bound_enters_then_expires_only_if_the_provider_waits
#[tokio::test(start_paused = true)]
async fn a_positive_or_nan_bound_drops_the_pending_provider_and_allows_an_immediate_answer() {
    for timeout in [0.01, f64::NAN] {
        let model = Model {
            held: true,
            ..heard()
        };
        let thinker = LlmThinker::new(
            model.clone(),
            LlmThinkerOptions {
                timeout: Some(timeout),
                ..LlmThinkerOptions::default()
            },
        )
        .unwrap();
        let error = thinker
            .think(
                &Thought::default(),
                &SharedAiContext::new(AiContext::default()),
            )
            .await
            .unwrap_err();
        assert_eq!(class(&error), "TimeoutError");
        assert_eq!(model.recorded().len(), 1);
        assert!(model.finished.load(Ordering::Acquire));
        let thinker = LlmThinker::new(
            heard(),
            LlmThinkerOptions {
                timeout: Some(timeout),
                ..LlmThinkerOptions::default()
            },
        )
        .unwrap();
        assert_eq!(
            thinker
                .think(
                    &Thought::default(),
                    &SharedAiContext::new(AiContext::default())
                )
                .await
                .unwrap()
                .text,
            "heard"
        );
    }
}

// Python: tests/test_speaking_thinker_options.py::test_an_unbounded_or_huge_thinker_waits_for_a_provider_that_was_really_entered
#[tokio::test(start_paused = true)]
async fn an_unbounded_or_huge_thinker_waits_for_a_provider_that_was_really_entered() {
    for timeout in [None, Some(f64::INFINITY), Some(1e300)] {
        let model = Model {
            held: true,
            ..heard()
        };
        let thinker = LlmThinker::new(
            model.clone(),
            LlmThinkerOptions {
                timeout,
                ..LlmThinkerOptions::default()
            },
        )
        .unwrap();
        let running = tokio::spawn(async move {
            thinker
                .think(
                    &Thought::default(),
                    &SharedAiContext::new(AiContext::default()),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), model.entered.notified())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!running.is_finished() && !model.finished.load(Ordering::Acquire));
        model.release.notify_one();
        assert_eq!(running.await.unwrap().unwrap().text, "heard");
        assert_eq!(model.recorded().len(), 1);
    }
}

// Python: tests/test_speaking_thinker_options.py::test_the_thinker_passes_the_token_bound_through_to_the_provider
#[tokio::test]
async fn the_thinker_passes_its_representable_token_bound_and_effort_to_the_provider() {
    for max_tokens in [None, Some(0), Some(600), Some(u32::MAX)] {
        let model = heard();
        let thinker = LlmThinker::new(
            model.clone(),
            LlmThinkerOptions {
                max_tokens,
                reasoning_effort: Some("low".into()),
                ..LlmThinkerOptions::default()
            },
        )
        .unwrap();
        thinker
            .think(
                &Thought::default(),
                &SharedAiContext::new(AiContext::default()),
            )
            .await
            .unwrap();
        assert_eq!(model.recorded().first().unwrap().max_tokens, max_tokens);
        assert_eq!(
            model
                .recorded()
                .first()
                .unwrap()
                .reasoning_effort
                .as_deref(),
            Some("low")
        );
    }
}

// Python: tests/test_speaking_thinker_options.py::test_the_thinker_reads_its_label_as_a_word_not_a_name_skeleton
#[tokio::test]
async fn the_thinker_reads_its_label_as_a_word_not_a_name_skeleton() {
    for (name, participant) in [
        ("You", true),
        ("YOU!", true),
        ("Yоu", true),
        ("Ｙｏｕ", true),
        ("Y\u{200b}o\u{345}u", true),
        ("Y o u", false),
        ("Y.o.u", false),
        ("You!more", false),
    ] {
        let model = heard();
        let thinker = LlmThinker::new(model.clone(), LlmThinkerOptions::default()).unwrap();
        let mut message = AiMessage::new(AiRole::User, format!("{name}: \"hello\""));
        message.metadata.insert("speaker".into(), Value::from(name));
        thinker
            .think(
                &Thought::default(),
                &SharedAiContext::new(AiContext {
                    messages: vec![message],
                    ..AiContext::default()
                }),
            )
            .await
            .unwrap();
        let label = if participant {
            format!("{name} (a participant)")
        } else {
            name.into()
        };
        assert_eq!(
            model
                .recorded()
                .first()
                .unwrap()
                .messages
                .first()
                .unwrap()
                .text()
                .lines()
                .nth(2),
            Some(format!("{label}: “hello”").as_str())
        );
    }
}

// Python: tests/test_speaking_thinker_options.py::test_the_thinker_keeps_previous_state_json_and_separates_a_labelled_summary
#[tokio::test]
async fn the_thinker_keeps_previous_state_json_and_separates_a_labelled_summary() {
    let model = heard();
    let thinker = LlmThinker::new(model.clone(), LlmThinkerOptions::default()).unwrap();
    let previous = Thought {
        text: "Earlier.\u{2028}“Quoted”\u{202e}".into(),
        want_to_say: vec!["say\u{85}this".into()],
        urgent: true,
    };
    let mut message = AiMessage::new(AiRole::User, "Summary.\n\nAlice: \"Bob: no.\\nNew line.\"");
    message.metadata = json!({"speaker":"Alice","leading_text":"Summary."})
        .as_object()
        .unwrap()
        .clone();
    thinker
        .think(
            &previous,
            &SharedAiContext::new(AiContext {
                messages: vec![message],
                ..AiContext::default()
            }),
        )
        .await
        .unwrap();
    let calls = model.recorded();
    let text = calls.first().unwrap().messages.first().unwrap().text();
    let lines = text.lines().collect::<Vec<_>>();
    let state: Value = serde_json::from_str(
        lines
            .first()
            .unwrap()
            .strip_prefix("Your previous thought: ")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(state, Value::Object(previous.as_state()));
    assert_eq!(
        lines.into_iter().skip(1).collect::<Vec<_>>(),
        [
            "The conversation, up to what was just said:",
            "“Summary.”",
            "Alice: “Bob: no. New line.”",
            "Your thought, now:",
        ]
    );
}

// Python: tests/test_thinker.py::test_thinker_input_leaves_the_turn_notes_out
#[tokio::test]
async fn the_thinker_leaves_turn_notes_out_and_keeps_its_supplied_provider_open() {
    let model = heard();
    let thinker = LlmThinker::new(model.clone(), LlmThinkerOptions::default()).unwrap();
    let messages = roomkit_ai::add_turn_note(
        vec![AiMessage::new(AiRole::User, "What now?")],
        "Answer in French.",
    );
    thinker
        .think(
            &Thought::default(),
            &SharedAiContext::new(AiContext {
                messages,
                ..AiContext::default()
            }),
        )
        .await
        .unwrap();
    let calls = model.recorded();
    let text = calls.first().unwrap().messages.first().unwrap().text();
    assert!(text.contains("What now?"));
    assert!(!text.contains("Answer in French"));
    thinker.close().await.unwrap();
    assert!(!model.closed.load(Ordering::Acquire));
}
