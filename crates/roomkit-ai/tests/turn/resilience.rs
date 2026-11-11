//! A turn a fallback provider answered (RFC §6.4): its record names the
//! provider that served it. And a stream read past its done event, where
//! the shared scenarios cannot say it: a scripted round writes no event
//! after its end.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::SteerOptions;
use roomkit::model::{Cancel, Metadata};
use roomkit::testing::TurnTap;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiToolCall, Date,
    FallbackRouter, MockAiProvider, MockAiProviderOptions, ModelInfo, ModelPricing, ProviderError,
    ProviderStream, StreamDone, StreamEvent, StreamTextDelta, Tools, tool_arguments,
};
use roomkit_core::rules::transcoding::text_of;
use serde_json::{Value, json};

use super::bounds::loop_end;
use super::support::{Room, channel, room_id, served};

/// What a [`Streams`] model does next.
enum Step {
    /// Hands an event.
    Event(Box<StreamEvent>),
    /// Waits for this, then goes on.
    Wait(Pin<Box<dyn Future<Output = ()> + Send>>),
}

/// A model that takes `steps`, whatever it is asked, at `pricing`.
#[derive(Clone, Default)]
struct Streams {
    steps: Arc<Mutex<VecDeque<Step>>>,
    pricing: Option<ModelPricing>,
}

impl Streams {
    fn set(&self, steps: Vec<Step>) {
        *self.steps.lock().unwrap_or_else(PoisonError::into_inner) = steps.into();
    }
}

impl AiProvider for Streams {
    fn model_name(&self) -> &'static str {
        "streams"
    }

    fn catalog_entry(&self) -> Option<ModelInfo> {
        self.pricing.clone().map(|pricing| ModelInfo {
            pricing: Some(pricing),
            ..ModelInfo::new("streams")
        })
    }

    fn generate<'a>(
        &'a self,
        _context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        std::future::ready(Ok(AiResponse::new("")))
    }

    fn generate_structured_stream<'a>(&'a self, _context: &'a AiContext) -> ProviderStream<'a> {
        let steps = std::mem::take(&mut *self.steps.lock().unwrap_or_else(PoisonError::into_inner));
        Box::pin(Steps(steps))
    }
}

/// The stream a [`Streams`] model hands.
struct Steps(VecDeque<Step>);

impl Stream for Steps {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let steps = &mut self.get_mut().0;
        loop {
            match steps.pop_front() {
                None => return Poll::Ready(None),
                Some(Step::Event(event)) => return Poll::Ready(Some(Ok(*event))),
                Some(Step::Wait(mut wait)) => {
                    if wait.as_mut().poll(context).is_pending() {
                        steps.push_front(Step::Wait(wait));
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

fn text(text: &str) -> Step {
    Step::Event(Box::new(StreamEvent::TextDelta(StreamTextDelta::new(text))))
}

fn done(finish_reason: &str, usage: Value) -> Step {
    Step::Event(Box::new(StreamEvent::Done(StreamDone {
        finish_reason: Some(finish_reason.to_owned()),
        usage: serde_json::from_value::<Metadata>(usage).unwrap(),
        ..StreamDone::default()
    })))
}

/// The agent's room over `model`.
async fn room_over(model: Streams) -> Room {
    room_under(model, AiChannelOptions::default()).await
}

/// The agent's room over `model`, under `options`.
async fn room_under(model: Streams, options: AiChannelOptions) -> Room {
    let agent = AiChannel::new(channel("assistant"), model, options).unwrap();
    Room::with_agent(agent).await
}

/// A price of $1 per input token and output token, a flat one unless
/// `long_context_threshold_tokens` says.
fn a_dollar_a_token(long_context_threshold_tokens: Option<u64>) -> ModelPricing {
    let verified = Date::new(2026, 8, 5).unwrap();
    ModelPricing {
        long_context_threshold_tokens,
        long_context_input_multiplier: 2.0,
        ..ModelPricing::new(1_000_000.0, 1_000_000.0, verified).unwrap()
    }
}

/// The turn's answer: its text, and what its record says it used.
async fn answer(room: &Room) -> (String, Value, Value, Value) {
    let rows = room.timeline().await;
    let row = rows
        .iter()
        .find(|row| row.metadata.contains_key("loop_end_reason"))
        .unwrap();
    let read = |name: &str| row.metadata.get(name).cloned().unwrap_or_default();
    (
        text_of(&row.content),
        read("ai_usage"),
        read("ai_last_usage"),
        read("loop_end_reason"),
    )
}

/// Python's `_StreamRound.stream` reads a stream to its end, taking each
/// event as it comes: what follows a done event reaches the room, each done
/// event's usage counts on the turn's, and one with none leaves the last
/// usage as it was.
#[tokio::test]
async fn a_stream_is_read_past_its_done_event() {
    let first = json!({ "input_tokens": 10, "output_tokens": 2 });
    let cases = [
        (
            json!({ "input_tokens": 1, "output_tokens": 1 }),
            json!({ "input_tokens": 11, "output_tokens": 3 }),
            json!({ "input_tokens": 1, "output_tokens": 1 }),
        ),
        (json!({}), first.clone(), first.clone()),
    ];
    for (second, total, last) in cases {
        let model = Streams::default();
        let room = room_over(model.clone()).await;
        model.set(vec![
            text("Voici"),
            done("stop", first.clone()),
            text(" la suite"),
            done("stop", second),
        ]);
        room.says("Hello?").await;
        assert_eq!(
            answer(&room).await,
            ("Voici la suite".to_owned(), total, last, json!("completed"))
        );
        room.kit.close().await.unwrap();
    }
}

/// A `Cancel` read at an event past the done event stops the read: that
/// event is dropped, and the usage the done event reported stays the
/// generation's, as Python's round keeps it once its stream was cut.
#[tokio::test]
async fn a_cancel_past_a_done_event_keeps_what_it_reported() {
    let used = json!({ "input_tokens": 10, "output_tokens": 2 });
    let model = Streams::default();
    let room = room_over(model.clone()).await;
    let kit = room.kit.downgrade();
    let cancel = Box::pin(async move {
        let kit = kit.upgrade().unwrap();
        kit.steer(
            &room_id(),
            &channel("assistant"),
            Cancel::default(),
            SteerOptions::default(),
        )
        .await
        .unwrap();
    });
    model.set(vec![
        text("Voici"),
        done("stop", used.clone()),
        Step::Wait(cancel),
        text(" jamais"),
    ]);
    room.says("Hello?").await;
    assert_eq!(
        answer(&room).await,
        ("Voici".to_owned(), used.clone(), used, json!("cancelled"))
    );
    room.kit.close().await.unwrap();
}

/// Python's `_ToolLoopState.count` raises at the done event itself, so its
/// round reads nothing after a usage the price cannot read: the turn's
/// failure is that, and what followed never reaches the room.
#[tokio::test]
async fn a_usage_the_price_cannot_read_stops_the_read_at_its_done_event() {
    let model = Streams {
        pricing: Some(a_dollar_a_token(None)),
        ..Streams::default()
    };
    let options = AiChannelOptions {
        turn_budget_usd: Some(100.0),
        ..AiChannelOptions::default()
    };
    let room = room_under(model.clone(), options).await;
    model.set(vec![
        text("Voici"),
        done("stop", json!({ "input_tokens": "many" })),
        text(" jamais"),
    ]);
    let result = room.says("Hello?").await;
    let message = result.error.map(|error| error.message).unwrap_or_default();
    assert!(message.contains("input_tokens"), "{message}");
    let said: Vec<String> = room
        .timeline()
        .await
        .iter()
        .map(|row| text_of(&row.content))
        .collect();
    assert!(said.iter().all(|text| !text.contains("jamais")), "{said:?}");
    room.kit.close().await.unwrap();
}

/// Python prices each done event as it comes, `cost_for` reading one
/// response's usage: two of 150 input tokens stay under a long-context
/// threshold of 200, at $300, where their sum would cross it, at $600.
#[tokio::test]
async fn each_done_event_is_priced_as_one_response() {
    let model = Streams {
        pricing: Some(a_dollar_a_token(Some(200))),
        ..Streams::default()
    };
    let options = AiChannelOptions {
        turn_budget_usd: Some(400.0),
        max_empty_retries: 0,
        tools: served().into(),
        ..AiChannelOptions::default()
    };
    let room = room_under(model.clone(), options).await;
    let tap = TurnTap::on(&room.kit);
    let call = AiToolCall {
        arguments: tool_arguments(r#"{"order": "1"}"#),
        ..AiToolCall::new("c1", "lookup_order")
    };
    let used = json!({ "input_tokens": 150, "output_tokens": 0 });
    model.set(vec![
        Step::Event(Box::new(StreamEvent::ToolCall(call.into()))),
        done("tool_calls", used.clone()),
        done("tool_calls", used),
    ]);
    room.says("Order 1?").await;
    assert_eq!(loop_end(&tap).0, json!("empty_response"));
    room.kit.close().await.unwrap();
}

// Python: tests/test_channels/test_ai_retry.py::TestFallbackRouter::test_fallback_triggers_after_retry_exhaustion
#[tokio::test]
async fn a_turn_the_fallback_answered_names_the_fallback_in_its_record() {
    let primary = ScriptedProvider::new([ScriptedRound::answering("")
        .refusing(503, "overloaded")
        .retrying()]);
    let fallback = MockAiProvider::new(MockAiProviderOptions {
        responses: vec!["from fallback".to_owned()],
        ..MockAiProviderOptions::default()
    });
    let names = (
        json!(fallback.model_name()),
        json!(AiProvider::name(&fallback)),
    );
    let router = FallbackRouter::new(Arc::new(primary), Arc::new(fallback));
    let options = AiChannelOptions {
        tools: Tools::default().into(),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::routed(channel("assistant"), router, options).unwrap();
    let room = Room::with_agent(agent).await;
    room.says("Hello?").await;
    let rows = room.timeline().await;
    let answer = rows
        .iter()
        .find(|row| row.metadata.contains_key("loop_end_reason"))
        .unwrap();
    assert_eq!(
        (
            answer.metadata["ai_model"].clone(),
            answer.metadata["ai_provider"].clone()
        ),
        names
    );
    room.kit.close().await.unwrap();
}
