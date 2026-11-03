//! A round's reasoning for the room's observers, where the shared scenarios
//! (`conformance/scenarios/streaming/`) cannot say it: a round that reasons
//! again after its text or a call, which a scripted round does not write.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::SubscribeRoomOptions;
use roomkit::realtime::{EphemeralEvent, EphemeralEventType};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, ProviderError, ProviderStream,
    StreamDone, StreamEvent, StreamTextDelta, StreamThinkingDelta, StreamToolCall,
};
use serde_json::Value;

use super::support::{Room, channel, room_id};

/// A model that streams `events`, whatever it is asked.
struct Streams(Mutex<VecDeque<StreamEvent>>);

impl AiProvider for Streams {
    fn model_name(&self) -> &'static str {
        "streams"
    }

    fn generate<'a>(
        &'a self,
        _context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        std::future::ready(Ok(AiResponse::new("")))
    }

    fn generate_structured_stream<'a>(&'a self, _context: &'a AiContext) -> ProviderStream<'a> {
        let events = std::mem::take(&mut *self.0.lock().unwrap_or_else(PoisonError::into_inner));
        Box::pin(Events(events))
    }
}

/// The events of one generation.
struct Events(VecDeque<StreamEvent>);

impl Stream for Events {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().0.pop_front().map(Ok))
    }
}

fn thinking(text: &str) -> StreamEvent {
    StreamEvent::ThinkingDelta(StreamThinkingDelta::new(text))
}

fn text(text: &str) -> StreamEvent {
    StreamEvent::TextDelta(StreamTextDelta::new(text))
}

/// The `THINKING_END` blocks a turn on `events` publishes, in order.
async fn blocks(events: Vec<StreamEvent>) -> Vec<String> {
    let provider = Streams(Mutex::new(events.into()));
    let options = AiChannelOptions {
        thinking_coalesce_ms: 60_000.0,
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(channel("assistant"), provider, options).unwrap();
    let room = Room::with_agent(agent).await;
    let ended: Arc<Mutex<Vec<String>>> = Arc::default();
    let seen = Arc::clone(&ended);
    let record = move |event: EphemeralEvent| {
        let seen = Arc::clone(&seen);
        async move {
            if event.r#type == EphemeralEventType::ThinkingEnd {
                let block = event.data.get("thinking").and_then(Value::as_str);
                let block = block.unwrap_or_default().to_owned();
                seen.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(block);
            }
        }
    };
    room.kit
        .subscribe_room(&room_id(), record, SubscribeRoomOptions::default())
        .await
        .unwrap();
    room.says("Why?").await;
    roomkit::testing::idle(&room.kit).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    room.kit.close().await.unwrap();
    ended.lock().unwrap_or_else(PoisonError::into_inner).clone()
}

// Python: tests/test_ai_streaming_thinking.py::test_each_thinking_end_carries_only_its_own_block_across_text
// and test_no_tools_path_also_scopes_each_window_to_its_block.
#[tokio::test]
async fn each_thinking_end_carries_only_its_own_block_across_text() {
    let events = vec![
        thinking("First "),
        thinking("block."),
        text("Some answer."),
        thinking("Second block."),
        text("The rest."),
        StreamEvent::Done(StreamDone::default()),
    ];
    assert_eq!(blocks(events).await, ["First block.", "Second block."]);
}

// Python: tests/test_ai_streaming_thinking.py::test_each_thinking_end_carries_only_its_own_block_across_a_tool_call
#[tokio::test]
async fn each_thinking_end_carries_only_its_own_block_across_a_tool_call() {
    let call = StreamToolCall::new("c1", "lookup");
    let events = vec![
        thinking("Before."),
        StreamEvent::ToolCall(call),
        thinking("After."),
        StreamEvent::Done(StreamDone::default()),
    ];
    assert_eq!(blocks(events).await, ["Before.", "After."]);
}
