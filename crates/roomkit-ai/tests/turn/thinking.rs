//! A round's reasoning for the room's observers, where the shared scenarios
//! (`conformance/scenarios/streaming/`) cannot say it: a round that reasons
//! again after its text or a call, which a scripted round does not write.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, RealtimeBackend, RealtimeFuture,
};
use roomkit::{RoomKit, RoomKitOptions, SubscribeRoomOptions};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiTool, ProviderError,
    ProviderStream, StreamDone, StreamEvent, StreamTextDelta, StreamThinkingDelta, StreamToolCall,
    ToolResult, Tools,
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
        publish_thinking_text: true,
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

// Python: tests/test_ai_streaming_thinking.py::test_the_tool_loop_end_of_round_close_also_scopes_its_block
// and test_no_tools_path_also_scopes_each_window_to_its_block: the second
// block closes at the end of the stream.
#[tokio::test]
async fn the_end_of_a_round_closes_its_last_block() {
    let events = vec![
        thinking("First."),
        text("Some answer."),
        thinking("Last."),
        StreamEvent::Done(StreamDone::default()),
    ];
    assert_eq!(blocks(events).await, ["First.", "Last."]);
}

// Python: channels/_ai_stream_round.py stream: a whole call closes no window,
// only a fragment (`StreamToolCallDelta`), which no provider here hands.
#[tokio::test]
async fn a_whole_call_closes_no_window() {
    let call = StreamToolCall::new("c1", "lookup");
    let events = vec![
        thinking("Before."),
        StreamEvent::ToolCall(call),
        thinking("After."),
        StreamEvent::Done(StreamDone::default()),
    ];
    assert_eq!(blocks(events).await, ["Before.After."]);
}

/// A realtime backend whose every publish never returns.
struct Stuck;

impl RealtimeBackend for Stuck {
    fn publish<'a>(&'a self, _channel: &'a str, _event: EphemeralEvent) -> RealtimeFuture<'a, ()> {
        Box::pin(std::future::pending())
    }

    fn subscribe<'a>(
        &'a self,
        _channel: &'a str,
        _callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        Box::pin(std::future::ready(String::new()))
    }

    fn unsubscribe<'a>(&'a self, _subscription_id: &'a str) -> RealtimeFuture<'a, bool> {
        Box::pin(std::future::ready(false))
    }
}

// Python: turns/_owner_publish.py TurnPublisher: what a turn publishes never
// stops the turn, whatever its backend does.
#[tokio::test]
async fn a_stuck_realtime_backend_stops_no_turn() {
    let provider = Streams(Mutex::new(
        vec![
            thinking("Hm."),
            text("Done."),
            StreamEvent::Done(StreamDone::default()),
        ]
        .into(),
    ));
    let agent =
        AiChannel::new(channel("assistant"), provider, AiChannelOptions::default()).unwrap();
    let kit = RoomKit::new(RoomKitOptions {
        realtime: Some(Arc::new(Stuck)),
        ..RoomKitOptions::default()
    });
    let room = Room::on_kit(kit, agent).await;
    let said = tokio::time::timeout(Duration::from_secs(5), room.says("Why?")).await;
    assert!(said.is_ok(), "the turn waited on the backend");
    // Not closed: the publisher stuck on it would take the close's grace.
}

// Python: turns/_owner_turn.py TurnPublisher.close at the turn's end: a turn
// that reasoned leaves nothing for a close from inside it to wait on, which
// waits for no work of its own turn (RFC §12.10.4).
#[tokio::test]
async fn a_close_from_within_a_turn_that_reasoned_takes_no_grace() {
    let provider = Streams(Mutex::new(
        vec![
            thinking("Hm."),
            StreamEvent::ToolCall(StreamToolCall::new("c1", "stop")),
            StreamEvent::Done(StreamDone::default()),
        ]
        .into(),
    ));
    let kit = RoomKit::new(RoomKitOptions::default());
    let weak = kit.downgrade();
    let stop = AiTool::new("stop", "Stops the kit").unwrap();
    let tools = Tools::new().tool(stop, move |_call, _context| {
        let weak = weak.clone();
        async move {
            if let Some(kit) = weak.upgrade() {
                // Past the publisher's first drain.
                for _ in 0..20 {
                    tokio::task::yield_now().await;
                }
                drop(kit.close().await);
            }
            Ok(ToolResult::Text("closed".to_owned()))
        }
    });
    let options = AiChannelOptions {
        tools: tools.into(),
        ..AiChannelOptions::default()
    };
    let agent = AiChannel::new(channel("assistant"), provider, options).unwrap();
    let room = Room::on_kit(kit, agent).await;
    tokio::time::timeout(Duration::from_secs(2), room.says("Stop."))
        .await
        .expect("the close took the grace");
    tokio::time::timeout(Duration::from_secs(2), room.kit.close())
        .await
        .expect("the close took the grace")
        .unwrap();
}
