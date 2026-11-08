//! `AiChannel::active_turns`: every turn the channel runs is counted, from
//! its first round to its end, as Python's `AIChannel.active_turns` counts
//! its loops. A caller retiring a channel reads it before closing it: a
//! channel taken out of the kit may still be answering a turn that holds
//! it.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::Channel;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventContent,
    EventSource, Room, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit_ai::memory::{MemoryProvider, MockMemoryProvider, MockMemoryProviderOptions};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiProvider, AiResponse, AiTool, ProviderError,
    ProviderStream, StreamEvent, StreamTextDelta, TextStream, ToolHandler, ToolResult,
};
use tokio::sync::{Notify, Semaphore};
use tokio::time::{Duration, Instant};

fn room_id() -> RoomId {
    RoomId::new("room-1").unwrap()
}

/// What a test provider holds each generation on: a gate that has no permit
/// until the test opens it, then one each generation takes and gives back,
/// and the signal that a generation reached it.
#[derive(Clone)]
struct Gate {
    permits: Arc<Semaphore>,
    reached: Arc<Notify>,
}

impl Gate {
    fn new() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(0)),
            reached: Arc::default(),
        }
    }

    /// Lets every generation held, and the next ones, go on.
    fn open(&self) {
        self.permits.add_permits(1);
    }

    /// Waits until a generation reached the gate.
    async fn reached(&self) {
        self.reached.notified().await;
    }

    /// The wait of a generation at the gate.
    fn wait(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            drop(self.permits.acquire().await);
        })
    }
}

/// A provider that streams each generation: "Working ", then, past its
/// gate, what its script says, or a failure when it `fails`.
#[derive(Clone)]
struct Gated {
    script: ScriptedProvider,
    gate: Gate,
    fails: bool,
}

impl Gated {
    fn new(rounds: impl IntoIterator<Item = ScriptedRound>) -> Self {
        Self {
            script: ScriptedProvider::new(rounds),
            gate: Gate::new(),
            fails: false,
        }
    }
}

/// The stream of a generation [`Gated`] holds.
struct HeldStream<'a> {
    first: Option<StreamEvent>,
    reached: Arc<Notify>,
    gate: Option<Pin<Box<dyn Future<Output = ()> + Send + 'a>>>,
    fails: bool,
    rest: ProviderStream<'a>,
}

impl Stream for HeldStream<'_> {
    type Item = Result<StreamEvent, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(first) = this.first.take() {
            this.reached.notify_one();
            return Poll::Ready(Some(Ok(first)));
        }
        if let Some(gate) = this.gate.as_mut() {
            if gate.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            this.gate = None;
            if this.fails {
                return Poll::Ready(Some(Err("provider went away".into())));
            }
        }
        this.rest.as_mut().poll_next(context)
    }
}

impl AiProvider for Gated {
    fn model_name(&self) -> &'static str {
        "gated"
    }

    fn generate<'a>(
        &'a self,
        context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        self.script.generate(context)
    }

    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        Box::pin(HeldStream {
            first: Some(StreamEvent::TextDelta(StreamTextDelta::new("Working "))),
            reached: Arc::clone(&self.gate.reached),
            gate: Some(self.gate.wait()),
            fails: self.fails,
            rest: self.script.generate_structured_stream(context),
        })
    }
}

/// A provider read through the default structured stream: one that streams
/// its text alone ("Working ", then, past its gate, "done"), or, when it
/// does not stream, one whose `generate` waits at its gate.
#[derive(Clone)]
struct Plain {
    script: ScriptedProvider,
    gate: Gate,
    streams_text: bool,
}

/// The text a [`Plain`] provider streams.
struct HeldText<'a> {
    pieces: Vec<&'static str>,
    reached: Arc<Notify>,
    gate: Option<Pin<Box<dyn Future<Output = ()> + Send + 'a>>>,
}

impl Stream for HeldText<'_> {
    type Item = Result<String, ProviderError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.pieces.len() == 2 {
            this.reached.notify_one();
            return Poll::Ready(Some(Ok(this.pieces.remove(0).to_owned())));
        }
        if let Some(gate) = this.gate.as_mut() {
            if gate.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            this.gate = None;
        }
        Poll::Ready(this.pieces.pop().map(|piece| Ok(piece.to_owned())))
    }
}

impl AiProvider for Plain {
    fn model_name(&self) -> &'static str {
        "plain"
    }

    fn supports_streaming(&self) -> bool {
        self.streams_text
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        self.gate.reached.notify_one();
        self.gate.wait().await;
        self.script.generate(context).await
    }

    fn generate_stream<'a>(&'a self, _context: &'a AiContext) -> TextStream<'a> {
        Box::pin(HeldText {
            pieces: vec!["Working ", "done"],
            reached: Arc::clone(&self.gate.reached),
            gate: Some(self.gate.wait()),
        })
    }
}

/// A turn of `agent` on a message, run apart.
fn turn<P: AiProvider>(
    agent: &AiChannel<P>,
) -> tokio::task::JoinHandle<Result<ChannelOutput, String>> {
    let agent = agent.clone();
    tokio::spawn(async move {
        let event = RoomEvent::new(
            room_id(),
            EventSource::new(ChannelId::new("sms1").unwrap(), ChannelType::SMS),
            EventContent::Text(TextContent::new("hi")),
        );
        let binding = ChannelBinding {
            category: ChannelCategory::Intelligence,
            ..ChannelBinding::new(ChannelId::new("ai1").unwrap(), room_id(), ChannelType::AI)
        };
        let context = RoomContext::new(Room::new(room_id()));
        agent
            .on_event(event, binding, context)
            .await
            .map_err(|error| error.to_string())
    })
}

fn channel<P: AiProvider>(provider: P, options: AiChannelOptions) -> AiChannel<P> {
    AiChannel::new(ChannelId::new("ai1").unwrap(), provider, options).unwrap()
}

/// An agent with a `search` tool its handler answers "ok".
fn with_search() -> AiChannelOptions {
    AiChannelOptions {
        tools: vec![AiTool::new("search", "Search").unwrap().into()],
        tool_handler: Some(ToolHandler::new(|_name, _arguments| async {
            Ok(ToolResult::Text("ok".to_owned()))
        })),
        ..AiChannelOptions::default()
    }
}

/// The text of the message a turn answered.
fn answer(output: &ChannelOutput) -> String {
    output
        .response_events
        .iter()
        .filter_map(|event| {
            if let EventContent::Text(text) = &event.content {
                Some(text.body.clone())
            } else {
                None
            }
        })
        .collect()
}

// Python: tests/test_channels/test_ai_active_turns.py::TestActiveTurns::test_idle_channel_counts_zero
#[test]
fn idle_channel_counts_zero() {
    let agent = channel(Gated::new([]), AiChannelOptions::default());
    assert_eq!(agent.active_turns(), 0);
}

// Python: tests/test_channels/test_ai_active_turns.py::TestActiveTurns::test_text_stream_counts_from_first_consumption_to_its_end
#[tokio::test]
async fn text_stream_counts_from_its_start_to_its_end() {
    let provider = Gated::new([ScriptedRound::answering("Hello from AI")]);
    let agent = channel(provider.clone(), AiChannelOptions::default());
    let running = turn(&agent);
    provider.gate.reached().await;
    assert_eq!(agent.active_turns(), 1);
    provider.gate.open();
    let output = running.await.unwrap().unwrap();
    assert_eq!(answer(&output), "Working Hello from AI");
    assert_eq!(agent.active_turns(), 0);
}

// Python: tests/test_channels/test_ai_active_turns.py::TestActiveTurns::test_streaming_tool_loop_counts_through_the_registry
#[tokio::test]
async fn streaming_tool_loop_counts_through_the_registry() {
    let provider = Gated::new([
        ScriptedRound::calling("", [("tc1", "search", "{}")]),
        ScriptedRound::answering("Done"),
    ]);
    let agent = channel(provider.clone(), with_search());
    let running = turn(&agent);
    provider.gate.reached().await;
    assert_eq!(agent.active_turns(), 1);
    provider.gate.open();
    running.await.unwrap().unwrap();
    assert_eq!(agent.active_turns(), 0);
}

// Python: tests/test_channels/test_ai_active_turns.py::TestActiveTurns::test_a_turn_read_through_generate_counts_while_generating
#[tokio::test]
async fn a_turn_read_through_generate_counts_while_generating() {
    let provider = Plain {
        script: ScriptedProvider::new([ScriptedRound::answering("Hello from AI")]),
        gate: Gate::new(),
        streams_text: false,
    };
    let agent = channel(provider.clone(), AiChannelOptions::default());
    let running = turn(&agent);
    provider.gate.reached().await;
    assert_eq!(agent.active_turns(), 1);
    provider.gate.open();
    running.await.unwrap().unwrap();
    assert_eq!(agent.active_turns(), 0);
}

// Python: tests/test_channels/test_ai_active_turns.py::TestActiveTurns::test_stream_cancelled_from_the_outside_returns_to_zero
#[tokio::test]
async fn stream_cancelled_from_the_outside_returns_to_zero() {
    let provider = Gated::new([ScriptedRound::answering("Hello from AI")]);
    let agent = channel(provider.clone(), AiChannelOptions::default());
    let running = turn(&agent);
    provider.gate.reached().await;
    assert_eq!(agent.active_turns(), 1);
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    assert_eq!(agent.active_turns(), 0);
}

// Python: tests/test_channels/test_ai_active_turns.py::TestActiveTurns::test_a_caller_that_waits_for_idle_closes_after_the_stream
#[tokio::test]
async fn a_caller_that_waits_for_idle_closes_after_the_stream() {
    let provider = Gated::new([ScriptedRound::answering("Hello from AI")]);
    let memory = Arc::new(MockMemoryProvider::new(MockMemoryProviderOptions::default()));
    let shared = Arc::clone(&memory);
    let shared: Arc<dyn MemoryProvider> = shared;
    let options = AiChannelOptions {
        memory: Some(shared),
        ..AiChannelOptions::default()
    };
    let agent = channel(provider.clone(), options);
    let running = turn(&agent);
    provider.gate.reached().await;
    let retiring = {
        let agent = agent.clone();
        tokio::spawn(async move {
            // Nothing to await but the count: polled, under a deadline.
            let deadline = Instant::now() + Duration::from_secs(5);
            while agent.active_turns() > 0 && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            agent.close().await;
        })
    };
    tokio::task::yield_now().await;
    assert!(!memory.closed());
    provider.gate.open();
    let output = running.await.unwrap().unwrap();
    retiring.await.unwrap();
    assert_eq!(answer(&output), "Working Hello from AI");
    assert!(memory.closed());
}

// Python: tests/test_channels/test_ai_active_turns.py::TestActiveTurns::test_a_provider_that_streams_text_alone_is_counted_too
#[tokio::test]
async fn a_provider_that_streams_text_alone_is_counted_too() {
    let provider = Plain {
        script: ScriptedProvider::new([]),
        gate: Gate::new(),
        streams_text: true,
    };
    let agent = channel(provider.clone(), AiChannelOptions::default());
    let running = turn(&agent);
    provider.gate.reached().await;
    assert_eq!(agent.active_turns(), 1);
    provider.gate.open();
    let output = running.await.unwrap().unwrap();
    assert_eq!(answer(&output), "Working done");
    assert_eq!(agent.active_turns(), 0);
}

// Python: tests/test_channels/test_ai_active_turns.py::TestActiveTurns::test_a_provider_error_mid_stream_returns_to_zero
#[tokio::test]
async fn a_provider_error_mid_stream_returns_to_zero() {
    let provider = Gated {
        fails: true,
        ..Gated::new([ScriptedRound::answering("unused")])
    };
    let agent = channel(provider.clone(), AiChannelOptions::default());
    let running = turn(&agent);
    provider.gate.reached().await;
    assert_eq!(agent.active_turns(), 1);
    provider.gate.open();
    let _failed = running.await.unwrap();
    assert_eq!(agent.active_turns(), 0);
}

// Python: tests/test_channels/test_ai_active_turns.py::TestActiveTurns::test_a_turn_without_tools_and_one_with_add_up
#[tokio::test]
async fn a_turn_without_tools_and_one_with_add_up() {
    let provider = Gated::new([
        ScriptedRound::calling("", [("tc1", "search", "{}")]),
        ScriptedRound::answering("Done"),
        ScriptedRound::answering("Done"),
    ]);
    let agent = channel(provider.clone(), with_search());
    let first = turn(&agent);
    provider.gate.reached().await;
    let second = turn(&agent);
    provider.gate.reached().await;
    assert_eq!(agent.active_turns(), 2);
    provider.gate.open();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    assert_eq!(agent.active_turns(), 0);
}
