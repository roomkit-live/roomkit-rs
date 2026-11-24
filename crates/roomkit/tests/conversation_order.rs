//! Where a streamed text reads in the conversation (RFC §6.4, RMK-2096).
//!
//! A record the room receives while a generation is under way, before its
//! first text, is stored before that text. The text names the row it follows
//! (`metadata["follows_index"]`), the stream's placeholder names the room's
//! last row before the first word, and a channel that places each text after
//! the row it follows reads the same conversation live and from the store.
//!
//! The stored rows' keys are the shared scenario's
//! (`streaming/a-text-follows-the-row-the-room-held-when-its-writing-began`);
//! this file holds what a streaming channel reads.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::collections::VecDeque;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventSource, EventType,
    InboundMessage, LoopEndMarker, LoopEndReason, RoomContext, RoomEvent, RoomId, TextContent,
    ToolCallContent, Visibility,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, CommitEventOptions, CreateRoomOptions,
    DeliveryStream, GetTimelineOptions, ProcessInboundOptions, ResponseItem, RoomKit,
    RoomKitOptions, StreamItem, current_turn_host,
};
use roomkit_core::rules::transcoding::text_of;
use serde_json::Value;
use tokio::sync::{Notify, oneshot};

const ASK: &str = "Ask the other session.";
const WAIT: &str = "I wait for the answer.";
const REPLY: &str = "Reply from @review: MAC-ERABLE";
const VALUE: &str = "The value is MAC-ERABLE.";

fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

fn room_id() -> RoomId {
    RoomId::new("r1").unwrap()
}

/// The row an event names as the one it follows.
fn followed(event: &RoomEvent) -> Option<u64> {
    event.metadata.get("follows_index").and_then(Value::as_u64)
}

/// Where a row reads: after the row it follows, else at its index.
type Place = (u64, u64, String);

fn place(event: &RoomEvent) -> Place {
    (
        followed(event).unwrap_or(event.index),
        event.index,
        text_of(&event.content),
    )
}

/// The texts of `rows` in the order they read, a row that says nothing aside.
fn read(mut rows: Vec<Place>) -> Vec<String> {
    rows.sort();
    rows.into_iter()
        .map(|(_, _, text)| text)
        .filter(|text| !text.is_empty())
        .collect()
}

/// A response that tells it started, then, when it is gated, writes nothing
/// until it is released: a model call under way before its first text.
struct Turn {
    started: Arc<Notify>,
    release: Option<oneshot::Receiver<()>>,
    rest: VecDeque<ResponseItem>,
}

impl Stream for Turn {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(release) = this.release.as_mut() {
            this.started.notify_one();
            if Pin::new(release).poll(context).is_pending() {
                return Poll::Pending;
            }
            this.release = None;
        }
        Poll::Ready(this.rest.pop_front().map(Ok))
    }
}

/// An agent that answers a person's first message with the turn it holds.
struct Agent(Mutex<Option<Turn>>);

impl Agent {
    /// Its turn says it waits, then, asked again within the turn, gives the
    /// value; its first generation waits to be released.
    fn waiting(started: &Arc<Notify>, released: oneshot::Receiver<()>) -> Self {
        let rest = VecDeque::from([
            ResponseItem::Text(WAIT.to_owned()),
            ResponseItem::SegmentBreak,
            ResponseItem::Text(VALUE.to_owned()),
            ResponseItem::LoopEnd(LoopEndMarker::new(LoopEndReason::Completed)),
        ]);
        Self(Mutex::new(Some(Turn {
            started: Arc::clone(started),
            release: Some(released),
            rest,
        })))
    }

    /// Its turn opens on a tool call, then gives the value.
    fn calling() -> Self {
        let rest = VecDeque::from([
            ResponseItem::ToolCallStart(ToolCallContent::new("lookup", "c1")),
            ResponseItem::ToolCallEnd(ToolCallContent::new("lookup", "c1")),
            ResponseItem::Text(VALUE.to_owned()),
            ResponseItem::LoopEnd(LoopEndMarker::new(LoopEndReason::Completed)),
        ]);
        Self(Mutex::new(Some(Turn {
            started: Arc::default(),
            release: None,
            rest,
        })))
    }
}

impl Channel for Agent {
    fn channel_id(&self) -> ChannelId {
        channel("agent")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        if event.r#type != EventType::Message {
            return ready(Ok(ChannelOutput::empty()));
        }
        let turn = self.0.lock().unwrap_or_else(PoisonError::into_inner).take();
        ready(Ok(match turn {
            Some(turn) => current_turn_host()
                .unwrap()
                .respond_with_stream(Box::pin(turn)),
            None => ChannelOutput::empty(),
        }))
    }
}

/// What a terminal shows.
#[derive(Default)]
struct View {
    placeholder: Option<RoomEvent>,
    shown: Vec<Place>,
    /// The text under way, after the row it follows, below every stored row.
    writing: Option<Place>,
    /// What it showed at each delta, the text under way included.
    at_each_delta: Vec<Vec<String>>,
}

impl View {
    fn read(&self) -> Vec<String> {
        read(self.shown.iter().chain(&self.writing).cloned().collect())
    }
}

/// A streaming transport that shows each text after the row it follows: the
/// text under way from the placeholder's row, then from the last row of its
/// stream.
#[derive(Clone, Default)]
struct Terminal(Arc<Mutex<View>>);

impl Terminal {
    fn view(&self) -> MutexGuard<'_, View> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn show(&self, event: &RoomEvent) {
        self.view().shown.push(place(event));
    }
}

impl Channel for Terminal {
    fn channel_id(&self) -> ChannelId {
        channel("terminal")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBSOCKET
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Transport
    }

    fn recipient_key(&self) -> Option<String> {
        None
    }

    fn supports_streaming_delivery(&self) -> bool {
        true
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.show(&event);
        ready(Ok(ChannelOutput::empty()))
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        event: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let mut follows = followed(&event).unwrap();
        self.view().placeholder = Some(event);
        while let Some(item) = stream.next().await {
            let item = item?;
            let mut view = self.view();
            if let StreamItem::Text(delta) = &item {
                let written = view.writing.take().map(|(_, _, text)| text);
                let text = written.unwrap_or_default() + delta;
                view.writing = Some((follows, u64::MAX, text));
                let showing = view.read();
                view.at_each_delta.push(showing);
            } else if let StreamItem::Row(row) = &item {
                view.writing = None;
                view.shown.push(place(row));
                follows = row.index;
            }
        }
        Ok(ChannelOutput::empty())
    }
}

/// A room with a customer's SMS, `terminal` and `agent`.
async fn a_room(terminal: &Terminal, agent: Agent) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(terminal.clone()).unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "terminal", "agent"] {
        kit.attach_channel(&room_id(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}

/// The customer asks, and the call returns once the turn is over.
async fn ask(kit: &RoomKit) {
    let message = InboundMessage::new(channel("sms"), "+15551234567", TextContent::new(ASK));
    let options = ProcessInboundOptions {
        room_id: Some(room_id()),
        ..Default::default()
    };
    kit.process_inbound(message, options).await.unwrap();
}

/// Another session's answer, kept in the room and shown on `terminal`.
async fn answer_arrives(kit: &RoomKit, terminal: &Terminal) {
    let answer = RoomEvent {
        r#type: EventType::System,
        ..RoomEvent::new(
            room_id(),
            EventSource::new(channel("terminal"), ChannelType::WEBSOCKET),
            TextContent::new(REPLY),
        )
    };
    let stored = kit
        .commit_event(&room_id(), answer, CommitEventOptions::default())
        .await
        .unwrap();
    terminal.show(&stored);
}

/// The rows of the room a person reads, once the kit is idle and closed.
async fn conversation(kit: RoomKit) -> Vec<RoomEvent> {
    roomkit::testing::idle(&kit).await;
    let stored = kit
        .get_timeline(&room_id(), GetTimelineOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
    stored
        .into_iter()
        .filter(|event| event.visibility != Visibility::Internal)
        .collect()
}

/// Another session's answer arrives between the start of a generation and
/// its first text. The placeholder names the message the turn answers, so
/// the channel shows the wait above the answer from its first word; live and
/// read back from the store, the conversation is the ask, the wait, the
/// answer, the value, each once.
// Python: tests/test_conversation_order.py::test_a_streaming_channel_reads_each_text_after_the_row_it_follows
#[tokio::test]
async fn a_streaming_channel_reads_each_text_after_the_row_it_follows() {
    let terminal = Terminal::default();
    let started = Arc::new(Notify::new());
    let (release, released) = oneshot::channel();
    let kit = a_room(&terminal, Agent::waiting(&started, released)).await;
    let asking = kit.clone();
    let asked = tokio::spawn(async move { ask(&asking).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        started.notified().await;
        answer_arrives(&kit, &terminal).await;
        release.send(()).unwrap();
        asked.await.unwrap();
    })
    .await
    .unwrap();
    let stored = conversation(kit).await;
    let asked = stored
        .iter()
        .find(|event| text_of(&event.content) == ASK)
        .unwrap();
    let read_back = read(stored.iter().map(place).collect());
    let view = terminal.view();
    let all = [ASK, WAIT, REPLY, VALUE].map(str::to_owned).to_vec();
    assert_eq!(
        (
            view.placeholder.as_ref().and_then(followed),
            view.at_each_delta.first().cloned(),
            view.read(),
            read_back,
        ),
        (
            Some(asked.index),
            Some([ASK, WAIT, REPLY].map(str::to_owned).to_vec()),
            all.clone(),
            all,
        )
    );
}

/// A turn that opens on a call: the event its stream is rendered under
/// names the message the turn answers, the last row the room held as the
/// read began, and the text names the call's end, the row the turn stored
/// before it, which the stream handed the channel.
// Python: tests/test_conversation_order.py::test_the_stream_s_event_names_the_room_s_last_row_and_a_text_the_turn_s
#[tokio::test]
async fn the_stream_s_event_names_the_room_s_last_row_and_a_text_the_turn_s() {
    let terminal = Terminal::default();
    let kit = a_room(&terminal, Agent::calling()).await;
    tokio::time::timeout(Duration::from_secs(5), ask(&kit))
        .await
        .unwrap();
    let stored = conversation(kit).await;
    let index_of = |wanted: EventType, text: &str| {
        stored
            .iter()
            .find(|event| event.r#type == wanted && text_of(&event.content) == text)
            .map(|event| event.index)
    };
    let value = stored
        .iter()
        .find(|event| text_of(&event.content) == VALUE)
        .unwrap();
    let end = stored
        .iter()
        .find(|event| event.r#type == EventType::ToolCallEnd)
        .unwrap();
    let view = terminal.view();
    assert_eq!(
        (
            view.placeholder.as_ref().and_then(followed),
            followed(value)
        ),
        (index_of(EventType::Message, ASK), Some(end.index))
    );
}
