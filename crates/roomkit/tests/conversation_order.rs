//! Where a streamed text reads in the conversation (RFC §6.4, RMK-2096).
//!
//! A record the room receives while a generation is under way, before its
//! first text, is stored before that text. The text names the row it follows
//! (`metadata["follows_index"]`), the stream's placeholder names it before
//! the first word, and a channel that places each text after the row it
//! follows reads the same conversation live and from the store.
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
    Visibility,
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

/// The texts of `rows` in the order they read.
fn read(mut rows: Vec<Place>) -> Vec<String> {
    rows.sort();
    rows.into_iter().map(|(_, _, text)| text).collect()
}

/// A response whose first generation tells it started, then writes nothing
/// until it is released: a model call under way before its first text.
struct Gated {
    started: Arc<Notify>,
    release: Option<oneshot::Receiver<()>>,
    rest: VecDeque<ResponseItem>,
}

impl Stream for Gated {
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

/// An agent whose turn says it waits, then, asked again within the turn,
/// gives the value.
struct Waiting {
    started: Arc<Notify>,
    release: Mutex<Option<oneshot::Receiver<()>>>,
}

impl Channel for Waiting {
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
        let release = self
            .release
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if event.r#type != EventType::Message || release.is_none() {
            return ready(Ok(ChannelOutput::empty()));
        }
        let rest = VecDeque::from([
            ResponseItem::Text(WAIT.to_owned()),
            ResponseItem::SegmentBreak,
            ResponseItem::Text(VALUE.to_owned()),
            ResponseItem::LoopEnd(LoopEndMarker::new(LoopEndReason::Completed)),
        ]);
        let stream = Gated {
            started: Arc::clone(&self.started),
            release,
            rest,
        };
        ready(Ok(current_turn_host()
            .unwrap()
            .respond_with_stream(Box::pin(stream))))
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

/// A room with a customer's SMS, the terminal and the agent, and what
/// releases the agent's first generation.
async fn a_room(terminal: &Terminal) -> (RoomKit, Arc<Notify>, oneshot::Sender<()>) {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(terminal.clone()).unwrap();
    let started = Arc::new(Notify::new());
    let (release, released) = oneshot::channel();
    kit.register_channel(Waiting {
        started: Arc::clone(&started),
        release: Mutex::new(Some(released)),
    })
    .unwrap();
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
    (kit, started, release)
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
    let (kit, started, release) = a_room(&terminal).await;
    let asking = kit.clone();
    let asked = tokio::spawn(async move {
        let message = InboundMessage::new(channel("sms"), "+15551234567", TextContent::new(ASK));
        let options = ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        asking.process_inbound(message, options).await.unwrap();
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        started.notified().await;
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
        release.send(()).unwrap();
        asked.await.unwrap();
        roomkit::testing::idle(&kit).await;
    })
    .await
    .unwrap();
    let stored = kit
        .get_timeline(&room_id(), GetTimelineOptions::default())
        .await
        .unwrap();
    kit.close().await.unwrap();
    let conversation: Vec<&RoomEvent> = stored
        .iter()
        .filter(|event| event.visibility != Visibility::Internal)
        .collect();
    let ask = conversation
        .iter()
        .find(|event| text_of(&event.content) == ASK)
        .unwrap();
    let read_back = read(conversation.iter().map(|event| place(event)).collect());
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
            Some(ask.index),
            Some([ASK, WAIT, REPLY].map(str::to_owned).to_vec()),
            all.clone(),
            all,
        )
    );
}
