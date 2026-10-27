//! A room with an SMS transport and an agent of the test's, its turns run
//! through the kit's turn backend, the faults a transport could make set on
//! it, and a tap that notes when each message crossed.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_core::Stream;
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    DirectEvent, EventContent, EventDirection, EventSource, EventType, InboundMessage, Metadata,
    RoomContext, RoomEvent, RoomId,
};
use roomkit_core::ops::{ChannelDeclaration, InboundResult};
use serde_json::Value;
use tokio::time::Instant;

use crate::stream::{DeliveryStream, ResponseItem};
use crate::testing::RecordingTransport;
use crate::turns::observe::{Carried, Fate};
use crate::{Channel, ChannelError, KitError, RoomKit, current_turn};

pub(super) fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

pub(super) fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// A kit whose room binds an SMS transport and `agent`.
pub(super) async fn kit_with(agent: impl Channel) -> RoomKit {
    let kit = RoomKit::new();
    kit.register_channel(RecordingTransport::new(channel("sms"), ChannelType::SMS))
        .unwrap();
    kit.create_room(&room_id()).await.unwrap();
    kit.attach(&room_id(), &channel("sms")).await.unwrap();
    bind(&kit, agent).await;
    kit
}

/// Registers `channel` and binds it to the room.
pub(super) async fn bind(kit: &RoomKit, channel: impl Channel) {
    let id = channel.declaration().channel_id;
    kit.register_channel(channel).unwrap();
    kit.attach(&room_id(), &id).await.unwrap();
}

/// Alice writes `text` by SMS.
pub(super) async fn says(kit: &RoomKit, text: &str) -> Result<InboundResult, KitError> {
    let message = InboundMessage::new(channel("sms"), "+15550001", EventContent::text(text));
    kit.process_inbound_in(&room_id(), message).await
}

/// What a fault does to the messages of `family` whose JSON holds `marks`.
pub(super) fn fault(kit: &RoomKit, family: Carried, marks: &'static str, fate: Fate) {
    kit.inner
        .turns
        .set_fault(Arc::new(move |carried, raw: &str| {
            if carried == family && raw.contains(marks) {
                fate
            } else {
                Fate::default()
            }
        }));
}

/// The mark of a message of `kind` in its envelope's JSON.
pub(super) fn of_type(kind: &'static str) -> &'static str {
    match kind {
        "heartbeat" => "\"message\":{\"type\":\"heartbeat\"",
        "cancel" => "\"message\":{\"type\":\"cancel\"",
        "accepted" => "\"message\":{\"type\":\"accepted\"",
        other => panic!("no mark for {other}"),
    }
}

/// The messages a kit's turns carried: when, their family, their type.
#[derive(Clone, Debug, Default)]
pub(super) struct Timed {
    seen: Arc<Mutex<Vec<(Duration, Carried, String)>>>,
}

impl Timed {
    pub(super) fn on(kit: &RoomKit, since: Instant) -> Self {
        let timed = Self::default();
        let seen = Arc::clone(&timed.seen);
        kit.inner.turns.set_tap(Arc::new(move |carried, raw: &str| {
            let json: Value = serde_json::from_str(raw).unwrap();
            let kind = json["message"]["type"]
                .as_str()
                .unwrap_or("request")
                .to_owned();
            seen.lock().unwrap().push((since.elapsed(), carried, kind));
        }));
        timed
    }

    /// The messages of `family` seen after `after`.
    pub(super) fn after(&self, family: Carried, after: Duration) -> Vec<(Duration, String)> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(at, carried, _)| *carried == family && *at > after)
            .map(|(at, _, kind)| (*at, kind.clone()))
            .collect()
    }

    /// How many messages of `family` and `kind` crossed.
    pub(super) fn count(&self, family: Carried, kind: &str) -> usize {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, carried, seen)| *carried == family && seen == kind)
            .count()
    }
}

/// An agent whose turn waits `wait`, then answers; it notes how its work
/// ended, and when.
#[derive(Clone)]
pub(super) struct Waiting {
    declaration: ChannelDeclaration,
    wait: Duration,
    since: Instant,
    pub(super) log: Arc<Mutex<Vec<(&'static str, Duration)>>>,
}

impl Waiting {
    pub(super) fn new(wait: Duration) -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel("ai"),
                ChannelType::AI,
                ChannelCategory::Intelligence,
                ChannelDirection::Bidirectional,
            ),
            wait,
            since: Instant::now(),
            log: Arc::default(),
        }
    }

    /// How its turns' work ended, in order, without when.
    pub(super) fn ends(&self) -> Vec<&'static str> {
        self.log
            .lock()
            .unwrap()
            .iter()
            .map(|(end, _)| *end)
            .collect()
    }
}

/// Notes that the turn's work was dropped unfinished, unless it answered.
struct Unfinished {
    agent: Waiting,
    answered: bool,
}

impl Drop for Unfinished {
    fn drop(&mut self) {
        if !self.answered {
            let at = self.agent.since.elapsed();
            self.agent.log.lock().unwrap().push(("dropped", at));
        }
    }
}

impl Channel for Waiting {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if event.source.channel_id == self.declaration.channel_id {
            return Ok(ChannelOutput::default());
        }
        let mut unfinished = Unfinished {
            agent: self.clone(),
            answered: false,
        };
        tokio::time::sleep(self.wait).await;
        unfinished.answered = true;
        self.log
            .lock()
            .unwrap()
            .push(("answered", self.since.elapsed()));
        let source = EventSource::new(channel("ai"), ChannelType::AI, EventDirection::Outbound);
        let answer = RoomEvent::new(
            event.id.clone(),
            event.room_id.clone(),
            EventType::Message,
            source,
            EventContent::text("Done."),
            event.created_at,
        );
        Ok(ChannelOutput::respond(vec![answer]))
    }
}

/// An agent whose turn streams `deltas` text deltas, then its end; it
/// counts the deltas its stream produced.
#[derive(Clone)]
pub(super) struct Streaming {
    declaration: ChannelDeclaration,
    deltas: usize,
    end: Metadata,
    pub(super) produced: Arc<AtomicUsize>,
}

impl Streaming {
    pub(super) fn new(deltas: usize) -> Self {
        let mut end = Metadata::new();
        end.insert("loop_end_reason".to_owned(), Value::from("completed"));
        Self::ending(deltas, end)
    }

    /// The agent, its stream ending on the record `end`.
    pub(super) fn ending(deltas: usize, end: Metadata) -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel("ai"),
                ChannelType::AI,
                ChannelCategory::Intelligence,
                ChannelDirection::Bidirectional,
            ),
            deltas,
            end,
            produced: Arc::default(),
        }
    }
}

/// The deltas of a [`Streaming`] agent's answer, each counted as it is read.
struct Deltas {
    left: usize,
    produced: Arc<AtomicUsize>,
    end: Option<Metadata>,
}

impl Stream for Deltas {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.left > 0 {
            this.left -= 1;
            this.produced.fetch_add(1, Ordering::SeqCst);
            return std::task::Poll::Ready(Some(Ok(ResponseItem::Text("word ".to_owned()))));
        }
        std::task::Poll::Ready(this.end.take().map(|end| Ok(ResponseItem::End(end))))
    }
}

impl Channel for Streaming {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let own = event.source.channel_id == self.declaration.channel_id;
        let deltas = Deltas {
            left: self.deltas,
            produced: Arc::clone(&self.produced),
            end: Some(self.end.clone()),
        };
        // The turn's host is read as the turn runs, within it.
        async move {
            if own {
                return Ok(ChannelOutput::default());
            }
            Ok(current_turn()
                .unwrap()
                .respond_with_stream(Box::pin(deltas)))
        }
    }
}

/// A WebSocket transport that streams: its reader reads `reads` items,
/// then stops pulling, and never returns.
#[derive(Clone)]
pub(super) struct Stuck {
    declaration: ChannelDeclaration,
    reads: usize,
}

impl Stuck {
    pub(super) fn new(reads: usize) -> Self {
        let mut declaration = ChannelDeclaration::new(
            channel("ws"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.supports_streaming_delivery = true;
        declaration.recipient_key = None;
        Self { declaration, reads }
    }
}

impl Channel for Stuck {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        for _ in 0..self.reads {
            stream.next().await;
        }
        std::future::pending().await
    }
}

/// An agent whose turn panics, as a provider's SDK may.
#[derive(Clone)]
pub(super) struct Panicking {
    declaration: ChannelDeclaration,
}

impl Panicking {
    pub(super) fn new() -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel("ai"),
                ChannelType::AI,
                ChannelCategory::Intelligence,
                ChannelDirection::Bidirectional,
            ),
        }
    }
}

impl Channel for Panicking {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let own = event.source.channel_id == self.declaration.channel_id;
        assert!(own, "the provider SDK panicked in on_event");
        std::future::ready(Ok(ChannelOutput::default()))
    }
}

/// An agent whose `on_event` does its work before its future, as the
/// examples write it: it reads `current_turn()` there, and spawns the task
/// that posts a note into the room, wrapped in `keep_context` as its
/// rustdoc shows; its future awaits the task, then answers.
#[derive(Clone)]
pub(super) struct Eager {
    declaration: ChannelDeclaration,
    pub(super) kit: Arc<Mutex<Option<RoomKit>>>,
    pub(super) turned: Arc<Mutex<Vec<bool>>>,
}

impl Eager {
    pub(super) fn new() -> Self {
        Self {
            declaration: ChannelDeclaration::new(
                channel("ai"),
                ChannelType::AI,
                ChannelCategory::Intelligence,
                ChannelDirection::Bidirectional,
            ),
            kit: Arc::default(),
            turned: Arc::default(),
        }
    }
}

impl Channel for Eager {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let own = event.source.channel_id == self.declaration.channel_id;
        let kit = self.kit.lock().unwrap().clone();
        let posting = kit.filter(|_| !own).map(|kit| {
            self.turned.lock().unwrap().push(current_turn().is_some());
            let room_id = event.room_id.clone();
            tokio::spawn(crate::keep_context(async move {
                let note = DirectEvent::new(channel("ai"), EventContent::text("(note)"));
                kit.send_event(&room_id, note).await.map(|_| ())
            }))
        });
        async move {
            if let Some(posting) = posting {
                posting.await.unwrap()?;
            }
            Ok(ChannelOutput::default())
        }
    }
}
