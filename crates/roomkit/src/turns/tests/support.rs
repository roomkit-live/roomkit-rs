//! A room with an SMS transport and an agent of the test's, its turns run
//! through the kit's turn backend, the faults a transport could make set on
//! it, and a tap that notes when each message crossed.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_core::Stream;
use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventDirection, EventSource, InboundMessage, ResponseMetadata, RoomContext, RoomEvent, RoomId,
    TextContent, ToolCallContent,
};
use roomkit_core::ops::ChannelConfig;
use serde_json::Value;
use tokio::time::Instant;

use crate::InboundResult;
use crate::stream::{DeliveryStream, LoopEndMarker, LoopEndReason, ResponseItem};
use crate::testing::{RecordingTransport, RecordingTransportOptions};
use crate::turns::observe::{Carried, Fate};
use crate::{
    AttachChannelOptions, Channel, ChannelError, CreateRoomOptions, ProcessInboundOptions, RoomKit,
    RoomKitError, RoomKitOptions, SendEventOptions, current_turn_host,
};

pub(super) fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

pub(super) fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// A kit whose room binds an SMS transport and `agent`.
pub(super) async fn kit_with(agent: impl Channel) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room_id()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room_id(), &channel("sms"), AttachChannelOptions::default())
        .await
        .unwrap();
    bind(&kit, agent).await;
    kit
}

/// Registers `channel` and binds it to the room.
pub(super) async fn bind(kit: &RoomKit, channel: impl Channel) {
    let id = channel.declaration().channel_id;
    kit.register_channel(channel).unwrap();
    kit.attach_channel(&room_id(), &id, AttachChannelOptions::default())
        .await
        .unwrap();
}

/// Alice writes `text` by SMS.
pub(super) async fn says(kit: &RoomKit, text: &str) -> Result<InboundResult, RoomKitError> {
    let message = InboundMessage::new(channel("sms"), "+15550001", TextContent::new(text));
    kit.process_inbound(
        message,
        ProcessInboundOptions {
            room_id: Some(room_id()),
            ..Default::default()
        },
    )
    .await
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
        "close" => "\"message\":{\"type\":\"close\"",
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
    declaration: ChannelConfig,
    wait: Duration,
    since: Instant,
    pub(super) log: Arc<Mutex<Vec<(&'static str, Duration)>>>,
}

impl Waiting {
    pub(super) fn new(wait: Duration) -> Self {
        Self {
            declaration: ChannelConfig::new(
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
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit_core::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
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
        let source = EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(channel("ai"), ChannelType::AI)
        };
        let answer = RoomEvent {
            id: event.id.clone(),
            created_at: event.created_at,
            ..RoomEvent::new(event.room_id.clone(), source, TextContent::new("Done."))
        };
        Ok(ChannelOutput {
            responded: true,
            response_events: vec![answer],
            ..ChannelOutput::empty()
        })
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

/// An agent whose turn streams `deltas` text deltas, then its end; it
/// counts the deltas its stream produced.
#[derive(Clone)]
pub(super) struct Streaming {
    declaration: ChannelConfig,
    deltas: usize,
    end: LoopEndMarker,
    pub(super) produced: Arc<AtomicUsize>,
    /// The turn's stream instead of its deltas, writing the turn's record.
    script: Option<Vec<Step>>,
    /// The room metadata its output updates, with its stream.
    pub(super) updates: roomkit_core::model::Metadata,
}

/// A step of a [`Streaming`] agent's scripted stream.
#[derive(Clone)]
pub(super) enum Step {
    /// A text delta.
    Text(&'static str),
    /// Writes a key of the turn's record.
    Write(&'static str, Value),
    /// Changes a list the record holds, no key written.
    Append(&'static str, &'static str),
    /// A call's start.
    Start(&'static str),
    /// A call's end.
    End(&'static str),
    /// The segment ends.
    Break,
    /// The stream waits, its burst sent, then goes on.
    Wait,
    /// The stream fails.
    Fail(&'static str),
    /// The stream waits forever; dropped there, it writes the record's `final`.
    Hold,
    /// The loop ends, completed.
    Completed,
}

/// A [`Streaming`] agent's scripted stream.
struct Scripted {
    steps: VecDeque<Step>,
    record: ResponseMetadata,
    holding: bool,
}

impl Drop for Scripted {
    fn drop(&mut self) {
        if self.holding {
            self.record.insert("final", Value::Bool(true));
        }
    }
}

impl Stream for Scripted {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll::{Pending, Ready};
        let this = self.get_mut();
        loop {
            let Some(step) = this.steps.pop_front() else {
                if this.holding {
                    return Pending;
                }
                return Ready(None);
            };
            let item = match step {
                Step::Text(text) => ResponseItem::Text(text.to_owned()),
                Step::Write(key, value) => {
                    this.record.insert(key, value);
                    continue;
                }
                Step::Append(key, value) => {
                    this.record.with_value(key, |list| {
                        if let Value::Array(list) = list {
                            list.push(Value::from(value));
                        }
                    });
                    continue;
                }
                Step::Start(id) => ResponseItem::ToolCallStart(ToolCallContent::new("t", id)),
                Step::End(id) => ResponseItem::ToolCallEnd(ToolCallContent::new("t", id)),
                Step::Break => ResponseItem::SegmentBreak,
                Step::Wait => {
                    context.waker().wake_by_ref();
                    return Pending;
                }
                Step::Fail(message) => return Ready(Some(Err(message.into()))),
                Step::Hold => {
                    this.holding = true;
                    continue;
                }
                Step::Completed => {
                    ResponseItem::LoopEnd(LoopEndMarker::new(LoopEndReason::Completed))
                }
            };
            return Ready(Some(Ok(item)));
        }
    }
}

impl Streaming {
    pub(super) fn new(deltas: usize) -> Self {
        Self::ending(deltas, LoopEndMarker::new(LoopEndReason::Completed))
    }

    /// The agent, its stream ending on `end`.
    pub(super) fn ending(deltas: usize, end: LoopEndMarker) -> Self {
        Self {
            declaration: ChannelConfig::new(
                channel("ai"),
                ChannelType::AI,
                ChannelCategory::Intelligence,
                ChannelDirection::Bidirectional,
            ),
            deltas,
            end,
            produced: Arc::default(),
            script: None,
            updates: roomkit_core::model::Metadata::new(),
        }
    }

    /// The agent, its stream `steps` and no end, writing the turn's record.
    pub(super) fn scripted(steps: Vec<Step>) -> Self {
        Self {
            script: Some(steps),
            ..Self::new(0)
        }
    }
}

/// The deltas of a [`Streaming`] agent's answer, each counted as it is read.
struct Deltas {
    left: usize,
    produced: Arc<AtomicUsize>,
    end: Option<LoopEndMarker>,
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
        std::task::Poll::Ready(this.end.take().map(|end| Ok(ResponseItem::LoopEnd(end))))
    }
}

impl Channel for Streaming {
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit_core::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
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
        let script = self.script.clone();
        let updates = self.updates.clone();
        // The turn's host is read as the turn runs, within it.
        async move {
            if own {
                return Ok(ChannelOutput::default());
            }
            let host = current_turn_host().unwrap();
            let Some(steps) = script else {
                return Ok(host.respond_with_stream(Box::pin(deltas)));
            };
            let record = ResponseMetadata::default();
            let stream = Scripted {
                steps: steps.into(),
                record: record.clone(),
                holding: false,
            };
            Ok(ChannelOutput {
                response_metadata: record,
                metadata_updates: updates,
                ..host.respond_with_stream(Box::pin(stream))
            })
        }
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

/// A WebSocket transport that streams: its reader reads `reads` items,
/// then stops pulling, and never returns.
#[derive(Clone)]
pub(super) struct Stuck {
    declaration: ChannelConfig,
    reads: usize,
    returns: bool,
    holds_answers: bool,
    reading_on: Option<ReadingOn>,
}

/// What a transport that reads its stream to its end does past it.
#[derive(Clone, Debug, Default)]
pub(super) struct ReadingOn {
    /// How long it flushes past the end; none, it never returns, unless it
    /// closed the kit.
    flush: Option<Duration>,
    /// The kit it closes, once at its first item or once past the end.
    pub(super) closes: Arc<Mutex<Option<RoomKit>>>,
    /// Whether it closes the kit reading, not past the end.
    closes_reading: bool,
    /// Told at its first item.
    pub(super) read: Arc<tokio::sync::Notify>,
    /// Told once it read the stream's end.
    pub(super) past_the_end: Arc<tokio::sync::Notify>,
    /// What its flush kept, once over.
    pub(super) kept: Arc<Mutex<Vec<&'static str>>>,
}

impl ReadingOn {
    pub(super) fn flushing(flush: Duration) -> Self {
        Self {
            flush: Some(flush),
            ..Self::default()
        }
    }

    pub(super) fn blocked() -> Self {
        Self::default()
    }

    /// A target that closes the kit `closes` holds, at its first item when
    /// `reading`, else past the end; it then keeps `closed`.
    pub(super) fn closing(reading: bool) -> Self {
        Self {
            closes_reading: reading,
            ..Self::default()
        }
    }

    /// Closes the kit it holds, if it still does, from its own code.
    async fn close_the_kit(&self) {
        let kit = self.closes.lock().unwrap().take();
        if let Some(kit) = kit {
            drop(kit.close().await);
            self.kept.lock().unwrap().push("closed");
        }
    }

    /// Reads `stream` to its end, then does what it does past it.
    async fn deliver(&self, mut stream: DeliveryStream) -> Result<ChannelOutput, ChannelError> {
        let mut dropped = DropNote(Some(Arc::clone(&self.kept)));
        let mut first = true;
        while stream.next().await.is_some() {
            if std::mem::take(&mut first) {
                self.read.notify_one();
                if self.closes_reading {
                    self.close_the_kit().await;
                }
            }
        }
        self.past_the_end.notify_one();
        if stream.cut() {
            self.kept.lock().unwrap().push("cut");
        }
        self.close_the_kit().await;
        if self.flush.is_none() && !self.kept.lock().unwrap().contains(&"closed") {
            std::future::pending::<()>().await;
        }
        if let Some(flush) = self.flush {
            tokio::time::sleep(flush).await;
            self.kept.lock().unwrap().push("flushed");
        }
        dropped.0 = None;
        Ok(ChannelOutput::empty())
    }
}

/// Notes a `deliver_stream` dropped before its end, while it holds the note's
/// list.
struct DropNote(Option<Arc<Mutex<Vec<&'static str>>>>);

impl Drop for DropNote {
    fn drop(&mut self) {
        if let Some(kept) = self.0.take() {
            kept.lock().unwrap().push("dropped");
        }
    }
}

impl Stuck {
    pub(super) fn new(reads: usize) -> Self {
        let mut declaration = ChannelConfig::new(
            channel("ws"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.supports_streaming_delivery = true;
        declaration.recipient_key = None;
        Self {
            declaration,
            reads,
            returns: false,
            holds_answers: false,
            reading_on: None,
        }
    }

    /// A transport that reads its stream to its end and works on past it.
    pub(super) fn reading_on(reading_on: &ReadingOn) -> Self {
        Self {
            reading_on: Some(reading_on.clone()),
            ..Self::new(0)
        }
    }

    /// A transport that does not stream, and whose delivery of an agent's
    /// answer never ends.
    pub(super) fn holding_answers() -> Self {
        let mut held = Self::new(0);
        held.declaration.supports_streaming_delivery = false;
        held.holds_answers = true;
        held
    }

    /// A transport whose reader reads `reads` items, then stops reading and
    /// returns: the reader's close.
    pub(super) fn returning(reads: usize) -> Self {
        Self {
            returns: true,
            ..Self::new(reads)
        }
    }
}

impl Channel for Stuck {
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit_core::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if let Some(reading_on) = &self.reading_on {
            return reading_on.deliver(stream).await;
        }
        for _ in 0..self.reads {
            stream.next().await;
        }
        if self.returns {
            return Ok(ChannelOutput::empty());
        }
        std::future::pending().await
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    async fn deliver(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if self.holds_answers && event.source.channel_id.as_str() == "ai" {
            std::future::pending::<()>().await;
        }
        Ok(ChannelOutput::empty())
    }
}

/// An agent whose turn panics, as a provider's SDK may.
#[derive(Clone)]
pub(super) struct Panicking {
    declaration: ChannelConfig,
}

impl Panicking {
    pub(super) fn new() -> Self {
        Self {
            declaration: ChannelConfig::new(
                channel("ai"),
                ChannelType::AI,
                ChannelCategory::Intelligence,
                ChannelDirection::Bidirectional,
            ),
        }
    }
}

impl Channel for Panicking {
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit_core::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
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

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

/// An agent whose `on_event` does its work before its future, as the
/// examples write it: it reads `current_turn_host()` there, and spawns the task
/// that posts a note into the room, wrapped in `keep_context` as its
/// rustdoc shows; its future awaits the task, then answers.
#[derive(Clone)]
pub(super) struct Eager {
    declaration: ChannelConfig,
    pub(super) kit: Arc<Mutex<Option<RoomKit>>>,
    pub(super) turned: Arc<Mutex<Vec<bool>>>,
}

impl Eager {
    pub(super) fn new() -> Self {
        Self {
            declaration: ChannelConfig::new(
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
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn direction(&self) -> ChannelDirection {
        self.declaration.direction
    }

    fn capabilities(&self) -> roomkit_core::model::ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        self.declaration.provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        self.declaration.recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.declaration.supports_streaming_delivery
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
            self.turned
                .lock()
                .unwrap()
                .push(current_turn_host().is_some());
            let room_id = event.room_id.clone();
            tokio::spawn(crate::keep_context(async move {
                let note = TextContent::new("(note)");
                kit.send_event(&room_id, &channel("ai"), note, SendEventOptions::default())
                    .await
                    .map(|_| ())
            }))
        });
        async move {
            if let Some(posting) = posting {
                posting.await.unwrap()?;
            }
            Ok(ChannelOutput::default())
        }
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}
