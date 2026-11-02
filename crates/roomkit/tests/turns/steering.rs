//! RFC §10.6.8 Steering: the owner sends a directive to each turn it
//! addresses, whose executor applies it through the channel; a channel
//! without steering takes a `Cancel` as its turn's cancel, counted as one.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::{
    Cancel, ChannelBinding, ChannelCategory, ChannelDirection, ChannelOutput, ChannelType,
    EventContent, EventType, InboundMessage, InjectMessage, RoomContext, RoomEvent,
    SteeringDirective, TextContent, ToolCallContent, ToolCallOutcome, ToolCallStatus,
};
use roomkit::testing::{RecordingTransport, RecordingTransportOptions};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    ProcessInboundOptions, ResponseItem, RoomKit, RoomKitOptions, SteerOptions, current_turn_host,
};
use tokio::sync::watch;

use crate::support::{channel, received, room};

/// An intelligence channel whose stream writes `before`, waits until
/// released, then writes `after`; with `steers`, a channel with steering,
/// recording what reached each turn and reaching one loop.
#[derive(Clone)]
struct Held {
    before: Vec<ResponseItem>,
    after: Vec<ResponseItem>,
    release: watch::Sender<bool>,
    steers: bool,
    steered: Arc<Mutex<Vec<(SteeringDirective, String)>>>,
    turn_ids: Arc<Mutex<Vec<String>>>,
    started: watch::Sender<usize>,
}

impl Held {
    fn new(steers: bool) -> Self {
        Self {
            before: vec![ResponseItem::Text("first".to_owned())],
            after: vec![ResponseItem::Text("second".to_owned())],
            release: watch::Sender::new(false),
            steers,
            steered: Arc::default(),
            turn_ids: Arc::default(),
            started: watch::Sender::new(0),
        }
    }

    /// The same channel, its stream starting a call, then waiting.
    fn in_a_call(mut self) -> Self {
        let mut start = ToolCallContent::new("lookup", "c1");
        start.arguments.insert("q".to_owned(), "x".into());
        self.before = vec![ResponseItem::ToolCallStart(start)];
        self.after = Vec::new();
        self
    }

    /// Returns once `count` turns read their first item.
    async fn started(&self, count: usize) {
        let mut started = self.started.subscribe();
        started.wait_for(|started| *started >= count).await.unwrap();
    }

    fn release(&self) {
        self.release.send_replace(true);
    }

    fn turn_ids(&self) -> Vec<String> {
        self.turn_ids.lock().unwrap().clone()
    }
}

/// The stream of a `Held` turn.
struct HeldStream {
    before: VecDeque<ResponseItem>,
    after: VecDeque<ResponseItem>,
    waiting: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    started: Option<watch::Sender<usize>>,
}

impl Stream for HeldStream {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if let Some(item) = this.before.pop_front() {
            if let Some(started) = this.started.take() {
                started.send_modify(|started| *started += 1);
            }
            return Poll::Ready(Some(Ok(item)));
        }
        if let Some(waiting) = this.waiting.as_mut() {
            if waiting.as_mut().poll(context).is_pending() {
                return Poll::Pending;
            }
            this.waiting = None;
        }
        Poll::Ready(this.after.pop_front().map(Ok))
    }
}

impl Channel for Held {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        channel("ai")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Bidirectional
    }

    fn steer_turn(&self, directive: &SteeringDirective, turn_id: &str) -> Option<usize> {
        if !self.steers {
            return None;
        }
        let steered = (directive.clone(), turn_id.to_owned());
        self.steered.lock().unwrap().push(steered);
        Some(1)
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Err("no inbound".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let held = self.clone();
        async move {
            if event.source.channel_id == channel("ai") {
                return Ok(ChannelOutput::default());
            }
            let turn = current_turn_host().unwrap();
            held.turn_ids
                .lock()
                .unwrap()
                .push(turn.turn_id().to_owned());
            let mut release = held.release.subscribe();
            let stream = HeldStream {
                before: held.before.into(),
                after: held.after.into(),
                waiting: Some(Box::pin(async move {
                    drop(release.wait_for(|release| *release).await);
                })),
                started: Some(held.started),
            };
            Ok(turn.respond_with_stream(Box::pin(stream)))
        }
    }
}

/// A kit whose room binds an SMS transport and `agent`.
async fn kit(agent: Held) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(agent).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    kit
}

/// A message to the room, its turn run in the background.
fn asked(kit: &RoomKit, text: &str) -> tokio::task::JoinHandle<()> {
    let (kit, text) = (kit.clone(), text.to_owned());
    tokio::spawn(async move {
        let message = InboundMessage::new(channel("sms"), "+15550000001", TextContent::new(text));
        let options = ProcessInboundOptions {
            room_id: Some(room()),
            ..Default::default()
        };
        kit.process_inbound(message, options).await.unwrap();
    })
}

async fn steer(
    kit: &RoomKit,
    directive: impl Into<SteeringDirective>,
    options: SteerOptions,
) -> usize {
    kit.steer(directive, &channel("ai"), options).await.unwrap()
}

fn in_room() -> SteerOptions {
    SteerOptions {
        room_id: Some(room()),
        ..SteerOptions::default()
    }
}

/// The agent's rows: each MESSAGE's text, each call's end.
async fn agent_rows(kit: &RoomKit) -> (Vec<String>, Vec<ToolCallContent>) {
    let timeline = kit.store().list_events(&room(), received()).await.unwrap();
    let mut texts = Vec::new();
    let mut ends = Vec::new();
    for event in timeline {
        if event.source.channel_id != channel("ai") {
            continue;
        }
        match (event.r#type, event.content) {
            (EventType::Message, EventContent::Text(text)) => texts.push(text.body),
            (EventType::ToolCallEnd, EventContent::ToolCall(end)) => ends.push(end),
            _ => {}
        }
    }
    (texts, ends)
}

// Python: tests/test_turns_control.py::test_a_directive_for_a_turn_reaches_that_turns_loop_even_unread
#[tokio::test]
async fn a_directive_for_a_turn_reaches_that_turns_loop() {
    let agent = Held::new(true);
    let kit = kit(agent.clone()).await;
    let turn = asked(&kit, "hello");
    agent.started(1).await;
    let turn_id = agent.turn_ids()[0].clone();

    let options = SteerOptions {
        turn_id: Some(turn_id.clone()),
        ..SteerOptions::default()
    };
    let reached = steer(&kit, InjectMessage::new("x"), options).await;
    agent.release();
    turn.await.unwrap();

    assert_eq!(reached, 1);
    assert_eq!(
        *agent.steered.lock().unwrap(),
        [(InjectMessage::new("x").into(), turn_id)]
    );
    assert_eq!(agent_rows(&kit).await.0, ["firstsecond"]);
    kit.close().await.unwrap();
}

// Python: tests/test_turns_control.py::test_a_cancel_for_a_channel_without_steering_cuts_its_turn
#[tokio::test]
async fn a_cancel_for_a_channel_without_steering_cuts_its_turn() {
    let agent = Held::new(false);
    let kit = kit(agent.clone()).await;
    let turn = asked(&kit, "hello");
    agent.started(1).await;

    let reached = steer(&kit, Cancel::default(), in_room()).await;
    // The turn ends as a closed stream would: its reader reads no error.
    turn.await.unwrap();

    assert_eq!(reached, 1);
    assert_eq!(agent_rows(&kit).await.0, ["first"]);
    kit.close().await.unwrap();
}

// Python: tests/test_turns_control.py::test_a_directive_other_than_cancel_reaches_no_channel_without_steering
#[tokio::test]
async fn a_directive_other_than_cancel_reaches_no_channel_without_steering() {
    let agent = Held::new(false);
    let kit = kit(agent.clone()).await;
    let turn = asked(&kit, "hello");
    agent.started(1).await;

    let reached = steer(&kit, InjectMessage::new("x"), in_room()).await;
    agent.release();
    turn.await.unwrap();

    assert_eq!(reached, 0);
    assert_eq!(agent_rows(&kit).await.0, ["firstsecond"]);
    kit.close().await.unwrap();
}

// Python: tests/test_turns_control.py::test_a_cancel_reaches_every_turn_of_the_room_another_directive_the_latest
#[tokio::test]
async fn a_cancel_reaches_every_turn_of_the_room_another_directive_the_latest() {
    let agent = Held::new(true);
    let kit = kit(agent.clone()).await;
    let turns = [asked(&kit, "one"), asked(&kit, "two")];
    agent.started(2).await;
    let turn_ids = agent.turn_ids();

    let one = steer(&kit, InjectMessage::new("x"), in_room()).await;
    let latest = agent.steered.lock().unwrap().last().unwrap().1.clone();
    let every = steer(&kit, Cancel::default(), in_room()).await;
    agent.release();
    for turn in turns {
        turn.await.unwrap();
    }

    assert_eq!((one, every), (1, 2));
    assert_eq!(latest, turn_ids[1]);
    kit.close().await.unwrap();
}

// Python: tests/test_turns_control.py::test_a_call_a_steering_cancel_left_open_ends_cancelled
// Python: tests/test_turns_control.py::test_a_call_left_open_ends_cancelled_for_a_reader_that_comes_late
#[tokio::test]
async fn a_call_a_steering_cancel_left_open_ends_cancelled() {
    let agent = Held::new(false).in_a_call();
    let kit = kit(agent.clone()).await;
    let turn = asked(&kit, "hello");
    agent.started(1).await;

    let reached = steer(&kit, Cancel::default(), in_room()).await;
    turn.await.unwrap();

    let (_, ends) = agent_rows(&kit).await;
    let ended: Vec<_> = ends
        .iter()
        .map(|end| {
            let error = end.error.as_deref();
            (end.tool_id.as_str(), end.outcome, end.status, error)
        })
        .collect();
    assert_eq!(reached, 1);
    // Cut, not failed: the turn's reader reads its end as after a close.
    assert_eq!(
        ended,
        [(
            "c1",
            Some(ToolCallOutcome::Cancelled),
            ToolCallStatus::Failed,
            Some("cancelled")
        )]
    );
    kit.close().await.unwrap();
}

/// An intelligence channel that takes a room's turns one at a time and
/// answers beside a stream, which nobody would read.
struct Buffered;

impl Channel for Buffered {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        channel("ai")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn serializes_room_turns(&self) -> bool {
        true
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Err("no inbound".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let answer = RoomEvent::new(
            event.room_id.clone(),
            roomkit::model::EventSource::new(channel("ai"), ChannelType::AI),
            TextContent::new("buffered"),
        );
        std::future::ready(Ok(ChannelOutput {
            responded: true,
            response_events: vec![answer],
            ..ChannelOutput::empty()
        }))
    }
}

// Python: tests/test_room_turns_in_order.py::test_a_turn_that_waited_its_place_and_answers_beside_its_stream_fails
#[tokio::test]
async fn a_turn_that_waited_its_place_and_answers_beside_its_stream_fails() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(Buffered).unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let message = InboundMessage::new(channel("sms"), "+15550000001", TextContent::new("go"));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..Default::default()
    };

    let result = kit.process_inbound(message, options).await.unwrap();

    let error = result.error.map(|error| error.error_type);
    assert_eq!(error.as_deref(), Some("TypeError"));
    assert_eq!(agent_rows(&kit).await.0, Vec::<String>::new());
    kit.close().await.unwrap();
}
