//! RFC §10.6.4: how a stream's deltas reach a streaming target.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelOutput, ChannelType, EventContent,
    InboundMessage, LoopEndMarker, LoopEndReason, RoomContext, RoomEvent, TextContent,
    ToolCallContent, ToolCallStatus,
};
use roomkit::testing::TurnTap;
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    DeliveryStream, ProcessInboundOptions, ResponseItem, RoomKit, RoomKitOptions, StreamItem,
    current_turn_host,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions};
use roomkit_core::ops::ChannelConfig;
use serde_json::{Value, json};

use crate::support::{asks, channel, received, room};
use roomkit_core::rules::transcoding::text_of;

/// A WebSocket transport that streams and keeps the length of each text
/// it is handed, its output saying `says`; with `stalls`, it tells it
/// and reads nothing; with `lingers`, it reads to the stream's end, tells
/// it, and never returns.
#[derive(Clone)]
struct Counting {
    sizes: Arc<Mutex<Vec<usize>>>,
    says: roomkit::model::Metadata,
    stalls: Option<Arc<tokio::sync::Notify>>,
    lingers: Option<Arc<tokio::sync::Notify>>,
    /// Whether the reader waits at every fifth item, as Python's test reads.
    pauses: bool,
    /// Whether it fails once it read the stream's end.
    fails: bool,
}

impl Counting {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        let mut declaration = ChannelConfig::new(
            channel("ws"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.supports_streaming_delivery = true;
        declaration.recipient_key = None;
        declaration
    }
}

impl Channel for Counting {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        Self::config().channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        Self::config().channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        Self::config().category
    }

    fn direction(&self) -> ChannelDirection {
        Self::config().direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        Self::config().capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        Self::config().provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        Self::config().recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        Self::config().supports_streaming_delivery
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if let Some(stalls) = &self.stalls {
            stalls.notify_one();
            std::future::pending::<()>().await;
        }
        let mut read = 0_u32;
        while let Some(item) = stream.next().await {
            if let Ok(StreamItem::Text(text)) = item {
                self.sizes.lock().unwrap().push(text.len());
            }
            read += 1;
            if self.pauses && read.is_multiple_of(5) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        if let Some(lingers) = &self.lingers {
            lingers.notify_one();
            std::future::pending::<()>().await;
        }
        if self.fails {
            return Err("the target failed".into());
        }
        Ok(ChannelOutput {
            response_metadata: self.says.clone().into(),
            ..ChannelOutput::default()
        })
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl std::future::Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

/// RFC §10.6.4 (RMK-1017, D1): the deltas a stream yields without a wait
/// between them cross as one item, their text joined and their number
/// told, up to what a pull grants, 64, as the Python implementation's
/// `FrameSender` sends them: 200 deltas at once reach the target as 4
/// items of 246, 284, 320 and 40 characters, carrying 64, 64, 64 and 8
/// deltas, as Python's (`rmk1017-lightreview5/lr5_probe_py.out` D1). On
/// 8a2ca47 each delta crossed as an item of its own: 200 items.
#[tokio::test]
async fn deltas_yielded_together_cross_as_one_item() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(roomkit::testing::RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        roomkit::testing::RecordingTransportOptions::default(),
    ))
    .unwrap();
    let target = Counting {
        sizes: Arc::default(),
        says: roomkit::model::Metadata::new(),
        stalls: None,
        lingers: None,
        pauses: false,
        fails: false,
    };
    kit.register_channel(target.clone()).unwrap();
    let words: Vec<String> = (0..200).map(|n| format!("w{n} ")).collect();
    let model = ScriptedProvider::new([ScriptedRound::writing(words.iter().map(String::as_str))]);
    kit.register_channel(
        AiChannel::new(channel("ai"), model, AiChannelOptions::default()).unwrap(),
    )
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ws", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let tap = TurnTap::on(&kit);
    asks(&kit, "Count.").await;
    roomkit::testing::idle(&kit).await;
    let bursts: Vec<(usize, Value)> = tap
        .messages()
        .into_iter()
        .filter(|message| message.family == "frame")
        .filter_map(|message| {
            let frame: Value = serde_json::from_str(&message.json).unwrap();
            let item = frame["message"]["item"].clone();
            (item["kind"] == "text").then(|| {
                let text = item["text"].as_str().unwrap_or_default().len();
                (text, item["deltas"].clone())
            })
        })
        .collect();
    let events = kit.store().list_events(&room(), received()).await.unwrap();
    let answer = events.last().map(|event| text_of(&event.content).len());
    assert_eq!(
        (target.sizes.lock().unwrap().clone(), bursts, answer),
        (
            vec![246, 284, 320, 40],
            vec![
                (246, json!(64)),
                (284, json!(64)),
                (320, json!(64)),
                (40, json!(8))
            ],
            Some(890)
        )
    );
    kit.close().await.unwrap();
}

/// A response's items, each ready at once, then the error it fails with,
/// if any.
struct Items(VecDeque<ResponseItem>, Option<ChannelError>);

impl Stream for Items {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let items = self.get_mut();
        Poll::Ready(
            items
                .0
                .pop_front()
                .map(Ok)
                .or_else(|| items.1.take().map(Err)),
        )
    }
}

/// Items written with a wait at every seventh until the first break, as
/// Python's test writes them: what follows it may cross as one joined item,
/// at the edge of the pull window.
struct Waiting {
    items: VecDeque<ResponseItem>,
    polls: usize,
    broke: bool,
}

impl Stream for Waiting {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        this.polls += 1;
        if !this.broke && this.polls.is_multiple_of(7) {
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        let item = this.items.pop_front();
        this.broke |= matches!(item, Some(ResponseItem::SegmentBreak));
        Poll::Ready(item.map(Ok))
    }
}

/// Text with a piece of reasoning at every third delta, a break, text, a
/// call's start, then text: as Python's
/// `test_deltas_bursts_and_thinking_before_each_break_never_stall_the_stream`.
fn interleaved(deltas: usize) -> VecDeque<ResponseItem> {
    let mut items = VecDeque::new();
    for at in 0..deltas {
        items.push_back(ResponseItem::Text(format!("t{at} ")));
        if at % 3 == 0 {
            items.push_back(ResponseItem::Thinking("x".to_owned()));
        }
    }
    items.push_back(ResponseItem::SegmentBreak);
    items.extend((0..deltas).map(|at| ResponseItem::Text(format!("u{at} "))));
    items.push_back(ResponseItem::ToolCallStart(ToolCallContent::new("t", "c")));
    items.push_back(ResponseItem::Text("z".to_owned()));
    items
}

// Python: tests/test_turns_pull.py::test_deltas_bursts_and_thinking_before_each_break_never_stall_the_stream
/// The reader and the executor count the same items however the deltas
/// cross, reasoning apart: every pull is honoured, nothing stalls.
#[tokio::test]
async fn deltas_bursts_and_thinking_before_each_break_never_stall_the_stream() {
    for deltas in [1, 32, 33, 64, 65, 130] {
        let kit = RoomKit::new(RoomKitOptions::default());
        kit.register_channel(roomkit::testing::RecordingTransport::new(
            channel("sms"),
            ChannelType::SMS,
            roomkit::testing::RecordingTransportOptions::default(),
        ))
        .unwrap();
        let target = Counting {
            sizes: Arc::default(),
            says: roomkit::model::Metadata::new(),
            stalls: None,
            lingers: None,
            pauses: true,
            fails: false,
        };
        kit.register_channel(target.clone()).unwrap();
        let writing = Writing {
            cut: false,
            interleaves: Some(deltas),
        };
        kit.register_channel(writing).unwrap();
        kit.create_room(CreateRoomOptions {
            room_id: Some(room()),
            ..Default::default()
        })
        .await
        .unwrap();
        for id in ["sms", "ws", "ai"] {
            kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
        let tap = TurnTap::on(&kit);
        let wrote: usize = interleaved(deltas)
            .iter()
            .map(|item| {
                if let ResponseItem::Text(text) = item {
                    text.len()
                } else {
                    0
                }
            })
            .sum();
        tokio::time::timeout(Duration::from_secs(3), async {
            asks(&kit, "Count.").await;
            roomkit::testing::idle(&kit).await;
        })
        .await
        .unwrap();
        let read: usize = target.sizes.lock().unwrap().iter().sum();
        assert_eq!(read, wrote);
        let thinking = tap
            .messages()
            .into_iter()
            .filter(|message| message.family == "frame")
            .filter(|message| {
                let frame: Value = serde_json::from_str(&message.json).unwrap();
                frame["message"]["item"]["kind"] == "thinking"
            })
            .count();
        assert_eq!(thinking, deltas.div_ceil(3));
        kit.close().await.unwrap();
    }
}

/// An intelligence channel whose own stream writes text, a call's start,
/// two deltas, the call's end, then text again, as `ResponseItem` lets a
/// channel write it; the call carries its intent (RFC §6.4). One that is
/// `cut` writes text, then its stream is cancelled.
struct Writing {
    cut: bool,
    /// Writes [`interleaved`] of this many deltas instead.
    interleaves: Option<usize>,
}

impl Writing {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        ChannelConfig::new(
            channel("ai"),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        )
    }
}

impl Channel for Writing {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        Self::config().channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        Self::config().channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        Self::config().category
    }

    fn direction(&self) -> ChannelDirection {
        Self::config().direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        Self::config().capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        Self::config().provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        Self::config().recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        Self::config().supports_streaming_delivery
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let own = event.source.channel_id == channel("ai");
        let cut = self.cut;
        let interleaves = self.interleaves;
        async move {
            if own {
                return Ok(ChannelOutput::default());
            }
            if cut {
                let items = VecDeque::from([ResponseItem::Text("The ".to_owned())]);
                let cancelled = Some(roomkit::RoomKitError::Cancelled.into());
                return Ok(current_turn_host()
                    .unwrap()
                    .respond_with_stream(Box::pin(Items(items, cancelled))));
            }
            if let Some(deltas) = interleaves {
                let items = Waiting {
                    items: interleaved(deltas),
                    polls: 0,
                    broke: false,
                };
                return Ok(current_turn_host()
                    .unwrap()
                    .respond_with_stream(Box::pin(items)));
            }
            let mut start = ToolCallContent::new("look", "c1");
            start.intent = Some("I look.".to_owned());
            let mut end = start.clone();
            end.status = ToolCallStatus::Completed;
            end.result = json!("ok");
            let items = VecDeque::from([
                ResponseItem::Text("a ".to_owned()),
                ResponseItem::ToolCallStart(start),
                ResponseItem::Text("b ".to_owned()),
                ResponseItem::Text("c ".to_owned()),
                ResponseItem::ToolCallEnd(end),
                ResponseItem::Text("d ".to_owned()),
                ResponseItem::LoopEnd(LoopEndMarker::new(LoopEndReason::Completed)),
            ]);
            Ok(current_turn_host()
                .unwrap()
                .respond_with_stream(Box::pin(Items(items, None))))
        }
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl std::future::Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

/// A WebSocket transport that streams and keeps what it read, in order.
#[derive(Clone)]
struct Logging {
    read: Arc<Mutex<Vec<String>>>,
}

impl Logging {
    /// The configuration the kit keeps of the channel.
    fn config() -> ChannelConfig {
        let mut declaration = ChannelConfig::new(
            channel("ws"),
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        );
        declaration.supports_streaming_delivery = true;
        declaration.recipient_key = None;
        declaration
    }
}

impl Channel for Logging {
    fn channel_id(&self) -> roomkit::model::ChannelId {
        Self::config().channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        Self::config().channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        Self::config().category
    }

    fn direction(&self) -> ChannelDirection {
        Self::config().direction
    }

    fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
        Self::config().capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        Self::config().provider_name.clone()
    }

    fn recipient_key(&self) -> Option<String> {
        Self::config().recipient_key.clone()
    }

    fn supports_streaming_delivery(&self) -> bool {
        Self::config().supports_streaming_delivery
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        while let Some(item) = stream.next().await {
            let line = match item {
                Ok(StreamItem::Text(text)) => format!("text {text:?}"),
                Ok(StreamItem::Row(row)) => {
                    format!("row {:?} {:?}", row.r#type, text_of(&row.content))
                }
                Ok(StreamItem::LoopEnd(end)) => format!("loop_end {}", end.reason),
                Ok(_) => "other".to_owned(),
                Err(error) => format!("error {error}"),
            };
            self.read.lock().unwrap().push(line);
        }
        Ok(ChannelOutput::default())
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl std::future::Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl std::future::Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}

/// The intent of each tool row of `events`.
fn tool_intents(events: &[RoomEvent]) -> Vec<Option<String>> {
    events
        .iter()
        .filter_map(|event| {
            let EventContent::ToolCall(row) = &event.content else {
                return None;
            };
            Some(row.intent.clone())
        })
        .collect()
}

/// The turn link's frames the tap saw, a line each, but its heartbeats and
/// its acceptance: an item by its kind, a text with its deltas.
fn frame_lines(tap: &TurnTap) -> Vec<String> {
    tap.messages()
        .into_iter()
        .filter(|message| message.family == "frame")
        .filter_map(|message| {
            let frame: Value = serde_json::from_str(&message.json).unwrap();
            let frame = &frame["message"];
            let kind = frame["type"].as_str().unwrap_or_default();
            if matches!(kind, "heartbeat" | "accepted") {
                return None;
            }
            let item = &frame["item"];
            Some(match item["kind"].as_str() {
                Some("text") => format!("text {} {}", item["text"], item["deltas"]),
                Some(other) => other.to_owned(),
                None => kind.to_owned(),
            })
        })
        .collect()
}

/// RFC §10.6.4 (RMK-1017, light review 6, S7): text a channel's stream
/// writes inside a call, between its start and its end, crosses in order:
/// the burst kept goes before the call's end, so the call's end never
/// waits on it. Bounded: on a mutation that kept the burst past the end
/// (`B_end_burst`), the call never returned.
#[tokio::test(start_paused = true)]
async fn text_written_inside_a_call_crosses_in_order() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(roomkit::testing::RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        roomkit::testing::RecordingTransportOptions::default(),
    ))
    .unwrap();
    let target = Logging {
        read: Arc::default(),
    };
    kit.register_channel(target.clone()).unwrap();
    kit.register_channel(Writing {
        cut: false,
        interleaves: None,
    })
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ws", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let tap = TurnTap::on(&kit);
    let message = InboundMessage::new(channel("sms"), "+15550000001", TextContent::new("Go."));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..Default::default()
    };
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        kit.process_inbound(message, options),
    )
    .await
    .expect("the call returns")
    .unwrap();
    roomkit::testing::idle(&kit).await;
    let frames = frame_lines(&tap);
    let events = kit.store().list_events(&room(), received()).await.unwrap();
    let stored: Vec<String> = events
        .iter()
        .filter(|event| event.source.channel_id == channel("ai"))
        .map(|event| format!("{:?} {:?}", event.r#type, text_of(&event.content)))
        .collect();
    // The call's intent crosses the turn link with its start and its end.
    let intent = Some("I look.".to_owned());
    assert_eq!(tool_intents(&events), [intent.clone(), intent]);
    let lines = |lines: &[&str]| {
        lines
            .iter()
            .map(|&line| line.to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(
        (
            result.error,
            frames,
            target.read.lock().unwrap().clone(),
            stored
        ),
        (
            None,
            lines(&[
                "output",
                "text \"a \" null",
                "tool_start",
                "text \"b c \" 2",
                "tool_end",
                "text \"d \" null",
                "loop_end",
                "stream_end",
                "end"
            ]),
            lines(&[
                "text \"a \"",
                "row Message \"a \"",
                "row ToolCallStart \"[Tool call: look]\"",
                "text \"b c \"",
                "row ToolCallEnd \"[Tool call: look]\"",
                "text \"d \"",
                "row Message \"b c d \"",
                "loop_end completed"
            ]),
            lines(&[
                "Message \"a \"",
                "ToolCallStart \"[Tool call: look]\"",
                "ToolCallEnd \"[Tool call: look]\"",
                "Message \"b c d \""
            ])
        )
    );
    kit.close().await.unwrap();
}

/// A turn whose stream is cancelled after it wrote fails as Python's
/// turn backend reports it, `TurnLinkError`, "The turn ended in the middle
/// of its stream" (`tasks/probes/step5-review/py_stream_cancelled.out`);
/// it read "The turn ended before its output".
#[tokio::test]
async fn a_stream_cancelled_after_it_wrote_ends_in_the_middle_of_its_stream() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(roomkit::testing::RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        roomkit::testing::RecordingTransportOptions::default(),
    ))
    .unwrap();
    kit.register_channel(Writing {
        cut: true,
        interleaves: None,
    })
    .unwrap();
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
    let message = InboundMessage::new(channel("sms"), "+15550000001", TextContent::new("Go."));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..Default::default()
    };
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        kit.process_inbound(message, options),
    )
    .await
    .expect("the call returns")
    .unwrap();
    assert_eq!(
        result.error.map(|error| (error.error_type, error.message)),
        Some((
            "TurnLinkError".to_owned(),
            "The turn ended in the middle of its stream".to_owned()
        ))
    );
    kit.close().await.unwrap();
}

// RFC §6.4, §10.1 step 15: what a streaming target's `deliver_stream()`
// returns is not its caller's, as Python's `_deliver_segments` keeps
// nothing of it: a `turns` it says never reaches the caller's record,
// which names the agent's turn alone.
#[tokio::test]
async fn a_streaming_target_s_own_turns_never_reach_the_caller() {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(roomkit::testing::RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        roomkit::testing::RecordingTransportOptions::default(),
    ))
    .unwrap();
    let mut says = roomkit::model::Metadata::new();
    says.insert(
        "turns".to_owned(),
        json!({"intruder": {"loop_end_reason": "completed"}}),
    );
    let target = Counting {
        sizes: Arc::default(),
        says,
        stalls: None,
        lingers: None,
        pauses: false,
        fails: false,
    };
    kit.register_channel(target).unwrap();
    let model = ScriptedProvider::new([ScriptedRound::writing(["The answer."])]);
    kit.register_channel(
        AiChannel::new(channel("ai"), model, AiChannelOptions::default()).unwrap(),
    )
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["sms", "ws", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }

    let result = asks(&kit, "Answer.").await;

    let turns: Vec<String> = result
        .response_metadata
        .get("turns")
        .and_then(|turns| Some(turns.as_object()?.keys().cloned().collect()))
        .unwrap_or_default();
    assert_eq!(turns, ["ai"]);
    kit.close().await.unwrap();
}

// RFC §23.3, §12.2 step 13s (HITL c7e4a96a): a task's cancel has no timeout
// to bound its wait, so it does not wait for the reader of its worker's
// stream: a streaming target shared into the child room that reads nothing
// does not hold the cancel, and ends on its own, within the close's grace.
#[tokio::test(start_paused = true)]
async fn a_cancel_does_not_wait_for_a_streaming_target_that_reads_nothing() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let stalled = Arc::new(tokio::sync::Notify::new());
    let target = Counting {
        sizes: Arc::default(),
        says: roomkit::model::Metadata::new(),
        stalls: Some(Arc::clone(&stalled)),
        lingers: None,
        pauses: false,
        fails: false,
    };
    kit.register_channel(target).unwrap();
    let model = ScriptedProvider::new([ScriptedRound::writing(["Working."])]);
    kit.register_channel(
        AiChannel::new(channel("ai"), model, AiChannelOptions::default()).unwrap(),
    )
    .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    kit.attach_channel(&room(), &channel("ws"), AttachChannelOptions::default())
        .await
        .unwrap();
    let options = roomkit::DelegateOptions {
        share_channels: Some(vec![channel("ws")]),
        ..roomkit::DelegateOptions::default()
    };
    let task = kit
        .delegate(&room(), &channel("ai"), "Go.", options)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), stalled.notified())
        .await
        .unwrap();

    let cancel = kit.cancel_task(&task.id, roomkit::CancelTaskOptions::default());
    let cancelled = tokio::time::timeout(Duration::from_secs(5), cancel).await;

    assert_eq!(
        (cancelled, task.result().map(|ended| ended.status)),
        (Ok(true), Some(roomkit::model::TaskStatus::Cancelled))
    );
    kit.close().await.unwrap();
}

// RFC §10.1, §10.6.8 (RMK-1323): the cancel cuts the turn whatever the target
// does with it: a tool that hangs is cut though the target fails on the
// stream's end, and neither the call nor the close waits for that tool.
#[tokio::test(start_paused = true)]
async fn a_cut_target_that_fails_leaves_no_tool_running() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let target = Counting {
        sizes: Arc::default(),
        says: roomkit::model::Metadata::new(),
        stalls: None,
        lingers: None,
        pauses: false,
        fails: true,
    };
    kit.register_channel(target).unwrap();
    let waiting = Arc::new(tokio::sync::Notify::new());
    let told = Arc::clone(&waiting);
    let wait = move |_call, _context| {
        let told = Arc::clone(&told);
        async move {
            told.notify_one();
            std::future::pending::<Result<roomkit_ai::ToolResult, roomkit_ai::ToolError>>().await
        }
    };
    let model = ScriptedProvider::new([
        ScriptedRound::calling("Hello.", [("t1", "wait", "{}")]),
        ScriptedRound::answering("Bye."),
    ]);
    let options = AiChannelOptions {
        tools: roomkit_ai::Tools::new()
            .tool(roomkit_ai::AiTool::new("wait", "d").unwrap(), wait)
            .into(),
        ..AiChannelOptions::default()
    };
    kit.register_channel(AiChannel::new(channel("ai"), model, options).unwrap())
        .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["ws", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let mut message = InboundMessage::new(channel("ws"), "alice", TextContent::new("hi"));
    message.delivery_key = Some("k".to_owned());
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    let sender = kit.clone();
    let processed = tokio::spawn(async move { sender.process_inbound(message, options).await });
    waiting.notified().await;
    let start = tokio::time::Instant::now();
    kit.cancel_delivery("k", roomkit::CancelDeliveryOptions::default())
        .await;
    let processed = processed.await.unwrap().unwrap();
    kit.close().await.unwrap();
    assert_eq!(start.elapsed(), Duration::ZERO);
    assert_eq!(
        processed.cancellation_reason.as_deref(),
        Some("caller_cancelled")
    );
}

// RFC §12.10.4 (RMK-1326): a close called from a tool of the turn spares that
// turn's run, not the target its read left: it waits for that target within its
// grace, then drops it, as a close from outside does.
#[tokio::test(start_paused = true)]
async fn a_close_from_a_turn_s_tool_gives_a_target_left_reading_its_grace() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let target = Counting {
        sizes: Arc::default(),
        says: roomkit::model::Metadata::new(),
        stalls: None,
        lingers: Some(Arc::new(tokio::sync::Notify::new())),
        pauses: false,
        fails: false,
    };
    kit.register_channel(target).unwrap();
    let took = Arc::new(Mutex::new(None));
    let (closer, noted) = (kit.clone(), Arc::clone(&took));
    let close = move |_call, _context| {
        let (closer, noted) = (closer.clone(), Arc::clone(&noted));
        async move {
            let start = tokio::time::Instant::now();
            drop(closer.close().await);
            *noted.lock().unwrap() = Some(start.elapsed());
            Ok(roomkit_ai::ToolResult::Text("closed".to_owned()))
        }
    };
    let model = ScriptedProvider::new([
        ScriptedRound::calling("Hello.", [("t1", "close", "{}")]),
        ScriptedRound::answering("Bye."),
    ]);
    let options = AiChannelOptions {
        tools: roomkit_ai::Tools::new()
            .tool(roomkit_ai::AiTool::new("close", "d").unwrap(), close)
            .into(),
        ..AiChannelOptions::default()
    };
    kit.register_channel(AiChannel::new(channel("ai"), model, options).unwrap())
        .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["ws", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let message = InboundMessage::new(channel("ws"), "alice", TextContent::new("hi"));
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    drop(kit.process_inbound(message, options).await);
    kit.close().await.unwrap();
    let took = *took.lock().unwrap();
    assert_eq!(took, Some(Duration::from_secs(5)));
}

// RFC §12.2 step 13s, §13.6 (RMK-1236, RMK-1326): Python's close finds the
// cascade cancelled and leaves it to end within its grace: a delivery is
// cancelled by its key, and its streaming target, handed the stream's end,
// never returns; the close drops it at the end of its grace, and the call,
// its read stored, has answered by then.
#[tokio::test(start_paused = true)]
async fn a_close_gives_a_target_whose_read_was_stopped_its_grace() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let ended = Arc::new(tokio::sync::Notify::new());
    let target = Counting {
        sizes: Arc::default(),
        says: roomkit::model::Metadata::new(),
        stalls: None,
        lingers: Some(Arc::clone(&ended)),
        pauses: false,
        fails: false,
    };
    kit.register_channel(target).unwrap();
    let waiting = Arc::new(tokio::sync::Notify::new());
    let told = Arc::clone(&waiting);
    let wait = move |_call, _context| {
        let told = Arc::clone(&told);
        async move {
            told.notify_one();
            std::future::pending::<Result<roomkit_ai::ToolResult, roomkit_ai::ToolError>>().await
        }
    };
    let model = ScriptedProvider::new([
        ScriptedRound::calling("Hello.", [("t1", "wait", "{}")]),
        ScriptedRound::answering("Bye."),
    ]);
    let options = AiChannelOptions {
        tools: roomkit_ai::Tools::new()
            .tool(roomkit_ai::AiTool::new("wait", "d").unwrap(), wait)
            .into(),
        ..AiChannelOptions::default()
    };
    kit.register_channel(AiChannel::new(channel("ai"), model, options).unwrap())
        .unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["ws", "ai"] {
        kit.attach_channel(&room(), &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let mut message = InboundMessage::new(channel("ws"), "alice", TextContent::new("hi"));
    message.delivery_key = Some("k".to_owned());
    let options = ProcessInboundOptions {
        room_id: Some(room()),
        ..ProcessInboundOptions::default()
    };
    let sender = kit.clone();
    let processed = tokio::spawn(async move { sender.process_inbound(message, options).await });
    tokio::time::timeout(Duration::from_secs(5), waiting.notified())
        .await
        .unwrap();
    let stopped = kit
        .cancel_delivery("k", roomkit::CancelDeliveryOptions::default())
        .await;
    tokio::time::timeout(Duration::from_secs(5), ended.notified())
        .await
        .unwrap();

    let start = tokio::time::Instant::now();
    kit.close().await.unwrap();
    let closed_in = start.elapsed();

    let reason = processed.await.unwrap().unwrap().cancellation_reason;
    assert_eq!(
        (stopped, closed_in, reason.as_deref()),
        (1, Duration::from_secs(5), Some("caller_cancelled"))
    );
}
