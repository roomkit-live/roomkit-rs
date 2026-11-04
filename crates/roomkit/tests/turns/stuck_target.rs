//! RFC §12.2 step 13s (RMK-1326, HITL 81395c77, d406164d): a streaming
//! target that never returns once a cut handed it its stream's end, or while
//! it waits on its own between two reads: what it holds, and what it does
//! not. Measured beside Python's, with a target that blocks in a `finally`
//! (Python's other shapes return at once).

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelOutput, ChannelType, EventType,
    InboundMessage, RoomContext, RoomEvent, RoomId, TextContent,
};
use roomkit::{
    AttachChannelOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    DeliveryStream, ProcessInboundOptions, RoomKit, RoomKitOptions, StreamItem,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions};
use roomkit_core::ops::ChannelConfig;
use serde_json::json;
use tokio::sync::Notify;
use tokio::time::{Instant, timeout};

use crate::support::{channel, received};

/// Where a target gets stuck, on its first stream only.
#[derive(Clone, Copy, Debug)]
enum Stuck {
    /// In its own wait, between two reads: a socket that takes no more.
    BetweenReads,
    /// Past the end of the stream its read was stopped on.
    PastTheEnd,
}

/// A target that leaves while the call it read the start of runs, its next
/// read in flight.
struct LeavesWhileACallRuns {
    running: Arc<Notify>,
    release: Arc<Notify>,
}

struct Target {
    stuck: Stuck,
    first: AtomicBool,
    told: Arc<Notify>,
    read: Arc<Mutex<Vec<String>>>,
}

impl Target {
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

impl Channel for Target {
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

    fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
        let config = Self::config();
        let addressing = roomkit::ChannelAddressing {
            addressing: config.addressing,
            recipient_key: config.recipient_key.clone(),
            chat_key: config.chat_key.clone(),
            replies_to_sender: false,
        };
        (addressing, config.country_code.clone())
    }

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let first = self.first.swap(false, Ordering::SeqCst);
        while let Some(item) = stream.next().await {
            if let Ok(StreamItem::Text(text)) = item {
                self.read.lock().unwrap().push(text);
            }
            if first {
                self.told.notify_one();
                if matches!(self.stuck, Stuck::BetweenReads) {
                    std::future::pending::<()>().await;
                }
            }
        }
        if first {
            std::future::pending::<()>().await;
        }
        Ok(ChannelOutput::empty())
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
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

fn options(room: &RoomId) -> ProcessInboundOptions {
    ProcessInboundOptions {
        room_id: Some(room.clone()),
        ..ProcessInboundOptions::default()
    }
}

fn message(body: &str) -> InboundMessage {
    InboundMessage::new(channel("ws"), "alice", TextContent::new(body))
}

/// A kit with `stuck`'s target and an agent that calls a tool that never
/// returns, then answers; rooms `a` and `b`, a first message in `a` under
/// the key `k`, cut as the target waits.
struct Cut {
    kit: RoomKit,
    read: Arc<Mutex<Vec<String>>>,
    first: tokio::task::JoinHandle<Result<roomkit::InboundResult, roomkit::RoomKitError>>,
    a: RoomId,
    b: RoomId,
}

async fn cut(stuck: Stuck, serialize: bool) -> Cut {
    let kit = RoomKit::new(RoomKitOptions::default());
    let told = Arc::new(Notify::new());
    let read = Arc::new(Mutex::new(Vec::new()));
    kit.register_channel(Target {
        stuck,
        first: AtomicBool::new(true),
        told: Arc::clone(&told),
        read: Arc::clone(&read),
    })
    .unwrap();
    let model = ScriptedProvider::new([
        ScriptedRound::calling("Hello.", [("t1", "wait", "{}")]),
        ScriptedRound::answering("second"),
        ScriptedRound::answering("third"),
    ]);
    let running = Arc::new(Notify::new());
    let on_run = Arc::clone(&running);
    let wait = move |_call, _context| {
        let on_run = Arc::clone(&on_run);
        async move {
            on_run.notify_one();
            std::future::pending::<Result<roomkit_ai::ToolResult, roomkit_ai::ToolError>>().await
        }
    };
    let agent_options = AiChannelOptions {
        tools: roomkit_ai::Tools::new()
            .tool(roomkit_ai::AiTool::new("wait", "d").unwrap(), wait)
            .into(),
        serialize_room_turns: serialize,
        ..AiChannelOptions::default()
    };
    kit.register_channel(AiChannel::new(channel("ai"), model, agent_options).unwrap())
        .unwrap();
    let (a, b) = (RoomId::new("a").unwrap(), RoomId::new("b").unwrap());
    for room in [&a, &b] {
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        for id in ["ws", "ai"] {
            kit.attach_channel(room, &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
    }
    let mut one = message("one");
    one.delivery_key = Some("k".to_owned());
    let sender = kit.clone();
    let opts = options(&a);
    let first = tokio::spawn(async move { sender.process_inbound(one, opts).await });
    timeout(LONG, told.notified()).await.unwrap();
    if matches!(stuck, Stuck::PastTheEnd) {
        timeout(LONG, running.notified()).await.unwrap();
    }
    let cancelled = kit
        .cancel_delivery("k", roomkit::CancelDeliveryOptions::default())
        .await;
    assert_eq!(cancelled, 1);
    Cut {
        kit,
        read,
        first,
        a,
        b,
    }
}

/// Longer than anything a test here waits for, on its clock.
const LONG: Duration = Duration::from_secs(60);

// An ordinary agent: the cut target holds its caller alone. Python's blocked
// `finally`: the same, next message 0.011 s, other room 0.003 s.
#[tokio::test(start_paused = true)]
async fn a_stuck_target_holds_its_caller_and_no_other_work() {
    for stuck in [Stuck::BetweenReads, Stuck::PastTheEnd] {
        let mut cut = cut(stuck, false).await;
        assert!(timeout(LONG, &mut cut.first).await.is_err(), "{stuck:?}");

        let start = Instant::now();
        let next = cut.kit.process_inbound(message("two"), options(&cut.a));
        timeout(LONG, next).await.unwrap().unwrap();
        let other = cut.kit.process_inbound(message("three"), options(&cut.b));
        timeout(LONG, other).await.unwrap().unwrap();
        assert_eq!(start.elapsed(), Duration::ZERO, "{stuck:?}");
        assert_eq!(
            cut.read.lock().unwrap().as_slice(),
            ["Hello.", "second", "third"],
            "{stuck:?}"
        );

        let start = Instant::now();
        cut.kit.close().await.unwrap();
        assert_eq!(start.elapsed(), Duration::from_secs(5), "{stuck:?}");
        timeout(LONG, &mut cut.first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

// An agent that takes the room's turns one after the other: the room's next
// message waits for the cut target until the kit's close, the other room goes
// on. Python's blocked `finally` waits too; Python with no `finally` does not,
// at 0.003 s. The maintainer decides on HITL d406164d.
#[tokio::test(start_paused = true)]
async fn a_stuck_target_holds_the_room_s_line_of_a_serializing_agent() {
    for stuck in [Stuck::BetweenReads, Stuck::PastTheEnd] {
        let mut cut = cut(stuck, true).await;
        assert!(timeout(LONG, &mut cut.first).await.is_err(), "{stuck:?}");

        let next = cut.kit.process_inbound(message("two"), options(&cut.a));
        assert!(timeout(LONG, next).await.is_err(), "{stuck:?}");
        let other = cut.kit.process_inbound(message("three"), options(&cut.b));
        timeout(LONG, other).await.unwrap().unwrap();

        cut.kit.close().await.unwrap();
        timeout(LONG, &mut cut.first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

macro_rules! streaming_channel_declaration {
    () => {
        fn channel_id(&self) -> roomkit::model::ChannelId {
            Target::config().channel_id.clone()
        }

        fn channel_type(&self) -> ChannelType {
            Target::config().channel_type.clone()
        }

        fn category(&self) -> ChannelCategory {
            Target::config().category
        }

        fn direction(&self) -> ChannelDirection {
            Target::config().direction
        }

        fn capabilities(&self) -> roomkit::model::ChannelCapabilities {
            Target::config().capabilities.clone()
        }

        fn provider_name(&self) -> Option<String> {
            Target::config().provider_name.clone()
        }

        fn recipient_key(&self) -> Option<String> {
            Target::config().recipient_key.clone()
        }

        fn supports_streaming_delivery(&self) -> bool {
            Target::config().supports_streaming_delivery
        }

        fn address_config(&self) -> (roomkit::ChannelAddressing, Option<roomkit::CountryCode>) {
            let config = Target::config();
            let addressing = roomkit::ChannelAddressing {
                addressing: config.addressing,
                recipient_key: config.recipient_key.clone(),
                chat_key: config.chat_key.clone(),
                replies_to_sender: false,
            };
            (addressing, config.country_code.clone())
        }
    };
}

impl Channel for LeavesWhileACallRuns {
    streaming_channel_declaration!();

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        while let Some(item) = stream.next().await {
            let started = matches!(
                &item,
                Ok(StreamItem::Row(row)) if row.r#type == EventType::ToolCallStart
            );
            if started {
                break;
            }
        }
        tokio::select! {
            _ = stream.next() => {}
            () = self.running.notified() => {}
        }
        self.release.notify_one();
        Ok(ChannelOutput::empty())
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
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

// Python: tests/tasks/test_stopped_stream_turn_end.py::test_a_reader_gone_while_a_call_runs_leaves_no_generation_in_flight
#[tokio::test(start_paused = true)]
async fn a_reader_gone_while_a_call_runs_leaves_no_generation_in_flight() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let running = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    kit.register_channel(LeavesWhileACallRuns {
        running: Arc::clone(&running),
        release: Arc::clone(&release),
    })
    .unwrap();
    let used = json!({ "input_tokens": 1000, "output_tokens": 10 });
    let model =
        ScriptedProvider::new([
            ScriptedRound::calling("Checking.", [("c1", "lookup", "{}")])
                .with_usage(used.as_object().unwrap().clone()),
        ]);
    let wait = move |_call, _context| {
        let (running, release) = (Arc::clone(&running), Arc::clone(&release));
        async move {
            running.notify_one();
            release.notified().await;
            Ok::<_, roomkit_ai::ToolError>(roomkit_ai::ToolResult::Text("found".to_owned()))
        }
    };
    let options_of_agent = AiChannelOptions {
        tools: roomkit_ai::Tools::new()
            .tool(roomkit_ai::AiTool::new("lookup", "d").unwrap(), wait)
            .into(),
        ..AiChannelOptions::default()
    };
    kit.register_channel(AiChannel::new(channel("ai"), model, options_of_agent).unwrap())
        .unwrap();
    let room = RoomId::new("r").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["ws", "ai"] {
        kit.attach_channel(&room, &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }

    let sent = kit.process_inbound(message("Go."), options(&room));
    timeout(LONG, sent).await.unwrap().unwrap();

    let rows = kit.store().list_events(&room, received()).await.unwrap();
    let said = rows
        .iter()
        .find(|row| row.source.channel_id == channel("ai") && row.r#type == EventType::Message)
        .unwrap();
    assert_eq!(said.metadata["loop_end_reason"], json!("cancelled"));
    assert_eq!(said.metadata["ai_usage_complete"], json!(true));
    kit.close().await.unwrap();
}
