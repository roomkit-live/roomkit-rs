//! RFC §12.2 step 13s (RMK-1326, RMK-1343, HITL 81395c77, d406164d): a
//! streaming target that never returns once a cut handed it its stream's end,
//! or while it waits on its own between two reads: what it holds, and what it
//! does not. Measured beside Python's, with a target that blocks in a `finally`
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
    DeliveryStream, HookExecution, HookOptions, HookTrigger, ProcessInboundOptions, RoomKit,
    RoomKitOptions, StreamItem,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions};
use roomkit_core::ops::ChannelConfig;
use serde_json::{Value, json};
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
    /// Past that end for a while: it flushes, then returns.
    Flushes,
    /// Past that end for a while: it flushes, then fails.
    FailsFlushing,
    /// At that end, with no wait: it fails.
    FailsAtOnce,
}

/// How the agent takes the room's turns.
#[derive(Clone, Copy, Debug)]
enum Turns {
    /// Side by side.
    Ordinary,
    /// One after the other.
    Serialized,
    /// One after the other, a message handed to the running one: roomkit-tui's.
    Steered,
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
    delivered: Arc<Mutex<Vec<EventType>>>,
    /// How long a delivery takes it.
    delivers_in: Duration,
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
        if !first {
            return Ok(ChannelOutput::empty());
        }
        if matches!(self.stuck, Stuck::FailsAtOnce) {
            return Err(std::io::Error::other("the flush failed").into());
        }
        if matches!(self.stuck, Stuck::BetweenReads | Stuck::PastTheEnd) {
            std::future::pending::<()>().await;
        }
        tokio::time::sleep(FLUSH).await;
        self.read.lock().unwrap().push("flushed".to_owned());
        if matches!(self.stuck, Stuck::FailsFlushing) {
            return Err(std::io::Error::other("the flush failed").into());
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
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.delivered.lock().unwrap().push(event.r#type);
        let takes = self.delivers_in;
        async move {
            tokio::time::sleep(takes).await;
            Ok(ChannelOutput::empty())
        }
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
    delivered: Arc<Mutex<Vec<EventType>>>,
    /// Told of each `ON_ERROR`.
    reported: Arc<Notify>,
    first: tokio::task::JoinHandle<Result<roomkit::InboundResult, roomkit::RoomKitError>>,
    a: RoomId,
    b: RoomId,
}

async fn cut(stuck: Stuck, turns: Turns) -> Cut {
    cut_with(stuck, turns, Duration::ZERO).await
}

/// [`cut`], its target taking `delivers_in` for each delivery.
async fn cut_with(stuck: Stuck, turns: Turns, delivers_in: Duration) -> Cut {
    let bench = bench(stuck, turns, delivers_in, true).await;
    let kit = bench.kit;
    let mut one = message("one");
    one.delivery_key = Some("k".to_owned());
    let sender = kit.clone();
    let opts = options(&bench.a);
    let first = tokio::spawn(async move { sender.process_inbound(one, opts).await });
    timeout(LONG, bench.told.notified()).await.unwrap();
    if !matches!(stuck, Stuck::BetweenReads) {
        timeout(LONG, bench.running.notified()).await.unwrap();
    }
    let cancelled = kit
        .cancel_delivery("k", roomkit::CancelDeliveryOptions::default())
        .await;
    assert_eq!(cancelled, 1);
    Cut {
        kit,
        read: bench.read,
        delivered: bench.delivered,
        reported: bench.reported,
        first,
        a: bench.a,
        b: bench.b,
    }
}

/// A kit with `stuck`'s target and an agent that writes "Hello." and calls a
/// tool that never returns, then answers; rooms `a` and `b` bind the target,
/// and the agent when `agent_bound`.
struct Bench {
    kit: RoomKit,
    /// Told once the target read its first item.
    told: Arc<Notify>,
    /// Told once the tool runs.
    running: Arc<Notify>,
    read: Arc<Mutex<Vec<String>>>,
    delivered: Arc<Mutex<Vec<EventType>>>,
    /// Told of each `ON_ERROR`.
    reported: Arc<Notify>,
    /// Each `ON_ERROR`'s payload.
    errors: Arc<Mutex<Vec<RoomEvent>>>,
    a: RoomId,
    b: RoomId,
}

async fn bench(stuck: Stuck, turns: Turns, delivers_in: Duration, agent_bound: bool) -> Bench {
    let kit = RoomKit::new(RoomKitOptions::default());
    let told = Arc::new(Notify::new());
    let read = Arc::new(Mutex::new(Vec::new()));
    let delivered = Arc::new(Mutex::new(Vec::new()));
    kit.register_channel(Target {
        stuck,
        first: AtomicBool::new(true),
        told: Arc::clone(&told),
        read: Arc::clone(&read),
        delivered: Arc::clone(&delivered),
        delivers_in,
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
        serialize_room_turns: matches!(turns, Turns::Serialized),
        steer_room_turns: matches!(turns, Turns::Steered),
        ..AiChannelOptions::default()
    };
    kit.register_channel(AiChannel::new(channel("ai"), model, agent_options).unwrap())
        .unwrap();
    let reported = Arc::new(Notify::new());
    let errors = Arc::new(Mutex::new(Vec::new()));
    let on_error = Arc::clone(&reported);
    let kept = Arc::clone(&errors);
    kit.hook(
        HookTrigger::OnError,
        move |event: RoomEvent, _context| {
            kept.lock().unwrap().push(event);
            on_error.notify_one();
            async {}
        },
        HookOptions {
            execution: HookExecution::Async,
            name: "error".to_owned(),
            ..Default::default()
        },
    )
    .unwrap();
    let (a, b) = (RoomId::new("a").unwrap(), RoomId::new("b").unwrap());
    let bound: &[&str] = if agent_bound { &["ws", "ai"] } else { &["ws"] };
    for room in [&a, &b] {
        kit.create_room(CreateRoomOptions {
            room_id: Some(room.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        for id in bound {
            kit.attach_channel(room, &channel(id), AttachChannelOptions::default())
                .await
                .unwrap();
        }
    }
    Bench {
        kit,
        told,
        running,
        read,
        delivered,
        reported,
        errors,
        a,
        b,
    }
}

/// What each `ON_ERROR` payload says: its source, its error, its category.
fn reported(errors: &Mutex<Vec<RoomEvent>>) -> Vec<(String, Value, Value)> {
    errors
        .lock()
        .unwrap()
        .iter()
        .map(|error| {
            (
                error.source.channel_id.to_string(),
                error.metadata["error"].clone(),
                error.metadata["error_category"].clone(),
            )
        })
        .collect()
}

/// What a target that fails past its stream's end reports, once.
fn the_flush_failed() -> Vec<(String, Value, Value)> {
    vec![(
        "ai".to_owned(),
        json!("the flush failed"),
        json!("streaming"),
    )]
}

/// Longer than anything a test here waits for, on its clock.
const LONG: Duration = Duration::from_secs(60);

/// How long a target that flushes works past its stream's end.
const FLUSH: Duration = Duration::from_millis(200);

// An ordinary agent: the cut target holds its caller alone. Python's blocked
// `finally`: the same, next message 0.011 s, other room 0.003 s.
#[tokio::test(start_paused = true)]
async fn a_stuck_target_holds_its_caller_and_no_other_work() {
    for stuck in [Stuck::BetweenReads, Stuck::PastTheEnd] {
        let mut cut = cut(stuck, Turns::Ordinary).await;
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

/// A room's rows as the store holds them in their order, but its bindings:
/// each one's type, its text and the turn's end it records.
async fn rows_of(kit: &RoomKit, room: &RoomId) -> Vec<(EventType, String, Option<Value>)> {
    let rows = kit.store().list_events(room, received()).await.unwrap();
    rows.iter()
        .filter(|row| row.r#type != EventType::ChannelAttached)
        .map(|row| {
            (
                row.r#type,
                roomkit_core::rules::transcoding::text_of(&row.content),
                row.metadata.get("loop_end_reason").cloned(),
            )
        })
        .collect()
}

/// What room `a` holds of a cut in a call's run: the message that asked, and
/// the turn's rows, its record on its last message.
fn cut_in_a_call() -> Vec<(EventType, String, Option<Value>)> {
    vec![
        (EventType::Message, "one".to_owned(), None),
        (
            EventType::Message,
            "Hello.".to_owned(),
            Some(json!("cancelled")),
        ),
        (
            EventType::ToolCallStart,
            "[Tool call: wait]".to_owned(),
            None,
        ),
        (EventType::ToolCallEnd, "[Tool call: wait]".to_owned(), None),
    ]
}

// An agent that takes the room's turns one after the other, or steers them: at
// the cut the read stores what it has and gives its place up, and only its
// caller still waits for the target (RMK-1343, HITL d406164d). Python with no
// `finally`: the next message 0.003 s. Python's blocked `finally` waits there,
// the target's defect, and stores nothing: on that the maintainer chose apart.
#[tokio::test(start_paused = true)]
async fn a_stuck_target_holds_its_caller_and_not_the_room_s_line() {
    for turns in [Turns::Serialized, Turns::Steered] {
        for stuck in [Stuck::BetweenReads, Stuck::PastTheEnd] {
            let mut cut = cut(stuck, turns).await;
            assert!(timeout(LONG, &mut cut.first).await.is_err(), "{stuck:?}");
            let mut expected = if matches!(stuck, Stuck::PastTheEnd) {
                cut_in_a_call()
            } else {
                vec![
                    (EventType::Message, "one".to_owned(), None),
                    (
                        EventType::Message,
                        "Hello.".to_owned(),
                        Some(json!("cancelled")),
                    ),
                ]
            };
            assert_eq!(
                rows_of(&cut.kit, &cut.a).await,
                expected,
                "{stuck:?} {turns:?}"
            );

            let start = Instant::now();
            let next = cut.kit.process_inbound(message("two"), options(&cut.a));
            timeout(LONG, next).await.unwrap().unwrap();
            let other = cut.kit.process_inbound(message("three"), options(&cut.b));
            timeout(LONG, other).await.unwrap().unwrap();
            assert_eq!(start.elapsed(), Duration::ZERO, "{stuck:?} {turns:?}");
            expected.extend([
                (EventType::Message, "two".to_owned(), None),
                (
                    EventType::Message,
                    "second".to_owned(),
                    Some(json!("completed")),
                ),
            ]);
            assert_eq!(
                rows_of(&cut.kit, &cut.a).await,
                expected,
                "{stuck:?} {turns:?}"
            );

            let start = Instant::now();
            cut.kit.close().await.unwrap();
            assert_eq!(
                start.elapsed(),
                Duration::from_secs(5),
                "{stuck:?} {turns:?}"
            );
            timeout(LONG, &mut cut.first)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }
}

// A target that flushes past its cut: the room's next turn does not wait for it,
// where Python's `finally` of 0.2 s holds it 0.2 s (HITL d406164d), and its caller
// does, as Python's: the call returns once the flush is over.
#[tokio::test(start_paused = true)]
async fn the_room_s_next_turn_starts_while_a_cut_target_flushes() {
    let mut cut = cut(Stuck::Flushes, Turns::Serialized).await;
    let start = Instant::now();
    let next = cut.kit.process_inbound(message("two"), options(&cut.a));
    timeout(LONG, next).await.unwrap().unwrap();
    assert_eq!(start.elapsed(), Duration::ZERO);
    assert_eq!(
        cut.read.lock().unwrap().as_slice(),
        ["Hello.", "second"],
        "the flush is not over"
    );
    // The call's end the stream no longer carries reaches it meanwhile; in
    // Python, past its `finally`.
    assert_eq!(
        cut.delivered.lock().unwrap().as_slice(),
        [EventType::ToolCallEnd]
    );

    timeout(LONG, &mut cut.first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(start.elapsed(), FLUSH);
    assert_eq!(
        cut.read.lock().unwrap().as_slice(),
        ["Hello.", "second", "flushed"]
    );
    cut.kit.close().await.unwrap();
}

// A target that fails once it flushed past its cut: what the cut stored stands,
// and its caller reads the failure, whichever ends first, the cut's end or the
// run's, a delivery still running holding the run (RMK-1343, HITL fcaf1ecd).
// Python, whose `finally` fails before anything is stored, stores the text
// unmarked, with no record.
#[tokio::test(start_paused = true)]
async fn a_target_that_fails_flushing_leaves_what_the_cut_stored() {
    for delivers_in in [Duration::ZERO, FLUSH * 5] {
        let mut cut = cut_with(Stuck::FailsFlushing, Turns::Serialized, delivers_in).await;
        let result = timeout(LONG, &mut cut.first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let error = result.error.as_ref().map(|error| error.message.as_str());
        assert_eq!(error, Some("the flush failed"), "{delivers_in:?}");
        assert_eq!(
            result.response_metadata.get("turns").unwrap()["ai"]["loop_end_reason"],
            json!("cancelled"),
            "{delivers_in:?}"
        );
        let said = result
            .response_events
            .iter()
            .find(|row| row.r#type == EventType::Message);
        assert_eq!(
            said.and_then(|row| row.metadata.get("loop_end_reason")),
            Some(&json!("cancelled")),
            "{delivers_in:?}"
        );
        timeout(LONG, cut.reported.notified()).await.unwrap();
        assert_eq!(rows_of(&cut.kit, &cut.a).await, cut_in_a_call());
        cut.kit.close().await.unwrap();
    }
}

// A target that fails at its stream's end, with no wait: its read ends before
// anything is stored, as Python's: the text unmarked with no record, its failure
// its caller's, whichever ends first (HITL fcaf1ecd).
#[tokio::test(start_paused = true)]
async fn a_target_that_fails_at_once_stays_as_python() {
    for delivers_in in [Duration::ZERO, FLUSH * 5] {
        let mut cut = cut_with(Stuck::FailsAtOnce, Turns::Serialized, delivers_in).await;
        let result = timeout(LONG, &mut cut.first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let error = result.error.as_ref().map(|error| error.message.as_str());
        assert_eq!(error, Some("the flush failed"), "{delivers_in:?}");
        assert_eq!(
            result.response_metadata.get("turns"),
            None,
            "{delivers_in:?}"
        );
        let mut unrecorded = cut_in_a_call();
        unrecorded[1].2 = None;
        assert_eq!(rows_of(&cut.kit, &cut.a).await, unrecorded);
        cut.kit.close().await.unwrap();
    }
}

// RFC §12.2 step 13s (RMK-1330, HITL 267527a7): the kit's close cuts the turn a
// target reads and leaves the target to end alone; one that fails past its
// stream's end, at once or once it flushed, is reported to ON_ERROR once, as the
// response's source, as Python's target whose `finally` fails.
#[tokio::test(start_paused = true)]
async fn a_target_that_fails_past_the_close_s_cut_fires_on_error() {
    for stuck in [Stuck::FailsAtOnce, Stuck::FailsFlushing] {
        let bench = bench(stuck, Turns::Ordinary, Duration::ZERO, true).await;
        let sender = bench.kit.clone();
        let opts = options(&bench.a);
        let first = tokio::spawn(async move { sender.process_inbound(message("one"), opts).await });
        timeout(LONG, bench.told.notified()).await.unwrap();
        timeout(LONG, bench.running.notified()).await.unwrap();

        bench.kit.close().await.unwrap();
        timeout(LONG, first).await.unwrap().unwrap().unwrap();

        assert_eq!(reported(&bench.errors), the_flush_failed(), "{stuck:?}");
    }
}

// RFC §12.2 step 13s, §23.3 (RMK-1330, HITL 267527a7): cancel_task cuts the
// worker's turn a shared target reads and leaves the target to end alone; one
// that fails past its stream's end, at once or once it flushed, is reported to
// ON_ERROR once, as the response's source, as Python's.
#[tokio::test(start_paused = true)]
async fn a_target_that_fails_past_a_task_s_cancel_fires_on_error() {
    for stuck in [Stuck::FailsAtOnce, Stuck::FailsFlushing] {
        let bench = bench(stuck, Turns::Ordinary, Duration::ZERO, false).await;
        let shared = roomkit::DelegateOptions {
            share_channels: Some(vec![channel("ws")]),
            ..roomkit::DelegateOptions::default()
        };
        let task = bench
            .kit
            .delegate(&bench.a, &channel("ai"), "Go.", shared)
            .await
            .unwrap();
        timeout(LONG, bench.told.notified()).await.unwrap();
        timeout(LONG, bench.running.notified()).await.unwrap();

        let cancel = bench
            .kit
            .cancel_task(&task.id, roomkit::CancelTaskOptions::default());
        assert!(timeout(LONG, cancel).await.unwrap(), "{stuck:?}");
        timeout(LONG, bench.reported.notified()).await.unwrap();
        roomkit::testing::idle(&bench.kit).await;

        assert_eq!(reported(&bench.errors), the_flush_failed(), "{stuck:?}");
        bench.kit.close().await.unwrap();
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

/// A target that waits, once it read a row of its first response, until it is
/// released: its read of the failure comes after.
struct WaitsOnARow {
    first: AtomicBool,
    told: Arc<Notify>,
    release: Arc<Notify>,
}

impl Channel for WaitsOnARow {
    streaming_channel_declaration!();

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let first = self.first.swap(false, Ordering::SeqCst);
        while let Some(item) = stream.next().await {
            if first && matches!(item, Ok(StreamItem::Row(_))) {
                self.told.notify_one();
                self.release.notified().await;
            }
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

// A response that failed before the cut, its target still working on the failed
// text's row: the room's next turn does not wait for it, and its caller reads
// the response's failure. Python's cut wins there (RMK-1387).
#[tokio::test(start_paused = true)]
async fn a_response_failed_before_a_cut_keeps_its_failure() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let told = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    kit.register_channel(WaitsOnARow {
        first: AtomicBool::new(true),
        told: Arc::clone(&told),
        release: Arc::clone(&release),
    })
    .unwrap();
    let agent_options = AiChannelOptions {
        serialize_room_turns: true,
        ..AiChannelOptions::default()
    };
    let model = ScriptedProvider::new([
        ScriptedRound::writing(["Hello."]).failing("boom"),
        ScriptedRound::answering("second"),
    ]);
    kit.register_channel(AiChannel::new(channel("ai"), model, agent_options).unwrap())
        .unwrap();
    let a = RoomId::new("a").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(a.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ["ws", "ai"] {
        kit.attach_channel(&a, &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    let mut one = message("one");
    one.delivery_key = Some("k".to_owned());
    let sender = kit.clone();
    let opts = options(&a);
    let first = tokio::spawn(async move { sender.process_inbound(one, opts).await });
    timeout(LONG, told.notified()).await.unwrap();
    let cancelled = kit
        .cancel_delivery("k", roomkit::CancelDeliveryOptions::default())
        .await;
    assert_eq!(cancelled, 1);
    let start = Instant::now();
    let next = kit.process_inbound(message("two"), options(&a));
    timeout(LONG, next).await.unwrap().unwrap();
    assert_eq!(start.elapsed(), Duration::ZERO);

    release.notify_one();
    let first = timeout(LONG, first).await.unwrap().unwrap().unwrap();
    let error = first.error.as_ref().map(|error| error.message.as_str());
    assert_eq!(error, Some("boom"));
    kit.close().await.unwrap();
}

// Python: tests/tasks/test_stopped_stream_turn_end.py, a reader's stop cancels
// the generation it read, which tells nothing: the record holds what the
// generations before it told, and reads incomplete, whatever the generation in
// flight went on to say here (RMK-1333 review).
mod a_read_in_flight {
    use std::pin::Pin;
    use std::task::{Context, Poll, ready};

    use futures_core::Stream;
    use roomkit_ai::{
        AiContext, AiProvider, AiResponse, ProviderError, ProviderStream, StreamEvent,
    };

    use super::*;

    struct HoldsDone {
        script: ScriptedProvider,
        round: usize,
        at: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct Held<'a> {
        inner: ProviderStream<'a>,
        hold: Option<(Arc<Notify>, Arc<Notify>)>,
        holding: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
        event: Option<Result<StreamEvent, ProviderError>>,
    }

    impl Stream for Held<'_> {
        type Item = Result<StreamEvent, ProviderError>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let this = self.get_mut();
            if let Some(holding) = this.holding.as_mut() {
                ready!(holding.as_mut().poll(cx));
                this.holding = None;
                return Poll::Ready(this.event.take());
            }
            let next = ready!(this.inner.as_mut().poll_next(cx));
            if matches!(next, Some(Ok(StreamEvent::Done(_))))
                && let Some((at, release)) = this.hold.take()
            {
                this.event = next;
                at.notify_one();
                this.holding = Some(Box::pin(async move { release.notified().await }));
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            Poll::Ready(next)
        }
    }

    impl AiProvider for HoldsDone {
        fn name(&self) -> &'static str {
            "scripted"
        }

        fn model_name(&self) -> &'static str {
            "scripted"
        }

        fn supports_structured_streaming(&self) -> bool {
            true
        }

        async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
            self.script.generate(context).await
        }

        fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
            let hold = self.script.requests().len() == self.round;
            Box::pin(Held {
                inner: self.script.generate_structured_stream(context),
                hold: hold.then(|| (Arc::clone(&self.at), Arc::clone(&self.release))),
                holding: None,
                event: None,
            })
        }
    }

    struct Leaves {
        at: Arc<Notify>,
        release: Arc<Notify>,
        after: &'static str,
    }

    impl Channel for Leaves {
        streaming_channel_declaration!();

        async fn deliver_stream(
            &self,
            mut stream: DeliveryStream,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> Result<ChannelOutput, ChannelError> {
            while let Some(item) = stream.next().await {
                if matches!(&item, Ok(StreamItem::Text(text)) if text == self.after) {
                    break;
                }
            }
            {
                let read = stream.next();
                tokio::pin!(read);
                tokio::select! {
                    biased;
                    _ = &mut read => panic!("the read gave an item before the gate"),
                    () = self.at.notified() => {}
                }
                tokio::select! {
                    biased;
                    _ = &mut read => panic!("the read gave an item before the release"),
                    () = tokio::time::sleep(Duration::from_millis(10)) => {}
                }
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

    fn used(input: u64) -> roomkit::model::Metadata {
        json!({ "input_tokens": input, "output_tokens": 10 })
            .as_object()
            .unwrap()
            .clone()
    }

    /// A generation holds its done event while its reader leaves: the usage
    /// it then tells comes on the read in flight.
    #[tokio::test(start_paused = true)]
    async fn as_its_reader_leaves_is_not_told() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let at = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        kit.register_channel(Leaves {
            at: Arc::clone(&at),
            release: Arc::clone(&release),
            after: "Two.",
        })
        .unwrap();
        let script = ScriptedProvider::new([
            ScriptedRound::calling("One.", [("c1", "lookup", "{}")]).with_usage(used(1000)),
            ScriptedRound::answering("Two.").with_usage(used(2000)),
        ]);
        let model = HoldsDone {
            script: script.clone(),
            round: 1,
            at,
            release,
        };
        let lookup = move |_call, _context| async move {
            Ok::<_, roomkit_ai::ToolError>(roomkit_ai::ToolResult::Text("found".to_owned()))
        };
        let options_of_agent = AiChannelOptions {
            tools: roomkit_ai::Tools::new()
                .tool(roomkit_ai::AiTool::new("lookup", "d").unwrap(), lookup)
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
        roomkit::testing::idle(&kit).await;

        let rows = kit.store().list_events(&room, received()).await.unwrap();
        let said = rows
            .iter()
            .rfind(|row| row.source.channel_id == channel("ai") && row.r#type == EventType::Message)
            .unwrap();
        assert_eq!(said.metadata["loop_end_reason"], json!("cancelled"));
        assert_eq!(said.metadata["ai_usage"], Value::Object(used(1000)));
        assert_eq!(said.metadata["ai_last_usage"], Value::Object(used(1000)));
        assert_eq!(said.metadata["ai_usage_complete"], json!(false));
        kit.close().await.unwrap();
    }
}

/// A target that, past the end of its first stream, flushes, then calls back
/// into its kit and fails as the kit refuses the call.
struct RefusedPastTheEnd {
    kit: Arc<std::sync::OnceLock<RoomKit>>,
    first: AtomicBool,
    told: Arc<Notify>,
    refused: Arc<Mutex<Option<String>>>,
}

impl Channel for RefusedPastTheEnd {
    streaming_channel_declaration!();

    async fn deliver_stream(
        &self,
        mut stream: DeliveryStream,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let first = self.first.swap(false, Ordering::SeqCst);
        while stream.next().await.is_some() {
            if first {
                self.told.notify_one();
            }
        }
        if !first {
            return Ok(ChannelOutput::empty());
        }
        tokio::time::sleep(FLUSH).await;
        let kit = self.kit.get().unwrap().clone();
        let late = CreateRoomOptions {
            room_id: Some(RoomId::new("late").unwrap()),
            ..Default::default()
        };
        let error = kit.create_room(late).await.unwrap_err();
        *self.refused.lock().unwrap() = Some(format!("{error:?}"));
        Err(error.into())
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

/// Registers an agent that writes "Hello." and calls a tool that never
/// returns: what tells once the tool runs.
fn waiting_agent(kit: &RoomKit) -> Arc<Notify> {
    let running = Arc::new(Notify::new());
    let on_run = Arc::clone(&running);
    let wait = move |_call, _context| {
        let on_run = Arc::clone(&on_run);
        async move {
            on_run.notify_one();
            std::future::pending::<Result<roomkit_ai::ToolResult, roomkit_ai::ToolError>>().await
        }
    };
    let options = AiChannelOptions {
        tools: roomkit_ai::Tools::new()
            .tool(roomkit_ai::AiTool::new("wait", "d").unwrap(), wait)
            .into(),
        ..AiChannelOptions::default()
    };
    let model = ScriptedProvider::new([ScriptedRound::calling("Hello.", [("t1", "wait", "{}")])]);
    kit.register_channel(AiChannel::new(channel("ai"), model, options).unwrap())
        .unwrap();
    running
}

/// Keeps each `ON_ERROR` payload `kit` fires.
fn errors_of(kit: &RoomKit) -> Arc<Mutex<Vec<RoomEvent>>> {
    let errors = Arc::new(Mutex::new(Vec::new()));
    let kept = Arc::clone(&errors);
    let on_error = move |event: RoomEvent, _context| {
        kept.lock().unwrap().push(event);
        async {}
    };
    let options = HookOptions {
        execution: HookExecution::Async,
        name: "error".to_owned(),
        ..Default::default()
    };
    kit.hook(HookTrigger::OnError, on_error, options).unwrap();
    errors
}

/// A room `a` binding `ids`.
async fn room_binding(kit: &RoomKit, ids: &[&str]) -> RoomId {
    let a = RoomId::new("a").unwrap();
    kit.create_room(CreateRoomOptions {
        room_id: Some(a.clone()),
        ..Default::default()
    })
    .await
    .unwrap();
    for id in ids {
        kit.attach_channel(&a, &channel(id), AttachChannelOptions::default())
            .await
            .unwrap();
    }
    a
}

// RFC §12.10.4 (RMK-1330 review): a target left to end alone by the close's cut,
// whose failure past it is the kit's own close, a call back into the kit it
// refused, is no failure ON_ERROR reports, as Python's `ended_by_close`.
#[tokio::test(start_paused = true)]
async fn a_target_the_close_refuses_past_its_cut_fires_no_on_error() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let held = Arc::new(std::sync::OnceLock::new());
    held.set(kit.clone()).unwrap();
    let (told, refused) = (Arc::new(Notify::new()), Arc::new(Mutex::new(None)));
    kit.register_channel(RefusedPastTheEnd {
        kit: held,
        first: AtomicBool::new(true),
        told: Arc::clone(&told),
        refused: Arc::clone(&refused),
    })
    .unwrap();
    let running = waiting_agent(&kit);
    let errors = errors_of(&kit);
    let a = room_binding(&kit, &["ws", "ai"]).await;
    let sender = kit.clone();
    let first =
        tokio::spawn(async move { sender.process_inbound(message("one"), options(&a)).await });
    timeout(LONG, told.notified()).await.unwrap();
    timeout(LONG, running.notified()).await.unwrap();

    kit.close().await.unwrap();
    timeout(LONG, first).await.unwrap().unwrap().unwrap();

    assert_eq!(refused.lock().unwrap().as_deref(), Some("KitClosed"));
    assert_eq!(errors.lock().unwrap().len(), 0);
}

// RFC §12.2 step 13s, §23.3 (RMK-1330 review): the worker's response failed, its
// shared target still working on the failed text's row when cancel_task cuts
// the task: the cut wins, no ON_ERROR fires, as Python's.
#[tokio::test(start_paused = true)]
async fn a_response_failed_before_a_task_s_cut_fires_no_on_error() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let (told, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    kit.register_channel(WaitsOnARow {
        first: AtomicBool::new(true),
        told: Arc::clone(&told),
        release: Arc::clone(&release),
    })
    .unwrap();
    let model = ScriptedProvider::new([ScriptedRound::writing(["Hello."]).failing("boom")]);
    kit.register_channel(
        AiChannel::new(channel("ai"), model, AiChannelOptions::default()).unwrap(),
    )
    .unwrap();
    let errors = errors_of(&kit);
    let a = room_binding(&kit, &["ws"]).await;
    let shared = roomkit::DelegateOptions {
        share_channels: Some(vec![channel("ws")]),
        ..roomkit::DelegateOptions::default()
    };
    let task = kit
        .delegate(&a, &channel("ai"), "Go.", shared)
        .await
        .unwrap();
    timeout(LONG, told.notified()).await.unwrap();

    let cancel = kit.cancel_task(&task.id, roomkit::CancelTaskOptions::default());
    assert!(timeout(LONG, cancel).await.unwrap());
    release.notify_one();
    roomkit::testing::idle(&kit).await;

    assert_eq!(errors.lock().unwrap().len(), 0);
    kit.close().await.unwrap();
}
