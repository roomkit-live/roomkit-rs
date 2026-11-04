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
        if !first {
            return Ok(ChannelOutput::empty());
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
    delivered: Arc<Mutex<Vec<EventType>>>,
    /// Told of each `ON_ERROR`.
    reported: Arc<Notify>,
    first: tokio::task::JoinHandle<Result<roomkit::InboundResult, roomkit::RoomKitError>>,
    a: RoomId,
    b: RoomId,
}

async fn cut(stuck: Stuck, turns: Turns) -> Cut {
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
    let on_error = Arc::clone(&reported);
    kit.hook(
        HookTrigger::OnError,
        move |_event: RoomEvent, _context| {
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
    if !matches!(stuck, Stuck::BetweenReads) {
        timeout(LONG, running.notified()).await.unwrap();
    }
    let cancelled = kit
        .cancel_delivery("k", roomkit::CancelDeliveryOptions::default())
        .await;
    assert_eq!(cancelled, 1);
    Cut {
        kit,
        read,
        delivered,
        reported,
        first,
        a,
        b,
    }
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
// and its caller reads the failure (RMK-1343). On HITL ... Python, whose `finally`
// fails before anything is stored, stores the text unmarked, with no record.
#[tokio::test(start_paused = true)]
async fn a_target_that_fails_flushing_leaves_what_the_cut_stored() {
    let mut cut = cut(Stuck::FailsFlushing, Turns::Serialized).await;
    let result = timeout(LONG, &mut cut.first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        result
            .error
            .as_ref()
            .is_some_and(|error| error.message.contains("the flush failed")),
        "{:?}",
        result.error
    );
    timeout(LONG, cut.reported.notified()).await.unwrap();
    assert_eq!(rows_of(&cut.kit, &cut.a).await, cut_in_a_call());
    cut.kit.close().await.unwrap();
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
