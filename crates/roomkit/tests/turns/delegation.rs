//! A delegated turn on the trace path (RFC §23.3 steps 4 to 6), through a
//! kit and an AI agent that streams: the child room keeps the agent's
//! trace, text segments and calls in order, each row answering the task's
//! description, and the task ends with the agent's last segment. A turn
//! cut short, by its response's failure, a cancel of its task or a caller
//! that stops waiting, leaves no call open in it (RFC §12.2 step 13s).

use std::collections::VecDeque;
use std::future::{Future, pending, ready};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use pretty_assertions::assert_eq;
use roomkit::model::{
    Cancel, ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, EventContent,
    EventType, InboundMessage, RoomContext, RoomEvent, RoomId, TaskStatus, ToolCallContent,
    ToolCallStatus, Visibility,
};
use roomkit::{
    CancelTaskOptions, Channel, ChannelError, ConversationStore, CreateRoomOptions,
    DelegateOptions, GetTimelineOptions, ResponseItem, RoomKit, RoomKitOptions, current_turn_host,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannel, AiChannelOptions, AiTool, SteerOptions, ToolResult, Tools};
use tokio::sync::Notify;

use crate::support::{channel, kit, room};

/// How long a wait that should end may take before the test fails it.
const BOUND: Duration = Duration::from_secs(5);

/// What a row of the child room says: its text, or its call's tool.
fn said(content: &EventContent) -> String {
    if let EventContent::Text(text) = content {
        return text.body.clone();
    }
    if let EventContent::ToolCall(call) = content {
        return call.tool_name.clone();
    }
    format!("{content:?}")
}

// Python: tests/tasks/test_child_room_trace.py::TestPersistChildStream::test_persists_tool_calls_and_text_segments_in_order
#[tokio::test(start_paused = true)]
async fn a_streamed_worker_s_trace_keeps_its_calls_and_segments_in_order() {
    let kit = kit(|_call, _context| async { Ok(ToolResult::Text("42".to_owned())) }).await;

    let options = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };
    let task = kit
        .delegate(&room(), &channel("ai"), "What is it?", options)
        .await
        .unwrap();

    let result = task.result().unwrap();
    let rows: Vec<_> = kit
        .get_timeline(&task.child_room_id, GetTimelineOptions::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|row| row.visibility != Visibility::Internal)
        .collect();
    let description = rows.first().map(|row| row.id.clone());
    let trace: Vec<(EventType, String, bool)> = rows
        .iter()
        .skip(1)
        .map(|row| {
            let answers = row.responds_to == description;
            (row.r#type, said(&row.content), answers)
        })
        .collect();
    assert_eq!(
        (result.status, result.output.as_deref(), trace),
        (
            TaskStatus::Completed,
            Some("It is 42."),
            vec![
                (EventType::Message, "Let me check.".to_owned(), true),
                (EventType::ToolCallStart, "lookup".to_owned(), true),
                (EventType::ToolCallEnd, "lookup".to_owned(), true),
                (EventType::Message, "It is 42.".to_owned(), true),
            ]
        )
    );
}

/// What a row of the worker's trace says: its type, its text or its call's
/// tool, and a call's status and error.
type Row = (EventType, String, Option<ToolCallStatus>, Option<String>);

/// The rows `worker` wrote in `room_id`, in order.
async fn rows_of(kit: &RoomKit, room_id: &RoomId, worker: &str) -> Vec<Row> {
    let timeline = kit
        .get_timeline(room_id, GetTimelineOptions::default())
        .await
        .unwrap();
    timeline
        .into_iter()
        .filter(|row| row.source.channel_id == channel(worker))
        .map(|row| {
            let (status, error) = if let EventContent::ToolCall(call) = &row.content {
                (Some(call.status), call.error.clone())
            } else {
                (None, None)
            };
            (row.r#type, said(&row.content), status, error)
        })
        .collect()
}

/// A kit whose room `r1` binds nothing, `worker` registered.
async fn kit_of(worker: impl Channel) -> RoomKit {
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(worker).unwrap();
    let create = CreateRoomOptions {
        room_id: Some(room()),
        ..CreateRoomOptions::default()
    };
    kit.create_room(create).await.unwrap();
    kit
}

/// How a `Streaming` agent's response ends once its items are read.
#[derive(Clone, Copy)]
enum End {
    /// It fails with this message.
    Fails(&'static str),
    /// It never ends, telling `waiting` it was read that far.
    Hangs,
}

/// The agent `worker`, whose turn answers with a stream of `items`, then
/// `end`.
#[derive(Clone)]
struct Streaming {
    items: Vec<ResponseItem>,
    end: End,
    waiting: Arc<Notify>,
}

impl Streaming {
    fn new(items: impl IntoIterator<Item = ResponseItem>, end: End) -> Self {
        Self {
            items: items.into_iter().collect(),
            end,
            waiting: Arc::default(),
        }
    }
}

/// A `Streaming` agent's response.
struct Response {
    items: VecDeque<ResponseItem>,
    end: Option<End>,
    waiting: Arc<Notify>,
}

impl Stream for Response {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let response = self.get_mut();
        if let Some(item) = response.items.pop_front() {
            return Poll::Ready(Some(Ok(item)));
        }
        match response.end {
            Some(End::Fails(message)) => {
                response.end = None;
                Poll::Ready(Some(Err(message.into())))
            }
            Some(End::Hangs) => {
                response.waiting.notify_one();
                Poll::Pending
            }
            None => Poll::Ready(None),
        }
    }
}

impl Channel for Streaming {
    fn channel_id(&self) -> ChannelId {
        channel("worker")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Err("an agent is sent no message".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        let output = if event.source.channel_id == self.channel_id() {
            ChannelOutput::empty()
        } else {
            let response = Response {
                items: self.items.iter().cloned().collect(),
                end: Some(self.end),
                waiting: Arc::clone(&self.waiting),
            };
            current_turn_host()
                .unwrap()
                .respond_with_stream(Box::pin(response))
        };
        ready(Ok(output))
    }
}

/// The call `search` starting, as its start row says it.
fn search_starts() -> ResponseItem {
    ResponseItem::ToolCallStart(ToolCallContent::new("search", "t1"))
}

// Python: tests/tasks/test_child_room_trace.py::TestAChildTraceCutShort::test_a_failed_stream_closes_its_open_call
#[tokio::test]
async fn a_failed_response_closes_the_call_it_left_open() {
    let text = ResponseItem::Text("Looking. ".to_owned());
    let worker = Streaming::new([text, search_starts()], End::Fails("upstream 500"));
    let kit = kit_of(worker).await;

    let options = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };
    let task = kit
        .delegate(&room(), &channel("worker"), "Do the task.", options)
        .await
        .unwrap();

    let result = task.result().unwrap();
    let rows = rows_of(&kit, &task.child_room_id, "worker").await;
    assert_eq!(
        (result.status, result.error.as_deref(), rows),
        (
            TaskStatus::Failed,
            Some("upstream 500"),
            vec![
                (EventType::Message, "Looking. ".to_owned(), None, None),
                (
                    EventType::ToolCallStart,
                    "search".to_owned(),
                    Some(ToolCallStatus::Pending),
                    None
                ),
                (
                    EventType::ToolCallEnd,
                    "search".to_owned(),
                    Some(ToolCallStatus::Failed),
                    Some("turn failed".to_owned())
                ),
            ]
        )
    );
    kit.close().await.unwrap();
}

// Python: tests/tasks/test_child_room_trace.py::TestAChildTraceCutShort::test_a_cancelled_stream_keeps_its_text_marked_cancelled
#[tokio::test]
async fn a_cancelled_task_keeps_its_text_marked_cancelled() {
    let text = ResponseItem::Text("Half an ans".to_owned());
    let worker = Streaming::new([text], End::Hangs);
    let kit = kit_of(worker.clone()).await;
    let task = kit
        .delegate(
            &room(),
            &channel("worker"),
            "Do the task.",
            DelegateOptions::default(),
        )
        .await
        .unwrap();
    tokio::time::timeout(BOUND, worker.waiting.notified())
        .await
        .unwrap();

    let cancelled = kit
        .cancel_task(&task.id, CancelTaskOptions::default())
        .await;

    let timeline = kit
        .get_timeline(&task.child_room_id, GetTimelineOptions::default())
        .await
        .unwrap();
    let rows: Vec<(String, Option<serde_json::Value>)> = timeline
        .iter()
        .filter(|row| row.source.channel_id == channel("worker"))
        .map(|row| (said(&row.content), row.metadata.get("cancelled").cloned()))
        .collect();
    assert_eq!(
        (cancelled, rows),
        (
            true,
            vec![("Half an ans".to_owned(), Some(serde_json::json!(true)))]
        )
    );
    kit.close().await.unwrap();
}

// Python: tests/tasks/test_child_room_trace.py::TestAChildTraceCutShort::test_a_delegation_cancelled_while_its_tool_runs_leaves_no_call_open
#[tokio::test]
async fn an_inline_delegation_cut_while_its_tool_runs_leaves_no_call_open() {
    let started = Arc::new(Notify::new());
    let model = ScriptedProvider::new([
        ScriptedRound::calling("Working.", [("tc1", "slow", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let running = Arc::clone(&started);
    let slow = move |_call, _context| {
        let running = Arc::clone(&running);
        async move {
            running.notify_one();
            pending::<Result<ToolResult, roomkit_ai::ToolError>>().await
        }
    };
    let options = AiChannelOptions {
        tools: Tools::new()
            .tool(AiTool::new("slow", "d").unwrap(), slow)
            .into(),
        ..AiChannelOptions::default()
    };
    let worker = AiChannel::new(channel("worker"), model, options).unwrap();
    let kit = kit_of(worker.clone()).await;
    let options = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };

    let (parent, agent) = (room(), channel("worker"));
    tokio::select! {
        _ = kit.delegate(&parent, &agent, "go", options) => {
            panic!("the delegation ended before its cut")
        }
        () = async { tokio::time::timeout(BOUND, started.notified()).await.unwrap() } => {}
    }
    roomkit::testing::idle(&kit).await;

    let child = kit
        .store()
        .list_rooms(roomkit::store::ListRoomsOptions::default())
        .await
        .unwrap()
        .into_iter()
        .find(|listed| listed.id != room())
        .unwrap();
    let rows = rows_of(&kit, &child.id, "worker").await;
    let loops = worker.steer(Cancel::default(), SteerOptions::default());
    assert_eq!(
        (rows, loops.unwrap()),
        (
            vec![
                (EventType::Message, "Working.".to_owned(), None, None),
                (
                    EventType::ToolCallStart,
                    "slow".to_owned(),
                    Some(ToolCallStatus::Pending),
                    None
                ),
                (
                    EventType::ToolCallEnd,
                    "slow".to_owned(),
                    Some(ToolCallStatus::Failed),
                    Some("cancelled".to_owned())
                ),
            ],
            0
        )
    );
    kit.close().await.unwrap();
}

/// An agent named `id` that says `first` and calls `tool`, then answers
/// `last`.
fn calling_agent<F, R>(
    id: &str,
    first: &str,
    tool: &str,
    run: F,
    last: &str,
) -> AiChannel<ScriptedProvider>
where
    F: Fn(roomkit::model::ToolCall, roomkit_ai::ToolContext) -> R + Send + Sync + 'static,
    R: Future<Output = Result<ToolResult, roomkit_ai::ToolError>> + Send + 'static,
{
    let model = ScriptedProvider::new([
        ScriptedRound::calling(first, [("c1", tool, "{}")]),
        ScriptedRound::answering(last),
    ]);
    let options = AiChannelOptions {
        tools: Tools::new()
            .tool(AiTool::new(tool, "d").unwrap(), run)
            .into(),
        ..AiChannelOptions::default()
    };
    AiChannel::new(channel(id), model, options).unwrap()
}

// RFC §23.3: a deferred delivery the worker's tool started runs apart from
// the task, as Python's `_defer_inbound_delivery` runs it on a task of its
// own: cancelling the task leaves the turns of that delivery to their end.
#[tokio::test]
async fn a_cancel_leaves_a_delivery_the_worker_deferred_to_its_end() {
    let kit_slot: Arc<std::sync::Mutex<Option<RoomKit>>> = Arc::default();
    let posted = Arc::new(Notify::new());
    let (held, told) = (Arc::clone(&kit_slot), Arc::clone(&posted));
    let post = move |_call, _context| {
        let (held, told) = (Arc::clone(&held), Arc::clone(&told));
        async move {
            let kit = held.lock().unwrap().clone().unwrap();
            let message = InboundMessage::new(
                channel("sms"),
                "+15550000001",
                roomkit::model::TextContent::new("from the worker"),
            );
            let options = roomkit::ProcessInboundOptions {
                room_id: Some(room()),
                defer_delivery: true,
                ..roomkit::ProcessInboundOptions::default()
            };
            kit.process_inbound(message, options).await.unwrap();
            told.notify_one();
            pending::<Result<ToolResult, roomkit_ai::ToolError>>().await
        }
    };
    let worker = calling_agent("worker", "Posting.", "post", post, "Worker done.");
    let (started, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let (running, released) = (Arc::clone(&started), Arc::clone(&release));
    let slow = move |_call, _context| {
        let (running, released) = (Arc::clone(&running), Arc::clone(&released));
        async move {
            running.notify_one();
            released.notified().await;
            Ok(ToolResult::Text("slow done".to_owned()))
        }
    };
    let helper = calling_agent("helper", "Helping.", "slow", slow, "Helper done.");
    let kit = kit_of(worker).await;
    let sms = roomkit::testing::RecordingTransport::new(
        channel("sms"),
        ChannelType::SMS,
        roomkit::testing::RecordingTransportOptions::default(),
    );
    kit.register_channel(sms).unwrap();
    kit.register_channel(helper).unwrap();
    for id in ["sms", "helper"] {
        kit.attach_channel(
            &room(),
            &channel(id),
            roomkit::AttachChannelOptions::default(),
        )
        .await
        .unwrap();
    }
    *kit_slot.lock().unwrap() = Some(kit.clone());
    let task = kit
        .delegate(
            &room(),
            &channel("worker"),
            "go",
            DelegateOptions::default(),
        )
        .await
        .unwrap();
    tokio::time::timeout(BOUND, async {
        posted.notified().await;
        started.notified().await;
    })
    .await
    .unwrap();

    let cancelled = kit
        .cancel_task(&task.id, CancelTaskOptions::default())
        .await;
    release.notify_one();
    roomkit::testing::idle(&kit).await;

    let rows = rows_of(&kit, &room(), "helper").await;
    assert_eq!(
        (cancelled, rows),
        (
            true,
            vec![
                (EventType::Message, "Helping.".to_owned(), None, None),
                (
                    EventType::ToolCallStart,
                    "slow".to_owned(),
                    Some(ToolCallStatus::Pending),
                    None
                ),
                (
                    EventType::ToolCallEnd,
                    "slow".to_owned(),
                    Some(ToolCallStatus::Completed),
                    None
                ),
                (EventType::Message, "Helper done.".to_owned(), None, None),
            ]
        )
    );
    kit.close().await.unwrap();
}

/// An agent `worker` whose turn streams only its end at `max_rounds`, its
/// output carrying `metadata` beside the stream.
struct EndsAtMaxRounds {
    metadata: roomkit::model::Metadata,
}

impl Channel for EndsAtMaxRounds {
    fn channel_id(&self) -> ChannelId {
        channel("worker")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Err("an agent is sent no message".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        if event.source.channel_id == self.channel_id() {
            return ready(Ok(ChannelOutput::empty()));
        }
        let end = roomkit::model::LoopEndMarker::new(roomkit::model::LoopEndReason::MaxRounds);
        let items = Items(VecDeque::from([Ok(ResponseItem::LoopEnd(end))]));
        let mut output = current_turn_host()
            .unwrap()
            .respond_with_stream(Box::pin(items));
        output.response_metadata.extend(self.metadata.clone());
        ready(Ok(output))
    }
}

/// A response whose items are all ready.
struct Items(VecDeque<Result<ResponseItem, ChannelError>>);

impl Stream for Items {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.get_mut().0.pop_front())
    }
}

// Python: core/event_router.py stream_record, read by
// core/mixins/_child_execution.py _persist_child_stream: the turn's record
// lies over the stream's response metadata, so a stream's end at
// `max_rounds` fails its task whatever else its output said, inline and
// on the room path a shared transport opens.
#[tokio::test]
async fn a_stream_s_end_fails_its_task_beside_what_its_output_said() {
    for shared in [false, true] {
        let mut metadata = roomkit::model::Metadata::new();
        metadata.insert("model".to_owned(), serde_json::json!("m"));
        let kit = kit_of(EndsAtMaxRounds { metadata }).await;
        let sms = roomkit::testing::RecordingTransport::new(
            channel("sms"),
            ChannelType::SMS,
            roomkit::testing::RecordingTransportOptions::default(),
        );
        kit.register_channel(sms).unwrap();
        kit.attach_channel(
            &room(),
            &channel("sms"),
            roomkit::AttachChannelOptions::default(),
        )
        .await
        .unwrap();
        let options = DelegateOptions {
            wait: true,
            share_channels: shared.then(|| vec![channel("sms")]),
            ..DelegateOptions::default()
        };

        let task = kit
            .delegate(&room(), &channel("worker"), "go", options)
            .await
            .unwrap();

        let result = task.result().unwrap();
        assert_eq!(
            (
                shared,
                result.status,
                result.error.as_deref(),
                result.metadata.get("loop_end_reason").cloned()
            ),
            (
                shared,
                TaskStatus::Failed,
                Some("The worker's turn ended max_rounds before its answer"),
                Some(serde_json::json!("max_rounds"))
            )
        );
        kit.close().await.unwrap();
    }
}

/// An agent `worker` whose streamed turn says `Looking.`, then fails with a
/// provider's refusal raised as Python's `ProviderError`.
struct RefusedMidStream;

impl Channel for RefusedMidStream {
    fn channel_id(&self) -> ChannelId {
        channel("worker")
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Err("an agent is sent no message".into()))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        if event.source.channel_id == self.channel_id() {
            return ready(Ok(ChannelOutput::empty()));
        }
        let refusal = std::io::Error::other("upstream 503");
        let raised: ChannelError = Box::new(roomkit::ClassedError::new(
            "roomkit.providers.ai.base:ProviderError",
            refusal,
        ));
        let items = Items(VecDeque::from([
            Ok(ResponseItem::Text("Looking.".to_owned())),
            Err(raised),
        ]));
        let output = current_turn_host()
            .unwrap()
            .respond_with_stream(Box::pin(items));
        ready(Ok(output))
    }
}

// Python: tests/tasks/test_task_turn_record.py, a failed task keeps the
// error its worker's turn raised (tasks/models.py _as_raised), its class
// the provider's, never a channel's, recorded by its last name as Python's
// `error_type_name` reads `roomkit.providers.ai.base:ProviderError`: the
// scenario
// tasks/a-failed-task-keeps-the-error-its-worker-s-turn-raised, which
// roomkit-ai's provider errors pass once they name their class.
#[tokio::test]
async fn a_failed_task_keeps_the_class_its_worker_s_error_names() {
    let kit = kit_of(RefusedMidStream).await;
    let options = DelegateOptions {
        wait: true,
        ..DelegateOptions::default()
    };

    let task = kit
        .delegate(&room(), &channel("worker"), "Find it.", options)
        .await
        .unwrap();

    let result = task.result().unwrap();
    let exception = result.exception.unwrap();
    assert_eq!(
        (
            result.status,
            exception.error_type.as_str(),
            exception.message.as_str()
        ),
        (TaskStatus::Failed, "ProviderError", "upstream 503")
    );
    kit.close().await.unwrap();
}
