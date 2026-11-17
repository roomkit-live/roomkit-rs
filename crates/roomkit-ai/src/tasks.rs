//! The task tools an agent is given as its own (RFC §23.4): Python's
//! `roomkit.tasks.TaskStatusTool` and `CancelTaskTool`, for an agent run in
//! the kit's process. They read the room and the channel of the call they
//! serve ([`current_tool_call`]) and answer as the tools
//! [`setup_task_tools`](roomkit::RoomKit::setup_task_tools) has the room's
//! owner serve.

use std::sync::LazyLock;

use roomkit::model::Metadata;
use roomkit::orchestration::StrategyAgent;
use roomkit::tasks::{
    CANCEL_TASK_TOOL, DelegateHandler, TASK_STATUS_TOOL, TaskToolWindow, cancel_task_definition,
    delegate_tool_definition, task_status_definition,
};
use roomkit::{ConversationStore, RoomKit, RoomKitError, WeakRoomKit};
use roomkit_core::rules::python_repr;
use serde_json::{Value, json};

use crate::tools::{
    AiTool, Tool, ToolError, ToolFuture, ToolResult, current_tool_call, current_tool_room_metadata,
};

#[cfg(test)]
#[path = "tasks/names.rs"]
mod names;

/// The generic delegate tool, its worker free: Python's `DELEGATE_TOOL`.
pub static DELEGATE_TOOL: LazyLock<AiTool> =
    LazyLock::new(|| definition(&delegate_tool_definition(&[])));

/// A delegate tool whose worker can only be one of `targets`, each an agent
/// id and its description; the generic [`DELEGATE_TOOL`] for none: the tool
/// [`setup_delegation`] serves from it refuses a call naming another.
/// Python's `build_delegate_tool(targets)`.
#[must_use]
pub fn build_delegate_tool(targets: &[(String, Option<String>)]) -> AiTool {
    definition(&delegate_tool_definition(targets))
}

/// What [`setup_delegation`] takes beside the agent and the handler:
/// Python's `setup_delegation(channel, handler, *, tool=None)`.
#[derive(Clone, Debug, Default)]
pub struct SetupDelegationOptions {
    /// Python `tool=None`: the tool declared, as [`build_delegate_tool`]
    /// builds one; `None`, the generic [`DELEGATE_TOOL`].
    pub tool: Option<AiTool>,
}

/// Serves `delegate_task` on `channel`, live or named by the id of one the
/// kit holds, with `handler`, in the room of each call (RFC §23.4): one
/// agent serving several rooms delegates from each. The tool gains a
/// `conversation` argument when the handler's `max_conversation` is above
/// 0. Python's `setup_delegation`.
///
/// # Errors
///
/// [`RoomKitError::Runtime`] when delegation is already set up on the
/// channel; [`RoomKitError::ChannelNotRegistered`] for an id naming no
/// channel of the kit.
pub fn setup_delegation<S: ConversationStore>(
    channel: impl Into<StrategyAgent>,
    handler: &DelegateHandler<S>,
    options: SetupDelegationOptions,
) -> Result<(), RoomKitError> {
    let SetupDelegationOptions { tool } = options;
    let definition = tool.map(|tool| {
        json!({
            "name": tool.name,
            "description": tool.description,
            "parameters": tool.parameters,
        })
    });
    handler.serve_tool(&channel.into(), definition)
}

/// What [`TaskStatusTool::new`] takes beside the kit: Python's
/// `TaskStatusTool(kit, *, window=500, limit=10)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskStatusToolOptions {
    /// Python `window=500`: how many of the bus's latest entries are read.
    pub window: usize,
    /// Python `limit=10`: how many tasks, the latest, an answer lists at
    /// most.
    pub limit: usize,
}

impl Default for TaskStatusToolOptions {
    fn default() -> Self {
        Self {
            window: 500,
            limit: 10,
        }
    }
}

/// From a tool a worker calls in its task's child room: says how far the
/// task got ("12/20 s", "3 sources of 5"), for the room that delegated it
/// (RFC §23.3). Python's `post_task_progress(kit, detail)`.
///
/// Posts an `info` entry under the worker, action `task`, with the task's
/// metadata, its detail bounded; it does not end the task. Whether it
/// posted: a call outside a task's child room posts nothing, outside a
/// tool call included. The agent's next turn reads it in its notes, and
/// `task_status` among the room's tasks (RFC §23.4).
///
/// # Errors
///
/// The store's failure to read the call's room.
pub async fn post_task_progress<S: ConversationStore>(
    kit: &RoomKit<S>,
    detail: &str,
) -> Result<bool, RoomKitError> {
    let Some(child_room_id) = crate::tools::current_tool_room_id() else {
        return Ok(false);
    };
    kit.post_task_progress_in(&child_room_id, detail).await
}

/// `task_status`: the tasks of the room of the call, read from the kit's
/// [`status_bus`](roomkit::RoomKit::status_bus) (RFC §23.4). Python's
/// `TaskStatusTool`.
///
/// A tool an agent is given on its own, among its
/// [`tools`](crate::AiChannelOptions::tools), independently of the
/// delegation helpers. It answers with the tasks posted for the room of the
/// call (delegations, orchestration workers): those running and those
/// ended, the latest state of each, with its worker, the task, and its
/// result when it completed; `task_id` narrows the answer to one task. It
/// reads the bus for that room only: one room's tasks never show in
/// another. It holds its kit without keeping it alive
/// ([`WeakRoomKit`]), the kit holding the agent.
pub struct TaskStatusTool<S: ConversationStore> {
    kit: WeakRoomKit<S>,
    window: TaskToolWindow,
}

impl<S: ConversationStore> Clone for TaskStatusTool<S> {
    fn clone(&self) -> Self {
        Self {
            kit: self.kit.clone(),
            window: self.window,
        }
    }
}

impl<S: ConversationStore> std::fmt::Debug for TaskStatusTool<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskStatusTool")
            .field("window", &self.window)
            .finish_non_exhaustive()
    }
}

impl<S: ConversationStore> TaskStatusTool<S> {
    /// The tool reading `kit`'s bus, as `options` bound it.
    pub fn new(kit: &RoomKit<S>, options: TaskStatusToolOptions) -> Self {
        let TaskStatusToolOptions { window, limit } = options;
        Self {
            kit: kit.downgrade(),
            window: TaskToolWindow { window, limit },
        }
    }
}

impl<S: ConversationStore> Tool for TaskStatusTool<S> {
    fn definition(&self) -> AiTool {
        definition(&task_status_definition())
    }

    fn handler(&self, name: String, arguments: Metadata) -> ToolFuture {
        let (kit, window) = (self.kit.clone(), self.window);
        Box::pin(async move {
            // A channel chains its tools' handlers, the first to answer
            // wins: a call for another tool is declined, so the next
            // handler serves it.
            if name != TASK_STATUS_TOOL {
                return Err(not_served(&name));
            }
            let Some(room_id) = current_tool_call().and_then(|call| call.room_id) else {
                return Ok(outside(TASK_STATUS_TOOL));
            };
            let kit = kit.upgrade().ok_or_else(kit_gone)?;
            let metadata = current_tool_room_metadata();
            let answer = kit
                .task_status_answer(&room_id, &arguments, window, metadata.as_deref())
                .await;
            Ok(ToolResult::Text(answer))
        })
    }
}

/// What [`CancelTaskTool::new`] takes beside the kit: Python's
/// `CancelTaskTool(kit, *, window=500)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CancelTaskToolOptions {
    /// Python `window=500`: how many of the bus's latest entries are read
    /// to find the task.
    pub window: usize,
}

impl Default for CancelTaskToolOptions {
    fn default() -> Self {
        Self { window: 500 }
    }
}

/// `cancel_task`: cancels one task of the room of the call (RFC §23.4).
/// Python's `CancelTaskTool`.
///
/// A tool an agent is given on its own, as [`TaskStatusTool`], so that a
/// request the person changed or dropped stops its work. It reaches only
/// the tasks the status bus lists for the room of the call: never another
/// room's, nor a strategy's worker run, which that strategy follows
/// itself. A running task ends `cancelled`; the agent that cancelled it is
/// not handed the cancellation back, the tool's answer having told it. A
/// task of the room it may not cancel is answered still running, and why;
/// a task the bus lists ended is answered as it lists it. It holds its kit
/// without keeping it alive ([`WeakRoomKit`]).
pub struct CancelTaskTool<S: ConversationStore> {
    kit: WeakRoomKit<S>,
    window: usize,
}

impl<S: ConversationStore> Clone for CancelTaskTool<S> {
    fn clone(&self) -> Self {
        Self {
            kit: self.kit.clone(),
            window: self.window,
        }
    }
}

impl<S: ConversationStore> std::fmt::Debug for CancelTaskTool<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CancelTaskTool")
            .field("window", &self.window)
            .finish_non_exhaustive()
    }
}

impl<S: ConversationStore> CancelTaskTool<S> {
    /// The tool cancelling `kit`'s tasks, as `options` bound it.
    pub fn new(kit: &RoomKit<S>, options: CancelTaskToolOptions) -> Self {
        Self {
            kit: kit.downgrade(),
            window: options.window,
        }
    }
}

impl<S: ConversationStore> Tool for CancelTaskTool<S> {
    fn definition(&self) -> AiTool {
        definition(&cancel_task_definition())
    }

    fn handler(&self, name: String, arguments: Metadata) -> ToolFuture {
        let (kit, window) = (self.kit.clone(), self.window);
        Box::pin(async move {
            if name != CANCEL_TASK_TOOL {
                return Err(not_served(&name));
            }
            let call = current_tool_call();
            let Some(room_id) = call.as_ref().and_then(|call| call.room_id.clone()) else {
                return Ok(outside(CANCEL_TASK_TOOL));
            };
            let channel_id = call.and_then(|call| call.channel_id);
            let kit = kit.upgrade().ok_or_else(kit_gone)?;
            let window = TaskToolWindow { window, limit: 0 };
            let answer = kit
                .cancel_task_answer(&room_id, channel_id.as_ref(), &arguments, window)
                .await;
            Ok(ToolResult::Text(answer))
        })
    }
}

/// A definition the kit writes, as the model is told of it.
fn definition(written: &Value) -> AiTool {
    let text = |key: &str| {
        written
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    AiTool {
        name: text("name"),
        description: text("description"),
        parameters: written.get("parameters").cloned().unwrap_or(Value::Null),
        tags: Vec::new(),
        changes_state: false,
        defer_loading: false,
    }
}

/// The decline of a call for another tool: Python's
/// `UnservedToolCallError(f"tool {name!r} is not served here")`.
fn not_served(name: &str) -> ToolError {
    ToolError::UnservedToolCall(format!("tool {} is not served here", python_repr(name)))
}

/// The answer of a call made outside any conversation: Python's
/// `{"error": "<tool> answers only within a conversation"}`.
fn outside(tool: &str) -> ToolResult {
    ToolResult::Text(format!(
        "{{\"error\": \"{tool} answers only within a conversation\"}}"
    ))
}

/// The failure of a call whose kit is gone: no turn of it runs any more.
fn kit_gone() -> ToolError {
    ToolError::Raised {
        kind: "KitClosedError".to_owned(),
        message: RoomKitError::KitClosed.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::future::{pending, ready};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use roomkit::model::{
        ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
        RoomContext, RoomEvent, RoomId, TaskStatus, ToolCall,
    };
    use roomkit::orchestration::RecentOptions;
    use roomkit::tasks::{DelegatedTask, WaitOptions};
    use roomkit::testing::{
        EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions, idle,
    };
    use roomkit::{
        AttachChannelOptions, CancelTaskOptions, Channel, ChannelError, CreateRoomOptions,
        DelegateOptions, InMemoryStore, RoomKitOptions,
    };
    use serde_json::json;
    use tokio::sync::Notify;

    use super::*;
    use crate::tools::{
        ToolContext, ToolEntry, ToolTurnContextOptions, served_by, tool_turn_context,
    };
    use crate::{AiChannel, AiChannelOptions, MockAiProvider, MockAiProviderOptions};

    const BOUND: Duration = Duration::from_secs(5);

    fn channel(id: &str) -> ChannelId {
        ChannelId::new(id).unwrap()
    }

    /// An agent whose every turn tells `started`, then never ends when it
    /// `hangs`; it records the events it is asked.
    #[derive(Clone)]
    struct Agent {
        id: ChannelId,
        hangs: bool,
        started: Arc<Notify>,
        asked: Arc<Mutex<Vec<RoomEvent>>>,
    }

    impl Agent {
        fn new(id: &str, hangs: bool) -> Self {
            Self {
                id: channel(id),
                hangs,
                started: Arc::default(),
                asked: Arc::default(),
            }
        }
    }

    impl Channel for Agent {
        fn channel_id(&self) -> ChannelId {
            self.id.clone()
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
            let agent = self.clone();
            async move {
                agent.asked.lock().unwrap().push(event);
                agent.started.notify_one();
                if agent.hangs {
                    pending::<()>().await;
                }
                Ok(ChannelOutput::empty())
            }
        }
    }

    /// Room `r`: the speaking agent, which tasks are handed back to, and a
    /// worker.
    struct Room {
        kit: RoomKit<InMemoryStore>,
        speaker: Agent,
        worker: Agent,
    }

    impl Room {
        async fn open(hangs: bool) -> Self {
            let kit = RoomKit::new(RoomKitOptions::default());
            let (speaker, worker) = (Agent::new("speaker", false), Agent::new("worker", hangs));
            kit.register_channel(speaker.clone()).unwrap();
            if hangs {
                kit.register_channel(worker.clone()).unwrap();
            } else {
                let answers = EchoAgent::new(
                    channel("worker"),
                    "A thousand.",
                    EchoAgentOptions::default(),
                );
                kit.register_channel(answers).unwrap();
            }
            let sms = RecordingTransport::new(
                channel("sms"),
                ChannelType::SMS,
                RecordingTransportOptions::default(),
            );
            kit.register_channel(sms).unwrap();
            let create = CreateRoomOptions {
                room_id: Some(RoomId::new("r").unwrap()),
                ..CreateRoomOptions::default()
            };
            kit.create_room(create).await.unwrap();
            kit.attach_channel(
                &RoomId::new("r").unwrap(),
                &channel("sms"),
                AttachChannelOptions::default(),
            )
            .await
            .unwrap();
            let as_agent = AttachChannelOptions {
                category: Some(ChannelCategory::Intelligence),
                ..AttachChannelOptions::default()
            };
            kit.attach_channel(&RoomId::new("r").unwrap(), &channel("speaker"), as_agent)
                .await
                .unwrap();
            Self {
                kit,
                speaker,
                worker,
            }
        }

        async fn running_task(&self) -> DelegatedTask {
            let delegation = DelegateOptions {
                notify: Some(channel("speaker")),
                ..DelegateOptions::default()
            };
            let task = self
                .kit
                .delegate(
                    &RoomId::new("r").unwrap(),
                    &channel("worker"),
                    "Count to a thousand.",
                    delegation,
                )
                .await
                .unwrap();
            if self.worker.hangs {
                tokio::time::timeout(BOUND, self.worker.started.notified())
                    .await
                    .unwrap();
            }
            task
        }

        fn handed_back(&self) -> usize {
            self.speaker.asked.lock().unwrap().len()
        }
    }

    /// `tool`'s answer to a call of `name` in room `r` by `channel_id`, as
    /// a channel serving it runs the call.
    async fn served(tool: Arc<dyn Tool>, name: &str, arguments: Value, channel_id: &str) -> Value {
        let (toolset, _) = served_by(None, vec![ToolEntry::Tool(tool)])
            .unwrap()
            .unwrap();
        let call = ToolCall::new("call-1", name, serde_json::from_value(arguments).unwrap());
        let context = ToolContext::new(RoomId::new("r").unwrap(), channel(channel_id));
        match toolset.call(call, context).await.unwrap() {
            ToolResult::Text(text) => serde_json::from_str(&text).unwrap(),
            ToolResult::Json(value) => value,
            ToolResult::Parts(parts) => panic!("a task tool answers no parts: {parts:?}"),
        }
    }

    async fn cancel(room: &Room, task_id: &str, channel_id: &str) -> Value {
        let tool = Arc::new(CancelTaskTool::new(
            &room.kit,
            CancelTaskToolOptions::default(),
        ));
        served(
            tool,
            CANCEL_TASK_TOOL,
            json!({ "task_id": task_id }),
            channel_id,
        )
        .await
    }

    // Python: tests/tasks/test_cancel_task.py::test_the_agent_cancels_a_running_task_and_is_not_told_twice
    #[tokio::test]
    async fn the_agent_cancels_a_running_task_and_is_not_told_twice() {
        let room = Room::open(true).await;
        let task = room.running_task().await;

        let answer = cancel(&room, &task.id, "speaker").await;
        let result = task
            .wait(WaitOptions {
                timeout: Some(BOUND),
            })
            .await
            .unwrap();
        room.kit.close().await.unwrap();

        assert_eq!(
            (answer, result.status, room.handed_back()),
            (
                json!({
                    "task_id": task.id,
                    "status": "cancelled",
                    "message": "Cancelled: its result will not come back.",
                }),
                TaskStatus::Cancelled,
                0
            )
        );
    }

    // Python: tests/tasks/test_cancel_task.py::test_two_cancels_at_once_cancel_the_task_once
    #[tokio::test]
    async fn two_cancels_at_once_cancel_the_task_once() {
        for second in ["speaker", "other"] {
            let room = Room::open(true).await;
            let task = room.running_task().await;

            let (first, again) = tokio::join!(
                cancel(&room, &task.id, "speaker"),
                cancel(&room, &task.id, second)
            );
            let result = task
                .wait(WaitOptions {
                    timeout: Some(BOUND),
                })
                .await
                .unwrap();
            room.kit.close().await.unwrap();

            assert_eq!(
                (
                    &first["status"],
                    &again["status"],
                    result.status,
                    room.handed_back()
                ),
                (
                    &json!("cancelled"),
                    &json!("cancelled"),
                    TaskStatus::Cancelled,
                    0
                ),
                "second cancel by {second}"
            );
        }
    }

    // Python: tests/tasks/test_cancel_task.py::test_a_cancel_by_another_agent_still_tells_the_notified_one
    #[tokio::test]
    async fn a_cancel_by_another_agent_still_tells_the_notified_one() {
        let room = Room::open(true).await;
        let task = room.running_task().await;

        let answer = cancel(&room, &task.id, "assistant-bis").await;
        tokio::time::timeout(BOUND, room.speaker.started.notified())
            .await
            .unwrap();
        room.kit.close().await.unwrap();

        let handed = room.speaker.asked.lock().unwrap().clone();
        let status = handed
            .first()
            .and_then(|event| event.metadata.get("task_status").cloned());
        assert_eq!(
            (answer["status"].clone(), status),
            (json!("cancelled"), Some(json!("cancelled")))
        );
    }

    // Python: tests/tasks/test_cancel_task.py::test_a_task_that_already_ended_is_left_as_it_stands
    #[tokio::test]
    async fn a_task_that_already_ended_is_left_as_it_stands() {
        let room = Room::open(false).await;
        let task = room.running_task().await;
        task.wait(WaitOptions {
            timeout: Some(BOUND),
        })
        .await
        .unwrap();

        let answer = cancel(&room, &task.id, "speaker").await;
        room.kit.close().await.unwrap();

        assert_eq!(
            (answer, task.result().map(|result| result.status)),
            (
                json!({
                    "task_id": task.id,
                    "status": "completed",
                    "message": "The task had already ended.",
                }),
                Some(TaskStatus::Completed)
            )
        );
    }

    // Python: tests/tasks/test_cancel_task.py::test_a_call_without_a_task_or_outside_a_conversation_is_refused
    #[tokio::test]
    async fn a_call_without_a_task_or_outside_a_conversation_is_refused() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let tool = Arc::new(CancelTaskTool::new(&kit, CancelTaskToolOptions::default()));
        let missing = served(
            Arc::<CancelTaskTool<InMemoryStore>>::clone(&tool),
            CANCEL_TASK_TOOL,
            json!({}),
            "speaker",
        )
        .await;
        let outside = tool
            .handler(CANCEL_TASK_TOOL.to_owned(), Metadata::new())
            .await
            .unwrap();
        kit.close().await.unwrap();

        assert_eq!(
            (missing, outside),
            (
                json!({ "error": "task_id is required" }),
                ToolResult::Text(
                    "{\"error\": \"cancel_task answers only within a conversation\"}".to_owned()
                )
            )
        );
    }

    // Python: tests/tasks/test_cancel_task.py::test_cancel_task_declines_another_tools_call
    #[tokio::test]
    async fn cancel_task_declines_another_tools_call() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let tool = CancelTaskTool::new(&kit, CancelTaskToolOptions::default());
        let answer = tool
            .handler(TASK_STATUS_TOOL.to_owned(), Metadata::new())
            .await;
        assert!(matches!(answer, Err(ToolError::UnservedToolCall(_))));
    }

    // Python: tests/tasks/test_cancel_task.py::test_the_definition_asks_for_the_task_id
    #[test]
    fn the_definition_asks_for_the_task_id() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let definition = CancelTaskTool::new(&kit, CancelTaskToolOptions::default()).definition();
        assert_eq!(
            (definition.name.as_str(), &definition.parameters["required"]),
            (CANCEL_TASK_TOOL, &json!(["task_id"]))
        );
    }

    /// Another tool of the same channel, listed after `task_status`.
    struct EchoTool;

    impl Tool for EchoTool {
        fn definition(&self) -> AiTool {
            AiTool::new("echo", "").unwrap()
        }

        fn handler(&self, _name: String, arguments: Metadata) -> ToolFuture {
            Box::pin(async move { Ok(ToolResult::Text(format!("echo {}", json!(arguments)))) })
        }
    }

    // Python: tests/test_task_status.py::test_task_status_leaves_another_tools_call_to_that_tool
    #[tokio::test]
    async fn task_status_leaves_another_tools_call_to_that_tool() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let create = CreateRoomOptions {
            room_id: Some(RoomId::new("r").unwrap()),
            ..CreateRoomOptions::default()
        };
        kit.create_room(create).await.unwrap();
        let status: Arc<dyn Tool> =
            Arc::new(TaskStatusTool::new(&kit, TaskStatusToolOptions::default()));
        let entries = vec![ToolEntry::Tool(status), ToolEntry::Tool(Arc::new(EchoTool))];
        let (toolset, _) = served_by(None, entries).unwrap().unwrap();
        let context = || ToolContext::new(RoomId::new("r").unwrap(), channel("speaker"));
        let call = |name: &str, arguments: Value| {
            ToolCall::new("call-1", name, serde_json::from_value(arguments).unwrap())
        };
        let echoed = toolset
            .call(call("echo", json!({ "x": 1 })), context())
            .await
            .unwrap();
        let listed = toolset
            .call(call(TASK_STATUS_TOOL, json!({})), context())
            .await
            .unwrap();
        kit.close().await.unwrap();

        assert_eq!(
            (echoed, listed),
            (
                ToolResult::Text("echo {\"x\":1}".to_owned()),
                ToolResult::Text("{\"tasks\": []}".to_owned())
            )
        );
    }

    // Python: tests/tasks/test_cancel_task.py::test_setup_task_tools_sets_up_both_tools_or_neither
    #[tokio::test]
    async fn setup_task_tools_sets_up_both_tools_or_neither() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let own: Arc<dyn Tool> =
            Arc::new(CancelTaskTool::new(&kit, CancelTaskToolOptions::default()));
        let options = AiChannelOptions {
            tools: vec![ToolEntry::Tool(own)],
            ..AiChannelOptions::default()
        };
        let model = MockAiProvider::new(MockAiProviderOptions {
            responses: vec!["x".to_owned()],
            ..MockAiProviderOptions::default()
        });
        let agent = AiChannel::new(channel("speaker"), model, options).unwrap();
        kit.register_channel(agent).unwrap();

        let first = kit.setup_task_tools(&channel("speaker"));
        // Neither tool was set up: a second setup is refused the same way,
        // never as one made already.
        let again = kit.setup_task_tools(&channel("speaker"));
        kit.close().await.unwrap();

        let refused = |outcome: Result<(), RoomKitError>| match outcome {
            Err(RoomKitError::ToolNameCollision(message)) => message,
            other => format!("{other:?}"),
        };
        let expected = "Tool 'cancel_task' is already one of the channel's own tools on channel \
                        'speaker': rename one of them (RFC §21.1)";
        assert_eq!(
            (refused(first), refused(again)),
            (expected.to_owned(), expected.to_owned())
        );
    }

    #[tokio::test]
    async fn a_host_cancel_still_tells_the_agent() {
        // Python: tests/tasks/test_cancel_task.py::test_a_task_the_host_cancels_is_still_handed_back_to_the_agent
        let room = Room::open(true).await;
        let task = room.running_task().await;
        let cancelled = room
            .kit
            .cancel_task(&task.id, CancelTaskOptions::default())
            .await;
        tokio::time::timeout(BOUND, room.speaker.started.notified())
            .await
            .unwrap();
        room.kit.close().await.unwrap();
        assert_eq!((cancelled, room.handed_back()), (true, 1));
    }

    /// Python's `UnservedToolCallError(f"tool {name!r} is not served here")`:
    /// the name as Python's `repr` writes it.
    #[test]
    fn another_tool_s_call_is_declined_naming_it_as_python_s_repr() {
        assert_eq!(
            not_served("o'k").to_string(),
            ToolError::UnservedToolCall("tool \"o'k\" is not served here".to_owned()).to_string()
        );
    }

    // Python: tests/tasks/test_task_progress_note.py::test_progress_outside_a_tasks_room_posts_nothing,
    // and tasks/status.py post_task_progress's default worker name.
    #[tokio::test]
    async fn progress_outside_a_task_s_room_posts_nothing_and_inside_names_the_worker() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let mut child = Metadata::new();
        child.insert("parent_room_id".to_owned(), json!("r"));
        child.insert("task_id".to_owned(), json!("t1"));
        for (room, metadata) in [("r", Metadata::new()), ("r::task-t1", child)] {
            let create = CreateRoomOptions {
                room_id: Some(RoomId::new(room).unwrap()),
                metadata,
                ..CreateRoomOptions::default()
            };
            kit.create_room(create).await.unwrap();
        }
        let within = |room: &str| ToolTurnContextOptions {
            room_id: Some(RoomId::new(room).unwrap()),
            ..ToolTurnContextOptions::default()
        };
        let outside_a_call = post_task_progress(&kit, "12/20 s").await.unwrap();
        let in_its_parent = tool_turn_context(within("r"), post_task_progress(&kit, "12/20 s"))
            .await
            .unwrap()
            .unwrap();
        let in_no_room = tool_turn_context(within("gone"), post_task_progress(&kit, "12/20 s"))
            .await
            .unwrap()
            .unwrap();
        idle(&kit).await;
        let nothing = kit.status_bus().recent(RecentOptions::default()).await;
        let in_its_room =
            tool_turn_context(within("r::task-t1"), post_task_progress(&kit, "12/20 s"))
                .await
                .unwrap()
                .unwrap();
        idle(&kit).await;
        let posted = kit.status_bus().recent(RecentOptions::default()).await;
        kit.close().await.unwrap();

        assert_eq!(
            (outside_a_call, in_its_parent, in_no_room, nothing.len()),
            (false, false, false, 0)
        );
        assert!(in_its_room);
        let [entry] = posted.as_slice() else {
            panic!("one entry posted: {posted:?}");
        };
        assert_eq!(
            (
                entry.agent_id.as_str(),
                entry.action.as_str(),
                entry.detail.as_str()
            ),
            ("worker", "task", "12/20 s")
        );
        assert_eq!(entry.metadata.get("task_id"), Some(&json!("t1")));
        assert_eq!(entry.metadata.get("room_id"), Some(&json!("r")));
    }
}
