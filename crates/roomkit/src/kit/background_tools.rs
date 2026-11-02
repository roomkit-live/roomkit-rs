//! The task tools the kit sets up on an agent, which the room's owner
//! serves wherever the agent's turns run (RFC §10.6.5, §23.4): Python's
//! `BackgroundMixin.setup_background` and `setup_task_tools`,
//! `tasks.background.background_tool_server`, `TaskStatusTool` and
//! `CancelTaskTool`.
//!
//! `work_in_background` starts long work as a task, as `start_task` does,
//! for the agent of the call alone; `task_status` lists the tasks of the
//! room of the call; `cancel_task` cancels one of them, never another
//! room's, its caller not told twice.

use std::sync::{Arc, PoisonError};

use roomkit_core::model::{ChannelId, Metadata, RoomId};
use serde_json::{Value, json};

use super::RoomKit;
use super::start_task::StartTaskOptions;
use crate::error::{RoomKitError, repr};
use crate::store::ConversationStore;
use crate::tasks::json::{dumps_object, json as write_json, object};
use crate::tasks::lines::{Line, ROOM_TASKS_WINDOW, room_lines};
use crate::tasks::text::identifier;
use crate::tasks::{AGENT_REACH, TaskReach};
use crate::turn::{OwnerCall, OwnerToolAnswer};
use crate::turns::served::{OwnerServe, OwnerToolEntry};

/// The tool that starts long work as a task: Python's `BACKGROUND_TOOL`.
const BACKGROUND_TOOL: &str = "work_in_background";

/// The tool that lists the room's tasks: Python's `TASK_STATUS_TOOL`.
const TASK_STATUS_TOOL: &str = "task_status";

/// The tool that cancels one of them: Python's `CANCEL_TASK_TOOL`.
const CANCEL_TASK_TOOL: &str = "cancel_task";

/// How many tasks, the latest, `task_status` lists at most.
const TASK_STATUS_LIMIT: usize = 10;

/// What a task's own work answers `work_in_background`: Python's
/// `_NESTED`.
const NESTED: &str = "You are already working on a task in the background: do the work now \
with your tools and end with what you found. A task starts no task of its own.";

/// How an agent's background tasks run (RFC §23.4): on `worker`, the agent
/// itself when `None`, their result relayed or posted. Python's
/// `BackgroundSetup(worker=None, hand_back='relay')`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BackgroundSetup {
    pub(crate) worker: Option<ChannelId>,
    pub(crate) hand_back: String,
}

impl Default for BackgroundSetup {
    fn default() -> Self {
        Self {
            worker: None,
            hand_back: "relay".to_owned(),
        }
    }
}

/// What [`RoomKit::setup_background`] takes beside the agent: Python's
/// `setup_background(agent, *, worker=None, hand_back='relay')`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetupBackgroundOptions {
    /// Python `worker=None`: the channel that does the agent's tasks; the
    /// agent itself when `None`.
    pub worker: Option<ChannelId>,
    /// Python `hand_back='relay'`: `relay`, the agent says the result in a
    /// turn of its own, or `post`, it is posted as it is, as its message.
    pub hand_back: String,
}

impl Default for SetupBackgroundOptions {
    fn default() -> Self {
        Self {
            worker: None,
            hand_back: "relay".to_owned(),
        }
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// Gives `agent` the `work_in_background` tool (RFC §23.4), served by
    /// the room's owner wherever its turns run: it starts long work as a
    /// task and ends its turn, the work running apart. Python's
    /// `setup_background(agent, *, worker=None, hand_back='relay')`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4); [`RoomKitError::Value`] for an unknown hand-back;
    /// [`RoomKitError::ChannelNotRegistered`] for an agent the kit does not
    /// hold; [`RoomKitError::Runtime`] for a tool already set up on the
    /// agent; [`RoomKitError::ToolNameCollision`] for its name taken by one
    /// of the agent's own tools.
    pub fn setup_background(
        &self,
        agent: &ChannelId,
        options: SetupBackgroundOptions,
    ) -> Result<(), RoomKitError> {
        self.inner.open()?;
        let SetupBackgroundOptions { worker, hand_back } = options;
        if !["relay", "post"].contains(&hand_back.as_str()) {
            return Err(RoomKitError::Value(format!(
                "hand_back must be one of ('relay', 'post'), not {}",
                repr(&hand_back)
            )));
        }
        self.registered_agent(agent)?;
        if self.inner.owner_tools.serves(agent, BACKGROUND_TOOL) {
            return Err(already_set_up("setup_background", agent));
        }
        let entry = OwnerToolEntry {
            definition: background_definition(),
            serve: serve_with(work_in_background),
        };
        self.register_owner_tools(agent, vec![entry])?;
        self.inner
            .background_setups
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(agent.clone(), BackgroundSetup { worker, hand_back });
        Ok(())
    }

    /// Gives `agent` `task_status` and `cancel_task` (RFC §23.4), served by
    /// the room's owner wherever its turns run: the tasks of the room of
    /// the call, and a cancel of one of them, never another room's. Both
    /// or neither. Python's `setup_task_tools(agent)`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4); [`RoomKitError::ChannelNotRegistered`] for an agent the
    /// kit does not hold; [`RoomKitError::Runtime`] for tools already set up
    /// on the agent; [`RoomKitError::ToolNameCollision`] for a name one of
    /// the agent's own tools takes or the owner serves it already, neither
    /// set up (Python's `ToolNameCollisionError`).
    pub fn setup_task_tools(&self, agent: &ChannelId) -> Result<(), RoomKitError> {
        self.inner.open()?;
        self.registered_agent(agent)?;
        if self.inner.owner_tools.serves(agent, TASK_STATUS_TOOL) {
            return Err(already_set_up("setup_task_tools", agent));
        }
        let entries = vec![
            OwnerToolEntry {
                definition: task_status_definition(),
                serve: serve_with(task_status),
            },
            OwnerToolEntry {
                definition: cancel_task_definition(),
                serve: serve_with(cancel_task),
            },
        ];
        self.register_owner_tools(agent, entries)
    }

    /// Refuses an agent the kit does not hold, or one whose turns never
    /// declare the tools the room's owner serves (RFC §10.6.5): a tool set
    /// up for it would never be seen. Python's `owner_tools_of`.
    fn registered_agent(&self, agent: &ChannelId) -> Result<(), RoomKitError> {
        let Some(channel) = self.inner.channel(agent) else {
            return Err(RoomKitError::ChannelNotRegistered(agent.clone()));
        };
        if !channel.declaration().takes_owner_tools {
            return Err(RoomKitError::Value(format!(
                "Channel '{}' does not take the tools its room owner serves (RFC §10.6.5): its \
                 turns would never declare them",
                agent.as_str()
            )));
        }
        Ok(())
    }

    /// Sets up `entries` for `agent`, all or none: a name one of the
    /// agent's own tools takes, or one the owner serves it already, is
    /// refused (RFC §21.1), as Python's registry's `register_all`.
    fn register_owner_tools(
        &self,
        agent: &ChannelId,
        entries: Vec<OwnerToolEntry<S>>,
    ) -> Result<(), RoomKitError> {
        let own = self
            .inner
            .channel(agent)
            .map(|channel| channel.own_tool_names())
            .unwrap_or_default();
        if let Some(entry) = entries
            .iter()
            .find(|entry| own.iter().any(|name| name == entry.name()))
        {
            return Err(collision(
                entry.name(),
                "one of the channel's own tools",
                agent,
            ));
        }
        self.inner
            .owner_tools
            .register_all(agent, entries)
            .map_err(|name| collision(&name, "served by the channel", agent))
    }

    /// How `agent`'s background tasks run: its setup, or the default.
    pub(crate) fn background_setup(&self, agent: &ChannelId) -> BackgroundSetup {
        self.inner
            .background_setups
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(agent)
            .cloned()
            .unwrap_or_default()
    }
}

impl<S: ConversationStore> super::Inner<S> {
    /// Drops every tool set up for `channel_id`, and how its background
    /// tasks run: the channel was unregistered, and one registered again
    /// under its id gets its own setup (Python's `unregister_channel`,
    /// `_owner_tools.forget`).
    pub(super) fn forget_owner_tools(&self, channel_id: &ChannelId) {
        self.owner_tools.forget(channel_id);
        self.background_setups
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(channel_id);
    }
}

/// The refusal of a tool under a name `clash` already takes on `agent`:
/// Python's `ToolNameCollisionError`, in its words.
pub(super) fn collision(name: &str, clash: &str, agent: &ChannelId) -> RoomKitError {
    RoomKitError::ToolNameCollision(format!(
        "Tool '{name}' is already {clash} on channel '{}': rename one of them (RFC §21.1)",
        agent.as_str()
    ))
}

/// The refusal of a setup made twice on one agent: Python's
/// `RuntimeError`.
fn already_set_up(setup: &str, agent: &ChannelId) -> RoomKitError {
    RoomKitError::Runtime(format!(
        "{setup}() already called for channel '{}'",
        agent.as_str()
    ))
}

/// What serves a tool with `answer`, the kit and the call to its text.
fn serve_with<S, F, Fut>(answer: F) -> OwnerServe<S>
where
    S: ConversationStore,
    F: Fn(RoomKit<S>, OwnerCall) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String, RoomKitError>> + Send + 'static,
{
    Arc::new(move |kit, call| {
        let answering = answer(kit, call);
        Box::pin(async move {
            match answering.await {
                Ok(text) => OwnerToolAnswer::Served(Value::String(text)),
                Err(error) => OwnerToolAnswer::Failed {
                    kind: error.class_name(),
                    message: error.to_string(),
                },
            }
        })
    })
}

/// `work_in_background`'s answer to `call`, in the room of the call, for
/// the agent whose turn made it alone (RFC §23.4): the task started, or
/// why not.
async fn work_in_background<S: ConversationStore>(
    kit: RoomKit<S>,
    call: OwnerCall,
) -> Result<String, RoomKitError> {
    let room = kit.store().get_room(&call.room_id).await?;
    let in_a_task = room
        .as_ref()
        .and_then(|room| room.metadata.get("task_id"))
        .is_some_and(|task_id| !matches!(task_id, Value::Null) && task_id != "");
    if in_a_task {
        return Ok(error_answer(NESTED));
    }
    let task = call
        .arguments
        .get("task")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_owned();
    let started = kit
        .start_task(
            &call.room_id,
            &call.channel_id,
            task,
            StartTaskOptions::default(),
        )
        .await;
    let started = match started {
        Ok(started) => started,
        Err(RoomKitError::Value(message)) => return Ok(error_answer(&message)),
        Err(error) => return Err(error),
    };
    // The agent as the room addresses it: its channel id, a discussion's
    // handle once discussions come (RFC §19.7.5 rule 4).
    let agent = identifier(call.channel_id.as_str(), "agent");
    let back = if started.hand_back == "post" {
        format!("its result is posted as {agent}'s message")
    } else {
        format!("its result comes back to {agent} as a turn of its own")
    };
    let message = format!(
        "The work runs in the background and {back} when it is done. If this turn has not said \
         so yet, say in one short sentence that it is under way; then end your turn, without \
         saying it again."
    );
    let (task_id, status, message) = (json!(started.id), json!("started"), json!(message));
    Ok(dumps_object(
        [
            ("task_id", &task_id),
            ("status", &status),
            ("message", &message),
        ],
        true,
    ))
}

/// `task_status`'s answer to `call`: the tasks of the room of the call,
/// the latest ten, or the one its `task_id` names (RFC §23.4).
async fn task_status<S: ConversationStore>(
    kit: RoomKit<S>,
    call: OwnerCall,
) -> Result<String, RoomKitError> {
    let window = TaskToolWindow {
        window: ROOM_TASKS_WINDOW,
        limit: TASK_STATUS_LIMIT,
    };
    Ok(kit
        .task_status_answer(&call.room_id, &call.arguments, window)
        .await)
}

/// `cancel_task`'s answer to `call`: a task of the room of the call
/// cancelled, or as it stands (RFC §23.4).
async fn cancel_task<S: ConversationStore>(
    kit: RoomKit<S>,
    call: OwnerCall,
) -> Result<String, RoomKitError> {
    let window = TaskToolWindow {
        window: ROOM_TASKS_WINDOW,
        limit: TASK_STATUS_LIMIT,
    };
    Ok(kit
        .cancel_task_answer(
            &call.room_id,
            Some(&call.channel_id),
            &call.arguments,
            window,
        )
        .await)
}

/// How far a task tool reads the bus: how many of its latest entries
/// (Python's `window=500`), and how many tasks `task_status` lists at most
/// (`limit=10`).
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TaskToolWindow {
    /// How many of the bus's latest entries are read.
    pub window: usize,
    /// How many tasks, the latest, `task_status` lists at most.
    pub limit: usize,
}

impl<S: ConversationStore> RoomKit<S> {
    /// `task_status`'s answer in `room_id`, the room of the call: the
    /// tasks the bus lists for it, the latest `window.limit`, or the one
    /// the call's `task_id` names (RFC §23.4). What Python's
    /// `TaskStatusTool.handler` answers once it read the room of the call;
    /// for roomkit-ai's `TaskStatusTool`, outside the public API.
    #[doc(hidden)]
    pub async fn task_status_answer(
        &self,
        room_id: &RoomId,
        arguments: &Metadata,
        window: TaskToolWindow,
    ) -> String {
        task_status_lines(self, room_id, arguments, window).await
    }

    /// `cancel_task`'s answer in `room_id`, the room of the call, for
    /// `channel_id`, the channel whose call it is, which a cancel does not
    /// tell twice (RFC §23.4). What Python's `CancelTaskTool.handler`
    /// answers once it read the call; for roomkit-ai's `CancelTaskTool`,
    /// outside the public API.
    #[doc(hidden)]
    pub async fn cancel_task_answer(
        &self,
        room_id: &RoomId,
        channel_id: Option<&ChannelId>,
        arguments: &Metadata,
        window: TaskToolWindow,
    ) -> String {
        cancel_task_lines(self, room_id, channel_id, arguments, window.window).await
    }
}

/// The tasks of `room_id` as `task_status` lists them.
async fn task_status_lines<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    arguments: &Metadata,
    window: TaskToolWindow,
) -> String {
    let lines = room_lines(kit.status_bus(), room_id.as_str(), window.window).await;
    let wanted = wanted_task(arguments);
    let mut tasks: Vec<&Line> = lines.iter().collect();
    if !wanted.is_empty() {
        tasks.retain(|line| line.get("task_id").and_then(Value::as_str) == Some(wanted.as_str()));
        if tasks.is_empty() {
            let message = json!(format!("No task {wanted} here."));
            let mut written = String::from("{\"tasks\": [], \"message\": ");
            write_json(&mut written, &message, true);
            written.push('}');
            return written;
        }
    }
    tasks.drain(..tasks.len().saturating_sub(window.limit));
    let mut written = String::from("{\"tasks\": [");
    for (at, line) in tasks.iter().enumerate() {
        if at > 0 {
            written.push_str(", ");
        }
        object(
            &mut written,
            line.members()
                .iter()
                .map(|(name, value)| (name.as_str(), value)),
            false,
        );
    }
    written.push_str("]}");
    written
}

/// `cancel_task`'s answer in `room_id` for `channel_id`: a task of the
/// room cancelled, or as it stands (RFC §23.4).
async fn cancel_task_lines<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    channel_id: Option<&ChannelId>,
    arguments: &Metadata,
    window: usize,
) -> String {
    let task_id = wanted_task(arguments);
    if task_id.is_empty() {
        return error_answer("task_id is required");
    }
    let Some(line) = line_of(kit, room_id.as_str(), &task_id, window).await else {
        return answer(&task_id, "unknown", &format!("No task {task_id} here."));
    };
    if line.get("status").and_then(Value::as_str) == Some("running") {
        return answer_running(kit, room_id, channel_id, &task_id, window).await;
    }
    // Ended: nothing to act on, so as the bus lists it, as task_status does.
    let status = line.get("status").map_or_else(String::new, value_text);
    answer(&task_id, &status, "The task had already ended.")
}

/// The answer for a task the bus lists running for `room_id`, from what
/// the kit knows of it: cancelled when the room may cancel it, ended when
/// it ended meanwhile or before, else still running and why, or no task
/// of the room.
async fn answer_running<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    channel_id: Option<&ChannelId>,
    task_id: &str,
    window: usize,
) -> String {
    let record = &kit.inner.room_tasks;
    let reach = record.reach(task_id, room_id.as_str());
    if kit
        .cancel_task_for(task_id, channel_id, room_id.as_str(), AGENT_REACH)
        .await
    {
        return answer(
            task_id,
            "cancelled",
            "Cancelled: its result will not come back.",
        );
    }
    if reach == Some(TaskReach::Tool) || record.ended_with(task_id, room_id.as_str()).is_some() {
        return answer_ended(kit, room_id, task_id, window).await;
    }
    out_of_reach(task_id, reach)
}

/// The answer for a task of the room that ended: with the status the kit
/// recorded, else the bus's once it shows the end.
async fn answer_ended<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
    task_id: &str,
    window: usize,
) -> String {
    let recorded = kit.inner.room_tasks.ended_with(task_id, room_id.as_str());
    let status = if let Some(status) = recorded {
        status
    } else {
        let line = line_of(kit, room_id.as_str(), task_id, window).await;
        line.and_then(|line| line.get("status").map(value_text))
            .filter(|status| status != "running")
            .unwrap_or_else(|| "ended".to_owned())
    };
    answer(task_id, &status, "The task had already ended.")
}

/// The answer for a task the room lists running that its call may not
/// cancel: a task of the room is still running, and why; any other is no
/// task here.
fn out_of_reach(task_id: &str, reach: Option<TaskReach>) -> String {
    let why = match reach {
        Some(TaskReach::Turn) => "it runs inside the turn that asked for it, and ends with it",
        Some(TaskReach::Strategy) => "the strategy that runs it follows it",
        Some(TaskReach::Tool) | None => {
            return answer(task_id, "unknown", &format!("No task {task_id} here."));
        }
    };
    answer(
        task_id,
        "running",
        &format!("Still running, and not cancellable here: {why}."),
    )
}

/// The line the bus lists for `task_id` in `room_id`, read from its
/// latest `window` entries.
async fn line_of<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &str,
    task_id: &str,
    window: usize,
) -> Option<Line> {
    room_lines(kit.status_bus(), room_id, window)
        .await
        .into_iter()
        .find(|line| line.get("task_id").and_then(Value::as_str) == Some(task_id))
}

/// The task a call names, as Python's `str(arguments.get("task_id") or
/// "")`.
fn wanted_task(arguments: &Metadata) -> String {
    match arguments.get("task_id") {
        None | Some(Value::Null) => String::new(),
        Some(value) => value_text(value),
    }
}

/// A value as Python's `str` writes a string: itself, unquoted.
fn value_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other @ (Value::Null
        | Value::Bool(_)
        | Value::Number(_)
        | Value::Array(_)
        | Value::Object(_)) => other.to_string(),
    }
}

/// `{"task_id": ..., "status": ..., "message": ...}`.
fn answer(task_id: &str, status: &str, message: &str) -> String {
    let (task_id, status, message) = (json!(task_id), json!(status), json!(message));
    dumps_object(
        [
            ("task_id", &task_id),
            ("status", &status),
            ("message", &message),
        ],
        true,
    )
}

/// `{"error": message}`.
fn error_answer(message: &str) -> String {
    let message = json!(message);
    dumps_object([("error", &message)], true)
}

/// `work_in_background`'s definition: Python's `BACKGROUND_TOOL_DEF`.
fn background_definition() -> Value {
    json!({
        "name": BACKGROUND_TOOL,
        "description": "Start long work as a background task, so the conversation goes on: going \
    through many records, logs, traces or files, or an analysis that takes several tool calls. It runs \
    away from the conversation, handed its last messages, and its result comes back when it is done. \
    Say once, in one short sentence, that the work is under way (before the call or after it, never \
    both), and end your turn. A quick look you answer now, without it.",
        "parameters": {
            "type": "object",
            "properties": {
                "task": {
                    "type": "string",
                    "description": "What to do and what to report, precisely.",
                },
            },
            "required": ["task"],
        },
    })
}

/// `task_status`'s definition: Python's `TaskStatusTool.definition`.
#[doc(hidden)]
pub fn task_status_definition() -> Value {
    json!({
        "name": TASK_STATUS_TOOL,
        "description": "List the background tasks of this conversation: those still running and \
    those that ended, with the agent working on each, what was asked, and the result once it \
    completed.",
        "parameters": {
            "type": "object",
            "properties": {
                "task_id": {
                    "type": "string",
                    "description": "Only this task (its id, from a delegation's answer).",
                },
            },
            "required": [],
        },
    })
}

/// `cancel_task`'s definition: Python's `CancelTaskTool.definition`.
#[doc(hidden)]
pub fn cancel_task_definition() -> Value {
    json!({
        "name": CANCEL_TASK_TOOL,
        "description": "Cancel a background task of this conversation that is still running, \
    when what it was asked to do is no longer wanted: the request changed or was dropped. Its result \
    will not come back.",
        "parameters": {
            "type": "object",
            "properties": {
                "task_id": {
                    "type": "string",
                    "description": "The task to cancel (its id, from a delegation's answer).",
                },
            },
            "required": ["task_id"],
        },
    })
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use std::future::{Future, ready};

    use roomkit_core::model::{
        ChannelBinding, ChannelCategory, ChannelOutput, ChannelType, InboundMessage, RoomContext,
        RoomEvent,
    };

    use super::*;
    use crate::testing::{EchoAgent, EchoAgentOptions};
    use crate::{Channel, ChannelDeclaration, ChannelError, CreateRoomOptions, RoomKitOptions};

    fn channel(id: &str) -> ChannelId {
        ChannelId::new(id).unwrap()
    }

    /// An agent whose turns declare the tools the room's owner serves, as
    /// an AI channel's do.
    struct Speaker;

    impl Channel for Speaker {
        fn channel_id(&self) -> ChannelId {
            channel("speaker")
        }

        fn channel_type(&self) -> ChannelType {
            ChannelType::AI
        }

        fn category(&self) -> ChannelCategory {
            ChannelCategory::Intelligence
        }

        fn declaration(&self) -> ChannelDeclaration {
            ChannelDeclaration::new(self.channel_id())
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
    }

    // Python: tests/test_declared_strategies.py::test_no_owner_tool_is_set_up_for_a_channel_that_takes_none
    #[test]
    fn no_owner_tool_is_set_up_for_a_channel_that_takes_none() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let agent = EchoAgent::new(channel("echo"), "ok", EchoAgentOptions::default());
        kit.register_channel(agent).unwrap();
        let refused = |outcome: Result<(), RoomKitError>| match outcome {
            Err(RoomKitError::Value(message)) => message.contains("does not take the tools"),
            _ => false,
        };
        assert_eq!(
            (
                refused(kit.setup_task_tools(&channel("echo"))),
                refused(kit.setup_background(&channel("echo"), SetupBackgroundOptions::default()))
            ),
            (true, true)
        );
    }

    // Python: tests/tasks/test_background_tasks.py::test_setup_declares_the_tool_on_the_agent
    #[tokio::test]
    async fn setup_background_declares_the_tool_once() {
        let kit = RoomKit::new(RoomKitOptions::default());
        kit.register_channel(Speaker).unwrap();
        let first = kit.setup_background(&channel("speaker"), SetupBackgroundOptions::default());
        let again = kit.setup_background(&channel("speaker"), SetupBackgroundOptions::default());
        let declared: Vec<Value> = kit
            .inner
            .owner_tools
            .declared(&channel("speaker"), &RoomId::new("r").unwrap())
            .into_iter()
            .map(|owned| owned.tool["name"].clone())
            .collect();
        assert_eq!(
            (
                first.is_ok(),
                again.map_err(|error| match error {
                    RoomKitError::Runtime(message) => message,
                    other => format!("not a RuntimeError: {other:?}"),
                }),
                declared
            ),
            (
                true,
                Err("setup_background() already called for channel 'speaker'".to_owned()),
                vec![json!(BACKGROUND_TOOL)]
            )
        );
    }

    // Python: tests/test_declared_strategies.py::test_an_agent_registered_again_gets_its_own_tools_only
    #[tokio::test]
    async fn an_agent_registered_again_gets_its_own_tools_only() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let speaker = channel("speaker");
        kit.register_channel(Speaker).unwrap();
        let task_tools = kit.setup_task_tools(&speaker);
        kit.setup_background(&speaker, SetupBackgroundOptions::default())
            .unwrap();
        kit.unregister_channel(&speaker).unwrap();
        let declared = |kit: &RoomKit| -> Vec<Value> {
            let room = RoomId::new("r").unwrap();
            let owned = kit.inner.owner_tools.declared(&speaker, &room);
            owned
                .into_iter()
                .map(|owned| owned.tool["name"].clone())
                .collect()
        };
        let left = declared(&kit);
        kit.register_channel(Speaker).unwrap();
        let again = kit.setup_background(&speaker, SetupBackgroundOptions::default());

        assert_eq!(
            (task_tools.is_ok(), left, again.is_ok(), declared(&kit)),
            (
                true,
                Vec::<Value>::new(),
                true,
                vec![json!(BACKGROUND_TOOL)]
            )
        );
    }

    // Python: tests/tasks/test_background_tasks.py::test_the_tool_is_refused_in_a_task_s_child_room
    #[tokio::test]
    async fn the_tool_is_refused_in_a_task_s_child_room() {
        let kit = RoomKit::new(RoomKitOptions::default());
        let mut metadata = Metadata::new();
        metadata.insert("task_id".to_owned(), json!("task-x"));
        let child = RoomId::new("r::task-x").unwrap();
        let options = CreateRoomOptions {
            room_id: Some(child.clone()),
            metadata,
            ..CreateRoomOptions::default()
        };
        kit.create_room(options).await.unwrap();
        let mut arguments = Metadata::new();
        arguments.insert("task".to_owned(), json!("More work."));
        let call = OwnerCall {
            room_id: child,
            channel_id: channel("speaker"),
            tool_call_id: "c1".to_owned(),
            arguments,
        };
        let answer = work_in_background(kit, call).await.unwrap();
        assert_eq!(answer, error_answer(NESTED));
    }

    // Python: tests/tasks/test_background_tasks.py::test_setup_refuses_an_unknown_hand_back
    #[test]
    fn setup_refuses_an_unknown_hand_back() {
        let kit = RoomKit::new(RoomKitOptions::default());
        kit.register_channel(Speaker).unwrap();
        let options = SetupBackgroundOptions {
            hand_back: "shout".to_owned(),
            ..SetupBackgroundOptions::default()
        };
        let refused = kit.setup_background(&channel("speaker"), options);
        let declared = kit
            .inner
            .owner_tools
            .declared(&channel("speaker"), &RoomId::new("r").unwrap());
        assert_eq!(
            (refused.err().map(|error| error.to_string()), declared.len()),
            (
                Some(
                    RoomKitError::Value(
                        "hand_back must be one of ('relay', 'post'), not 'shout'".to_owned()
                    )
                    .to_string()
                ),
                0
            )
        );
    }

    // Python: tests/tasks/test_cancel_task.py::test_the_definition_asks_for_the_task_id
    #[test]
    fn the_cancel_tool_s_definition_asks_for_the_task_id() {
        let definition = cancel_task_definition();
        assert_eq!(
            (&definition["name"], &definition["parameters"]["required"]),
            (&json!("cancel_task"), &json!(["task_id"]))
        );
    }

    #[test]
    fn a_task_out_of_reach_says_why_or_is_no_task_here() {
        assert_eq!(
            [
                out_of_reach("t", Some(TaskReach::Turn)),
                out_of_reach("t", None),
            ],
            [
                "{\"task_id\": \"t\", \"status\": \"running\", \"message\": \"Still running, and \
                 not cancellable here: it runs inside the turn that asked for it, and ends with \
                 it.\"}"
                    .to_owned(),
                "{\"task_id\": \"t\", \"status\": \"unknown\", \"message\": \"No task t here.\"}"
                    .to_owned(),
            ]
        );
    }
}
