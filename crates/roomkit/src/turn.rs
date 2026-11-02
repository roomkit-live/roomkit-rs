//! An agent's turn, as the code it runs reaches it (RFC §10.6.5, §21.4):
//! [`current_turn_host`] gives the [`TurnHost`] of the intelligence turn the
//! current task runs within, which writes to the turn's room and runs the
//! kit's tool hooks (RFC §9.3).

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use roomkit_core::model::{
    ChannelId, ChannelOutput, ChannelType, DirectEvent, EventContent, EventId, HookAction,
    Metadata, Observation, Room, RoomContext, RoomEvent, RoomId, Timestamp, ToolCall,
    ToolCallOutcome,
};
use roomkit_core::ops::{Environment, SystemEnvironment};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec, HookTrigger};
use serde_json::Value;
use tokio::task::AbortHandle;

use crate::channel::{BoxFuture, DynChannel};
use crate::context::ExecutionContext;
use crate::error::RoomKitError;
use crate::events::FrameworkEvent;
use crate::hooks::{self, HookHandler};
use crate::human_input::PendingInputEvent;
use crate::kit::Inner;
use crate::orchestration::{StatusEntry, StatusLevel};
use crate::store::ConversationStore;
use crate::stream::ResponseStream;
use crate::tasks::Kind;
use crate::tasks::{DelegatedTask, DelegatedTaskResult};
use crate::tool_calls::{ToolCallEvent, ToolCallReport, error_envelope};
use crate::turns::wire::{
    ADD_OBSERVATION, AddObservation, BETWEEN_TURNS, DELEGATE, Delegate, POST_STATUS, PostStatus,
    ROOM_TASKS, RoomTasks, UPDATE_ROOM_METADATA, UpdateRoomMetadata, WAIT_TASK, WaitTask,
    WireError,
};
use crate::turns::{HostLink, TurnError, send_event_arguments};
use crate::{DelegateOptions, RoomKit, SendEventOptions, UpdateRoomMetadataOptions};

mod inputs;

tokio::task_local! {
    static TURN: TurnHost;
}

/// The turn of an intelligence channel the current task runs within, while
/// its `on_event()` runs and after it in work it began, as long as its host
/// can reach the room's owner ([`TurnHost::reachable`]); `None` anywhere
/// else, a host whose kit is gone included (RFC §21.4
/// `current_turn_host()`).
///
/// The kit's turn executor sets it around every intelligence turn's work,
/// its `on_event()` whole and its stream, so a tool the turn calls reads it
/// at any depth, without a kit clone of its own. A task the turn spawns
/// reads it when its future is wrapped in
/// [`keep_context`](crate::keep_context), as does an observer of its tool
/// calls, after the turn too.
///
/// ```
/// // Outside every turn there is none.
/// assert!(roomkit::current_turn_host().is_none());
/// ```
pub fn current_turn_host() -> Option<TurnHost> {
    turn_in_scope().filter(TurnHost::reachable)
}

/// The turn's host the current task runs within, reachable or not.
pub(crate) fn turn_in_scope() -> Option<TurnHost> {
    TURN.try_with(Clone::clone).ok()
}

/// The turn's host (RFC §10.6.5 `TurnHost`): the turn's room and agent,
/// its one verb, `send_event`, and the tool hooks the agent's tool loop
/// runs on each call (RFC §9.3). Cloning it is cheap.
///
/// The turn runs on the kit's turn executor, behind the turn backend (RFC
/// §10.6): `send_event` crosses to the room's owner as a `host_call` while
/// the turn runs, and as a `room_call` after it, from work the turn began
/// (a task it spawned, an observer of its calls). After the turn, the
/// owner takes it from the room's last turn of the agent alone, as a new
/// arrival at the depth of its own event, or at the floor of the observer
/// that sends it.
#[derive(Clone)]
pub struct TurnHost {
    shared: Arc<Shared>,
}

struct Shared {
    room_id: RoomId,
    channel_id: ChannelId,
    channel_type: ChannelType,
    context: RoomContext,
    kit: Arc<dyn TurnKit>,
    /// The stream the turn answered with, until the kit takes it.
    stream: Mutex<Option<ResponseStream>>,
    /// The way to the room's owner.
    link: Arc<HostLink>,
    /// The depth of the turn's response (RFC §8.3).
    response_depth: u32,
    /// The calls the gate let run, until they are reported.
    calls: Mutex<Calls>,
}

/// The calls of a turn its gate let run.
#[derive(Debug, Default)]
struct Calls {
    /// The arguments each call runs with, by its id: what the gate left.
    ran_with: HashMap<String, Metadata>,
    /// The calls running: let run, not reported yet.
    running: Vec<ToolCall>,
}

/// What a call the turn's cut stopped reads (RFC §9.3), as the Python
/// implementation writes it.
const CUT_HINT: &str = "The turn ended before its result.";

impl std::fmt::Debug for TurnHost {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnHost")
            .field("room_id", &self.shared.room_id)
            .field("channel_id", &self.shared.channel_id)
            .finish_non_exhaustive()
    }
}

/// What the model reads under `metadata["arguments"]` of a
/// `BEFORE_TOOL_USE` hook's result: the arguments the call runs with.
const ARGUMENTS: &str = "arguments";

/// What the gate decided on a call before it runs (RFC §9.3, §21.1).
///
/// For roomkit-ai's tool loop, which reads it from
/// `TurnHost::before_tool_use`.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ToolGate {
    /// The call may run, with the arguments the hooks left it.
    Allowed(Metadata),
    /// The call is refused: the model reads `text`, the observers
    /// `error_detail`.
    Refused {
        /// What the model reads.
        text: String,
        /// What failed, for the observers alone.
        error_detail: Option<String>,
    },
}

impl TurnHost {
    /// Whether the turn it serves still runs, Python's `open`.
    pub fn open(&self) -> bool {
        self.shared.link.is_open()
    }

    /// Whether a verb can still reach the room's owner, Python's
    /// `reachable`: during the turn, or after it from work the turn began
    /// while the kit lives (RFC §10.6.3 `room_call`).
    pub fn reachable(&self) -> bool {
        self.shared.link.reaches_owner()
    }

    /// The id of the turn it serves, as the turn backend names it (RFC
    /// §10.6.8): what `RoomKit::steer` addresses a turn by. Python's
    /// private `current_turn_id()`, which the AI channel's loop reads.
    #[doc(hidden)]
    pub fn turn_id(&self) -> &str {
        self.shared.link.turn_id()
    }

    /// Answers the turn with `stream`, its response as it is written (RFC
    /// §6.1 `response_stream`, §6.4 "One loop, streamed or not"): the
    /// channel's `on_event()` returns the output this gives, to which it may
    /// add tasks, observations and room metadata updates, never response
    /// events. The kit reads the stream once the event's delivery set is
    /// over, stores each row as it ends, a text segment, a call's start or
    /// end, and delivers it to the room's targets while the turn goes on; a
    /// transport that streams reads it as it is written (step 4, decision
    /// 9). Nothing of the stream runs but as it is read. A second call
    /// replaces the first stream, which is dropped unread.
    pub fn respond_with_stream(&self, stream: ResponseStream) -> ChannelOutput {
        *self
            .shared
            .stream
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(stream);
        ChannelOutput::default()
    }

    /// The stream the turn answered with, once its `on_event()` returned.
    pub(crate) fn take_stream(&self) -> Option<ResponseStream> {
        self.shared
            .stream
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// Sends `content` from `channel_id` into `room_id`, as
    /// [`RoomKit::send_event`](crate::RoomKit::send_event) does, with the
    /// same options: Python's `TurnHost.send_event(room_id, channel_id,
    /// content, **options)`. The event is at least as deep as the turn's
    /// response while the turn runs, and as the work that sends it
    /// otherwise (RFC §10.5, §8.3, §10.6.5). The owner refuses a room other
    /// than the turn's, and an organization other than its room's.
    ///
    /// # Errors
    ///
    /// As `send_event`, the kit's error rebuilt when it carries no data;
    /// [`RoomKitError::TurnLink`] for a link that failed;
    /// [`RoomKitError::RemoteTurn`] for any other, among them the owner's
    /// refusal of another room and of a call after the turn made once a
    /// later turn of the agent was sent in the room.
    pub async fn send_event(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        content: impl Into<EventContent>,
        options: SendEventOptions,
    ) -> Result<RoomEvent, RoomKitError> {
        let floor = ExecutionContext::current().turn_depth().unwrap_or(0);
        let organization_id = options.organization_id.clone();
        let event = options.event(channel_id.clone(), content.into());
        let arguments = send_event_arguments(room_id, event, organization_id, floor)
            .map_err(|why| TurnError::new("TypeError", why.0).into_kit_error())?;
        let sent = self
            .shared
            .link
            .verb(crate::turns::SEND_EVENT, arguments, floor)
            .await?;
        serde_json::from_value(sent).map_err(|error| {
            TurnError::new(
                "frame_invalid",
                format!("the event sent could not be read: {error}"),
            )
            .into_kit_error()
        })
    }

    /// Posts `agent_id`'s `action` at `status` on the kit's status bus
    /// (RFC §19.8), as [`StatusBus::post`](crate::orchestration::StatusBus::post)
    /// does on the room's owner: Python's `TurnHost.post_status(agent_id,
    /// action, status, *, detail='', metadata=None)`. The entry names the
    /// turn's room in its metadata's `room_id` when it names none, and
    /// fires that room's `ON_STATUS_POSTED` hooks.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`] for a status naming another room than the
    /// turn's, or one only the framework posts: a task's (action `task`),
    /// or a worker run's entry (metadata `task_run`); a link's failure as
    /// [`send_event`](Self::send_event)'s.
    pub async fn post_status(
        &self,
        agent_id: impl Into<String>,
        action: impl Into<String>,
        status: StatusLevel,
        options: PostStatusOptions,
    ) -> Result<(), RoomKitError> {
        let arguments = PostStatus {
            agent_id: agent_id.into(),
            action: action.into(),
            status,
            detail: options.detail,
            metadata: options.metadata,
        };
        self.verb(POST_STATUS, &arguments).await.map(drop)
    }

    /// Merges `metadata` into the turn's room's metadata on the room's
    /// owner, as [`RoomKit::update_room_metadata`] does: the room as stored
    /// after. Python's `TurnHost.update_room_metadata(room_id, metadata, *,
    /// organization_id=None)`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`] for another room than the turn's, or another
    /// organization than its room's; the kit's error otherwise.
    pub async fn update_room_metadata(
        &self,
        room_id: &RoomId,
        metadata: Metadata,
        options: UpdateRoomMetadataOptions,
    ) -> Result<Room, RoomKitError> {
        let arguments = UpdateRoomMetadata {
            room_id: room_id.clone(),
            metadata,
            organization_id: options.organization_id,
        };
        let room = self.verb(UPDATE_ROOM_METADATA, &arguments).await?;
        decoded(room)
    }

    /// Stores `observation` for its room on the room's owner (RFC §5.11):
    /// Python's `TurnHost.add_observation(observation)`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`] for an observation of another room than the
    /// turn's; the store's error otherwise.
    pub async fn add_observation(&self, observation: Observation) -> Result<(), RoomKitError> {
        let arguments = AddObservation { observation };
        self.verb(ADD_OBSERVATION, &arguments).await.map(drop)
    }

    /// Delegates `task` to `agent_id` from `room_id` in the background, as
    /// [`RoomKit::delegate`] does on the room's owner: the task's id, which
    /// [`wait_task`](Self::wait_task) waits for. Python's
    /// `TurnHost.delegate(room_id, agent_id, task, **options)`, its options
    /// those of a background delegation that are data: `wait`,
    /// `on_complete` and `organization_id` are refused.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`] for another room than the turn's, or an
    /// option the verb does not carry; the delegation's refusal otherwise;
    /// after the turn, the verb needs it: a `RuntimeError`.
    pub async fn delegate(
        &self,
        room_id: &RoomId,
        agent_id: &ChannelId,
        task: impl Into<String>,
        options: DelegateOptions,
    ) -> Result<String, RoomKitError> {
        if options.wait || options.on_complete.is_some() || options.organization_id.is_some() {
            return Err(RoomKitError::Value(
                "The arguments of verb \"delegate\" are not valid: a turn's host delegates in \
                 the background, with data only"
                    .to_owned(),
            ));
        }
        let arguments = Delegate {
            room_id: room_id.clone(),
            agent_id: agent_id.clone(),
            task: task.into(),
            context: options.context,
            share_channels: options.share_channels,
            notify: options.notify,
            post_status: options.post_status,
            brief: options.brief,
            conversation: options.conversation,
            hand_back: options.hand_back,
        };
        let task_id = self.verb(DELEGATE, &arguments).await?;
        decoded(task_id)
    }

    /// The result of a task a host of this room delegated, once it ends:
    /// Python's `TurnHost.wait_task(task_id, timeout=None)`; `None` when
    /// the timeout passes first, or for a task the room's hosts did not
    /// delegate.
    ///
    /// # Errors
    ///
    /// A link's failure as [`send_event`](Self::send_event)'s; after the
    /// turn, the verb needs it: a `RuntimeError`.
    pub async fn wait_task(
        &self,
        task_id: &str,
        options: WaitTaskOptions,
    ) -> Result<Option<DelegatedTaskResult>, RoomKitError> {
        let arguments = WaitTask {
            task_id: task_id.to_owned(),
            timeout: options.timeout.map(|timeout| timeout.as_secs_f64()),
        };
        let waited = self.verb(WAIT_TASK, &arguments).await?;
        if waited.is_null() {
            return Ok(None);
        }
        let mut result: DelegatedTaskResult =
            decoded(waited.get("result").cloned().unwrap_or(Value::Null))?;
        // The failure crossed beside the result, its record kept.
        result.exception = waited
            .get("exception")
            .filter(|exception| !exception.is_null())
            .and_then(|exception| serde_json::from_value::<WireError>(exception.clone()).ok())
            .map(|crossed| crate::turns::to_record(&crossed));
        Ok(Some(result))
    }

    /// The turn's room's tasks, as `task_status` reads them on the room's
    /// owner (RFC §23.4): Python's `TurnHost.room_tasks()`.
    ///
    /// # Errors
    ///
    /// A link's failure as [`send_event`](Self::send_event)'s; after the
    /// turn, the verb needs it: a `RuntimeError`.
    pub async fn room_tasks(&self) -> Result<Vec<Metadata>, RoomKitError> {
        let lines = self.verb(ROOM_TASKS, &RoomTasks::default()).await?;
        decoded(lines)
    }

    /// The definitions of the tools the room's owner serves this turn (RFC
    /// §10.6.5), each `{name, description, parameters}`: what an agent
    /// declares beside its own. For an AI channel's turn, not a public
    /// verb.
    #[doc(hidden)]
    pub fn owner_tools(&self) -> Vec<Value> {
        self.shared.link.owner_tools()
    }

    /// Runs the call `tool_call_id` of the owner's tool `name` with
    /// `arguments` on the room's owner, at least as deep as the turn's
    /// response (RFC §10.6.5): the handler's answer, or how it did not
    /// answer. For an AI channel's turn, not a public verb.
    #[doc(hidden)]
    pub async fn call_owner_tool(
        &self,
        name: &str,
        arguments: Metadata,
        tool_call_id: &str,
    ) -> OwnerToolAnswer {
        let floor = ExecutionContext::current()
            .turn_depth()
            .unwrap_or(0)
            .max(self.shared.response_depth);
        self.shared
            .link
            .call_tool(name, arguments, tool_call_id, floor)
            .await
    }

    /// Sends `verb` with `arguments` to the room's owner, at least as deep
    /// as the current work: its result as data. After the turn, a verb
    /// that needs it is refused before it is sent, as Python's host refuses
    /// it (`RuntimeError`).
    async fn verb<T: serde::Serialize>(
        &self,
        verb: &str,
        arguments: &T,
    ) -> Result<Value, RoomKitError> {
        if !self.open() && !BETWEEN_TURNS.contains(&verb) {
            let mut sent = BETWEEN_TURNS.to_vec();
            sent.sort_unstable();
            let message = format!(
                "{verb} needs a turn: after its turn ended, a host sends {} only (RFC §10.6.5)",
                sent.join(", ")
            );
            return Err(TurnError::new("RuntimeError", message).into_kit_error());
        }
        let floor = ExecutionContext::current().turn_depth().unwrap_or(0);
        let Ok(Value::Object(arguments)) = serde_json::to_value(arguments) else {
            return Err(TurnError::new(
                "TypeError",
                format!("the arguments of verb {verb:?} cannot cross the turn backend"),
            )
            .into_kit_error());
        };
        self.shared.link.verb(verb, arguments, floor).await
    }

    /// Runs the kit's `BEFORE_TOOL_USE` hooks on `call`, in chain order
    /// (RFC §9.3), as the Python implementation runs them: a hook's
    /// `HookResult::modify(event)` is read by the next and runs the call
    /// with the event's arguments; a rewrite under `metadata["arguments"]`
    /// is shown to no later hook, and the last one has the last word,
    /// arguments that are not an object refusing the call. A block refuses
    /// the call with its reason. The trigger fails closed: a hook that
    /// raises, runs past its timeout or returns nothing refuses the call
    /// with the fixed refusal, its name and error for the observers alone.
    /// The ASYNC hooks observe the call as the chain left it, once it let
    /// it through, and decide nothing. Emits the `before_tool_use`
    /// framework event with the chain's verdict (RFC §8.2).
    ///
    /// For roomkit-ai's tool loop, which calls it once per call: a tool or
    /// a channel that calls it too runs the hooks twice.
    #[doc(hidden)]
    pub async fn before_tool_use(&self, call: &ToolCall) -> ToolGate {
        let hooks = self.hooks(HookTrigger::BeforeToolUse);
        let original = self.event_of(&ToolCallReport::new(call.clone()));
        let chain = self.gate(&hooks, original).await;
        if chain.ended.is_none() {
            self.observe_with(&hooks, &chain.current, &ExecutionContext::current(), 0);
        }
        let (allowed, reason) = chain.verdict();
        self.emit_before_tool_use(call, allowed, reason);
        let gate = chain.decision(&call.name);
        if let ToolGate::Allowed(arguments) = &gate {
            self.runs(call, arguments);
        }
        gate
    }

    /// The SYNC `BEFORE_TOOL_USE` chain of `hooks` on `original`, the call
    /// as the model made it, run until a hook blocks or fails.
    async fn gate(
        &self,
        hooks: &[(HookSpec, Arc<dyn HookHandler<ToolCallEvent>>)],
        original: ToolCallEvent,
    ) -> GateChain {
        let mut chain = GateChain {
            current: original,
            rewritten: None,
            ended: None,
        };
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Sync)
        {
            let ran = hooks::run(
                Arc::clone(handler),
                chain.current.clone(),
                self.shared.context.clone(),
                spec.timeout,
                |_| None,
            )
            .await;
            self.report_timeout(spec, &ran);
            chain.ended = chain.read(spec, ran);
            if chain.ended.is_some() {
                break;
            }
        }
        chain
    }

    /// Emits the `before_tool_use` framework event of `call` (RFC §8.2), as
    /// the Python implementation's executor emits it.
    fn emit_before_tool_use(&self, call: &ToolCall, allowed: bool, reason: Option<String>) {
        let mut data = Metadata::new();
        data.insert("tool_name".to_owned(), Value::from(call.name.as_str()));
        data.insert("tool_call_id".to_owned(), Value::from(call.id.as_str()));
        data.insert("allowed".to_owned(), Value::Bool(allowed));
        data.insert("reason".to_owned(), reason.map_or(Value::Null, Value::from));
        self.emit("before_tool_use", data);
    }

    /// Emits the framework event `event_type` with `data`, about the
    /// turn's room and agent.
    fn emit(&self, event_type: &str, data: Metadata) {
        let ids = (
            Some(self.shared.room_id.clone()),
            Some(self.shared.channel_id.clone()),
            None,
        );
        self.shared.kit.emit(event_type.to_owned(), ids, data);
    }

    /// Emits the `hook_timeout` framework event of the tool hook `spec` when
    /// it ran past its timeout, SYNC or ASYNC (RFC §8.2, §9.1), as the
    /// Python implementation's engine emits it: about the turn's room and
    /// agent.
    fn report_timeout<P>(&self, spec: &HookSpec, ran: &HookOutcome<P>) {
        if !matches!(ran, HookOutcome::TimedOut) {
            return;
        }
        let mut data = Metadata::new();
        data.insert("hook_name".to_owned(), Value::from(spec.name.as_str()));
        data.insert(
            "trigger".to_owned(),
            serde_json::to_value(spec.trigger).unwrap_or_default(),
        );
        data.insert(
            "timeout".to_owned(),
            Value::from(spec.timeout.as_secs_f64()),
        );
        self.emit("hook_timeout", data);
    }

    /// The gate let `call` run with `arguments`: they cross before what
    /// the stream yields next (RFC §10.6.3 `tool_args`), and a cut reports
    /// it cancelled until it is reported.
    fn runs(&self, call: &ToolCall, arguments: &Metadata) {
        let mut calls = self.calls();
        calls.ran_with.insert(call.id.clone(), arguments.clone());
        let mut running = call.clone();
        running.arguments.clone_from(arguments);
        calls.running.push(running);
    }

    /// Runs `read` on the arguments each call of the turn runs with, by
    /// its id, whose end was not sent yet.
    pub(crate) fn with_ran_with<T>(&self, read: impl FnOnce(&HashMap<String, Metadata>) -> T) -> T {
        read(&self.calls().ran_with)
    }

    /// The end of the call `call_id` was sent: what it ran with is kept no
    /// more.
    pub(crate) fn forget_ran_with(&self, call_id: &str) {
        self.calls().ran_with.remove(call_id);
    }

    fn calls(&self) -> std::sync::MutexGuard<'_, Calls> {
        self.shared
            .calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs the kit's SYNC `ON_TOOL_CALL` hooks on `report`, a served call
    /// or one nothing served, in chain order (RFC §9.3), as the Python
    /// implementation runs them: each reads the call as the one before left
    /// it; `metadata["result"]` replaces the result, a JSON `null` included,
    /// and `metadata["structured_content"]` its structured copy;
    /// `modify(event)` carries a new result; a block withholds the result,
    /// which fails the call `blocked` with no structured copy. A hook may
    /// serve a call nothing served. A hook that raises, runs past its
    /// timeout or returns nothing is skipped, unless it fails closed, which
    /// withholds the result. A call refused, failed or cancelled is returned
    /// as it is: it reaches the observers only.
    ///
    /// For roomkit-ai's tool loop, which calls it once per call: a tool or
    /// a channel that calls it too runs the hooks twice.
    #[doc(hidden)]
    pub async fn on_tool_call(&self, mut report: ToolCallReport) -> ToolCallReport {
        let unserved = report.outcome == Some(ToolCallOutcome::Unserved);
        if report.outcome != Some(ToolCallOutcome::Served) && !unserved {
            return report;
        }
        let hooks = self.hooks(HookTrigger::OnToolCall);
        let mut event = self.event_of(&report);
        // The chain reads a call nothing served as Python hands it: no
        // result, and no error yet.
        event.is_error = false;
        if unserved {
            event.result = None;
        }
        let mut chain = ToolCallChain::new(event);
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Sync)
        {
            let ran = hooks::run(
                Arc::clone(handler),
                chain.current.clone(),
                self.shared.context.clone(),
                spec.timeout,
                |_| None,
            )
            .await;
            self.report_timeout(spec, &ran);
            if let Some(reason) = chain.read(spec, ran) {
                return withheld(report, &reason);
            }
        }
        let Some(result) = chain.current.result else {
            // Nothing served the call: the loop reports it failed, once,
            // with what the hooks that failed said.
            report.error_detail = chain.errors_detail();
            return report;
        };
        if unserved {
            report.outcome = Some(ToolCallOutcome::Served);
        }
        report.result = result;
        report.structured_content = chain.current.structured_content.and_then(renderable);
        report
    }

    /// Reports `report`, a call that ended, to the kit's ASYNC `ON_TOOL_CALL`
    /// hooks, each run in the background within its timeout, what it raises
    /// logged (RFC §9.3: every call fires `ON_TOOL_CALL` once).
    ///
    /// They run as tasks of the turn: a post they make while the turn's
    /// delivery runs draws one of its cascade's passes, a direct change
    /// draws nothing, and what they send is at least as deep as the turn
    /// was now, once the turn is over too (RFC §8.3, "The hook calls of a
    /// cascade").
    ///
    /// For roomkit-ai's tool loop, which calls it once per call: a tool or
    /// a channel that calls it too runs the hooks twice.
    #[doc(hidden)]
    pub fn report_tool_call(&self, report: &ToolCallReport) {
        {
            let mut calls = self.calls();
            if let Some(position) = calls
                .running
                .iter()
                .position(|call| call.id == report.call.id)
            {
                calls.running.remove(position);
            }
        }
        let execution = ExecutionContext::current();
        let floor = execution.turn_depth().unwrap_or(0);
        self.observe(report, &execution, floor);
    }

    /// Reports the calls the turn's cut stopped while they ran `cancelled`
    /// to the kit's observers (RFC §9.3): each with the arguments it ran
    /// with, what it reads saying the turn ended first, at least as deep as
    /// the turn's response, in `execution`, the turn's context.
    pub(crate) fn report_cut(&self, execution: &ExecutionContext, reason: &str) {
        let running = std::mem::take(&mut self.calls().running);
        for call in running {
            let tool = serde_json::to_string(&call.name).unwrap_or_default();
            let read = format!(
                "{{\"error\": \"Tool call cancelled\", \"tool\": {tool}, \"hint\": \"{CUT_HINT}\"}}"
            );
            let report = ToolCallReport::new(call)
                .ended(ToolCallOutcome::Cancelled, Value::String(read))
                .with_error_detail(format!("cancelled: {reason}"));
            self.observe(&report, execution, self.shared.response_depth);
        }
    }

    /// Reports `report` to the kit's ASYNC `ON_TOOL_CALL` hooks in the
    /// background, in `execution`, at least `floor` deep, and emits its
    /// `tool_call` framework event (RFC §8.2).
    fn observe(&self, report: &ToolCallReport, execution: &ExecutionContext, floor: u32) {
        let event = self.event_of(report);
        let hooks = self.hooks(HookTrigger::OnToolCall);
        self.observe_with(&hooks, &event, execution, floor);
        self.emit_tool_call(&event);
    }

    /// Runs the ASYNC hooks of `hooks` on `event` in the background, each
    /// within its timeout, what it raises logged (RFC §9.3), in
    /// `execution`, at least `floor` deep.
    fn observe_with(
        &self,
        hooks: &[(HookSpec, Arc<dyn HookHandler<ToolCallEvent>>)],
        event: &ToolCallEvent,
        execution: &ExecutionContext,
        floor: u32,
    ) {
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Async)
        {
            let observing = hooks::run(
                Arc::clone(handler),
                event.clone(),
                self.shared.context.clone(),
                spec.timeout,
                |_| None,
            );
            let (host, spec) = (self.clone(), spec.clone());
            let bounded = async move {
                let observed = observing.await;
                host.report_timeout(&spec, &observed);
                match observed {
                    HookOutcome::TimedOut => {
                        tracing::warn!(target: "roomkit::hooks", hook = %spec.name, "a tool call's observer timed out");
                    }
                    HookOutcome::Raised(error) => {
                        tracing::warn!(target: "roomkit::hooks", hook = %spec.name, error = %error.message, "a tool call's observer raised");
                    }
                    HookOutcome::Returned(_) | HookOutcome::Invalid { .. } => {}
                }
            };
            let task = execution
                .clone()
                .at_least(floor)
                .scope(TURN.scope(self.clone(), bounded));
            if !self.shared.kit.spawn_observer(Box::pin(task)) {
                tracing::debug!(target: "roomkit::hooks", "the kit is shut down: a tool call's observer is not run");
            }
        }
    }

    /// Emits the `tool_call` framework event of `event` (RFC §8.2), its
    /// failure's and cancellation's markers when they are set, as the
    /// Python implementation emits it.
    fn emit_tool_call(&self, event: &ToolCallEvent) {
        let mut data = Metadata::new();
        data.insert("tool_name".to_owned(), Value::from(event.name.as_str()));
        data.insert(
            "tool_call_id".to_owned(),
            Value::from(event.tool_call_id.as_str()),
        );
        data.insert(
            "channel_type".to_owned(),
            serde_json::to_value(&event.channel_type).unwrap_or_default(),
        );
        for (marker, set) in [
            ("is_error", event.is_error),
            ("cancelled", event.cancelled),
            ("refused", event.refused),
            ("refused_but_ran", event.refused_but_ran),
        ] {
            if set {
                data.insert(marker.to_owned(), Value::Bool(true));
            }
        }
        self.emit("tool_call", data);
    }

    /// Runs `future` as this turn's work: [`current_turn_host`] gives this host
    /// while it runs.
    pub(crate) async fn scope<F: Future>(self, future: F) -> F::Output {
        TURN.scope(self, future).await
    }

    /// The hooks on `trigger` for this agent's calls, the kit's and its
    /// room's, its SYNC chain in chain order, then its observers.
    fn hooks(&self, trigger: HookTrigger) -> Vec<(HookSpec, Arc<dyn HookHandler<ToolCallEvent>>)> {
        let agent = (&self.shared.room_id, &self.shared.channel_id);
        self.shared
            .kit
            .tool_hooks(trigger, agent, &self.shared.channel_type)
    }

    /// `report` as its hooks read it, now.
    fn event_of(&self, report: &ToolCallReport) -> ToolCallEvent {
        report.event(
            &self.shared.channel_id,
            &self.shared.channel_type,
            &self.shared.room_id,
            self.shared.kit.now(),
        )
    }
}

/// `report` with its result withheld by a hook: the model reads `reason`
/// in an error's envelope, the call fails `blocked`, with no structured
/// copy (RFC §9.3).
fn withheld(report: ToolCallReport, reason: &str) -> ToolCallReport {
    let mut report = report.ended(
        ToolCallOutcome::Blocked,
        Value::String(error_envelope(reason)),
    );
    report.structured_content = None;
    report
}

/// `copy` as a structured copy a surface can render: an object, or none,
/// as the Python implementation reads it.
fn renderable(copy: Value) -> Option<Value> {
    if copy.is_object() {
        return Some(copy);
    }
    tracing::warn!(
        target: "roomkit::hooks",
        "an ON_TOOL_CALL hook left a structured_content that is not an object: \
         the call's structured copy is dropped"
    );
    None
}

/// The SYNC `ON_TOOL_CALL` chain on one call, as the Python
/// implementation folds it: the call as the last hook left it, whether it
/// stands served, and what the hooks that failed said.
struct ToolCallChain {
    current: ToolCallEvent,
    served: bool,
    errors: Vec<(String, String)>,
}

impl ToolCallChain {
    fn new(event: ToolCallEvent) -> Self {
        Self {
            served: event.result.is_some(),
            current: event,
            errors: Vec::new(),
        }
    }

    /// Folds what the hook `spec` produced into the call; the reason its
    /// result is withheld when the hook blocked or failed closed.
    fn read(&mut self, spec: &HookSpec, ran: HookOutcome<ToolCallEvent>) -> Option<String> {
        let result = match ran {
            HookOutcome::Returned(result) => result,
            HookOutcome::Raised(error) => return self.failed(spec, "hook_error", error.message),
            HookOutcome::TimedOut => {
                let late = format!("timeout ({}s)", python_seconds(spec.timeout));
                return self.failed(spec, "hook_timeout", late);
            }
            HookOutcome::Invalid { what, .. } => {
                return self.failed(spec, "hook_invalid_result", what);
            }
        };
        if result.action == HookAction::Block {
            let reason = result.reason.filter(|reason| !reason.is_empty());
            return Some(reason.unwrap_or_else(|| "blocked".to_owned()));
        }
        let mut rewrote = false;
        match (result.action, result.event) {
            (HookAction::Modify, Some(modified)) => {
                self.current = modified;
                rewrote = true;
            }
            (HookAction::Modify, None) => {
                let unusable = "modify returned nothing, expected ToolCallEvent".to_owned();
                let closed = self.failed(spec, "hook_invalid_result", unusable);
                if closed.is_some() {
                    return closed;
                }
            }
            (HookAction::Allow | HookAction::Block, _) => {}
        }
        if rewrote || !result.metadata.is_empty() {
            self.fold(&result.metadata);
        }
        None
    }

    /// Writes a hook's `metadata` rewrite into the call: `result` replaces
    /// its result, `structured_content` its structured copy; a served call
    /// a hook empties stays served, its result `null`.
    fn fold(&mut self, metadata: &Metadata) {
        if let Some(result) = metadata.get("result") {
            self.current.result = (!result.is_null()).then(|| result.clone());
        }
        if let Some(copy) = metadata.get("structured_content") {
            self.current.structured_content = renderable(copy.clone());
        }
        if self.served && self.current.result.is_none() {
            self.current.result = Some(Value::from("null"));
        }
        self.served = self.current.result.is_some();
    }

    /// The hook `spec` failed with `error`, of `outcome`: the reason the
    /// result is withheld when the hook fails closed; else it is skipped.
    fn failed(&mut self, spec: &HookSpec, outcome: &str, error: String) -> Option<String> {
        tracing::warn!(
            target: "roomkit::hooks",
            hook = %spec.name,
            %error,
            "an ON_TOOL_CALL hook failed"
        );
        self.errors.push((spec.name.clone(), error));
        spec.fail_closed.then(|| format!("{outcome}:{}", spec.name))
    }

    /// What the hooks that failed said, for the observers alone.
    fn errors_detail(&self) -> Option<String> {
        if self.errors.is_empty() {
            return None;
        }
        let said: Vec<String> = self
            .errors
            .iter()
            .map(|(hook, error)| format!("{hook}: {error}"))
            .collect();
        Some(said.join("; "))
    }
}

/// `duration` in seconds as Python prints a float: `30.0`, `0.5`.
fn python_seconds(duration: std::time::Duration) -> String {
    let seconds = duration.as_secs_f64();
    if seconds.fract() == 0.0 {
        format!("{seconds:.1}")
    } else {
        format!("{seconds}")
    }
}

/// The SYNC `BEFORE_TOOL_USE` chain on one call, as the Python
/// implementation runs it: the call as the last hook left it, the last
/// rewrite under `metadata["arguments"]`, and how the chain ended.
struct GateChain {
    current: ToolCallEvent,
    rewritten: Option<Value>,
    ended: Option<GateEnd>,
}

/// How a `BEFORE_TOOL_USE` chain ended before its last hook.
enum GateEnd {
    /// A hook blocked, giving this reason.
    Blocked(Option<String>),
    /// A hook failed, and the trigger fails closed: `reason` for the
    /// framework event, `detail` for the observers.
    Failed { reason: String, detail: String },
}

impl GateChain {
    /// Reads what the hook `spec` produced; how the chain ended when it
    /// blocked or failed.
    fn read(&mut self, spec: &HookSpec, ran: HookOutcome<ToolCallEvent>) -> Option<GateEnd> {
        let result = match ran {
            HookOutcome::Returned(result) => result,
            HookOutcome::Raised(error) => {
                let reason = format!("hook {} failed: {}", spec.name, error.message);
                return Some(failed(spec, "hook_error", reason, &error.message));
            }
            HookOutcome::TimedOut => {
                let seconds = python_seconds(spec.timeout);
                let reason = format!("hook {} timed out after {seconds}s", spec.name);
                let error = format!("timeout ({seconds}s)");
                return Some(failed(spec, "hook_timeout", reason, &error));
            }
            HookOutcome::Invalid { what, .. } => {
                let got = what
                    .strip_prefix("expected HookResult, got ")
                    .unwrap_or(&what);
                let reason = format!("hook {} returned {got} instead of HookResult", spec.name);
                return Some(failed(spec, "hook_invalid_result", reason, &what));
            }
        };
        if let Some(arguments) = result.metadata.get(ARGUMENTS) {
            self.rewritten = Some(arguments.clone());
        }
        match (result.action, result.event) {
            (HookAction::Block, _) => Some(GateEnd::Blocked(result.reason)),
            (HookAction::Modify, Some(modified)) => {
                self.current = modified;
                None
            }
            (HookAction::Modify, None) => {
                let reason = format!("hook {} returned an unusable payload", spec.name);
                let error = "modify returned nothing, expected ToolCallEvent";
                Some(failed(spec, "hook_invalid_result", reason, error))
            }
            (HookAction::Allow, _) => None,
        }
    }

    /// What the `before_tool_use` framework event reports: whether the
    /// chain let the call through, and the reason a block or a failure
    /// gave.
    fn verdict(&self) -> (bool, Option<String>) {
        match &self.ended {
            None => (true, None),
            Some(GateEnd::Blocked(reason)) => (false, reason.clone()),
            Some(GateEnd::Failed { reason, .. }) => (false, Some(reason.clone())),
        }
    }

    /// The gate's decision on the call `name`, as Python's
    /// `before_tool_decision` reads the chain: rewritten arguments that
    /// are not an object refuse it before a block or a failure is read.
    fn decision(self, name: &str) -> ToolGate {
        if self
            .rewritten
            .as_ref()
            .is_some_and(|arguments| !arguments.is_object())
        {
            tracing::error!(
                target: "roomkit::hooks",
                tool = %name,
                "a BEFORE_TOOL_USE hook returned non-object arguments: the call is refused"
            );
            return refused(name, None, None);
        }
        match (self.ended, self.rewritten) {
            (Some(GateEnd::Blocked(reason)), _) => refused(name, reason, None),
            (Some(GateEnd::Failed { detail, .. }), _) => refused(name, None, Some(detail)),
            (None, Some(Value::Object(arguments))) => ToolGate::Allowed(arguments),
            (None, _) => ToolGate::Allowed(self.current.arguments),
        }
    }
}

/// The hook `spec` failed with `error`, of `outcome`, in a trigger that
/// fails closed: Python's reason, `<outcome>:<name>` for a hook that fails
/// closed itself, `trigger_reason` otherwise; the hook's name and error
/// for the observers alone (RFC §9.3).
fn failed(spec: &HookSpec, outcome: &str, trigger_reason: String, error: &str) -> GateEnd {
    tracing::warn!(
        target: "roomkit::hooks",
        hook = %spec.name,
        %error,
        "a BEFORE_TOOL_USE hook failed: the call is refused"
    );
    let reason = if spec.fail_closed {
        format!("{outcome}:{}", spec.name)
    } else {
        trigger_reason
    };
    GateEnd::Failed {
        reason,
        detail: format!("{}: {error}", spec.name),
    }
}

/// A call the gate refused: a block's reason as the model reads it, or
/// the fixed refusal when it gave none (RFC §9.3); what failed for the
/// observers alone.
fn refused(name: &str, reason: Option<String>, error_detail: Option<String>) -> ToolGate {
    let text = match reason.filter(|reason| !reason.is_empty()) {
        Some(reason) => error_envelope(&reason),
        None => denied(name),
    };
    ToolGate::Refused { text, error_detail }
}

/// The fixed refusal of a call the gate stopped (RFC §9.3).
fn denied(name: &str) -> String {
    error_envelope(&format!("Tool '{name}' denied by pre-execution hook."))
}

/// What the turn backend needs of its kit, whatever its store: the owner's
/// one verb, the executor's tool hooks and channels, its background tasks,
/// and the framework events a turn emits.
/// How the room's owner answered a call of a tool it serves a turn (RFC
/// §10.6.5): the handler's answer as the model reads it, or why it gave
/// none. For an AI channel's turn.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub enum OwnerToolAnswer {
    /// The handler's answer.
    Served(Value),
    /// The handler refused the call: nothing ran; the model reads these
    /// words.
    Refused(String),
    /// The handler ran and raised an error of `kind`, saying `message`.
    Failed {
        /// The error's kind.
        kind: String,
        /// Its message.
        message: String,
    },
    /// Nothing on the owner serves the tool.
    Unserved(String),
}

/// A call of a tool the owner serves, as its handler reads it: the room of
/// the call, the channel whose turn made it, and its arguments.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OwnerCall {
    pub(crate) room_id: RoomId,
    pub(crate) channel_id: ChannelId,
    pub(crate) tool_call_id: String,
    pub(crate) arguments: Metadata,
}

pub(crate) trait TurnKit: Send + Sync {
    /// The tools the owner serves the turns of `channel_id` (RFC §10.6.5),
    /// as a turn's request lists them.
    fn owner_tools(&self, channel_id: &ChannelId) -> Vec<crate::turns::wire::OwnerTool>;

    /// Serves `call` of the owner's tool `name`: its handler's answer.
    fn serve_owner_tool(
        &self,
        name: String,
        call: OwnerCall,
    ) -> BoxFuture<'static, OwnerToolAnswer>;

    /// Injects `event` into `room_id`.
    fn send_event(
        &self,
        room_id: RoomId,
        event: DirectEvent,
    ) -> BoxFuture<'static, Result<RoomEvent, RoomKitError>>;

    /// The tool hooks of `trigger` that fire for calls of the agent
    /// `channel_id` of `channel_type` in the room `room_id`, the kit's and
    /// the room's: its SYNC chain in chain order, then its observers.
    fn tool_hooks(
        &self,
        trigger: HookTrigger,
        agent: (&RoomId, &ChannelId),
        channel_type: &ChannelType,
    ) -> Vec<(HookSpec, Arc<dyn HookHandler<ToolCallEvent>>)>;

    /// The `ON_USER_INPUT_REQUIRED` hooks that fire for the requests of
    /// the agent `channel_id` of `channel_type` in the room `room_id`, the
    /// kit's and the room's: its SYNC chain in chain order, then its
    /// observers (RFC §9.3).
    fn input_hooks(
        &self,
        agent: (&RoomId, &ChannelId),
        channel_type: &ChannelType,
    ) -> Vec<(HookSpec, Arc<dyn HookHandler<PendingInputEvent>>)>;

    /// The kit's clock, now.
    fn now(&self) -> Timestamp;

    /// Runs `task` in the background, tracked, in the turn's unit; `false`
    /// once the kit is shut down.
    fn spawn(&self, task: BoxFuture<'static, ()>) -> bool {
        self.spawn_tracked(task).is_some()
    }

    /// Runs `task` in the background, tracked, in the unit of the kit's
    /// work the current code runs in, the turn's: what aborts it; `None`
    /// once the kit is shut down.
    fn spawn_tracked(&self, task: BoxFuture<'static, ()>) -> Option<AbortHandle>;

    /// Runs `task`, an observer hook's body bounded by its timeout, in the
    /// background, tracked as an observer, which the kit's close lets
    /// finish; `false` once the kit is shut down.
    fn spawn_observer(&self, task: BoxFuture<'static, ()>) -> bool;

    /// The channel the kit registered under `channel_id`, live.
    fn channel(&self, channel_id: &ChannelId) -> Option<Arc<dyn DynChannel>>;

    /// Emits a framework event of `event_type` to the kit's handlers, now:
    /// one a turn emitted on its executor, placed as its frame places it.
    fn emit(
        &self,
        event_type: String,
        ids: (Option<RoomId>, Option<ChannelId>, Option<EventId>),
        data: Metadata,
    );

    /// Whether the current task is one the kit's `close()` is called from:
    /// a turn run in it runs on whatever cuts it (RFC §12.10.4).
    fn acting(&self) -> bool;

    /// Posts `entry` on the kit's status bus, its publication run in the
    /// background as the kit's own work (RFC §19.8).
    fn post_status(&self, entry: StatusEntry) -> Result<(), RoomKitError>;

    /// Merges `metadata` into `room_id`'s metadata, for `organization_id`
    /// (RFC §5.1).
    fn update_room_metadata(
        &self,
        room_id: RoomId,
        metadata: Metadata,
        organization_id: Option<String>,
    ) -> BoxFuture<'static, Result<Room, RoomKitError>>;

    /// Stores `observation` for its room (RFC §5.11).
    fn add_observation(
        &self,
        observation: Observation,
    ) -> BoxFuture<'static, Result<(), RoomKitError>>;

    /// `room_id` as the store holds it, `None` for a room it does not hold.
    fn room(&self, room_id: RoomId) -> BoxFuture<'static, Result<Option<Room>, RoomKitError>>;

    /// Delegates `options.task` in the background for a turn whose
    /// response is `floor` deep, keeping the task for `wait_task`: its id.
    fn delegate(
        &self,
        arguments: Delegate,
        floor: u32,
    ) -> BoxFuture<'static, Result<String, RoomKitError>>;

    /// The task `task_id` a host of `room_id` delegated, once it ends or
    /// `timeout` passes; `None` for a task the hosts did not delegate or a
    /// wait that timed out.
    fn wait_task(
        &self,
        room_id: RoomId,
        task_id: String,
        timeout: Option<std::time::Duration>,
    ) -> BoxFuture<'static, Option<DelegatedTaskResult>>;

    /// The tasks of `room_id`, as its status bus lists them (RFC §23.4).
    fn room_tasks(&self, room_id: RoomId) -> BoxFuture<'static, Vec<Metadata>>;
}

impl<S: ConversationStore> TurnKit for Weak<Inner<S>> {
    fn owner_tools(&self, channel_id: &ChannelId) -> Vec<crate::turns::wire::OwnerTool> {
        self.upgrade()
            .map(|inner| inner.owner_tools.declared(channel_id))
            .unwrap_or_default()
    }

    fn serve_owner_tool(
        &self,
        name: String,
        call: OwnerCall,
    ) -> BoxFuture<'static, OwnerToolAnswer> {
        let kit = self.upgrade().map(|inner| RoomKit { inner });
        Box::pin(async move {
            let Some(kit) = kit else {
                return OwnerToolAnswer::Failed {
                    kind: "KitClosedError".to_owned(),
                    message: "the kit is closed".to_owned(),
                };
            };
            let Some(serve) = kit.inner.owner_tools.lookup(&call.channel_id, &name) else {
                return OwnerToolAnswer::Unserved(format!(
                    "No tool {name:?} is served by the room owner"
                ));
            };
            serve(kit, call).await
        })
    }

    fn send_event(
        &self,
        room_id: RoomId,
        event: DirectEvent,
    ) -> BoxFuture<'static, Result<RoomEvent, RoomKitError>> {
        let kit = self.upgrade().map(|inner| RoomKit { inner });
        Box::pin(async move {
            let kit = kit.ok_or(RoomKitError::KitClosed)?;
            kit.inject(&room_id, None, event).await
        })
    }

    fn tool_hooks(
        &self,
        trigger: HookTrigger,
        agent: (&RoomId, &ChannelId),
        channel_type: &ChannelType,
    ) -> Vec<(HookSpec, Arc<dyn HookHandler<ToolCallEvent>>)> {
        self.upgrade()
            .map(|inner| inner.tool_hooks(trigger, agent, channel_type))
            .unwrap_or_default()
    }

    fn input_hooks(
        &self,
        agent: (&RoomId, &ChannelId),
        channel_type: &ChannelType,
    ) -> Vec<(HookSpec, Arc<dyn HookHandler<PendingInputEvent>>)> {
        self.upgrade()
            .map(|inner| inner.input_hooks(agent, channel_type))
            .unwrap_or_default()
    }

    fn now(&self) -> Timestamp {
        self.upgrade().map_or_else(
            || SystemEnvironment.now(),
            |inner| inner.environment().now(),
        )
    }

    fn spawn_tracked(&self, task: BoxFuture<'static, ()>) -> Option<AbortHandle> {
        let inner = self.upgrade()?;
        inner
            .tasks
            .spawn_joined(Kind::Turn, task)
            .map(|task| task.abort_handle())
    }

    fn spawn_observer(&self, task: BoxFuture<'static, ()>) -> bool {
        self.upgrade()
            .and_then(|inner| inner.tasks.spawn_alone(Kind::Observer, task))
            .is_some()
    }

    fn channel(&self, channel_id: &ChannelId) -> Option<Arc<dyn DynChannel>> {
        self.upgrade()?.channel(channel_id)
    }

    fn acting(&self) -> bool {
        self.upgrade().is_some_and(|inner| inner.tasks.acting())
    }

    fn post_status(&self, entry: StatusEntry) -> Result<(), RoomKitError> {
        let inner = self.upgrade().ok_or(RoomKitError::KitClosed)?;
        inner.open()?;
        let publication = inner.status_bus.publication(entry);
        if inner
            .tasks
            .spawn_alone(Kind::Observer, publication)
            .is_none()
        {
            return Err(RoomKitError::KitClosed);
        }
        Ok(())
    }

    fn update_room_metadata(
        &self,
        room_id: RoomId,
        metadata: Metadata,
        organization_id: Option<String>,
    ) -> BoxFuture<'static, Result<Room, RoomKitError>> {
        let kit = self.upgrade().map(|inner| RoomKit { inner });
        Box::pin(async move {
            let kit = kit.ok_or(RoomKitError::KitClosed)?;
            let options = UpdateRoomMetadataOptions { organization_id };
            kit.update_room_metadata(&room_id, metadata, options).await
        })
    }

    fn add_observation(
        &self,
        observation: Observation,
    ) -> BoxFuture<'static, Result<(), RoomKitError>> {
        let kit = self.upgrade().map(|inner| RoomKit { inner });
        Box::pin(async move {
            let kit = kit.ok_or(RoomKitError::KitClosed)?;
            kit.inner.open()?;
            kit.store().add_observation(observation).await?;
            Ok(())
        })
    }

    fn room(&self, room_id: RoomId) -> BoxFuture<'static, Result<Option<Room>, RoomKitError>> {
        let kit = self.upgrade().map(|inner| RoomKit { inner });
        Box::pin(async move {
            let kit = kit.ok_or(RoomKitError::KitClosed)?;
            Ok(kit.store().get_room(&room_id).await?)
        })
    }

    fn delegate(
        &self,
        arguments: Delegate,
        floor: u32,
    ) -> BoxFuture<'static, Result<String, RoomKitError>> {
        let kit = self.upgrade().map(|inner| RoomKit { inner });
        Box::pin(async move {
            let kit = kit.ok_or(RoomKitError::KitClosed)?;
            let Delegate {
                room_id,
                agent_id,
                task,
                context,
                share_channels,
                notify,
                post_status,
                brief,
                conversation,
                hand_back,
            } = arguments;
            let options = DelegateOptions {
                context,
                share_channels,
                notify,
                post_status,
                brief,
                conversation,
                hand_back,
                ..DelegateOptions::default()
            };
            // Read where the turn's call was made: its result continues the
            // turn's chain (RFC §23.3).
            let delegating = kit.delegate(&room_id, &agent_id, task, options);
            let task: DelegatedTask = ExecutionContext::current()
                .at_least(floor)
                .scope(Box::pin(delegating))
                .await?;
            kit.inner.host_tasks.keep(task.clone());
            Ok(task.id)
        })
    }

    fn wait_task(
        &self,
        room_id: RoomId,
        task_id: String,
        timeout: Option<std::time::Duration>,
    ) -> BoxFuture<'static, Option<DelegatedTaskResult>> {
        let task = self
            .upgrade()
            .and_then(|inner| inner.host_tasks.of_room(&task_id, room_id.as_str()));
        Box::pin(async move {
            let options = crate::tasks::WaitOptions { timeout };
            task?.wait(options).await.ok()
        })
    }

    fn room_tasks(&self, room_id: RoomId) -> BoxFuture<'static, Vec<Metadata>> {
        let bus = self.upgrade().map(|inner| inner.status_bus.clone());
        Box::pin(async move {
            let Some(bus) = bus else {
                return Vec::new();
            };
            crate::tasks::lines::room_task_lines(
                &bus,
                room_id.as_str(),
                crate::tasks::lines::ROOM_TASKS_WINDOW,
            )
            .await
        })
    }

    fn emit(
        &self,
        event_type: String,
        (room_id, channel_id, event_id): (Option<RoomId>, Option<ChannelId>, Option<EventId>),
        data: Metadata,
    ) {
        let Some(inner) = self.upgrade() else {
            return;
        };
        let event = FrameworkEvent {
            r#type: event_type,
            room_id,
            channel_id,
            event_id,
            timestamp: inner.environment().now(),
            data,
        };
        drop(inner.events.emit(&inner.tasks, event));
    }
}

/// A verb's result read as `T`.
fn decoded<T: serde::de::DeserializeOwned>(result: Value) -> Result<T, RoomKitError> {
    serde_json::from_value(result).map_err(|error| {
        TurnError::new(
            "frame_invalid",
            format!("the verb's result could not be read: {error}"),
        )
        .into_kit_error()
    })
}

/// What [`TurnHost::wait_task`] takes beside the task: Python's
/// `wait_task(task_id, timeout=None)`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WaitTaskOptions {
    /// Python `timeout=None`: how long to wait at most; `None` waits for
    /// the task's end.
    pub timeout: Option<std::time::Duration>,
}

/// What [`TurnHost::post_status`] takes beside the agent, its action and
/// its level: Python's `post_status(..., *, detail='', metadata=None)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PostStatusOptions {
    /// Python `detail=''`: what the entry says.
    pub detail: String,
    /// Python `metadata=None`: what else it carries; its `room_id`, when it
    /// names one, is the turn's room.
    pub metadata: Option<Metadata>,
}

/// The host of the turn the intelligence channel `channel_id` takes in
/// `context`'s room, on the kit's turn executor: its verbs reach the owner
/// through `link`, its response `response_depth` deep.
pub(crate) fn host(
    kit: Arc<dyn TurnKit>,
    channel_id: ChannelId,
    channel_type: ChannelType,
    context: RoomContext,
    link: Arc<HostLink>,
    response_depth: u32,
) -> TurnHost {
    TurnHost {
        shared: Arc::new(Shared {
            room_id: context.room.id.clone(),
            channel_id,
            channel_type,
            context,
            kit,
            stream: Mutex::new(None),
            link,
            response_depth,
            calls: Mutex::new(Calls::default()),
        }),
    }
}

/// `future` within the turn the current task runs within, if any: for a
/// task the turn spawns (RFC §21.4).
pub(crate) fn within_current_turn<F>(future: F) -> impl Future<Output = F::Output> + Send + 'static
where
    F: Future + Send + 'static,
{
    let turn = turn_in_scope();
    async move {
        match turn {
            Some(turn) => TURN.scope(turn, future).await,
            None => future.await,
        }
    }
}
