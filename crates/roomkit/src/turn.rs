//! An agent's turn, as the code it runs reaches it (RFC §10.6.5, §21.4):
//! [`current_turn_host`] gives the [`TurnHost`] of the intelligence turn the
//! current task runs within, which writes to the turn's room and runs the
//! kit's tool hooks (RFC §9.3).

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use roomkit_core::model::{
    ChannelId, ChannelOutput, ChannelType, DirectEvent, EventContent, EventId, HookAction,
    Metadata, RoomContext, RoomEvent, RoomId, Timestamp, ToolCall, ToolCallOutcome,
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
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::stream::ResponseStream;
use crate::tasks::Kind;
use crate::tool_calls::{ToolCallEvent, ToolCallReport, error_envelope};
use crate::turns::{HostLink, TurnError, send_event_arguments};
use crate::{RoomKit, SendEventOptions};

tokio::task_local! {
    static TURN: TurnHost;
}

/// The turn of an intelligence channel the current task runs within, while
/// its `on_event()` runs; `None` anywhere else (RFC §21.4
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
        /// The reason a hook's block gave, for the `before_tool_use`
        /// framework event.
        reason: Option<String>,
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

    /// Runs the kit's `BEFORE_TOOL_USE` hooks on `call`, in chain order
    /// (RFC §9.3), as the Python implementation runs them: a hook's
    /// `HookResult::modify(event)` is read by the next and runs the call
    /// with the event's arguments; a rewrite under `metadata["arguments"]`
    /// is shown to no later hook, and the last one has the last word,
    /// arguments that are not an object refusing the call. A block refuses
    /// the call with its reason. The trigger fails closed: a hook that
    /// raises, runs past its timeout or returns nothing refuses the call
    /// with the fixed refusal, its name and error for the observers alone.
    /// The ASYNC hooks observe the call and decide nothing. Emits the
    /// `before_tool_use` framework event (RFC §8.2).
    ///
    /// For roomkit-ai's tool loop, which calls it once per call: a tool or
    /// a channel that calls it too runs the hooks twice.
    #[doc(hidden)]
    pub async fn before_tool_use(&self, call: &ToolCall) -> ToolGate {
        let hooks = self.hooks(HookTrigger::BeforeToolUse);
        let original = self.event_of(&ToolCallReport::new(call.clone()));
        self.observe_with(&hooks, &original, &ExecutionContext::current(), 0);
        let gate = self.gate(&hooks, original).await;
        let (allowed, reason) = match &gate {
            ToolGate::Allowed(_) => (true, None),
            ToolGate::Refused { reason, .. } => (false, reason.clone()),
        };
        self.emit_before_tool_use(call, allowed, reason);
        match gate {
            ToolGate::Allowed(arguments) => {
                self.runs(call, &arguments);
                ToolGate::Allowed(arguments)
            }
            refused @ ToolGate::Refused { .. } => refused,
        }
    }

    /// The decision of the SYNC `BEFORE_TOOL_USE` chain of `hooks` on
    /// `original`, the call as the model made it, as the Python
    /// implementation's `before_tool_decision` reads it: rewritten
    /// arguments that are not an object refuse the call before a block's
    /// reason or a failure is read.
    async fn gate(
        &self,
        hooks: &[(HookSpec, Arc<dyn HookHandler<ToolCallEvent>>)],
        original: ToolCallEvent,
    ) -> ToolGate {
        let mut current = original.clone();
        let mut rewritten: Option<Value> = None;
        let mut ended = None;
        for (spec, handler) in hooks
            .iter()
            .filter(|(spec, _)| spec.execution == HookExecution::Sync)
        {
            let ran = hooks::run(
                Arc::clone(handler),
                current.clone(),
                self.shared.context.clone(),
                spec.timeout,
                |_| None,
            )
            .await;
            let result = match ran {
                HookOutcome::Returned(result) => result,
                HookOutcome::Raised(error) => {
                    ended = Some(failed_closed(&original.name, spec, &error.message));
                    break;
                }
                HookOutcome::TimedOut => {
                    let late = format!("timeout ({}s)", python_seconds(spec.timeout));
                    ended = Some(failed_closed(&original.name, spec, &late));
                    break;
                }
                HookOutcome::Invalid { what, .. } => {
                    ended = Some(failed_closed(&original.name, spec, &what));
                    break;
                }
            };
            if let Some(arguments) = result.metadata.get(ARGUMENTS) {
                rewritten = Some(arguments.clone());
            }
            match (result.action, result.event) {
                (HookAction::Block, _) => {
                    ended = Some(blocked(&original.name, result.reason));
                    break;
                }
                (HookAction::Modify, Some(modified)) => current = modified,
                (HookAction::Modify, None) => {
                    let unusable = "modify returned nothing, expected ToolCallEvent";
                    ended = Some(failed_closed(&original.name, spec, unusable));
                    break;
                }
                (HookAction::Allow, _) => {}
            }
        }
        match rewritten {
            Some(Value::Object(_)) | None => {}
            Some(_) => {
                tracing::error!(
                    target: "roomkit::hooks",
                    tool = %original.name,
                    "a BEFORE_TOOL_USE hook returned non-object arguments: the call is refused"
                );
                return blocked(&original.name, None);
            }
        }
        match (ended, rewritten) {
            (Some(refused), _) => refused,
            (None, Some(Value::Object(arguments))) => ToolGate::Allowed(arguments),
            (None, _) => ToolGate::Allowed(current.arguments),
        }
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
            let name = spec.name.clone();
            let bounded = async move {
                match observing.await {
                    HookOutcome::TimedOut => {
                        tracing::warn!(target: "roomkit::hooks", hook = %name, "a tool call's observer timed out");
                    }
                    HookOutcome::Raised(error) => {
                        tracing::warn!(target: "roomkit::hooks", hook = %name, error = %error.message, "a tool call's observer raised");
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

    /// The kit's hooks on `trigger` for this agent's calls, its SYNC chain
    /// in chain order, then its observers.
    fn hooks(&self, trigger: HookTrigger) -> Vec<(HookSpec, Arc<dyn HookHandler<ToolCallEvent>>)> {
        self.shared
            .kit
            .tool_hooks(trigger, &self.shared.channel_id, &self.shared.channel_type)
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

/// A call refused by a hook's block: its reason as the model reads it, or
/// the fixed refusal when it gave none (RFC §9.3).
fn blocked(name: &str, reason: Option<String>) -> ToolGate {
    let reason = reason.filter(|reason| !reason.is_empty());
    let text = match &reason {
        Some(reason) => error_envelope(reason),
        None => denied(name),
    };
    ToolGate::Refused {
        text,
        error_detail: None,
        reason,
    }
}

/// A call refused because the gate `spec` failed with `error`: the fixed
/// refusal, the gate's name and error for the observers (RFC §9.3).
fn failed_closed(name: &str, spec: &HookSpec, error: &str) -> ToolGate {
    tracing::warn!(
        target: "roomkit::hooks",
        hook = %spec.name,
        tool = %name,
        %error,
        "a BEFORE_TOOL_USE hook failed: the call is refused"
    );
    ToolGate::Refused {
        text: denied(name),
        error_detail: Some(format!("{}: {error}", spec.name)),
        reason: None,
    }
}

/// The fixed refusal of a call the gate stopped (RFC §9.3).
fn denied(name: &str) -> String {
    error_envelope(&format!("Tool '{name}' denied by pre-execution hook."))
}

/// What the turn backend needs of its kit, whatever its store: the owner's
/// one verb, the executor's tool hooks and channels, its background tasks,
/// and the framework events a turn emits.
pub(crate) trait TurnKit: Send + Sync {
    /// Injects `event` into `room_id`.
    fn send_event(
        &self,
        room_id: RoomId,
        event: DirectEvent,
    ) -> BoxFuture<'static, Result<RoomEvent, RoomKitError>>;

    /// The tool hooks of `trigger` that fire for calls of the agent
    /// `channel_id` of `channel_type`: its SYNC chain in chain order, then
    /// its observers.
    fn tool_hooks(
        &self,
        trigger: HookTrigger,
        channel_id: &ChannelId,
        channel_type: &ChannelType,
    ) -> Vec<(HookSpec, Arc<dyn HookHandler<ToolCallEvent>>)>;

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
}

impl<S: ConversationStore> TurnKit for Weak<Inner<S>> {
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
        channel_id: &ChannelId,
        channel_type: &ChannelType,
    ) -> Vec<(HookSpec, Arc<dyn HookHandler<ToolCallEvent>>)> {
        self.upgrade()
            .map(|inner| inner.tool_hooks(trigger, channel_id, channel_type))
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
    let turn = current_turn_host();
    async move {
        match turn {
            Some(turn) => TURN.scope(turn, future).await,
            None => future.await,
        }
    }
}
