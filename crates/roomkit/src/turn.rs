//! An agent's turn, as the code it runs reaches it (RFC §10.6.5, §21.4):
//! [`current_turn_host`] gives the [`TurnHost`] of the intelligence turn the
//! current task runs within, which writes to the turn's room and runs the
//! kit's tool hooks (RFC §9.3).

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use roomkit_core::model::{
    ChannelId, ChannelOutput, ChannelType, DirectEvent, EventContent, EventDirection, EventSource,
    Metadata, RoomContext, RoomEvent, RoomId, ToolCall, ToolCallOutcome,
};
use roomkit_core::ops::FrameworkEvent;
use roomkit_core::rules::hook_registry::{HookSpec, HookTrigger};
use serde_json::Value;
use tokio::task::AbortHandle;

use crate::channel::{BoxFuture, DynChannel};
use crate::context::ExecutionContext;
use crate::error::RoomKitError;
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::stream::ResponseStream;
use crate::tasks::Kind;
use crate::tools::{ToolCallEvent, ToolHandler, ToolUse, ToolVerdict, error_envelope};
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
    /// (RFC §9.3): each reads the arguments the one before left. A block
    /// refuses the call with its reason; a hook that raises or runs past its
    /// timeout refuses it with the fixed refusal, the trigger failing closed,
    /// its name and error going to the observers alone.
    ///
    /// For roomkit-ai's tool loop, which calls it once per call: a tool or
    /// a channel that calls it too runs the hooks twice.
    #[doc(hidden)]
    pub async fn before_tool_use(&self, call: &ToolCall) -> ToolGate {
        let mut arguments = call.arguments.clone();
        for (spec, handler) in self.hooks(HookTrigger::BeforeToolUse) {
            let ToolHandler::Gate(gate) = handler else {
                continue;
            };
            let mut gated = call.clone();
            gated.arguments = arguments.clone();
            let event = self.event(gated);
            let decided =
                tokio::time::timeout(spec.timeout, gate(event, self.shared.context.clone())).await;
            match decided {
                Ok(Ok(ToolUse::Allow)) => {}
                Ok(Ok(ToolUse::Rewrite(rewritten))) => arguments = rewritten,
                Ok(Ok(ToolUse::Block(reason))) => return blocked(call, reason),
                Ok(Err(error)) => return failed_closed(call, &spec, &error.to_string()),
                Err(_elapsed) => {
                    let late = format!("did not answer within {} s", spec.timeout.as_secs_f64());
                    return failed_closed(call, &spec, &late);
                }
            }
        }
        self.runs(call, &arguments);
        ToolGate::Allowed(arguments)
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

    /// Runs the kit's SYNC `ON_TOOL_CALL` hooks on `event`, a served call,
    /// in chain order (RFC §9.3): each reads the result as the one before
    /// left it, may replace it, or withhold it, which fails the call
    /// `blocked`. A hook that raises or runs past its timeout is skipped. A
    /// call that was not served is returned as it is: a firing that cannot
    /// serve it reaches the observers only.
    ///
    /// For roomkit-ai's tool loop, which calls it once per call: a tool or
    /// a channel that calls it too runs the hooks twice.
    #[doc(hidden)]
    pub async fn on_tool_call(&self, mut event: ToolCallEvent) -> ToolCallEvent {
        if event.outcome != Some(ToolCallOutcome::Served) {
            return event;
        }
        for (spec, handler) in self.hooks(HookTrigger::OnToolCall) {
            let ToolHandler::Verdict(verdict) = handler else {
                continue;
            };
            let decided = tokio::time::timeout(
                spec.timeout,
                verdict(event.clone(), self.shared.context.clone()),
            )
            .await;
            match decided {
                Ok(Ok(ToolVerdict::Keep)) => {}
                Ok(Ok(ToolVerdict::Replace(result))) => event.result = result,
                Ok(Ok(ToolVerdict::Block(reason))) => {
                    let withheld = Value::String(withheld(reason));
                    return event.ended(ToolCallOutcome::Blocked, withheld);
                }
                Ok(Err(error)) => skipped(&spec, &error.to_string()),
                Err(_elapsed) => skipped(&spec, "it did not answer within its timeout"),
            }
        }
        event
    }

    /// Reports `event`, a call that ended, to the kit's ASYNC `ON_TOOL_CALL`
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
    pub fn report_tool_call(&self, event: &ToolCallEvent) {
        {
            let mut calls = self.calls();
            if let Some(position) = calls
                .running
                .iter()
                .position(|call| call.id == event.call.id)
            {
                calls.running.remove(position);
            }
        }
        let execution = ExecutionContext::current();
        let floor = execution.turn_depth().unwrap_or(0);
        self.observe(event, &execution, floor);
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
            let event = ToolCallEvent::new(
                self.shared.channel_id.clone(),
                self.shared.room_id.clone(),
                call,
            )
            .ended(ToolCallOutcome::Cancelled, Value::String(read))
            .with_error_detail(format!("cancelled: {reason}"));
            self.observe(&event, execution, self.shared.response_depth);
        }
    }

    /// Runs the kit's ASYNC `ON_TOOL_CALL` hooks on `event` in the
    /// background, in `execution`, at least `floor` deep.
    fn observe(&self, event: &ToolCallEvent, execution: &ExecutionContext, floor: u32) {
        for (spec, handler) in self.hooks(HookTrigger::OnToolCall) {
            let ToolHandler::Observer(observer) = handler else {
                continue;
            };
            let observing = observer(event.clone(), self.shared.context.clone());
            let name = spec.name.clone();
            let bounded = async move {
                if tokio::time::timeout(spec.timeout, observing).await.is_err() {
                    tracing::warn!(target: "roomkit::hooks", hook = %name, "a tool call's observer timed out");
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

    /// Runs `future` as this turn's work: [`current_turn_host`] gives this host
    /// while it runs.
    pub(crate) async fn scope<F: Future>(self, future: F) -> F::Output {
        TURN.scope(self, future).await
    }

    /// The kit's hooks on `trigger` for this agent's calls, its SYNC chain
    /// in chain order, then its observers.
    fn hooks(&self, trigger: HookTrigger) -> Vec<(HookSpec, ToolHandler)> {
        let source = EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(
                self.shared.channel_id.clone(),
                self.shared.channel_type.clone(),
            )
        };
        self.shared.kit.tool_hooks(trigger, &source)
    }

    /// `call` as its hooks read it.
    fn event(&self, call: ToolCall) -> ToolCallEvent {
        ToolCallEvent::new(
            self.shared.channel_id.clone(),
            self.shared.room_id.clone(),
            call,
        )
    }
}

/// What the model reads of a result a hook withheld: its reason in an
/// error's envelope, or `{"error": "blocked"}` when it gave none, as the
/// reference implementation writes it (RFC §9.3). A blank reason reads as
/// none, as a blank `ToolUse::Block` reason does; the reference
/// implementation keeps it as written (docs/rfc-gaps.md).
fn withheld(reason: Option<String>) -> String {
    let reason = reason.filter(|reason| !reason.trim().is_empty());
    error_envelope(reason.as_deref().unwrap_or("blocked"))
}

/// A call refused by a hook's block: its reason as the model reads it, or
/// the fixed refusal when it gave none (RFC §9.3).
fn blocked(call: &ToolCall, reason: Option<String>) -> ToolGate {
    let text = match reason.filter(|reason| !reason.trim().is_empty()) {
        Some(reason) => error_envelope(&reason),
        None => denied(call),
    };
    ToolGate::Refused {
        text,
        error_detail: None,
    }
}

/// A call refused because the gate `spec` failed with `error`: the fixed
/// refusal, the gate's name and error for the observers (RFC §9.3).
fn failed_closed(call: &ToolCall, spec: &HookSpec, error: &str) -> ToolGate {
    tracing::warn!(
        target: "roomkit::hooks",
        hook = %spec.name,
        tool = %call.name,
        %error,
        "a BEFORE_TOOL_USE hook failed: the call is refused"
    );
    ToolGate::Refused {
        text: denied(call),
        error_detail: Some(format!("{}: {error}", spec.name)),
    }
}

/// The fixed refusal of a call the gate stopped (RFC §9.3).
fn denied(call: &ToolCall) -> String {
    error_envelope(&format!(
        "Tool '{}' denied by pre-execution hook.",
        call.name
    ))
}

fn skipped(spec: &HookSpec, error: &str) {
    tracing::warn!(
        target: "roomkit::hooks",
        hook = %spec.name,
        %error,
        "an ON_TOOL_CALL hook failed: it is skipped"
    );
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

    /// The tool hooks of `trigger` that fire for calls of `source`: its
    /// SYNC chain in chain order, then its observers.
    fn tool_hooks(
        &self,
        trigger: HookTrigger,
        source: &EventSource,
    ) -> Vec<(HookSpec, ToolHandler)>;

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

    /// Publishes a framework event to the kit's subscribers.
    fn publish(&self, event: FrameworkEvent);

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
        source: &EventSource,
    ) -> Vec<(HookSpec, ToolHandler)> {
        self.upgrade()
            .map(|inner| inner.tool_hooks(trigger, source))
            .unwrap_or_default()
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

    fn publish(&self, event: FrameworkEvent) {
        if let Some(inner) = self.upgrade() {
            inner.publish(event);
        }
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
