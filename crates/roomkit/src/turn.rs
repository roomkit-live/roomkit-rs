//! An agent's turn, as the code it runs reaches it (RFC §10.6.5, §21.4):
//! [`current_turn`] gives the [`TurnHost`] of the intelligence turn the
//! current task runs within, which writes to the turn's room and runs the
//! kit's tool hooks (RFC §9.3).

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use roomkit_core::model::{
    ChannelId, ChannelOutput, ChannelType, DirectEvent, EventDirection, EventSource, Metadata,
    RoomContext, RoomEvent, RoomId, ToolCall, ToolCallOutcome,
};
use roomkit_core::rules::hook_registry::{HookSpec, HookTrigger};
use serde_json::Value;

use crate::RoomKit;
use crate::channel::BoxFuture;
use crate::context::ExecutionContext;
use crate::error::KitError;
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::stream::ResponseStream;
use crate::tools::{ToolCallEvent, ToolHandler, ToolUse, ToolVerdict, error_envelope};

tokio::task_local! {
    static TURN: TurnHost;
}

/// The turn of an intelligence channel the current task runs within, while
/// its `on_event()` runs; `None` anywhere else (RFC §21.4
/// `current_turn_host()`).
///
/// The kit sets it around every intelligence turn, so a tool the turn calls
/// reads it at any depth, without a kit clone of its own. A task the turn
/// spawns reads it when its future is wrapped in
/// [`keep_context`](crate::keep_context).
///
/// ```
/// // Outside every turn there is none.
/// assert!(roomkit::current_turn().is_none());
/// ```
pub fn current_turn() -> Option<TurnHost> {
    TURN.try_with(Clone::clone).ok()
}

/// The turn's host (RFC §10.6.5 `TurnHost`): the turn's room and agent,
/// its one verb, `send_event`, and the tool hooks the agent's tool loop
/// runs on each call (RFC §9.3). Cloning it is cheap; a clone kept past the
/// turn writes as any call does, at the depth of its own event.
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
}

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
    /// The turn's room.
    pub fn room_id(&self) -> &RoomId {
        &self.shared.room_id
    }

    /// The agent whose turn it is.
    pub fn channel_id(&self) -> &ChannelId {
        &self.shared.channel_id
    }

    /// The room as the turn received it (RFC §21.4 `current_tool_room()`).
    pub fn context(&self) -> &RoomContext {
        &self.shared.context
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

    /// Injects `event` into the turn's room, as
    /// [`RoomKit::send_event`](crate::RoomKit::send_event) does, at least as
    /// deep as the turn's response while the turn runs (RFC §10.5, §8.3).
    ///
    /// # Errors
    ///
    /// As `send_event`; [`KitError::ShutDown`] once the kit is gone.
    pub async fn send_event(&self, event: DirectEvent) -> Result<RoomEvent, KitError> {
        self.shared
            .kit
            .send_event(self.shared.room_id.clone(), event)
            .await
    }

    /// Runs the kit's `BEFORE_TOOL_USE` hooks on `call`, in chain order
    /// (RFC §9.3): each reads the arguments the one before left. A block
    /// refuses the call with its reason; a hook that raises or runs past its
    /// timeout refuses it with the fixed refusal, the trigger failing closed,
    /// its name and error going to the observers alone.
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
        ToolGate::Allowed(arguments)
    }

    /// Runs the kit's SYNC `ON_TOOL_CALL` hooks on `event`, a served call,
    /// in chain order (RFC §9.3): each reads the result as the one before
    /// left it, may replace it, or withhold it, which fails the call
    /// `blocked`. A hook that raises or runs past its timeout is skipped. A
    /// call that was not served is returned as it is: a firing that cannot
    /// serve it reaches the observers only.
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
                    let withheld = Value::String(error_envelope(&reason));
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
    pub fn report_tool_call(&self, event: &ToolCallEvent) {
        let execution = ExecutionContext::current();
        let floor = execution.turn_depth().unwrap_or(0);
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
            if !self.shared.kit.spawn(Box::pin(task)) {
                tracing::debug!(target: "roomkit::hooks", "the kit is shut down: a tool call's observer is not run");
            }
        }
    }

    /// Runs `future` as this turn's work: [`current_turn`] gives this host
    /// while it runs.
    pub(crate) async fn scope<F: Future>(self, future: F) -> F::Output {
        TURN.scope(self, future).await
    }

    /// The kit's hooks on `trigger` for this agent's calls, its SYNC chain
    /// in chain order, then its observers.
    fn hooks(&self, trigger: HookTrigger) -> Vec<(HookSpec, ToolHandler)> {
        let source = EventSource::new(
            self.shared.channel_id.clone(),
            self.shared.channel_type.clone(),
            EventDirection::Outbound,
        );
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

/// What a turn's host needs of its kit, whatever its store: the one verb,
/// the tool hooks, and a background task.
pub(crate) trait TurnKit: Send + Sync {
    /// Injects `event` into `room_id`.
    fn send_event(
        &self,
        room_id: RoomId,
        event: DirectEvent,
    ) -> BoxFuture<'static, Result<RoomEvent, KitError>>;

    /// The tool hooks of `trigger` that fire for calls of `source`: its
    /// SYNC chain in chain order, then its observers.
    fn tool_hooks(
        &self,
        trigger: HookTrigger,
        source: &EventSource,
    ) -> Vec<(HookSpec, ToolHandler)>;

    /// Runs `task` in the background, tracked; `false` once the kit is
    /// shut down.
    fn spawn(&self, task: BoxFuture<'static, ()>) -> bool;
}

impl<S: ConversationStore> TurnKit for Weak<Inner<S>> {
    fn send_event(
        &self,
        room_id: RoomId,
        event: DirectEvent,
    ) -> BoxFuture<'static, Result<RoomEvent, KitError>> {
        let kit = self.upgrade().map(|inner| RoomKit { inner });
        Box::pin(async move {
            let kit = kit.ok_or(KitError::ShutDown)?;
            kit.send_event(&room_id, event).await
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

    fn spawn(&self, task: BoxFuture<'static, ()>) -> bool {
        self.upgrade()
            .is_some_and(|inner| inner.tasks.spawn(task).is_some())
    }
}

/// The host of the turn the intelligence channel `channel_id` takes in
/// `context`'s room, for the kit `inner`.
pub(crate) fn host<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    channel_id: ChannelId,
    channel_type: ChannelType,
    context: RoomContext,
) -> TurnHost {
    TurnHost {
        shared: Arc::new(Shared {
            room_id: context.room.id.clone(),
            channel_id,
            channel_type,
            context,
            kit: Arc::new(Arc::downgrade(inner)),
            stream: Mutex::new(None),
        }),
    }
}

/// `future` within the turn the current task runs within, if any: for a
/// task the turn spawns (RFC §21.4).
pub(crate) fn within_current_turn<F>(future: F) -> impl Future<Output = F::Output> + Send + 'static
where
    F: Future + Send + 'static,
{
    let turn = current_turn();
    async move {
        match turn {
            Some(turn) => TURN.scope(turn, future).await,
            None => future.await,
        }
    }
}
