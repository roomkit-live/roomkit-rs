//! The tool hooks (RFC §9.3): `BEFORE_TOOL_USE`, which gates each call an
//! agent's tool loop makes, and `ON_TOOL_CALL`, which reports each call's
//! outcome. Their payload is a tool call, no event, so they register on the
//! kit through typed registrations ([`RoomKit::before_tool_use`],
//! [`RoomKit::on_tool_call`], [`RoomKit::observe_tool_calls`]), and an
//! agent runs them through its turn ([`TurnHost`](crate::TurnHost)).
//!
//! [`RoomKit::before_tool_use`]: crate::RoomKit::before_tool_use
//! [`RoomKit::on_tool_call`]: crate::RoomKit::on_tool_call
//! [`RoomKit::observe_tool_calls`]: crate::RoomKit::observe_tool_calls

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{ChannelId, Metadata, RoomContext, RoomId, ToolCall, ToolCallOutcome};
use roomkit_core::rules::hook_registry::{HookExecution, HookSpec, HookTrigger};
use serde_json::Value;

use crate::channel::BoxFuture;
use crate::hooks::HookError;

/// A tool call as its hooks read it (RFC §9.3): the agent that makes it, its
/// room, the call with the arguments it runs with (or those the gate had
/// when it stopped it), and, once it ended, its outcome, the result as the
/// model reads it and what failed.
///
/// `BEFORE_TOOL_USE` reads a call that has not run: no outcome, a `null`
/// result. `ON_TOOL_CALL` reads every call once it ended.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct ToolCallEvent {
    /// The agent whose turn makes the call.
    pub channel_id: ChannelId,
    /// The turn's room.
    pub room_id: RoomId,
    /// The call.
    pub call: ToolCall,
    /// How the call ended; `None` before it runs.
    pub outcome: Option<ToolCallOutcome>,
    /// The result as the model reads it: a text as a JSON string, any other
    /// value as itself; `null` before the call ends.
    pub result: Value,
    /// What failed, for the observers alone: a raised error's kind and
    /// message, a failing gate's name and error (RFC §9.3). Never the
    /// model's to read.
    pub error_detail: Option<String>,
}

impl ToolCallEvent {
    /// The call `call` of the agent `channel_id` in `room_id`, not run yet.
    pub fn new(channel_id: ChannelId, room_id: RoomId, call: ToolCall) -> Self {
        Self {
            channel_id,
            room_id,
            call,
            outcome: None,
            result: Value::Null,
            error_detail: None,
        }
    }

    /// The same call, ended with `outcome`, the model reading `result`.
    #[must_use]
    pub fn ended(mut self, outcome: ToolCallOutcome, result: Value) -> Self {
        self.outcome = Some(outcome);
        self.result = result;
        self
    }

    /// The same call, with `detail` for its observers.
    #[must_use]
    pub fn with_error_detail(mut self, detail: impl Into<String>) -> Self {
        self.error_detail = Some(detail.into());
        self
    }

    /// Whether the call ended other than served: refused, failed, blocked,
    /// served by nothing or cancelled (RFC §9.3).
    pub fn is_error(&self) -> bool {
        self.outcome
            .is_some_and(|outcome| outcome != ToolCallOutcome::Served)
    }

    /// Whether the call was refused before it ran.
    pub fn refused(&self) -> bool {
        self.outcome == Some(ToolCallOutcome::Refused)
    }

    /// Whether the call was cut before its outcome.
    pub fn cancelled(&self) -> bool {
        self.outcome == Some(ToolCallOutcome::Cancelled)
    }
}

/// What a `BEFORE_TOOL_USE` hook decides on a call (RFC §9.3, §9.4).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ToolUse {
    /// The call goes on.
    Allow,
    /// The call goes on with these arguments, which the agent checks again
    /// against the tool's schema (RFC §21.1).
    Rewrite(Metadata),
    /// The call is refused before it runs. The model reads the reason, the
    /// hook's own words, as `{"error": "<reason>"}`; with none, the fixed
    /// refusal `Tool 'x' denied by pre-execution hook.`.
    Block(Option<String>),
}

impl ToolUse {
    /// Refuses the call, the model reading `reason`.
    pub fn block(reason: impl Into<String>) -> Self {
        Self::Block(Some(reason.into()))
    }
}

/// What a SYNC `ON_TOOL_CALL` hook decides on a served call's result (RFC
/// §9.3).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum ToolVerdict {
    /// The result stands, as the hooks before left it.
    Keep,
    /// The model reads this value instead, a text as it is and any other
    /// value as its JSON; the next hook reads it too.
    Replace(Value),
    /// The result is withheld: the call fails, `blocked`, and the model
    /// reads the reason, the hook's own words, as `{"error": "<reason>"}`;
    /// with none, or a blank one, `{"error": "blocked"}`.
    Block(Option<String>),
}

impl ToolVerdict {
    /// Withholds the result, the model reading `reason`.
    pub fn block(reason: impl Into<String>) -> Self {
        Self::Block(Some(reason.into()))
    }
}

/// How a tool hook registers: its name, unique among the kit's hooks, its
/// place in its chain (lower first), and the longest it may run (RFC
/// §9.1). A name alone gives the RFC's defaults: priority 0, 30 seconds.
///
/// ```
/// use std::time::Duration;
/// use roomkit::ToolHookSpec;
///
/// // An approval that waits for a person needs more than 30 seconds.
/// let spec = ToolHookSpec::new("approve").with_timeout(Duration::from_secs(300));
/// assert_eq!(spec.timeout, Duration::from_secs(300));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ToolHookSpec {
    /// The hook's name.
    pub name: String,
    /// Its place in the chain: lower first.
    pub priority: i32,
    /// The longest it may run. A `BEFORE_TOOL_USE` hook past it refuses the
    /// call (RFC §9.3); any other is skipped.
    pub timeout: Duration,
}

impl ToolHookSpec {
    /// A hook named `name`, with priority 0 and a 30-second timeout.
    pub fn new(name: impl Into<String>) -> Self {
        let defaults = HookSpec::new("", HookTrigger::OnToolCall, HookExecution::Sync);
        Self {
            name: name.into(),
            priority: defaults.priority,
            timeout: defaults.timeout,
        }
    }

    /// The same hook, at `priority` in its chain.
    #[must_use]
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// The same hook, bounded by `timeout`.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// The registration on `trigger`, run as `execution`.
    pub(crate) fn on(self, trigger: HookTrigger, execution: HookExecution) -> HookSpec {
        let mut spec = HookSpec::new(self.name, trigger, execution);
        spec.priority = self.priority;
        spec.timeout = self.timeout;
        spec
    }
}

impl From<&str> for ToolHookSpec {
    fn from(name: &str) -> Self {
        Self::new(name)
    }
}

impl From<String> for ToolHookSpec {
    fn from(name: String) -> Self {
        Self::new(name)
    }
}

/// A `BEFORE_TOOL_USE` hook's handler, boxed.
pub(crate) type GateHandler = dyn Fn(ToolCallEvent, RoomContext) -> BoxFuture<'static, Result<ToolUse, HookError>>
    + Send
    + Sync;

/// A SYNC `ON_TOOL_CALL` hook's handler, boxed.
pub(crate) type VerdictHandler = dyn Fn(ToolCallEvent, RoomContext) -> BoxFuture<'static, Result<ToolVerdict, HookError>>
    + Send
    + Sync;

/// An ASYNC `ON_TOOL_CALL` hook's handler, boxed.
pub(crate) type ObserverHandler =
    dyn Fn(ToolCallEvent, RoomContext) -> BoxFuture<'static, ()> + Send + Sync;

/// The handler of a tool hook, by what it decides.
#[derive(Clone)]
pub(crate) enum ToolHandler {
    /// A `BEFORE_TOOL_USE` hook.
    Gate(Arc<GateHandler>),
    /// A SYNC `ON_TOOL_CALL` hook.
    Verdict(Arc<VerdictHandler>),
    /// An ASYNC `ON_TOOL_CALL` hook.
    Observer(Arc<ObserverHandler>),
}

impl std::fmt::Debug for ToolHandler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            Self::Gate(_) => "Gate",
            Self::Verdict(_) => "Verdict",
            Self::Observer(_) => "Observer",
        };
        formatter.debug_tuple(kind).finish_non_exhaustive()
    }
}

impl ToolHandler {
    /// `handler` as a `BEFORE_TOOL_USE` hook's.
    pub(crate) fn gate<F, Fut>(handler: F) -> Self
    where
        F: Fn(ToolCallEvent, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolUse, HookError>> + Send + 'static,
    {
        Self::Gate(Arc::new(move |event, context| {
            Box::pin(handler(event, context))
        }))
    }

    /// `handler` as a SYNC `ON_TOOL_CALL` hook's.
    pub(crate) fn verdict<F, Fut>(handler: F) -> Self
    where
        F: Fn(ToolCallEvent, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolVerdict, HookError>> + Send + 'static,
    {
        Self::Verdict(Arc::new(move |event, context| {
            Box::pin(handler(event, context))
        }))
    }

    /// `handler` as an ASYNC `ON_TOOL_CALL` hook's.
    pub(crate) fn observer<F, Fut>(handler: F) -> Self
    where
        F: Fn(ToolCallEvent, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        Self::Observer(Arc::new(move |event, context| {
            Box::pin(handler(event, context))
        }))
    }
}

/// `reason` as the model reads a refusal or a withheld result: `{"error":
/// "<reason>"}` (RFC §9.3).
pub(crate) fn error_envelope(reason: &str) -> String {
    let quoted = serde_json::to_string(reason).unwrap_or_else(|_| "\"\"".to_owned());
    format!("{{\"error\": {quoted}}}")
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn a_refusal_reads_as_an_error_envelope() {
        assert_eq!(
            error_envelope("Needs a manager's \"approval\""),
            r#"{"error": "Needs a manager's \"approval\""}"#
        );
    }

    #[test]
    fn a_call_s_markers_follow_its_outcome() {
        let call = ToolCall::new("call_1", "lookup", Metadata::new());
        let event = ToolCallEvent::new(
            ChannelId::new("ai").unwrap(),
            RoomId::new("room_1").unwrap(),
            call,
        );
        let markers =
            |event: &ToolCallEvent| (event.is_error(), event.refused(), event.cancelled());
        assert_eq!(
            [
                markers(&event),
                markers(&event.clone().ended(ToolCallOutcome::Served, Value::Null)),
                markers(&event.clone().ended(ToolCallOutcome::Refused, Value::Null)),
                markers(&event.clone().ended(ToolCallOutcome::Blocked, Value::Null)),
                markers(&event.ended(ToolCallOutcome::Cancelled, Value::Null)),
            ],
            [
                (false, false, false),
                (false, false, false),
                (true, true, false),
                (true, false, false),
                (true, false, true),
            ]
        );
    }
}
