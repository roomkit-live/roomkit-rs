//! The tool hooks' payload (RFC §9.3): `BEFORE_TOOL_USE`, which gates each
//! call an agent's tool loop makes, and `ON_TOOL_CALL`, which reports each
//! call's outcome, register on the kit as every hook does
//! ([`RoomKit::hook`](crate::RoomKit::hook)), their payload a
//! [`ToolCallEvent`]; an agent runs them through its turn
//! ([`TurnHost`](crate::TurnHost)).

use roomkit_core::model::{
    ChannelId, ChannelType, Metadata, RoomId, Timestamp, ToolCall, ToolCallOutcome,
};
use roomkit_core::ops::{Environment, SystemEnvironment};
use serde_json::Value;
use std::fmt::Write as _;

/// A tool call as its hooks read it (RFC §9.3), Python's
/// `ToolCallEvent(channel_id, channel_type, tool_call_id, name, arguments,
/// result=None, room_id=None, session=None, timestamp=<factory>,
/// is_error=False, cancelled=False, structured_content=None,
/// error_detail=None, refused=False, refused_but_ran=False, intent=None)`.
///
/// `BEFORE_TOOL_USE` reads a call that has not run: no result. A hook
/// returns `HookResult::modify(event)` to run the call with the event's
/// arguments, or `metadata["arguments"]` to rewrite them for the call
/// alone. `ON_TOOL_CALL` reads every call once it ended, its result as the
/// model reads it: `None` for a call nothing served, which a SYNC hook may
/// serve.
#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Python's ToolCallEvent carries its four markers as booleans"
)]
pub struct ToolCallEvent {
    /// The agent whose turn makes the call.
    pub channel_id: ChannelId,
    /// Its type.
    pub channel_type: ChannelType,
    /// The call's id.
    pub tool_call_id: String,
    /// The tool's name.
    pub name: String,
    /// Its arguments, a mapping (RFC §6.4).
    pub arguments: Metadata,
    /// What the model reads of the call: a text as a JSON string, any other
    /// value as itself; `None` before it ran, and for a call nothing served.
    pub result: Option<Value>,
    /// The turn's room.
    pub room_id: Option<RoomId>,
    /// When the event was made.
    pub timestamp: Timestamp,
    /// The call ended other than served: refused, failed, blocked, served
    /// by nothing or cancelled.
    pub is_error: bool,
    /// The call was cut before its outcome.
    pub cancelled: bool,
    /// The structured copy a surface renders of its result, when it has
    /// one.
    pub structured_content: Option<Value>,
    /// What failed, for the observers alone: a raised error's kind and
    /// message, a failing gate's name and error. Never the model's to read.
    pub error_detail: Option<String>,
    /// The call was refused before it ran.
    pub refused: bool,
    /// An agent that runs its own tools ran it although it was refused:
    /// always `false` here, the agents of this crate never do.
    pub refused_but_ran: bool,
    /// The sentence the model wrote for the person with the call (RFC §6.4,
    /// `tool_call_intents`).
    pub intent: Option<String>,
}

impl ToolCallEvent {
    /// The call `tool_call_id` of the tool `name` with `arguments`, by the
    /// agent `channel_id` of `channel_type`, not run yet; the other fields
    /// at Python's defaults, the timestamp the system clock's.
    pub fn new(
        channel_id: ChannelId,
        channel_type: ChannelType,
        tool_call_id: impl Into<String>,
        name: impl Into<String>,
        arguments: Metadata,
    ) -> Self {
        Self {
            channel_id,
            channel_type,
            tool_call_id: tool_call_id.into(),
            name: name.into(),
            arguments,
            result: None,
            room_id: None,
            timestamp: SystemEnvironment.now(),
            is_error: false,
            cancelled: false,
            structured_content: None,
            error_detail: None,
            refused: false,
            refused_but_ran: false,
            intent: None,
        }
    }
}

/// A call of an agent's tool loop as the loop and its turn's host hand it
/// to each other (RFC §9.3): the call with the arguments it runs with, and,
/// once it ended, its outcome, the result as the model reads it and what
/// failed. The hooks read it as a [`ToolCallEvent`].
///
/// For roomkit-ai's tool loop, which reads its outcome: outside the public
/// API.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub struct ToolCallReport {
    /// The call.
    pub call: ToolCall,
    /// How it ended; `None` before it runs.
    pub outcome: Option<ToolCallOutcome>,
    /// What the model reads; `null` before the call ends.
    pub result: Value,
    /// What failed, for the observers alone.
    pub error_detail: Option<String>,
    /// The structured copy of its result.
    pub structured_content: Option<Value>,
    /// An external agent ran the call despite a rejected permission (RFC §9.3).
    pub refused_but_ran: bool,
}

impl ToolCallReport {
    /// `call`, not run yet.
    pub fn new(call: ToolCall) -> Self {
        Self {
            call,
            outcome: None,
            result: Value::Null,
            error_detail: None,
            structured_content: None,
            refused_but_ran: false,
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

    /// The call as its observers read it, by the agent `channel_id` of
    /// `channel_type` in `room_id`, made at `timestamp`: no result before
    /// it ran.
    pub(crate) fn event(
        &self,
        channel_id: &ChannelId,
        channel_type: &ChannelType,
        room_id: &RoomId,
        timestamp: Timestamp,
    ) -> ToolCallEvent {
        let outcome = self.outcome;
        ToolCallEvent {
            channel_id: channel_id.clone(),
            channel_type: channel_type.clone(),
            tool_call_id: self.call.id.clone(),
            name: self.call.name.clone(),
            arguments: self.call.arguments.clone(),
            result: outcome.is_some().then(|| self.result.clone()),
            room_id: Some(room_id.clone()),
            timestamp,
            is_error: outcome.is_some_and(|outcome| outcome != ToolCallOutcome::Served),
            cancelled: outcome == Some(ToolCallOutcome::Cancelled),
            structured_content: self.structured_content.clone(),
            error_detail: self.error_detail.clone(),
            refused: outcome == Some(ToolCallOutcome::Refused),
            refused_but_ran: self.refused_but_ran,
            intent: self.call.intent.clone(),
        }
    }
}

/// `reason` as the model reads a refusal or a withheld result: `{"error":
/// "<reason>"}` (RFC §9.3), written as Python's `json.dumps` writes it,
/// every character past ASCII as `\uXXXX`.
pub(crate) fn error_envelope(reason: &str) -> String {
    let quoted = serde_json::to_string(reason).unwrap_or_else(|_| "\"\"".to_owned());
    let mut escaped = String::with_capacity(quoted.len());
    for character in quoted.chars() {
        if character.is_ascii() {
            escaped.push(character);
            continue;
        }
        let mut units = [0_u16; 2];
        for unit in character.encode_utf16(&mut units) {
            let _infallible = write!(escaped, "\\u{unit:04x}");
        }
    }
    format!("{{\"error\": {escaped}}}")
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
        // As Python's json.dumps: past ASCII escaped (RMK-1096).
        assert_eq!(
            error_envelope("Refusé — 🙅"),
            r#"{"error": "Refus\u00e9 \u2014 \ud83d\ude45"}"#
        );
    }

    /// Python's four markers follow the call's outcome; a call that has
    /// not run has no result.
    #[test]
    fn a_call_s_markers_follow_its_outcome() {
        let report = ToolCallReport::new(ToolCall::new("call_1", "lookup", Metadata::new()));
        let (agent, room) = (
            ChannelId::new("ai").unwrap(),
            RoomId::new("room_1").unwrap(),
        );
        let read = |report: ToolCallReport| {
            let event = report.event(&agent, &ChannelType::AI, &room, Timestamp::UNIX_EPOCH);
            (
                event.is_error,
                event.refused,
                event.cancelled,
                event.result.is_some(),
            )
        };
        let ended = |outcome| report.clone().ended(outcome, Value::from("x"));
        assert_eq!(
            [
                read(report.clone()),
                read(ended(ToolCallOutcome::Served)),
                read(ended(ToolCallOutcome::Refused)),
                read(ended(ToolCallOutcome::Blocked)),
                read(ended(ToolCallOutcome::Unserved)),
                read(ended(ToolCallOutcome::Cancelled)),
            ],
            [
                (false, false, false, false),
                (false, false, false, true),
                (true, true, false, true),
                (true, false, false, true),
                (true, false, false, true),
                (true, false, true, true),
            ]
        );
    }
}
