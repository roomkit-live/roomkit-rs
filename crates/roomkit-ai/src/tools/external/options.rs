//! Named keyword arguments of the external handler and its inherited helpers.

use roomkit::model::RoomId;

/// Python `_fire_before_hook` keyword arguments, at their defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FireBeforeHookOptions {
    /// Provider-assigned id, empty by default.
    pub tool_call_id: String,
    /// The room whose hooks run, or none outside a room.
    pub room_id: Option<RoomId>,
}

/// Python `process_tool_call` keyword arguments, at their defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcessToolCallOptions {
    /// Provider-assigned id, empty by default.
    pub tool_call_id: String,
    /// Optional host job identifier.
    pub job_id: Option<String>,
    /// Optional provider session identifier.
    pub session_id: Option<String>,
    /// Optional tenant identifier.
    pub tenant_id: Option<String>,
    /// The room whose hooks run, or none outside a room.
    pub room_id: Option<RoomId>,
}

/// Python `on_tool_result` keyword arguments, at their defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OnToolResultOptions {
    /// The provider's own failure flag, never inferred from result text.
    pub is_error: bool,
    /// Provider-assigned id, empty by default.
    pub tool_call_id: String,
    /// Optional host job identifier.
    pub job_id: Option<String>,
    /// The room whose hooks run.
    pub room_id: Option<RoomId>,
    /// The provider ran the call despite a rejection.
    pub refused_but_ran: bool,
    /// What failed while rejecting that call, for observers alone.
    pub error_detail: Option<String>,
}

/// Python `on_tool_cancelled` keyword arguments, at their defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OnToolCancelledOptions {
    /// Provider-assigned id, empty by default.
    pub tool_call_id: String,
    /// Optional host job identifier.
    pub job_id: Option<String>,
    /// The room whose hooks run.
    pub room_id: Option<RoomId>,
}

/// Python `on_tool_refused` keyword arguments, at their defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OnToolRefusedOptions {
    /// Provider-assigned id, empty by default.
    pub tool_call_id: String,
    /// Optional host job identifier.
    pub job_id: Option<String>,
    /// The room whose hooks run.
    pub room_id: Option<RoomId>,
    /// What failed in this refusal, for observers alone.
    pub detail: Option<String>,
}

/// The reporting helper's flags, as Python's `_fire_on_tool_hook` takes them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "the inherited report helper preserves Python's independent outcome flags"
)]
pub struct FireOnToolHookOptions {
    /// The provider reported an error.
    pub is_error: bool,
    /// The turn cut the call before its result.
    pub cancelled: bool,
    /// The handler refused the call before execution.
    pub refused: bool,
    /// What failed, for observers alone.
    pub error_detail: Option<String>,
    /// Provider-assigned id, empty by default.
    pub tool_call_id: String,
    /// The room whose hooks run.
    pub room_id: Option<RoomId>,
    /// The provider ran the call despite a rejection.
    pub refused_but_ran: bool,
}

/// Python `PolicyExternalToolHandler(policy=None)` keyword arguments.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PolicyExternalToolHandlerOptions {
    /// The admission policy, absent by default.
    pub policy: Option<super::ToolPolicy>,
}
