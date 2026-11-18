//! Results, failures and stream items carried by a running turn.
use roomkit_core::model::{
    Metadata, Observation, RoomEvent, Task, ToolCallOutcome, ToolCallStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// An error as it crosses (RFC §10.6.8): its kind, its message, what
/// classifies it, and its causes in order.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ErrorRecord {
    /// Error class as recorded by the sender.
    pub kind: String,
    /// Human-readable message recorded by the sender.
    pub message: String,
    /// JSON representations of the arguments supplied to the error.
    #[serde(default)]
    pub args: Vec<Value>,
    /// Named error attributes that can be represented as JSON.
    #[serde(default)]
    pub attributes: Metadata,
    /// Underlying errors in causal order.
    #[serde(default)]
    pub causes: Vec<ErrorRecord>,
    /// Formatted traceback captured by the process that raised the error.
    #[serde(default)]
    pub traceback: Option<String>,
    /// Ancestor error classes, nearest first.
    #[serde(default)]
    pub bases: Vec<String>,
}

impl ErrorRecord {
    /// Record a kind and message, leaving arguments, attributes, bases and causes empty.
    pub fn new(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
            args: Vec::new(),
            attributes: Metadata::new(),
            causes: Vec::new(),
            traceback: None,
            bases: Vec::new(),
        }
    }
}

/// A channel's output without its stream (RFC §10.6.3 `output`).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct WireOutput {
    /// Whether the channel answered the event.
    pub responded: bool,
    /// Events the owner is to publish as the channel’s response.
    pub response_events: Vec<RoomEvent>,
    /// Metadata returned alongside the channel response.
    pub response_metadata: Metadata,
    /// Tasks produced by the channel.
    pub tasks: Vec<Task>,
    /// Observations produced by the channel.
    pub observations: Vec<Observation>,
    /// Room metadata updates produced by the channel.
    pub metadata_updates: Metadata,
    /// Error reported by the sender, when the operation failed.
    pub error: Option<ErrorRecord>,
}

/// A call's result as a tool-call end carries it: a JSON value, or the
/// content parts the Python implementation's tools may answer with.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WireResult {
    /// The tool returned an ordered list of content parts.
    Parts {
        /// Structured content parts returned by the tool.
        parts: Vec<Value>,
    },
    /// The tool returned a JSON value.
    Value {
        /// Value returned by the operation.
        value: Value,
    },
}

impl From<Value> for WireResult {
    fn from(value: Value) -> Self {
        Self::Value { value }
    }
}

/// A call's end, as `tool_ended` carries it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolEnd {
    /// Name of the tool being called.
    pub tool_name: String,
    /// Identifier of this tool call.
    pub tool_id: String,
    /// Arguments supplied to the call.
    #[serde(default)]
    pub arguments: Metadata,
    /// Result returned by the call.
    pub result: WireResult,
    /// Status at completion of the tool call.
    pub status: ToolCallStatus,
    /// Elapsed duration of the call in milliseconds.
    #[serde(default)]
    pub duration_ms: u64,
    /// Error reported by the sender, when the operation failed.
    #[serde(default)]
    pub error: Option<String>,
    /// Structured result returned beside the text.
    #[serde(default)]
    pub structured_content: Option<Metadata>,
    /// Outcome of the operation as recorded by the sender.
    #[serde(default)]
    pub outcome: Option<ToolCallOutcome>,
    /// Whether execution took place despite a refusal.
    #[serde(default)]
    pub refused_but_ran: bool,
    /// Purpose attached to this tool call.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<String>,
}

/// One item of a turn's stream (RFC §10.6.3 `item`), named by its `kind`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Item {
    /// Text, or the text of deltas the channel yielded together, joined,
    /// with their number when more than one.
    Text {
        /// Text yielded or read by the channel.
        text: String,
        /// Number of coalesced text deltas, when more than one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deltas: Option<u32>,
    },
    /// A reasoning delta.
    Thinking {
        /// Reasoning delta yielded by the model.
        thinking: String,
    },
    /// A call's start.
    ToolStart {
        /// Name of the tool being called.
        tool_name: String,
        /// Identifier of this tool call.
        tool_id: String,
        /// Arguments supplied to the call.
        #[serde(default)]
        arguments: Metadata,
        /// Arguments the tool handler actually received.
        #[serde(default)]
        ran_with: Option<Metadata>,
        /// Purpose attached to this tool call.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        intent: Option<String>,
    },
    /// A call's end, naming the frame of its start.
    ToolEnd {
        /// Sequence number of the corresponding tool start.
        #[serde(default)]
        start: Option<u64>,
        /// Name of the tool being called.
        tool_name: String,
        /// Identifier of this tool call.
        tool_id: String,
        /// Arguments supplied to the call.
        #[serde(default)]
        arguments: Metadata,
        /// Result returned by the call.
        result: WireResult,
        /// Status at completion of the tool call.
        status: ToolCallStatus,
        /// Elapsed duration of the call in milliseconds.
        #[serde(default)]
        duration_ms: u64,
        /// Error reported by the sender, when the operation failed.
        #[serde(default)]
        error: Option<String>,
        /// Structured result returned beside the text.
        #[serde(default)]
        structured_content: Option<Metadata>,
        /// Outcome of the operation as recorded by the sender.
        #[serde(default)]
        outcome: Option<ToolCallOutcome>,
        /// Whether execution took place despite a refusal.
        #[serde(default)]
        refused_but_ran: bool,
        /// Purpose attached to this tool call.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        intent: Option<String>,
    },
    /// The loop goes on after a round that ended without a call.
    SegmentBreak,
    /// What a generation used, told after it, or before its end when
    /// `partial` (RFC §6.4).
    Usage {
        /// Usage reported by the model.
        #[serde(default)]
        usage: Metadata,
        /// Model associated with the usage.
        #[serde(default)]
        model: Option<String>,
        /// Provider associated with the usage.
        #[serde(default)]
        provider: Option<String>,
        /// Written only when true, as Python's codec writes it: a frame of
        /// a generation's own usage is as it was (RFC §6.4).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        partial: bool,
    },
    /// The loop's end: why, its rounds, its usage, the limits it ran under.
    LoopEnd(WireLoopEnd),
    /// The stream's end.
    StreamEnd,
    /// The application's instruction the turn read, which the owner keeps
    /// as the agent's own row (RFC §10.1.1), with its runtime record when
    /// the runtime wrote it, written only then (RFC §6.4): Python's
    /// `instruction_read`.
    InstructionRead {
        /// Text yielded or read by the channel.
        text: String,
        /// Runtime record stored alongside the instruction.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        runtime_record: Option<String>,
    },
}

/// What a loop's end crosses the wire with: Python's `LoopEndMarker` fields.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WireLoopEnd {
    /// Reason reported by the sender.
    pub reason: String,
    /// Number of rounds completed by the loop.
    #[serde(default)]
    pub rounds: u64,
    /// Usage reported by the model.
    #[serde(default)]
    pub usage: Metadata,
    /// Round limit in effect during this loop.
    #[serde(default)]
    pub max_rounds: Option<u64>,
    /// Time limit in effect, in seconds.
    #[serde(default)]
    pub timeout_seconds: Option<f64>,
    /// Token limit in effect for this loop.
    #[serde(default)]
    pub budget_tokens: Option<u64>,
    /// Cost limit in US dollars for this loop.
    #[serde(default)]
    pub budget_usd: Option<f64>,
    /// Usage reported by the last round.
    #[serde(default)]
    pub last_usage: Option<Metadata>,
    /// Whether usage from every round is available.
    #[serde(default)]
    pub usage_complete: Option<bool>,
    /// Model associated with the usage.
    #[serde(default)]
    pub model: Option<String>,
    /// Provider associated with the usage.
    #[serde(default)]
    pub provider: Option<String>,
}
