//! The numbered messages of the executor and owner.
use super::{ErrorRecord, InputRequest, Item, ToolEnd, WireOutput};
use crate::steering::SteeringDirective;
use roomkit_core::model::Metadata;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// What a call's handler reads of the turn it serves (RFC §10.6.5, §21.4).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallContext {
    /// Identifier assigned to the call by the model.
    pub tool_call_id: String,
    /// Room in which the work takes place.
    pub room_id: String,
    /// Channel whose work the message carries.
    pub channel_id: String,
    /// Depth of the response chain in the room.
    #[serde(default)]
    pub chain_depth: u32,
    /// Actor who initiated the call.
    pub actor_id: Option<String>,
    /// Tool names the actor is allowed to invoke.
    pub allowed_names: Option<Vec<String>>,
    /// Identity on whose behalf the call is made.
    pub requester: Option<String>,
    /// Metadata of the response or running call.
    pub response_metadata: Option<Metadata>,
}

impl CallContext {
    /// Locate a tool call in its room and channel.
    pub fn new(
        tool_call_id: impl Into<String>,
        room_id: impl Into<String>,
        channel_id: impl Into<String>,
    ) -> Self {
        Self {
            tool_call_id: tool_call_id.into(),
            room_id: room_id.into(),
            channel_id: channel_id.into(),
            ..Self::default()
        }
    }
}

/// A worker's call to a tool served by the room owner (RFC §10.6.5).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename = "tool_call")]
pub struct ToolCall {
    /// Identifier used to correlate the answer.
    pub call_id: String,
    /// Name of the requested tool.
    pub name: String,
    /// Arguments supplied to the handler.
    pub arguments: Metadata,
    /// Room, channel, actor and model call identity for this invocation.
    pub context: CallContext,
}

/// Changes measured against the record carried by one owner-served call.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct RecordChanges {
    /// Keys written to the response record, with their values.
    pub written: Metadata,
    /// Keys deleted from the response record.
    pub removed: Vec<String>,
}

/// A frame, executor to owner (RFC §10.6.3), carrying the turn by its
/// envelope.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Frame {
    /// The executor accepted the turn.
    Accepted {
        /// Executor that accepted the work.
        executor_id: String,
    },
    /// One end of the link reports that it is alive.
    Heartbeat,
    /// The channel returned its initial output.
    Output {
        /// Channel output the owner will apply.
        output: WireOutput,
        /// Whether a response stream follows the output.
        #[serde(default)]
        streams: bool,
    },
    /// One item read from the response stream.
    Item {
        /// Next item of the response stream.
        item: Item,
    },
    /// The handler fixed the arguments of a tool that has started.
    ToolArgs {
        /// Sequence number of the corresponding tool start.
        start: u64,
        /// Arguments supplied to the call.
        arguments: Metadata,
    },
    /// The tool call finished.
    ToolEnded {
        /// Sequence number of the corresponding tool start.
        start: u64,
        /// Completion data for the named tool call.
        end: ToolEnd,
    },
    /// Updated response record from the executor.
    Record {
        /// Response record reported by the executor.
        record: Metadata,
    },
    /// The executor reports a failure to the owner.
    Error {
        /// Error reported by the sender, when the operation failed.
        error: ErrorRecord,
    },
    /// Terminal frame of the turn.
    End,
    /// Ask the owner to perform a host verb.
    HostCall {
        /// Identifier used to correlate the call and its answer.
        call_id: String,
        /// Name of the requested operation.
        verb: String,
        /// Arguments supplied to the call.
        arguments: Metadata,
        /// Execution context visible to the owner-served handler.
        context: CallContext,
    },
    /// Stop a pending owner-served call.
    CallCancel {
        /// Identifier used to correlate the call and its answer.
        call_id: String,
    },
    /// A call to a tool the owner serves the turn (RFC §10.6.5): its
    /// handler runs on the owner.
    ToolCall {
        /// Identifier used to correlate the call and its answer.
        call_id: String,
        /// Name of the tool to invoke.
        name: String,
        /// Arguments supplied to the call.
        arguments: Metadata,
        /// Execution context visible to the owner-served handler.
        context: CallContext,
    },
    /// An ephemeral event a turn asks its owner to publish.
    Ephemeral {
        /// Topic on which the owner should publish the event.
        channel: String,
        /// Room event carried by this message.
        event: Value,
    },
    /// Ask the owner to publish a framework event.
    FrameworkEvent {
        /// Type of the framework event.
        event_type: String,
        /// Room in which the work takes place.
        #[serde(default)]
        room_id: Option<String>,
        /// Channel whose work the message carries.
        #[serde(default)]
        channel_id: Option<String>,
        /// Identifier of the associated event, when available.
        #[serde(default)]
        event_id: Option<String>,
        /// Fields of the framework event.
        #[serde(default)]
        data: Metadata,
    },
    /// How many loops a `steer` reached; none for a channel without
    /// steering, whose owner then cuts the turn on a `Cancel` (RFC
    /// §10.6.8).
    SteerResult {
        /// Identifier used to correlate a steering request with its answer.
        steer_id: String,
        /// Number of loops reached, when the channel provides steering.
        #[serde(default)]
        loops: Option<u64>,
    },
    /// A tool of the turn asked a person for input (RFC §10.6.5): sent
    /// before `ON_USER_INPUT_REQUIRED` fires, so the owner can answer it at
    /// once.
    InputRequest {
        /// Human-input request to register with the owner.
        #[serde(flatten, serialize_with = "super::message_fields")]
        request: Box<InputRequest>,
    },
    /// A request settled where its tool waits, by other than the owner's
    /// answer (an answer the handler took, a hook's block, its bound, a
    /// release): `reason` its status.
    InputClosed {
        /// Identifier of the pending human-input request.
        request_id: String,
        /// Reason reported by the sender.
        reason: String,
    },
    /// Whether the owner's `input_answer` settled its request: one settled
    /// first where its tool waits did not take it.
    InputTaken {
        /// Identifier of the pending human-input request.
        request_id: String,
        /// Whether the executor applied the decision.
        taken: bool,
    },
}

/// A control message, owner to executor, or the answer to a call (RFC
/// §10.6.3).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Control {
    /// Grant the executor permission to advance the response stream.
    Pull {
        /// Number of stream items the owner grants permission to read.
        #[serde(default = "one", deserialize_with = "pull_count")]
        count: u32,
        /// Number of stream items already read by the owner, counting each joined text delta.
        #[serde(default)]
        read: u64,
    },
    /// The owner closed the stream.
    Close,
    /// Cut the running turn with the given reason.
    Cancel {
        /// Reason reported by the sender.
        reason: String,
    },
    /// One end of the link reports that it is alive.
    Heartbeat,
    /// Answer an owner-served host verb.
    HostResult {
        /// Identifier used to correlate the call and its answer.
        call_id: String,
        /// Result returned by the call.
        #[serde(default)]
        result: Value,
        /// Error reported by the sender, when the operation failed.
        #[serde(default)]
        error: Option<ErrorRecord>,
        /// Changes made to the response record while serving the call.
        #[serde(default)]
        record_changes: Option<RecordChanges>,
    },
    /// A steering directive for the turn's loop (RFC §21.3), answered by
    /// `steer_result`.
    Steer {
        /// Identifier used to correlate a steering request with its answer.
        steer_id: String,
        /// Instruction to apply to the running turn.
        directive: SteeringDirective,
    },
    /// The owner's answer to one `tool_call`: `served` with the handler's
    /// answer as the model reads it, else its error.
    ToolResult {
        /// Identifier used to correlate the call and its answer.
        call_id: String,
        /// Outcome of the operation as recorded by the sender.
        outcome: ToolOutcome,
        /// Result returned by the call.
        #[serde(default)]
        result: Option<Value>,
        /// Error reported by the sender, when the operation failed.
        #[serde(default)]
        error: Option<ErrorRecord>,
        /// Structured result returned beside the text.
        #[serde(default)]
        structured_content: Option<Metadata>,
        /// Changes made to the response record while serving the call.
        #[serde(default)]
        record_changes: Option<RecordChanges>,
    },
    /// The outcome the owner recorded for an `input_request` (RFC
    /// §10.6.5): answered with its `value`, or rejected with its `reason`.
    InputAnswer {
        /// Identifier of the pending human-input request.
        request_id: String,
        /// Outcome of the operation as recorded by the sender.
        outcome: InputOutcome,
        /// Value returned by the operation.
        #[serde(default)]
        value: Option<String>,
        /// Reason reported by the sender.
        #[serde(default)]
        reason: Option<String>,
    },
}

/// The outcome an `input_answer` carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputOutcome {
    /// Answered with a value.
    Answered,
    /// Rejected with a reason.
    Rejected,
}

/// How the owner's handler took a `tool_call` (RFC §10.6.5, §21.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcome {
    /// It answered.
    Served,
    /// It refused the call: nothing ran.
    Refused,
    /// It ran and failed.
    Failed,
    /// Nothing on the owner serves it.
    Unserved,
}

fn one() -> u32 {
    1
}

fn pull_count<'de, D: serde::Deserializer<'de>>(input: D) -> Result<u32, D::Error> {
    std::num::NonZeroU32::deserialize(input).map(std::num::NonZeroU32::get)
}
