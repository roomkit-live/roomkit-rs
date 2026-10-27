//! What a turn's objects become on the wire, and back (RFC §10.6.3): a
//! channel's output, a stream's items, a direct event as `send_event`'s
//! arguments. Decoding builds new values: nothing the executor holds
//! reaches the owner.

use roomkit_core::model::{ChannelOutput, DirectEvent, Metadata, RoomId, ToolCallContent};
use serde_json::Value;

use super::error::{from_record, to_record};
use super::pipe::Uncarried;
use super::wire::{Item, SendEvent, ToolEnd, WireOutput, WireResult};
use crate::stream::ResponseItem;

/// The record's key that says how a turn ended (RFC §6.4).
const LOOP_END_REASON: &str = "loop_end_reason";

/// The record's key of what a turn's rounds used (RFC §6.4).
const AI_USAGE: &str = "ai_usage";

/// A channel's output without its stream; its provider's answer, a
/// transport's delivery alone carries, does not cross.
pub(crate) fn encode_output(output: ChannelOutput) -> WireOutput {
    WireOutput {
        responded: output.responded,
        response_events: output.response_events,
        response_metadata: output.response_metadata,
        tasks: output.tasks,
        observations: output.observations,
        metadata_updates: output.metadata_updates,
        error: output.error.as_ref().map(from_record),
    }
}

/// A new channel output from its wire form.
pub(crate) fn decode_output(wire: WireOutput) -> ChannelOutput {
    ChannelOutput {
        responded: wire.responded,
        response_events: wire.response_events,
        response_metadata: wire.response_metadata,
        provider_result: None,
        tasks: wire.tasks,
        observations: wire.observations,
        metadata_updates: wire.metadata_updates,
        error: wire.error.as_ref().map(to_record),
    }
}

/// A call's start as its item carries it.
pub(crate) fn encode_start(start: ToolCallContent) -> Item {
    Item::ToolStart {
        tool_name: start.tool_name,
        tool_id: start.tool_id,
        arguments: start.arguments,
        ran_with: None,
    }
}

/// A call's end as its item carries it, naming its start's frame.
pub(crate) fn encode_end_item(end: ToolCallContent, start: Option<u64>) -> Item {
    let end = encode_end(end);
    Item::ToolEnd {
        start,
        tool_name: end.tool_name,
        tool_id: end.tool_id,
        arguments: end.arguments,
        result: end.result,
        status: end.status,
        duration_ms: end.duration_ms,
        error: end.error,
        structured_content: end.structured_content,
        outcome: end.outcome,
        refused_but_ran: end.refused_but_ran,
    }
}

/// A call's end, as `tool_ended` carries it.
pub(crate) fn encode_end(end: ToolCallContent) -> ToolEnd {
    ToolEnd {
        tool_name: end.tool_name,
        tool_id: end.tool_id,
        arguments: end.arguments,
        result: WireResult::Value { value: end.result },
        status: end.status,
        duration_ms: end.duration_ms.unwrap_or(0),
        error: end.error,
        structured_content: end.structured_content,
        outcome: end.outcome,
        refused_but_ran: end.refused_but_ran,
    }
}

/// A new call's start row from its item's fields.
pub(crate) fn decode_start(
    tool_name: String,
    tool_id: String,
    arguments: Metadata,
) -> ToolCallContent {
    let mut start = ToolCallContent::new(tool_name, tool_id);
    start.arguments = arguments;
    start
}

/// A new call's end row from its wire form; content parts read as their
/// JSON list.
pub(crate) fn decode_end(end: ToolEnd) -> ToolCallContent {
    let mut row = ToolCallContent::new(end.tool_name, end.tool_id);
    row.arguments = end.arguments;
    row.result = match end.result {
        WireResult::Value { value } => value,
        WireResult::Parts { parts } => Value::Array(parts),
    };
    row.status = end.status;
    row.duration_ms = Some(end.duration_ms);
    row.error = end.error;
    row.structured_content = end.structured_content;
    row.outcome = end.outcome;
    row.refused_but_ran = end.refused_but_ran;
    row
}

/// A turn's record (RFC §6.4 "The turn's record") as the loop's end
/// carries it, after `rounds` rounds of calls: its reason and its usage;
/// and the record itself when the loop's end cannot carry it whole (a key
/// of its own, no reason, a usage that is no object), to cross before it
/// in a `record` frame. A record without a reason ends with an empty one.
pub(crate) fn encode_loop_end(record: &Metadata, rounds: u64) -> (Option<Metadata>, Item) {
    let reason = record.get(LOOP_END_REASON).and_then(Value::as_str);
    let usage = match record.get(AI_USAGE) {
        Some(Value::Object(usage)) => Some(usage),
        _ => None,
    };
    let whole = reason.is_some() && usage.is_some() && record.len() == 2;
    let item = Item::LoopEnd {
        reason: reason.unwrap_or_default().to_owned(),
        rounds,
        usage: usage.cloned().unwrap_or_default(),
        max_rounds: None,
        timeout_seconds: None,
        budget_tokens: None,
        budget_usd: None,
    };
    ((!whole).then(|| record.clone()), item)
}

/// A turn's record from its loop's end: its reason and its usage.
pub(crate) fn decode_loop_end(reason: String, usage: Metadata) -> ResponseItem {
    let mut record = Metadata::new();
    record.insert(LOOP_END_REASON.to_owned(), Value::String(reason));
    record.insert(AI_USAGE.to_owned(), Value::Object(usage));
    ResponseItem::End(record)
}

/// `event`, injected into `room_id` at least `floor` deep, as `send_event`'s
/// arguments (RFC §10.6.5): every argument data.
pub(crate) fn send_event_arguments(
    room_id: &RoomId,
    event: DirectEvent,
    floor: u32,
) -> Result<Metadata, Uncarried> {
    let arguments = SendEvent {
        room_id: room_id.clone(),
        channel_id: event.channel_id,
        content: event.content,
        event_type: event.event_type,
        chain_depth: event.chain_depth.max(floor),
        participant_id: event.participant_id,
        metadata: Some(event.metadata),
        visibility: event.visibility,
        provider: None,
        response_visibility: event.response_visibility,
        created_at: None,
        parent_event_id: event.parent_event_id,
        idempotency_key: event.idempotency_key,
        addressed_to: event.addressed_to,
        organization_id: None,
        standalone: event.standalone,
    };
    match serde_json::to_value(arguments) {
        Ok(Value::Object(arguments)) => Ok(arguments),
        Ok(_) => Err(Uncarried("send_event's arguments are no record".to_owned())),
        Err(error) => Err(Uncarried(error.to_string())),
    }
}

/// The direct event `send_event`'s arguments name, and its room.
///
/// # Errors
///
/// Arguments the verb does not take, or that do not read, refuse it; so do
/// a provider and an instant, which a direct event of this implementation
/// does not carry.
pub(crate) fn decode_send_event(
    arguments: Metadata,
) -> Result<(RoomId, Option<String>, DirectEvent), String> {
    let arguments: SendEvent =
        serde_json::from_value(Value::Object(arguments)).map_err(|error| error.to_string())?;
    if arguments.provider.is_some() || arguments.created_at.is_some() {
        return Err("send_event takes no provider and no created_at here".to_owned());
    }
    let event = DirectEvent {
        channel_id: arguments.channel_id,
        content: arguments.content,
        event_type: arguments.event_type,
        participant_id: arguments.participant_id,
        visibility: arguments.visibility,
        response_visibility: arguments.response_visibility,
        addressed_to: arguments.addressed_to,
        parent_event_id: arguments.parent_event_id,
        idempotency_key: arguments.idempotency_key,
        metadata: arguments.metadata.unwrap_or_default(),
        chain_depth: arguments.chain_depth,
        standalone: arguments.standalone,
    };
    Ok((arguments.room_id, arguments.organization_id, event))
}
