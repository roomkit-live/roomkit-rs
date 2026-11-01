//! What a turn's objects become on the wire, and back (RFC §10.6.3): a
//! channel's output, a stream's items, a direct event as `send_event`'s
//! arguments. Decoding builds new values: nothing the executor holds
//! reaches the owner.

use roomkit_core::model::{ChannelOutput, DirectEvent, Metadata, RoomId, ToolCallContent};
use serde_json::Value;

use super::error::{from_record, to_record};
use super::pipe::Uncarried;
use super::wire::{Item, SendEvent, ToolEnd, WireOutput, WireResult};
use crate::stream::{LoopEndMarker, LoopEndReason, ResponseItem, read_record};

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
        intent: start.intent,
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
        intent: end.intent,
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
        intent: end.intent,
    }
}

/// A new call's start row from its item's fields.
pub(crate) fn decode_start(
    tool_name: String,
    tool_id: String,
    arguments: Metadata,
    intent: Option<String>,
) -> ToolCallContent {
    let mut start = ToolCallContent::new(tool_name, tool_id);
    start.arguments = arguments;
    start.intent = intent;
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
    row.intent = end.intent;
    row
}

/// A loop's end as the wire carries it (RFC §10.6.3): `marker`'s fields,
/// as Python's `dataclasses.asdict` writes its `LoopEndMarker`; and the
/// turn's record when it holds more than Python's two keys, roomkit-rs's
/// own (RMK-1029), to cross before it in a `record` frame.
pub(crate) fn encode_loop_end(marker: &LoopEndMarker) -> (Option<Metadata>, Item) {
    let record = marker.record();
    let item = Item::LoopEnd {
        reason: marker.reason.to_string(),
        rounds: u64::from(marker.rounds),
        usage: marker.usage.clone(),
        max_rounds: marker.max_rounds.map(u64::from),
        timeout_seconds: marker.timeout.map(|timeout| timeout.as_secs_f64()),
        budget_tokens: marker.budget_tokens,
        budget_usd: marker.budget_usd,
    };
    ((record.len() > 2).then_some(record), item)
}

/// What a loop's end crosses the wire with: Python's `LoopEndMarker`
/// fields.
pub(crate) struct WireLoopEnd {
    pub(crate) reason: String,
    pub(crate) rounds: u64,
    pub(crate) usage: Metadata,
    pub(crate) max_rounds: Option<u64>,
    pub(crate) timeout_seconds: Option<f64>,
    pub(crate) budget_tokens: Option<u64>,
    pub(crate) budget_usd: Option<f64>,
}

/// A loop's end from the wire, roomkit-rs's own fields read from `record`,
/// the turn's record a `record` frame carried before it. A reason this
/// implementation does not know, a newer peer's, reads `error`, logged; a
/// limit out of range is none.
pub(crate) fn decode_loop_end(end: WireLoopEnd, record: Option<&Metadata>) -> ResponseItem {
    let reason = LoopEndReason::of(&end.reason).unwrap_or_else(|| {
        tracing::warn!(
            target: "roomkit::turns",
            reason = %end.reason,
            "a turn's loop ended for a reason this implementation does not know: read as error"
        );
        LoopEndReason::Error
    });
    let mut marker = LoopEndMarker {
        rounds: u32::try_from(end.rounds).unwrap_or(u32::MAX),
        usage: end.usage,
        max_rounds: end.max_rounds.and_then(|rounds| u32::try_from(rounds).ok()),
        timeout: end
            .timeout_seconds
            .and_then(|seconds| std::time::Duration::try_from_secs_f64(seconds).ok()),
        budget_tokens: end.budget_tokens,
        budget_usd: end.budget_usd,
        ..LoopEndMarker::new(reason)
    };
    if let Some(record) = record {
        read_record(&mut marker, record);
    }
    ResponseItem::LoopEnd(marker)
}

/// `event`, injected into `room_id` at least `floor` deep, as `send_event`'s
/// arguments (RFC §10.6.5): every argument data.
pub(crate) fn send_event_arguments(
    room_id: &RoomId,
    event: DirectEvent,
    organization_id: Option<String>,
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
        provider: event.provider,
        response_visibility: event.response_visibility,
        created_at: event.created_at,
        parent_event_id: event.parent_event_id,
        idempotency_key: event.idempotency_key,
        addressed_to: event.addressed_to,
        organization_id,
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
/// Arguments the verb does not take, or that do not read, refuse it.
pub(crate) fn decode_send_event(
    arguments: Metadata,
) -> Result<(RoomId, Option<String>, DirectEvent), String> {
    let arguments: SendEvent =
        serde_json::from_value(Value::Object(arguments)).map_err(|error| error.to_string())?;
    let event = DirectEvent {
        channel_id: arguments.channel_id,
        content: arguments.content,
        event_type: arguments.event_type,
        participant_id: arguments.participant_id,
        visibility: arguments.visibility,
        provider: arguments.provider,
        response_visibility: arguments.response_visibility,
        created_at: arguments.created_at,
        addressed_to: arguments.addressed_to,
        parent_event_id: arguments.parent_event_id,
        idempotency_key: arguments.idempotency_key,
        metadata: arguments.metadata.unwrap_or_default(),
        chain_depth: arguments.chain_depth,
        standalone: arguments.standalone,
    };
    Ok((arguments.room_id, arguments.organization_id, event))
}
