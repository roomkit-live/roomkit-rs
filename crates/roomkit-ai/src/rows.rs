//! A turn's loop, buffered into the rows its response returns (RFC §6.4):
//! each round's text as a MESSAGE, its calls' `TOOL_CALL_START` rows, then
//! their `TOOL_CALL_END` rows in call order, the last MESSAGE carrying the
//! turn's record.

use roomkit::model::{
    ChannelOutput, ErrorRecord, EventContent, EventSource, EventType, RoomEvent, ToolCall,
    ToolCallContent, ToolCallOutcome, ToolCallStatus,
};
use serde_json::Value;

use crate::provider::ProviderError;
use crate::stream::next;
use crate::tool_loop::{LoopEnd, ToolEnd, TurnItem, TurnItems};

/// The record's keys on the turn's last MESSAGE (RFC §6.4 "The turn's
/// record").
const LOOP_END_REASON: &str = "loop_end_reason";
const AI_USAGE: &str = "ai_usage";

/// The rows of one turn, built as its items come.
struct Rows<'a> {
    trigger: &'a RoomEvent,
    source: EventSource,
    events: Vec<RoomEvent>,
    /// The text of the segment the model is writing.
    segment: String,
    /// Where the last MESSAGE the turn wrote stands among `events`.
    last_message: Option<usize>,
}

/// The output of the turn of the agent of `source` on `trigger`: the rows
/// of `turn`'s items, in order. A provider's failure before any round fails
/// the turn; after one, the rows stand, their record saying `error`, and the
/// output carries the error (RFC §6.4 "A turn cut short").
///
/// # Errors
///
/// Returns the provider's error when the turn wrote nothing.
pub(crate) async fn collect(
    source: EventSource,
    trigger: &RoomEvent,
    mut turn: TurnItems<'_>,
) -> Result<ChannelOutput, ProviderError> {
    let mut rows = Rows {
        trigger,
        source,
        events: Vec::new(),
        segment: String::new(),
        last_message: None,
    };
    while let Some(item) = next(&mut turn).await {
        match item {
            Ok(item) => rows.take(item),
            Err(error) if rows.events.is_empty() => return Err(error),
            Err(error) => return Ok(rows.output(Some(&error))),
        }
    }
    Ok(rows.output(None))
}

impl Rows<'_> {
    fn take(&mut self, item: TurnItem) {
        match item {
            TurnItem::Text(delta) => self.segment.push_str(&delta),
            TurnItem::ToolStart(call) => {
                self.end_segment();
                let row = start_content(&call);
                self.push(EventType::ToolCallStart, row);
            }
            TurnItem::ToolEnd(end) => {
                let row = end_content(end);
                self.push(EventType::ToolCallEnd, row);
            }
            TurnItem::End(end) => {
                self.end_segment();
                self.record(&end);
            }
        }
    }

    /// The segment written so far, as a MESSAGE, when it says something.
    fn end_segment(&mut self) {
        let text = std::mem::take(&mut self.segment);
        if text.trim().is_empty() {
            return;
        }
        self.last_message = Some(self.events.len());
        self.push(EventType::Message, EventContent::text(text));
    }

    /// The turn's record on its last MESSAGE: its final text's, or the last
    /// one the turn wrote; with none, nothing records it.
    fn record(&mut self, end: &LoopEnd) {
        let Some(message) = self
            .last_message
            .and_then(|position| self.events.get_mut(position))
        else {
            return;
        };
        message
            .metadata
            .insert(LOOP_END_REASON.to_owned(), Value::from(end.reason.as_str()));
        message
            .metadata
            .insert(AI_USAGE.to_owned(), Value::Object(end.usage.clone()));
    }

    fn push(&mut self, event_type: EventType, content: EventContent) {
        // The pipeline gives each response its own id, depth and instant,
        // and makes it answer the event that asked: what is written here
        // is a placeholder.
        let row = RoomEvent::new(
            self.trigger.id.clone(),
            self.trigger.room_id.clone(),
            event_type,
            self.source.clone(),
            content,
            self.trigger.created_at,
        );
        self.events.push(row);
    }

    fn output(self, error: Option<&ProviderError>) -> ChannelOutput {
        let mut output = if self.events.is_empty() {
            ChannelOutput::default()
        } else {
            ChannelOutput::respond(self.events)
        };
        output.error = error.map(|error| ErrorRecord::new("ChannelError", error.to_string()));
        output
    }
}

/// A call's start row: its tool, its id and the model's arguments, pending.
fn start_content(call: &ToolCall) -> EventContent {
    let mut row = ToolCallContent::new(call.name.clone(), call.id.clone());
    row.arguments.clone_from(&call.arguments);
    EventContent::ToolCall(row)
}

/// A call's end row: the arguments it ran with, how it ended, what the
/// model read of it, and, when it did not serve, the same as its error.
fn end_content(end: ToolEnd) -> EventContent {
    let mut row = ToolCallContent::new(end.call.name, end.call.id);
    row.arguments = end.call.arguments;
    let served = end.outcome == ToolCallOutcome::Served;
    row.status = if served {
        ToolCallStatus::Completed
    } else {
        ToolCallStatus::Failed
    };
    row.outcome = Some(end.outcome);
    row.error = (!served).then(|| end.result.clone());
    row.result = Value::String(end.result);
    row.duration_ms = u64::try_from(end.duration.as_millis()).ok();
    EventContent::ToolCall(row)
}
