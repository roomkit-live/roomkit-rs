//! A turn's loop as its response: streamed to the kit item by item (RFC
//! §6.1 `response_stream`), or, for a turn run outside a kit, buffered into
//! the rows its output returns (RFC §6.4): each round's text as a MESSAGE,
//! its calls' `TOOL_CALL_START` rows, then their `TOOL_CALL_END` rows in
//! call order, the last MESSAGE carrying the turn's record.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use roomkit::model::{
    ChannelOutput, ErrorRecord, EventContent, EventSource, EventType, LoopEndMarker, RoomEvent,
    TextContent, ToolCall, ToolCallContent, ToolCallOutcome, ToolCallStatus,
};
use roomkit::{ChannelError, ClassedError, ResponseItem, ResponseStream};

use crate::provider::ProviderError;
use crate::stream::next;
use crate::tool_loop::{ToolEnd, TurnItem, TurnItems};

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
                let row = EventContent::ToolCall(start_row(&call));
                self.push(EventType::ToolCallStart, row);
            }
            TurnItem::ToolEnd(end) => {
                let row = EventContent::ToolCall(end_row(end));
                self.push(EventType::ToolCallEnd, row);
            }
            TurnItem::End(end) => {
                self.end_segment();
                self.record(&end);
            }
            TurnItem::SegmentBreak => self.end_segment(),
        }
    }

    /// The segment written so far, as a MESSAGE, when it says something.
    fn end_segment(&mut self) {
        let text = std::mem::take(&mut self.segment);
        if text.trim().is_empty() {
            return;
        }
        self.last_message = Some(self.events.len());
        self.push(EventType::Message, TextContent::new(text).into());
    }

    /// The turn's record on its last MESSAGE: its final text's, or the last
    /// one the turn wrote; with none, nothing records it.
    fn record(&mut self, end: &LoopEndMarker) {
        let Some(message) = self
            .last_message
            .and_then(|position| self.events.get_mut(position))
        else {
            return;
        };
        message.metadata.extend(end.record());
    }

    fn push(&mut self, event_type: EventType, content: EventContent) {
        // The pipeline gives each response its own id, depth and instant,
        // and makes it answer the event that asked: what is written here
        // is a placeholder.
        let row = RoomEvent::at(
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
            ChannelOutput {
                responded: true,
                response_events: self.events,
                ..ChannelOutput::empty()
            }
        };
        // Recorded under its class's name, as Python's `ProviderError` is
        // reported, not as a channel's.
        output.error = error.map(|error| {
            let raised = ClassedError::new(PROVIDER_ERROR, error.clone());
            ErrorRecord::new(raised.class(), error.to_string())
        });
        output
    }
}

/// The turn's response, its loop's items as the kit reads them (RFC §6.1
/// `response_stream`): text, each call's start and end rows, the turn's
/// record, or the provider's error that ends it.
pub(crate) fn response(turn: TurnItems<'static>) -> ResponseStream {
    Box::pin(Responding { turn })
}

/// A turn's items read as its response.
struct Responding {
    turn: TurnItems<'static>,
}

impl Stream for Responding {
    type Item = Result<ResponseItem, ChannelError>;

    fn poll_next(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let item = match Pin::new(&mut self.get_mut().turn).poll_next(context) {
            Poll::Ready(item) => item,
            Poll::Pending => return Poll::Pending,
        };
        Poll::Ready(item.map(|item| item.map(response_item).map_err(raised)))
    }
}

/// The class a provider's failure is raised as, Python's.
const PROVIDER_ERROR: &str = "roomkit.providers.ai.base:ProviderError";

/// A provider's failure as the turn raises it: under its Python class,
/// which the kit records (RMK-1208).
fn raised(error: ProviderError) -> ChannelError {
    Box::new(ClassedError::new(PROVIDER_ERROR, error))
}

/// `error`, a failure the turn met before its loop, raised under its
/// Python class when it is a provider's (the budget-aware memory's
/// context overflow), as it is.
pub(crate) fn provider_failure(error: ChannelError) -> ChannelError {
    match error.downcast::<ProviderError>() {
        Ok(failure) => raised(*failure),
        Err(other) => other,
    }
}

/// One item of the loop, as the response reads it.
fn response_item(item: TurnItem) -> ResponseItem {
    match item {
        TurnItem::Text(delta) => ResponseItem::Text(delta),
        TurnItem::ToolStart(call) => ResponseItem::ToolCallStart(start_row(&call)),
        TurnItem::ToolEnd(end) => ResponseItem::ToolCallEnd(end_row(end)),
        TurnItem::End(end) => ResponseItem::LoopEnd(end),
        TurnItem::SegmentBreak => ResponseItem::SegmentBreak,
    }
}

/// A call's start row: its tool, its id and the model's arguments, pending.
fn start_row(call: &ToolCall) -> ToolCallContent {
    let mut row = ToolCallContent::new(call.name.clone(), call.id.clone());
    row.arguments.clone_from(&call.arguments);
    row.intent.clone_from(&call.intent);
    row
}

/// A call's end row: the arguments it ran with, how it ended, what the
/// model read of it, and, when it did not serve, the same as its error.
fn end_row(end: ToolEnd) -> ToolCallContent {
    let mut row = ToolCallContent::new(end.call.name, end.call.id);
    row.arguments = end.call.arguments;
    row.intent = end.call.intent;
    let served = end.outcome == ToolCallOutcome::Served;
    row.status = if served {
        ToolCallStatus::Completed
    } else {
        ToolCallStatus::Failed
    };
    row.outcome = Some(end.outcome);
    row.error = (!served).then(|| end.result.as_text());
    row.result = end.result.into_value();
    row.duration_ms = u64::try_from(end.duration.as_millis()).ok();
    row
}
