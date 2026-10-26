//! The rows of a turn's streamed response (RFC §6.4, §12.2 step 13s): what
//! the reader read, cut into the rows the store holds.
//!
//! A text segment ends at a call's start, and at the response's end; a
//! blank one writes nothing. The turn's record rides the segment the turn
//! ends on; when the turn ends on no text, it is owed to the last message
//! the turn wrote, which is updated once the response is over. A response
//! its reader stopped once it began ends `cancelled`, its text kept and
//! marked so; a call it opened and did not end gets its end, `failed`.

use roomkit_core::model::{
    EventContent, EventId, EventSource, EventType, Metadata, RoomEvent, ToolCallContent,
    ToolCallOutcome, ToolCallStatus,
};
use serde_json::Value;

/// The record's key that says how the turn ended (RFC §6.4).
const LOOP_END_REASON: &str = "loop_end_reason";

/// The mark of a text its reader stopped (RFC §12.2 step 13s).
const CANCELLED: &str = "cancelled";

/// A call the response opened: its start, and whether its row was written.
#[derive(Debug)]
struct Open {
    start: ToolCallContent,
    written: bool,
}

/// The rows of one streamed response, as its reader reads it.
#[derive(Debug)]
pub(super) struct Segments {
    /// The source of the rows: the agent.
    source: EventSource,
    /// The event the response answers, whose id, room and instant the
    /// drafts carry until the pipeline stamps them.
    answers: RoomEvent,
    /// The text of the segment the reader is reading.
    text: String,
    /// The turn's record, once its end was read.
    record: Option<Metadata>,
    /// Whether no stored row carries the record yet.
    owed: bool,
    /// Whether the reader was handed anything: from there, a stop cuts the
    /// turn.
    began: bool,
    open: Vec<Open>,
    /// The last message the store holds of the response.
    last_message: Option<RoomEvent>,
}

impl Segments {
    /// The rows of `source`'s response to `answers`.
    pub(super) fn new(source: EventSource, answers: RoomEvent) -> Self {
        Self {
            source,
            answers,
            text: String::new(),
            record: None,
            owed: false,
            began: false,
            open: Vec::new(),
            last_message: None,
        }
    }

    /// The reader was handed an item.
    pub(super) fn handed(&mut self) {
        self.began = true;
    }

    /// The reader was handed `delta`.
    pub(super) fn text(&mut self, delta: &str) {
        self.text.push_str(delta);
    }

    /// A call starts: the text before it, a segment of its own, then its
    /// start row.
    pub(super) fn start(&mut self, start: ToolCallContent) -> Vec<RoomEvent> {
        let mut rows: Vec<RoomEvent> = self.segment(false).into_iter().collect();
        rows.push(self.draft(
            EventType::ToolCallStart,
            EventContent::ToolCall(start.clone()),
        ));
        self.open.push(Open {
            start,
            written: true,
        });
        rows
    }

    /// A call that is not open yet, read in flight once its reader stopped:
    /// opened, its row unwritten.
    pub(super) fn opened_unwritten(&mut self, start: ToolCallContent) {
        self.open.push(Open {
            start,
            written: false,
        });
    }

    /// A call ended: its end row, closing the earliest call open under its
    /// id.
    pub(super) fn end(&mut self, end: ToolCallContent) -> Vec<RoomEvent> {
        let mut rows = Vec::new();
        if let Some(position) = self
            .open
            .iter()
            .position(|open| open.start.tool_id == end.tool_id)
        {
            let open = self.open.remove(position);
            if !open.written {
                rows.push(self.draft(EventType::ToolCallStart, EventContent::ToolCall(open.start)));
            }
        }
        rows.push(self.draft(EventType::ToolCallEnd, EventContent::ToolCall(end)));
        rows
    }

    /// Whether a call is open: started, its end not read yet.
    pub(super) fn has_open_calls(&self) -> bool {
        !self.open.is_empty()
    }

    /// The turn reached its end: its record rides the next text segment, or
    /// the last message once the response is over.
    pub(super) fn end_turn(&mut self, record: Metadata) {
        self.record = Some(record);
        self.owed = true;
    }

    /// The reader stopped: a turn it began ends `cancelled`, unless its end
    /// was read already (RFC §6.4, §12.2 step 13s).
    pub(super) fn stopped(&mut self) {
        if !self.began || self.record.is_some() {
            return;
        }
        let mut record = Metadata::new();
        record.insert(LOOP_END_REASON.to_owned(), Value::from(CANCELLED));
        self.end_turn(record);
    }

    /// The text segment the response ends on, `cancelled` when its reader
    /// stopped, carrying the turn's record when its end was read.
    pub(super) fn last_text(&mut self, cancelled: bool) -> Option<RoomEvent> {
        self.segment(cancelled)
    }

    /// The ends of the calls still open, each `failed` with `error`, with
    /// the arguments `ran_with` says it ran with when its gate fixed them,
    /// and the start row of one whose start was never written (RFC §12.2
    /// step 13s: no start row stays pending; §10.6.4).
    pub(super) fn close_open(
        &mut self,
        error: &str,
        ran_with: impl Fn(&str) -> Option<Metadata>,
    ) -> Vec<RoomEvent> {
        let mut rows = Vec::new();
        for open in std::mem::take(&mut self.open) {
            if !open.written {
                rows.push(self.draft(
                    EventType::ToolCallStart,
                    EventContent::ToolCall(open.start.clone()),
                ));
            }
            let mut end = open.start;
            if let Some(arguments) = ran_with(&end.tool_id) {
                end.arguments = arguments;
            }
            end.status = ToolCallStatus::Failed;
            end.outcome = Some(ToolCallOutcome::Cancelled);
            end.error = Some(error.to_owned());
            end.result = Value::Null;
            rows.push(self.draft(EventType::ToolCallEnd, EventContent::ToolCall(end)));
        }
        rows
    }

    /// `row` is stored: the last message, and the record when it carries it.
    pub(super) fn stored(&mut self, row: &RoomEvent) {
        if row.event_type != EventType::Message {
            return;
        }
        if self.owed && row.metadata.contains_key(LOOP_END_REASON) {
            self.owed = false;
        }
        self.last_message = Some(row.clone());
    }

    /// The last message the store holds, and the record it is owed: the
    /// update to write once the response is over, when no row carried it
    /// (RFC §6.4 "The turn's record").
    pub(super) fn owed_record(&self) -> Option<(EventId, Metadata)> {
        let record = self.record.as_ref().filter(|_| self.owed)?;
        let last = self.last_message.as_ref()?;
        let mut metadata = last.metadata.clone();
        metadata.extend(record.clone());
        Some((last.id.clone(), metadata))
    }

    /// The text read so far as a message, when it says something; with the
    /// turn's record once its end was read, marked cancelled when its
    /// reader stopped or the turn ended so.
    fn segment(&mut self, cancelled: bool) -> Option<RoomEvent> {
        let text = std::mem::take(&mut self.text);
        if text.trim().is_empty() {
            return None;
        }
        let mut row = self.draft(EventType::Message, EventContent::text(text));
        if let Some(record) = self.record.as_ref().filter(|_| self.owed) {
            row.metadata.extend(record.clone());
            if is_cancelled(record) {
                row.metadata.insert(CANCELLED.to_owned(), Value::Bool(true));
            }
        }
        if cancelled {
            row.metadata.insert(CANCELLED.to_owned(), Value::Bool(true));
        }
        Some(row)
    }

    /// A row of `event_type` saying `content`, as the response wrote it:
    /// the pipeline gives it its id, its depth and its instant, and makes it
    /// answer the event the response answers.
    fn draft(&self, event_type: EventType, content: EventContent) -> RoomEvent {
        RoomEvent::new(
            self.answers.id.clone(),
            self.answers.room_id.clone(),
            event_type,
            self.source.clone(),
            content,
            self.answers.created_at,
        )
    }
}

/// Whether `record` says the turn ended cancelled.
fn is_cancelled(record: &Metadata) -> bool {
    record.get(LOOP_END_REASON).and_then(Value::as_str) == Some(CANCELLED)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_core::model::{ChannelId, ChannelType, EventDirection, RoomId, Timestamp};
    use serde_json::json;

    use super::*;

    fn segments() -> Segments {
        let source = EventSource::new(
            ChannelId::new("agent").unwrap(),
            ChannelType::AI,
            EventDirection::Outbound,
        );
        let answers = RoomEvent::new(
            EventId::new("evt_1").unwrap(),
            RoomId::new("room_1").unwrap(),
            EventType::Message,
            EventSource::new(
                ChannelId::new("sms").unwrap(),
                ChannelType::SMS,
                EventDirection::Inbound,
            ),
            EventContent::text("Hi"),
            Timestamp::from_unix_millis(0).unwrap(),
        );
        Segments::new(source, answers)
    }

    fn record(reason: &str) -> Metadata {
        json!({ "loop_end_reason": reason, "ai_usage": {} })
            .as_object()
            .cloned()
            .unwrap()
    }

    fn texts(rows: &[RoomEvent]) -> Vec<(EventType, String)> {
        rows.iter()
            .map(|row| {
                (
                    row.event_type,
                    roomkit_core::rules::transcoding::text_of(&row.content),
                )
            })
            .collect()
    }

    /// RFC §6.4: a call's start ends the text before it; the turn's record
    /// rides the text it ends on, and no update is owed.
    #[test]
    fn a_call_s_start_ends_a_segment_and_the_record_rides_the_last_text() {
        let mut segments = segments();
        segments.handed();
        segments.text("Let me ");
        segments.text("check.");
        let started = segments.start(ToolCallContent::new("lookup", "call_1"));
        segments.end(ToolCallContent::new("lookup", "call_1"));
        segments.text("Done.");
        segments.end_turn(record("completed"));
        let last = segments.last_text(false).unwrap();
        segments.stored(&last);
        assert_eq!(
            (
                texts(&started)[0].clone(),
                started[1].event_type,
                last.metadata.get("loop_end_reason").cloned(),
                segments.owed_record(),
            ),
            (
                (EventType::Message, "Let me check.".to_owned()),
                EventType::ToolCallStart,
                Some(json!("completed")),
                None,
            )
        );
    }

    /// RFC §12.2 step 13s: a reader that stops keeps the text it was handed,
    /// marked cancelled, and the turn records `cancelled`.
    #[test]
    fn a_stopped_reader_s_text_is_marked_cancelled_and_records_cancelled() {
        let mut segments = segments();
        segments.handed();
        segments.text("The answer ");
        segments.stopped();
        let last = segments.last_text(true).unwrap();
        assert_eq!(
            (
                roomkit_core::rules::transcoding::text_of(&last.content),
                last.metadata.get("cancelled").cloned(),
                last.metadata.get("loop_end_reason").cloned(),
            ),
            (
                "The answer ".to_owned(),
                Some(json!(true)),
                Some(json!("cancelled"))
            )
        );
    }

    /// RFC §12.2 step 13s: a call announced and not ended is closed
    /// `failed`, and one whose start was never written gets its start first.
    #[test]
    fn an_open_call_is_closed_failed_its_start_written_first_when_it_was_not() {
        let mut segments = segments();
        segments.handed();
        segments.start(ToolCallContent::new("lookup", "call_1"));
        segments.opened_unwritten(ToolCallContent::new("lookup", "call_2"));
        let closed = segments.close_open("cancelled", |_| None);
        let kinds: Vec<(EventType, Option<ToolCallStatus>)> = closed
            .iter()
            .map(|row| {
                let EventContent::ToolCall(call) = &row.content else {
                    unreachable!()
                };
                (row.event_type, Some(call.status))
            })
            .collect();
        assert_eq!(
            kinds,
            [
                (EventType::ToolCallEnd, Some(ToolCallStatus::Failed)),
                (EventType::ToolCallStart, Some(ToolCallStatus::Pending)),
                (EventType::ToolCallEnd, Some(ToolCallStatus::Failed)),
            ]
        );
    }

    /// RFC §6.4 "The turn's record": a turn that ends on no text owes its
    /// record to the last message it wrote, kept with that message's
    /// metadata.
    #[test]
    fn a_turn_ending_on_no_text_owes_its_record_to_its_last_message() {
        let mut segments = segments();
        segments.handed();
        segments.text("Checking.");
        let mut rows = segments.start(ToolCallContent::new("lookup", "call_1"));
        let mut message = rows.remove(0);
        message.id = EventId::new("evt_2").unwrap();
        message.metadata.insert("label".to_owned(), json!("kept"));
        segments.stored(&message);
        segments.end(ToolCallContent::new("lookup", "call_1"));
        segments.end_turn(record("empty_response"));
        assert_eq!(segments.last_text(false), None);
        let (id, metadata) = segments.owed_record().unwrap();
        assert_eq!(
            (
                id.as_str(),
                metadata.get("label").cloned(),
                metadata.get("loop_end_reason").cloned()
            ),
            ("evt_2", Some(json!("kept")), Some(json!("empty_response")))
        );
    }
}
