//! The rows of a turn's streamed response (RFC §6.4, §12.2 step 13s): what
//! the reader read, cut into the rows the store holds.
//!
//! A text segment ends at a call's start, and at the response's end; a
//! blank one writes nothing. The turn's live record rides each message as it
//! stands when the message is written; how the turn ended rides the segment
//! the turn ends on; when the turn ends on no text, it is owed to the last message
//! the turn wrote, or to its last row when it wrote none, which is updated once
//! the response is over. A response
//! its reader stopped once it began ends `cancelled`, its text kept and
//! marked so; a call it opened and did not end gets its end, `failed`.
//!
//! Every row of the response carries the turn's own `correlation_id`, a
//! fresh one per response, which the operation gives its read and its
//! failure's `ON_ERROR` payload carries too, as the Python implementation's
//! reader stamps a streamed response (`uuid4().hex`), whatever the event it
//! answers carries: what
//! the response's rows have in common, which a later turn's history pairs
//! a call's start and end by (RFC §6.4).

use std::collections::HashSet;

use roomkit_core::model::{
    EventContent, EventId, EventSource, EventType, Metadata, ResponseMetadata, RoomEvent,
    TextContent, ToolCallContent, ToolCallOutcome, ToolCallStatus,
};
use roomkit_core::rules::transcript::is_blank;
use serde_json::Value;

use super::UsageMarker;
use super::generations::GenerationsRead;

/// The record's key that says how the turn ended (RFC §6.4).
const LOOP_END_REASON: &str = "loop_end_reason";

/// The record's key that holds the turn's usage (RFC §6.4).
const AI_USAGE: &str = "ai_usage";

/// The caller's key for each turn's end, the framework's (RFC §6.7).
const TURNS: &str = "turns";

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
    /// The response's own id, which each of its rows carries.
    correlation_id: String,
    /// The text of the segment the reader is reading.
    text: String,
    /// The turn's live record (RFC §6.7), which rides each message as it
    /// stands when the message is written.
    live: ResponseMetadata,
    /// How the turn ended, once its end was read: `loop_end_reason`,
    /// `ai_usage`.
    record: Option<Metadata>,
    /// What the turn's generations told.
    generations: GenerationsRead,
    /// Whether no stored row carries the record yet.
    owed: bool,
    /// Whether the reader was handed anything: from there, a stop cuts the
    /// turn.
    began: bool,
    open: Vec<Open>,
    /// The last message the store holds of the response.
    last_message: Option<EventId>,
    /// The last row the store holds of the response.
    last_row: Option<EventId>,
}

impl Segments {
    /// The rows of `source`'s response to `answers`, each carrying
    /// `correlation_id`, the one the operation gave the response.
    pub(super) fn new(source: EventSource, answers: RoomEvent, correlation_id: String) -> Self {
        Self {
            source,
            answers,
            correlation_id,
            text: String::new(),
            live: ResponseMetadata::default(),
            record: None,
            owed: false,
            began: false,
            generations: GenerationsRead::default(),
            open: Vec::new(),
            last_message: None,
            last_row: None,
        }
    }

    /// The rows read `live`, the turn's record, as it stands when each
    /// message is written.
    pub(super) fn reading(mut self, live: ResponseMetadata) -> Self {
        self.live = live;
        self
    }

    /// The reader was handed an item.
    pub(super) fn handed(&mut self) {
        self.began = true;
    }

    /// A generation told what it used.
    pub(super) fn told(&mut self, marker: &UsageMarker) {
        self.generations.told(marker);
    }

    /// The loop goes on after a round without a call: a new generation.
    pub(super) fn segment_break(&mut self) {
        self.generations.next_generation();
    }

    /// The reader's read gave a call's start.
    pub(super) fn call_started(&mut self) {
        self.generations.call_started();
    }

    /// The reader's read gave a call's end. One read past a stopped reader
    /// does not come here: it asks no model again.
    pub(super) fn call_ended(&mut self) {
        self.generations.call_ended();
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
        // An end the channel's own record names holds, but `completed`, which
        // says it finished, not that the reader read it (Python's `end_stopped`).
        let named = self.live.get(LOOP_END_REASON).filter(|end| !end.is_null());
        if named.is_some_and(|end| end.as_str() != Some("completed")) {
            return;
        }
        let record = self.generations.cut_record();
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
    /// step 13s: no start row stays pending; §10.6.4). Of two calls under one
    /// id, the first is the one that ran (RFC §12.4).
    pub(super) fn close_open(
        &mut self,
        error: &str,
        ran_with: impl Fn(&str) -> Option<Metadata>,
    ) -> Vec<RoomEvent> {
        let mut rows = Vec::new();
        let mut ran = HashSet::new();
        for open in std::mem::take(&mut self.open) {
            if !open.written {
                rows.push(self.draft(
                    EventType::ToolCallStart,
                    EventContent::ToolCall(open.start.clone()),
                ));
            }
            let mut end = open.start;
            let fixed = ran
                .insert(end.tool_id.clone())
                .then(|| ran_with(&end.tool_id));
            if let Some(arguments) = fixed.flatten() {
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

    /// `row` is stored: the last row, the last message, and the record when
    /// it carries it.
    pub(super) fn stored(&mut self, row: &RoomEvent) {
        self.last_row = Some(row.id.clone());
        if row.r#type != EventType::Message {
            return;
        }
        if self.owed && row.metadata.contains_key(LOOP_END_REASON) {
            self.owed = false;
        }
        self.last_message = Some(row.id.clone());
    }

    /// The turn's record whole: its live record, with how it ended once
    /// its end was read or its reader stopped it (Python's `stream_record`).
    pub(super) fn record(&self) -> Metadata {
        let mut whole = self.live.to_metadata();
        whole.extend(
            self.record
                .iter()
                .flatten()
                .map(|(key, value)| (key.clone(), value.clone())),
        );
        whole
    }

    /// The turn's entry under its caller's `turns`, once its record names
    /// its end: its end and usage (Python's `stream_turn_entry`, RFC §6.4).
    pub(super) fn turn_entry(&self) -> Option<Metadata> {
        let record = self.record();
        let end = record.get(LOOP_END_REASON).filter(|end| !end.is_null())?;
        let mut entry = Metadata::new();
        entry.insert(LOOP_END_REASON.to_owned(), end.clone());
        if let Some(usage) = record.get(AI_USAGE).filter(|usage| !usage.is_null()) {
            entry.insert(AI_USAGE.to_owned(), usage.clone());
        }
        Some(entry)
    }

    /// The last message the store holds, else its last row, and the record
    /// it is owed: the update to write once the response is over, when no row
    /// carried it (RFC §6.4 "The turn's record"). Taken once.
    pub(super) fn take_owed_record(&mut self) -> Option<(EventId, Metadata)> {
        let record = self.record.clone().filter(|_| self.owed)?;
        let last = self.last_message.as_ref().or(self.last_row.as_ref())?;
        let id = last.clone();
        self.owed = false;
        Some((id, record))
    }

    /// The text read so far as a message, when it says something; with the
    /// turn's record once its end was read, marked cancelled when its
    /// reader stopped or the turn ended so.
    fn segment(&mut self, cancelled: bool) -> Option<RoomEvent> {
        let text = std::mem::take(&mut self.text);
        if is_blank(&text) {
            return None;
        }
        let mut row = self.draft(EventType::Message, TextContent::new(text).into());
        row.metadata = self.live.to_metadata();
        // `turns` is the framework's key: on no message, whoever commits it
        // (RFC §6.7, Python's `response_metadata`).
        row.metadata.shift_remove(TURNS);
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

    /// The empty event a streaming channel renders the response under
    /// (`deliver_stream`'s `event`), as Python's `_stream_placeholder`
    /// builds it: from the agent, one deeper than the event the response
    /// answers and answering it, in its thread, with the response's id and
    /// the visibility the trigger asks its responses for.
    pub(super) fn placeholder(&self) -> RoomEvent {
        let mut event = RoomEvent::new(
            self.answers.room_id.clone(),
            self.source.clone(),
            TextContent::new(""),
        );
        event.chain_depth = self.answers.chain_depth.saturating_add(1);
        event.responds_to = Some(self.answers.id.clone());
        event
            .parent_event_id
            .clone_from(&self.answers.parent_event_id);
        event.correlation_id = Some(self.correlation_id.clone());
        if let Some(visibility) = &self.answers.response_visibility {
            event.visibility = visibility.clone();
        }
        event
    }

    /// A row of `event_type` saying `content`, as the response wrote it,
    /// with the response's id: the pipeline gives it its id, its depth and
    /// its instant, and makes it answer the event the response answers.
    fn draft(&self, event_type: EventType, content: EventContent) -> RoomEvent {
        let mut row = RoomEvent::at(
            self.answers.id.clone(),
            self.answers.room_id.clone(),
            event_type,
            self.source.clone(),
            content,
            self.answers.created_at,
        );
        row.correlation_id = Some(self.correlation_id.clone());
        row
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
        let source = EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(ChannelId::new("agent").unwrap(), ChannelType::AI)
        };
        let answers = RoomEvent {
            id: EventId::new("evt_1").unwrap(),
            created_at: Timestamp::from_unix_millis(0).unwrap(),
            ..RoomEvent::new(
                RoomId::new("room_1").unwrap(),
                EventSource::new(ChannelId::new("sms").unwrap(), ChannelType::SMS),
                TextContent::new("Hi"),
            )
        };
        Segments::new(
            source,
            answers,
            "0123456789abcdef0123456789abcdef".to_owned(),
        )
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
                    row.r#type,
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
                started[1].r#type,
                last.metadata.get("loop_end_reason").cloned(),
                segments.take_owed_record(),
            ),
            (
                (EventType::Message, "Let me check.".to_owned()),
                EventType::ToolCallStart,
                Some(json!("completed")),
                None,
            )
        );
    }

    // Python: tests/test_inbound_turns_record.py, an end the channel's record
    // names holds on a reader's stop (core/mixins/_streaming_segments.py end_stopped).
    #[test]
    fn an_end_the_record_names_holds_when_its_reader_stops() {
        // A null is no end, and no usage (Python's `turn_summary`).
        let live = ResponseMetadata::new();
        live.insert(LOOP_END_REASON, Value::Null);
        assert_eq!(segments().reading(live.clone()).turn_entry(), None);
        live.insert(LOOP_END_REASON, Value::from("interrupted"));
        live.insert(AI_USAGE, Value::Null);
        let entry = segments().reading(live).turn_entry().expect("an end");
        assert_eq!(entry.get(AI_USAGE), None);

        for (named, end) in [
            (Value::from("interrupted"), Some(Value::from("interrupted"))),
            (Value::from("completed"), Some(Value::from(CANCELLED))),
            (Value::Null, Some(Value::from(CANCELLED))),
        ] {
            let live = ResponseMetadata::new();
            live.insert(LOOP_END_REASON, named);
            let mut segments = segments().reading(live);
            segments.handed();
            segments.stopped();
            let entry = segments.turn_entry().expect("an end");
            assert_eq!(entry.get(LOOP_END_REASON).cloned(), end);
        }
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
    /// `failed`, and one whose start was never written gets its start first;
    /// each end keeps its call's intent (RFC §6.4).
    #[test]
    fn an_open_call_is_closed_failed_its_start_written_first_when_it_was_not() {
        let mut segments = segments();
        segments.handed();
        let mut first = ToolCallContent::new("lookup", "call_1");
        first.intent = Some("I look it up.".to_owned());
        segments.start(first);
        segments.opened_unwritten(ToolCallContent::new("lookup", "call_2"));
        let closed = segments.close_open("cancelled", |_| None);
        let kinds: Vec<(EventType, ToolCallStatus, Option<&str>)> = closed
            .iter()
            .map(|row| {
                let EventContent::ToolCall(call) = &row.content else {
                    unreachable!()
                };
                (row.r#type, call.status, call.intent.as_deref())
            })
            .collect();
        assert_eq!(
            kinds,
            [
                (
                    EventType::ToolCallEnd,
                    ToolCallStatus::Failed,
                    Some("I look it up.")
                ),
                (EventType::ToolCallStart, ToolCallStatus::Pending, None),
                (EventType::ToolCallEnd, ToolCallStatus::Failed, None),
            ]
        );
    }

    /// RFC §12.4: of two calls open under one id, the first is the one that
    /// ran: the arguments its gate fixed are not the second's.
    // Python: core/mixins/_response_reader.py _cut_end reads each start's own.
    #[test]
    fn a_second_call_open_under_an_id_keeps_its_own_arguments() {
        let mut segments = segments();
        segments.handed();
        let call = |q: &str| {
            let mut call = ToolCallContent::new("lookup", "c1");
            call.arguments.insert("q".to_owned(), json!(q));
            call
        };
        segments.start(call("first"));
        segments.start(call("second"));
        let closed = segments.close_open("cancelled", |_| {
            let mut fixed = Metadata::new();
            fixed.insert("q".to_owned(), json!("fixed"));
            Some(fixed)
        });
        let read: Vec<&Value> = closed
            .iter()
            .map(|row| {
                let EventContent::ToolCall(call) = &row.content else {
                    unreachable!()
                };
                &call.arguments["q"]
            })
            .collect();
        assert_eq!(read, [&json!("fixed"), &json!("second")]);
    }

    /// RFC §6.4 "The turn's record": a turn that ends on no text owes its
    /// record to the last message it wrote.
    #[test]
    fn a_turn_ending_on_no_text_owes_its_record_to_its_last_message() {
        let mut segments = segments();
        segments.handed();
        segments.text("Checking.");
        let mut rows = segments.start(ToolCallContent::new("lookup", "call_1"));
        let mut message = rows.remove(0);
        message.id = EventId::new("evt_2").unwrap();
        segments.stored(&message);
        segments.end(ToolCallContent::new("lookup", "call_1"));
        segments.end_turn(record("empty_response"));
        assert_eq!(segments.last_text(false), None);
        let (id, record) = segments.take_owed_record().unwrap();
        assert_eq!(
            (id.as_str(), record.get("loop_end_reason").cloned()),
            ("evt_2", Some(json!("empty_response")))
        );
    }
}
