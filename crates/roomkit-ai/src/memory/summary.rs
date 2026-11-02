//! The summary a summarizing memory puts in place of the events it
//! summarized, shared by [`SummarizingMemory`](super::SummarizingMemory)
//! and [`CompactingMemory`](super::CompactingMemory) (RFC §6.4, §20.3):
//! Python's `roomkit.memory._summary`.

use std::collections::{BTreeSet, HashMap};

use roomkit::model::{
    ChannelId, ChannelType, EventId, EventType, Metadata, RoomContext, RoomEvent,
};
use roomkit_core::rules::transcript::{
    RUNTIME_RECORD, SUMMARY_HEADER, fence, people_count, quoted, turn_labels, turn_sources,
    without_mark_copies,
};
use serde_json::Value;

use super::token_estimator::extract_event_text;
use crate::provider::{AiMessage, AiMessageContent, AiRole};

/// Characters of one event the summarizer reads.
const EVENT_TEXT_LIMIT: usize = 2000;

/// The tag the summary is fenced in.
const CONVERSATION_SUMMARY_TAG: &str = "conversation_summary";

/// The provenance a summary's message carries.
const SUMMARY_KIND: &str = "summary";

/// What the summarizer is told it is.
pub(super) const SUMMARIZER_PROMPT: &str =
    "You are a conversation summarizer. Be concise and factual.";

/// The lines a summarizer reads for the turns it summarizes, for the agent
/// of `channel_id` answering `current` in the room of `context`, with
/// `kept`, the turns the memory keeps after the summary: each turn quoted
/// on one line after who said it, so that no turn can write a line of
/// another (RFC §6.4). Python's `SummaryLines`.
///
/// When the turns the memory retrieved and `current` hold several
/// speakers (a turn with no text, the agent's own and the application's
/// instruction aside), or several people take part, a participant's line
/// opens with its author's label, out of the quote, and `[assistant]`
/// names only this agent; otherwise a line reads `[assistant]` or
/// `[user]`.
#[derive(Clone, Debug)]
pub(super) struct SummaryLines<'a> {
    pub(super) context: &'a RoomContext,
    pub(super) current: Option<&'a RoomEvent>,
    pub(super) channel_id: Option<&'a ChannelId>,
    pub(super) kept: Vec<RoomEvent>,
}

impl SummaryLines<'_> {
    /// The lines for `events`, in their order.
    pub(super) fn lines(&self, events: &[RoomEvent]) -> Vec<String> {
        let labels = self.labels(events);
        events
            .iter()
            .map(|event| self.line(event, labels.get(&event.id).cloned().flatten()))
            .collect()
    }

    /// The labels the lines for `events` name participants by, sorted, for
    /// `MemoryResult::speakers`; none when they read `[user]`.
    pub(super) fn named(&self, events: &[RoomEvent]) -> Vec<String> {
        let labels = self.labels(events);
        let named: BTreeSet<String> = events
            .iter()
            .filter(|event| !self.own(event))
            .filter_map(|event| labels.get(&event.id).cloned().flatten())
            .collect();
        named.into_iter().collect()
    }

    /// Each turn's label, or none when the turns hold one speaker and
    /// fewer than two people take part.
    fn labels(&self, events: &[RoomEvent]) -> HashMap<EventId, Option<String>> {
        let mut turns: Vec<&RoomEvent> = Vec::new();
        for event in events.iter().chain(&self.kept).chain(self.current) {
            match turns.iter().position(|turn| turn.id == event.id) {
                // Python's dict keeps the first place and the last value.
                Some(at) => {
                    if let Some(turn) = turns.get_mut(at) {
                        *turn = event;
                    }
                }
                None => turns.push(event),
            }
        }
        let labels = turn_labels(turns.iter().copied(), self.context);
        // Speakers are counted by source (RFC §6.4): two nameless senders
        // of one channel are two, one sender under two names one.
        let sources = turn_sources(turns.iter().copied(), self.context);
        let counted: BTreeSet<&str> = turns
            .iter()
            .zip(&sources)
            .filter(|(turn, _)| self.counts(turn))
            .filter_map(|(_, source)| source.as_deref())
            .collect();
        let people = self
            .channel_id
            .map_or(0, |channel_id| people_count(self.context, channel_id));
        if people < 2 && counted.len() < 2 {
            return HashMap::new();
        }
        turns
            .iter()
            .map(|turn| turn.id.clone())
            .zip(labels)
            .collect()
    }

    /// Whether `event` counts toward the threshold, as the conversation's
    /// turns do.
    fn counts(&self, event: &RoomEvent) -> bool {
        if self.own(event) || event.r#type == EventType::Instruction {
            return false;
        }
        !extract_event_text(event).trim().is_empty()
    }

    /// Whether `event` is the summarized agent's own turn.
    fn own(&self, event: &RoomEvent) -> bool {
        event.source.channel_type == ChannelType::AI
            && self
                .channel_id
                .is_none_or(|channel_id| &event.source.channel_id == channel_id)
    }

    /// `event`'s line, after `label` when it has one and is not the
    /// agent's own.
    fn line(&self, event: &RoomEvent, label: Option<String>) -> String {
        let mut text = extract_event_text(event);
        // A copy of a runtime mark the summarizer could carry into the
        // summary is replaced at the source; the runtime's own records keep
        // theirs.
        if !written_by_runtime(&event.metadata) {
            text = without_mark_copies(&text);
        }
        let text = quoted(&text, EVENT_TEXT_LIMIT);
        match label {
            Some(label) if !self.own(event) => format!("{label}: {text}"),
            Some(_) | None => {
                let role = if event.source.channel_type == ChannelType::AI {
                    "assistant"
                } else {
                    "user"
                };
                format!("[{role}]: {text}")
            }
        }
    }
}

/// Whether `metadata`, an event's or a message's, says the runtime wrote
/// it.
fn written_by_runtime(metadata: &Metadata) -> bool {
    matches!(metadata.get(RUNTIME_RECORD), Some(Value::String(_)))
}

/// The message that stands for the summarized events: the summary header,
/// then `summary` fenced as data, a model's rewriting of what people said,
/// each copy of a runtime mark in it replaced. Python's `summary_message`.
pub(super) fn summary_message(summary: &str) -> AiMessage {
    let cleaned = without_mark_copies(summary);
    let mut message = AiMessage::new(
        AiRole::User,
        format!(
            "{SUMMARY_HEADER}\n{}",
            fence(CONVERSATION_SUMMARY_TAG, &cleaned)
        ),
    );
    message
        .metadata
        .insert(RUNTIME_RECORD.to_owned(), Value::from(SUMMARY_KIND));
    message
}

/// Whether `message` is a summary a memory of the runtime built: by its
/// provenance alone, never by its header, which anyone can type (RFC
/// §6.4). Python's `is_summary`.
pub(super) fn is_summary(message: &AiMessage) -> bool {
    message.metadata.get(RUNTIME_RECORD) == Some(&Value::from(SUMMARY_KIND))
}

/// The text of a prior summary among `messages`, if any.
pub(super) fn prior_summary(messages: &[AiMessage]) -> Option<String> {
    messages
        .iter()
        .find(|message| is_summary(message))
        .map(|message| match &message.content {
            AiMessageContent::Text(text) => text.clone(),
            AiMessageContent::Parts(_) => message.content.text(),
        })
}

/// What the summary reads when the summarizer failed on `count` events.
pub(super) fn unavailable(count: usize) -> String {
    format!("[Earlier conversation with {count} messages \u{2014} summary unavailable]")
}

/// What the summary reads when the summarizer answered nothing.
pub(super) const SUMMARY_FAILED: &str = "[Summary generation failed]";

#[cfg(test)]
mod tests {
    use roomkit::model::ParticipantId;

    use super::SummaryLines;
    use crate::memory::testing::{context, event};

    // Python: tests/test_people_taking_part.py::TestEveryReaderCountsSources::test_two_senders_with_no_name_on_one_channel_are_labelled
    #[test]
    fn two_nameless_senders_of_one_channel_label_the_summary_lines() {
        let context = context();
        let mut first = event("refund me", "e1");
        first.source.participant_id = Some(ParticipantId::new("+15550001").unwrap());
        let mut second = event("do not refund them", "e2");
        second.source.participant_id = Some(ParticipantId::new("+15550002").unwrap());
        let lines = SummaryLines {
            context: &context,
            current: None,
            channel_id: None,
            kept: Vec::new(),
        };
        let read = lines.lines(&[first, second]);
        assert!(read[0].starts_with("@ch-1: "), "{read:?}");
        assert!(read[1].starts_with("@ch-1 (2): "), "{read:?}");
    }
}
