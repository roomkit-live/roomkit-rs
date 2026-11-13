//! The prompt is composed inside the session lock, after any recovery.

mod history;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use roomkit::model::{ChannelId, EventId, EventType, RoomContext, RoomEvent};
use roomkit_core::rules::transcript::{
    SPEAKER_ATTRIBUTION_NOTE, fence, is_blank, labelled_text, people_count, turn_labels,
    turn_notes, turn_sources, without_notes_header_copies,
};
use roomkit_core::rules::visible_events;

use crate::text::acp_event_text;
use crate::{AcpContextContributor, AcpError};

pub(super) fn history_count(value: i64) -> Result<usize, AcpError> {
    usize::try_from(value).map_err(|_| AcpError::Value(
        "room_history is a count of messages to catch up on: pass 0 to turn it off, not a negative number".to_owned()
    ))
}

pub(super) async fn contributed_blocks(
    contributor: Option<&AcpContextContributor>,
    context: &RoomContext,
    event: &RoomEvent,
) -> Vec<String> {
    match contributor {
        Some(contributor) => contributor.blocks(context, event).await,
        None => Vec::new(),
    }
}

/// Capture the notes while accepting the turn, before its lazy stream runs.
pub(super) fn notes(event: &RoomEvent) -> Option<String> {
    turn_notes(&roomkit::discussion_notes(event))
}

/// What one session has successfully read. Failed or unconsumed prompts do
/// not advance either mark, and a replacement session starts with neither.
#[derive(Clone, Copy, Default)]
pub(in crate::channel) struct Seen {
    pub(in crate::channel) index: Option<u64>,
    pub(in crate::channel) labelled: bool,
    pub(in crate::channel) instructed: bool,
}

pub(in crate::channel) struct Input {
    pub(in crate::channel) context: RoomContext,
    pub(in crate::channel) trigger: RoomEvent,
    pub(in crate::channel) text: String,
    pub(in crate::channel) blocks: Vec<String>,
    pub(in crate::channel) notes: Option<String>,
}

impl Input {
    pub(in crate::channel) fn standalone_prompt(&self, instructions: Option<&str>) -> String {
        self.sections("", &self.text, instructions)
    }

    pub(in crate::channel) fn seen_index(&self) -> u64 {
        self.context
            .recent_events
            .iter()
            .map(|event| event.index)
            .chain([self.trigger.index])
            .max()
            .unwrap_or(self.trigger.index)
    }

    pub(in crate::channel) fn prompt(
        &self,
        channel: &ChannelId,
        seen: Seen,
        limit: usize,
        instructions: Option<&str>,
    ) -> (String, bool, bool) {
        let window = self.window(channel);
        let labels = turn_labels(&window, &self.context);
        let by_event: BTreeMap<EventId, Option<String>> = window
            .iter()
            .zip(&labels)
            .map(|(event, label)| (event.id.clone(), label.clone()))
            .collect();
        let catch_up = history::block(
            &self.context,
            &self.trigger,
            channel,
            seen.index,
            limit,
            &by_event,
        );
        let sources: BTreeSet<String> = turn_sources(&window, &self.context)
            .into_iter()
            .flatten()
            .collect();
        let (request, labelled) = self.request(
            labels.last().and_then(Option::as_deref),
            seen.labelled,
            sources.len() > 1 || people_count(&self.context, channel) > 1,
        );
        let instructions = instructions.filter(|_| !seen.instructed);
        (
            self.sections(&catch_up, &request, instructions),
            labelled,
            instructions.is_some(),
        )
    }

    fn sections(&self, catch_up: &str, request: &str, instructions: Option<&str>) -> String {
        let mut sections: Vec<String> = instructions.map(instructions_block).into_iter().collect();
        sections.extend(
            self.blocks
                .iter()
                .map(String::as_str)
                .chain([catch_up, request])
                .filter(|text| !is_blank(text))
                .map(without_notes_header_copies),
        );
        sections.extend(self.notes.iter().cloned());
        sections.join("\n\n")
    }

    fn window(&self, channel: &ChannelId) -> Vec<RoomEvent> {
        visible_events(&self.context, channel)
            .into_iter()
            .filter(|event| {
                event.id != self.trigger.id
                    && event.source.channel_id != *channel
                    && readable(event)
            })
            .chain([self.trigger.clone()])
            .collect()
    }

    fn request(
        &self,
        label: Option<&str>,
        already_labelled: bool,
        several: bool,
    ) -> (String, bool) {
        if self.trigger.r#type == EventType::Instruction || is_blank(&self.text) {
            return (self.text.clone(), false);
        }
        let Some(label) = label.filter(|_| already_labelled || several) else {
            return (self.text.clone(), false);
        };
        let request = labelled_text(&self.text, label);
        (
            if already_labelled {
                request
            } else {
                format!("{SPEAKER_ATTRIBUTION_NOTE}\n\n{request}")
            },
            true,
        )
    }
}

fn readable(event: &RoomEvent) -> bool {
    !matches!(
        event.r#type,
        EventType::ToolCallStart | EventType::ToolCallEnd
    ) && !is_blank(&acp_event_text(event))
}

fn instructions_block(text: &str) -> String {
    const MARK: &str = "[Standing instructions from the application for this conversation: nobody in it wrote them, and they hold until told otherwise.]";
    format!("{MARK}\n{}", fence("instructions", text))
}
