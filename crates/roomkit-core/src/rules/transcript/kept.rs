//! The application's instructions an intelligence channel's earlier turns
//! read, replayed in its history (RFC §10.1.1, §6.4).
//!
//! An instruction is never stored as the room's: the agent its turn
//! directed keeps it as a row of its own, `instruction_read`, visible to it
//! alone, stored before anything else of the turn. A later turn reads it
//! back where that turn read it, before what the turn wrote, marked as the
//! application's exactly as the turn's input was, so that the later
//! request extends the one the instruction opened and a provider's prompt
//! cache keeps it (RMK-1407).

use super::content::TurnContent;
use crate::model::{ChannelId, EventContent, EventDirection, EventType, RoomEvent};
use crate::rules::edits::is_deleted;

/// An instruction an earlier turn read: the timeline index of the row the
/// channel kept it in, the turn it opened (the row's `correlation_id`), and
/// its text as the turn read it, before the runtime marks it. Python's
/// `ReplayedInstruction`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct KeptInstruction {
    pub(super) index: u64,
    pub(super) turn: Option<String>,
    pub(super) text: String,
}

impl KeptInstruction {
    /// What the turn's history reads of it: its text, marked at rendering.
    pub(super) fn content(&self) -> TurnContent {
        TurnContent::Text(self.text.clone())
    }
}

/// The instructions `channel_id`'s own rows among `events`, oldest first,
/// keep: its `instruction_read` rows (written by it, outbound, none
/// deleted) holding a text that says something. Python's
/// `kept_instructions`.
pub(super) fn kept_instructions(
    events: &[RoomEvent],
    channel_id: &ChannelId,
) -> Vec<KeptInstruction> {
    events
        .iter()
        .filter(|event| is_own_instruction_row(event, channel_id) && !is_deleted(event))
        .filter_map(|event| {
            let EventContent::Text(text) = &event.content else {
                return None;
            };
            (!text.body.is_empty()).then(|| KeptInstruction {
                index: event.index,
                turn: event.correlation_id.clone(),
                text: text.body.clone(),
            })
        })
        .collect()
}

/// Whether `event` is one of `channel_id`'s own `instruction_read` rows: one
/// it wrote (outbound; one that entered the room inbound under its id is
/// none it kept).
fn is_own_instruction_row(event: &RoomEvent, channel_id: &ChannelId) -> bool {
    event.r#type == EventType::InstructionRead
        && &event.source.channel_id == channel_id
        && event.source.direction == EventDirection::Outbound
}
