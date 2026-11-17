//! Who asked for the task a strategy hands a model (RFC §6.4, §19.7): the
//! label the conversation gives a participant whose own words a strategy
//! quotes as the task. Python's `core/_requester.py` `asking_label`.

use super::speakers::{Speaker, labels_turns, people_taking_part, speaker, tell_apart};
use crate::model::{ChannelId, EventType, RoomContext, RoomEvent};
use crate::rules::authors::Authors;
use crate::rules::permissions::visible_events;
use crate::rules::transcoding::text_of;

/// The label of `event`'s author when the turns the agent `channel_id` may
/// read and `event` hold several speakers, or several people take part;
/// `None` in a one-to-one conversation, and for an event with no author.
/// Python's `asking_label`, what a strategy heads its task block with
/// (`Alice asked:`).
pub fn asking_label(
    event: &RoomEvent,
    context: &RoomContext,
    channel_id: &ChannelId,
) -> Option<String> {
    let window = visible_events(context, channel_id);
    let others = window
        .iter()
        .filter(|seen| seen.id != event.id && seen.source.channel_id != *channel_id)
        .filter(|seen| a_turn(seen));
    let mut authors = Authors::new(context);
    let mut speakers: Vec<(bool, Option<Speaker>)> = others
        .map(|seen| (true, speaker(seen, &mut authors)))
        .collect();
    speakers.push((a_turn(event), speaker(event, &mut authors)));
    tell_apart(speakers.iter_mut().filter_map(|(_, said)| said.as_mut()));
    let counted = speakers
        .iter()
        .filter(|(counts, _)| *counts)
        .filter_map(|(_, said)| said.as_ref())
        .map(|said| said.source.as_str());
    let several = labels_turns(counted, people_taking_part(context, channel_id).len());
    let (_, asker) = speakers.last()?;
    asker
        .as_ref()
        .filter(|_| several)
        .map(|said| said.label.clone())
}

/// Whether `event` is a turn the conversation counts: it has text, and is
/// neither a tool call's record nor the application's instruction.
fn a_turn(event: &RoomEvent) -> bool {
    !matches!(
        event.r#type,
        EventType::Instruction | EventType::ToolCallStart | EventType::ToolCallEnd
    ) && !text_of(&event.content).trim().is_empty()
}
