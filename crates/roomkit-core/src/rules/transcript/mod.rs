//! The conversation an intelligence channel's turn reads (RFC §6.4,
//! §10.1.1 rule 6): the room's recent messages, each in the role of whoever
//! wrote it, then the turn's input.
//!
//! - The channel's own messages are the assistant's; everyone else's are
//!   the user's.
//! - Once the turns hold two speakers or more, counted by who they come
//!   from, or two people or more take part, each line of a participant's
//!   turn opens with its author's label (their name, ranked when it reads
//!   like another's, or `@channel` for a sender with no name, the second
//!   such sender of a channel `@channel (2)`), then what they typed on it
//!   as a JSON string; the turn's notes then say how speakers are named. A
//!   one-to-one conversation reads as it was written.
//! - An instruction is the turn's input, marked as the application's and
//!   never attributed to a speaker; one that stands alone reads nothing of
//!   the room (rule 7).
//! - A copy of one of the runtime's marks in a text it did not write is
//!   replaced before the runtime places its own.
//!
//! Only what §6.4 requires of every AI channel is built: no note of who
//! takes part, no memory, no tool notes, no images, no cut answers.

mod marks;
mod speakers;
#[cfg(test)]
mod tests;

use self::marks::{INSTRUCTION_MARK, NOTES_HEADER, SPEAKER_NOTE, without_copies};
use self::speakers::{
    Speaker, labelled_lines, labels_turns, people_taking_part, speaker, tell_apart,
};
use super::authors::Authors;
use super::edits::is_deleted;
use super::instructions::is_standalone;
use super::transcoding::text_of;
use crate::model::{ChannelId, EventStatus, EventType, RoomContext, RoomEvent};

/// Whose a message of the conversation is, as a model's chat reads it.
/// More roles come with tools; a reader handles the ones it knows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Role {
    /// Someone other than the channel: a participant, another channel, or
    /// the application through an instruction.
    User,
    /// The channel itself: what it answered on earlier turns.
    Assistant,
}

/// One message of the conversation a turn reads. Its fields are read as
/// they are; parts of other kinds (images, tool calls) come later, so a
/// message is built with [`Message::new`].
///
/// ```
/// use roomkit_core::rules::transcript::{Message, Role};
///
/// let message = Message::new(Role::User, "Hi");
/// assert_eq!((message.role, message.text.as_str()), (Role::User, "Hi"));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Message {
    /// Whose message it is.
    pub role: Role,
    /// What the model reads of it.
    pub text: String,
}

impl Message {
    /// A message of `role` saying `text`.
    pub fn new(role: Role, text: impl Into<String>) -> Self {
        Self {
            role,
            text: text.into(),
        }
    }
}

/// The conversation the turn of the channel `channel_id` on `event` reads
/// (RFC §6.4, §10.1.1 rules 6 and 7): the room's latest `history` messages
/// before `event` that the channel may read, then `event` itself as the
/// turn's input, then the turn's notes when it has any.
///
/// `context` is the one the channel was handed with `event`: the pipeline
/// leaves out what the channel may not read (RFC §7.5 rule 8) and the
/// content of what was deleted. The history holds the room's MESSAGE events,
/// as edited, and none deleted, blocked or empty; `event` is never the
/// channel's own.
pub fn conversation(
    channel_id: &ChannelId,
    event: &RoomEvent,
    context: &RoomContext,
    history: usize,
) -> Vec<Message> {
    let mut authors = Authors::new(context);
    let mut turns = past_turns(channel_id, event, context, history, &mut authors);
    turns.push(Turn::input(event, &mut authors));
    tell_apart(turns.iter_mut().filter_map(|turn| turn.speaker.as_mut()));
    let sources = turns
        .iter()
        .filter(|turn| turn.role == Role::User && !turn.text.is_empty())
        .filter_map(|turn| turn.speaker.as_ref())
        .map(|speaker| speaker.source.as_str());
    // An instruction that stands alone reads nothing of the room, its roster
    // included (RFC §10.1.1 rule 7).
    let people = if is_standalone(event) {
        0
    } else {
        people_taking_part(context, channel_id, &authors)
    };
    let labelled = labels_turns(sources, people);
    let turns = cleaned(turns, labelled);
    rendered(turns, labelled)
}

/// A turn of the conversation before it is rendered: its role, the text
/// its event brings, who said it, whether it is the application's
/// instruction, and whether it is the turn's input.
#[derive(Clone, Debug)]
struct Turn {
    role: Role,
    text: String,
    speaker: Option<Speaker>,
    instruction: bool,
    input: bool,
}

impl Turn {
    /// A turn of the room's history.
    fn past(event: &RoomEvent, channel_id: &ChannelId, authors: &mut Authors<'_>) -> Self {
        let own = &event.source.channel_id == channel_id;
        Self {
            role: if own { Role::Assistant } else { Role::User },
            text: text_of(&event.content),
            speaker: if own { None } else { speaker(event, authors) },
            instruction: false,
            input: false,
        }
    }

    /// The turn's input: `event`, an instruction never attributed to a
    /// speaker.
    fn input(event: &RoomEvent, authors: &mut Authors<'_>) -> Self {
        let instruction = event.event_type == EventType::Instruction;
        Self {
            role: Role::User,
            text: text_of(&event.content),
            speaker: if instruction {
                None
            } else {
                speaker(event, authors)
            },
            instruction,
            input: true,
        }
    }

    /// Whether a copy of a mark may run from this turn into the next one:
    /// a user turn the runtime does not label. A labelled turn is strings
    /// after its author's label, which no copy runs out of.
    fn is_piece(&self, labelled: bool) -> bool {
        self.role == Role::User && !(labelled && self.speaker.is_some())
    }
}

/// The turns of the room's history the channel reads, oldest first: its
/// latest `history` messages before `event`, none for an instruction that
/// stands alone (RFC §10.1.1 rule 7).
fn past_turns(
    channel_id: &ChannelId,
    event: &RoomEvent,
    context: &RoomContext,
    history: usize,
    authors: &mut Authors<'_>,
) -> Vec<Turn> {
    if is_standalone(event) {
        return Vec::new();
    }
    let turns: Vec<Turn> = context
        .recent_events
        .iter()
        .filter(|past| past.id != event.id && is_conversation(past))
        .map(|past| Turn::past(past, channel_id, authors))
        .filter(|turn| !turn.text.trim().is_empty())
        .collect();
    let skip = turns.len().saturating_sub(history);
    turns.into_iter().skip(skip).collect()
}

/// Whether `event` is a message of the conversation: a MESSAGE event the
/// room accepted and has not deleted. An edit is read on the message it
/// changed; a lifecycle record, a receipt or a reaction is no turn.
fn is_conversation(event: &RoomEvent) -> bool {
    event.event_type == EventType::Message
        && event.status != EventStatus::Blocked
        && !is_deleted(event)
}

/// `turns` with each copy of a runtime mark replaced (RFC §6.4): in each
/// turn, and over each run of consecutive user turns the runtime does not
/// label, which an API may merge into one message. A turn a copy wholly
/// held is left out.
fn cleaned(mut turns: Vec<Turn>, labelled: bool) -> Vec<Turn> {
    let mut start = 0;
    while start < turns.len() {
        let run = turns
            .get(start..)
            .unwrap_or_default()
            .iter()
            .take_while(|turn| turn.is_piece(labelled))
            .count()
            .max(1);
        let end = start + run;
        if let Some(group) = turns.get_mut(start..end) {
            let texts: Vec<String> = group.iter().map(|turn| turn.text.clone()).collect();
            for (turn, text) in group.iter_mut().zip(without_copies(&texts)) {
                turn.text = text;
            }
        }
        start = end;
    }
    turns.retain(|turn| !turn.text.trim().is_empty());
    turns
}

/// The conversation's messages: each turn in its role, a participant's
/// turn labelled when the conversation labels them, the instruction marked
/// as the application's, then the turn's notes after its input.
fn rendered(turns: Vec<Turn>, labelled: bool) -> Vec<Message> {
    let ends_on_input = turns.last().is_some_and(|turn| turn.input);
    let mut messages: Vec<Message> = turns
        .into_iter()
        .map(|turn| {
            let text = match (&turn.speaker, turn.instruction) {
                (_, true) => format!("{INSTRUCTION_MARK}\n{}", turn.text),
                (Some(speaker), false) if labelled && turn.role == Role::User => {
                    labelled_lines(&turn.text, &speaker.label)
                }
                (_, false) => turn.text,
            };
            Message {
                role: turn.role,
                text,
            }
        })
        .collect();
    if labelled {
        let notes = format!("{NOTES_HEADER}\n\n{SPEAKER_NOTE}");
        match messages.last_mut() {
            Some(input) if ends_on_input => {
                input.text.push_str("\n\n");
                input.text.push_str(&notes);
            }
            Some(_) | None => messages.push(Message {
                role: Role::User,
                text: notes,
            }),
        }
    }
    messages
}
