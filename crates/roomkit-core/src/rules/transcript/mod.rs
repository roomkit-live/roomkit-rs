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
//! - Text from outside that a channel sets apart in a block of its own, as
//!   the instructions an MCP server publishes, is framed by [`fence`], so
//!   that it cannot close its block.
//!
//! - The channel's earlier rounds of tool calls are replayed at their
//!   place, after the input of the turn that made them and before its
//!   answer (RFC §6.4 "The tools of earlier turns"): the assistant's
//!   message making the calls, then a tool message per result, as the turn
//!   itself sent them, read from the channel's stored rows. A round older
//!   than the history's first message is left out, so that the history
//!   never opens on the assistant's calls.
//!
//! Only what §6.4 requires of every AI channel is built: no note of who
//! takes part, no memory, no tool notes, no cut answers. Author images are bounded and
//! carried separately; the runtime resolves opaque references only for vision providers.
//!
//! Within a turn, the tool loop extends the conversation with its rounds
//! (RFC §6.4, Appendix A.9 `ai_message_parts`): the assistant's message that
//! made calls ([`Message::assistant_calls`]), then a tool message per result
//! ([`Message::tool_result`]). A later turn's history replays them the same
//! way.

mod fence;
mod images;
pub use images::Image;
mod marks;
mod rounds;
mod speakers;
#[cfg(test)]
mod tests;

pub use self::fence::fence;
use self::marks::{INSTRUCTION_MARK, NOTES_HEADER, SPEAKER_NOTE, without_copies};
use self::rounds::{ToolRound, tool_rounds};
use self::speakers::{
    Speaker, labelled_lines, labels_turns, people_taking_part, speaker, tell_apart,
};
use super::authors::Authors;
use super::edits::is_deleted;
use super::instructions::is_standalone;
use super::transcoding::text_of;
use crate::model::{ChannelId, EventStatus, EventType, RoomContext, RoomEvent, ToolCall};

/// Whose a message of the conversation is, as a model's chat reads it; a
/// reader handles the ones it knows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Role {
    /// Someone other than the channel: a participant, another channel, or
    /// the application through an instruction.
    User,
    /// The channel itself: what it answered on earlier turns, and the calls
    /// a round made, within its turn and replayed in later turns' history.
    Assistant,
    /// A tool's result, within the turn whose round called it and replayed
    /// in later turns' history.
    Tool,
}

/// One message of the conversation a turn reads. Its fields are read as
/// they are; parts of other kinds (images) come later, so a message is
/// built with [`Message::new`], [`Message::assistant_calls`] or
/// [`Message::tool_result`].
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
    /// Bounded author images, kept separate from text (RFC §6.4).
    pub images: Vec<Image>,
    /// The calls an assistant's message makes, in the order its round made
    /// them; none on any other.
    pub tool_calls: Vec<ToolCall>,
    /// The call a tool's result answers; `None` on any other message.
    pub tool_call_id: Option<String>,
    /// Provider-owned data carried within the current tool loop (RFC §6.7,
    /// `AIMessage.metadata`), separate from visible text and room history.
    pub metadata: crate::model::Metadata,
}

impl Message {
    /// A message of `role` saying `text`.
    pub fn new(role: Role, text: impl Into<String>) -> Self {
        Self {
            role,
            text: text.into(),
            images: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            metadata: crate::model::Metadata::new(),
        }
    }

    /// The assistant's message of a round that said `text`, empty when it
    /// said nothing, and made `calls` (RFC §6.4).
    ///
    /// ```
    /// use roomkit_core::model::{Metadata, ToolCall};
    /// use roomkit_core::rules::transcript::{Message, Role};
    ///
    /// let call = ToolCall::new("call_1", "lookup_order", Metadata::new());
    /// let round = Message::assistant_calls("Let me check.", vec![call]);
    /// let result = Message::tool_result("call_1", "shipped");
    /// assert_eq!((round.role, result.role), (Role::Assistant, Role::Tool));
    /// assert_eq!(result.tool_call_id.as_deref(), Some("call_1"));
    /// ```
    pub fn assistant_calls(text: impl Into<String>, calls: Vec<ToolCall>) -> Self {
        Self {
            tool_calls: calls,
            ..Self::new(Role::Assistant, text)
        }
    }

    /// The result of the call `call_id`, as the model reads it.
    pub fn tool_result(call_id: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            tool_call_id: Some(call_id.into()),
            ..Self::new(Role::Tool, text)
        }
    }
}

/// The conversation the turn of the channel `channel_id` on `event` reads
/// (RFC §6.4, §10.1.1 rules 6 and 7): the room's latest `history` messages
/// before `event` that the channel may read, the channel's earlier rounds
/// of tool calls at their place among them, then `event` itself as the
/// turn's input, then the turn's notes when it has any.
///
/// `context` is the one the channel was handed with `event`: the pipeline
/// leaves out what the channel may not read (RFC §7.5 rule 8) and the
/// content of what was deleted. The history holds the room's MESSAGE events,
/// as edited, and none deleted, blocked or empty; `event` is never the
/// channel's own.
///
/// The rounds replayed are read from the channel's own tool rows among the
/// context's events (RFC §6.4 "The tools of earlier turns"): each call's
/// arguments as the model wrote them (its start row's) and the result it
/// read (its end row's), for the calls a tool answered (served, failed or
/// blocked) to a tool `replayed` says is work the agent did, never a
/// housekeeping tool of its own. The last 8 such calls are replayed, the 3
/// most recent with their result whole up to 6,000 characters, the older
/// ones a preview. A round at or after `event`'s place, or before the
/// history's first message, is left out; an instruction that stands alone
/// replays none.
pub fn conversation(
    channel_id: &ChannelId,
    event: &RoomEvent,
    context: &RoomContext,
    history: usize,
    replayed: impl Fn(&str) -> bool,
) -> Vec<Message> {
    let mut authors = Authors::new(context);
    let mut turns = past_turns(channel_id, event, context, history, &mut authors);
    if !turns.is_empty() {
        let rounds = tool_rounds(&context.recent_events, channel_id, &replayed);
        place(&mut turns, rounds, rounds_before(event));
    }
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
    let mut messages = rendered(turns, labelled);
    images::bound(&mut messages);
    messages
}

/// A turn of the conversation before it is rendered: its role, the text
/// its event brings, who said it, whether it is the application's
/// instruction, whether it is the turn's input, its event's place in the
/// timeline, and the rounds of tool calls read after it.
#[derive(Clone, Debug)]
struct Turn {
    role: Role,
    images: Vec<Image>,
    text: String,
    speaker: Option<Speaker>,
    instruction: bool,
    input: bool,
    index: u64,
    rounds: Vec<ToolRound>,
}

impl Turn {
    /// A turn of the room's history.
    fn past(event: &RoomEvent, channel_id: &ChannelId, authors: &mut Authors<'_>) -> Self {
        let own = &event.source.channel_id == channel_id;
        Self {
            role: if own { Role::Assistant } else { Role::User },
            images: if own {
                Vec::new()
            } else {
                images::collect(&event.content)
            },
            text: text_of(&event.content),
            speaker: if own { None } else { speaker(event, authors) },
            instruction: false,
            input: false,
            index: event.index,
            rounds: Vec::new(),
        }
    }

    /// The turn's input: `event`, an instruction never attributed to a
    /// speaker.
    fn input(event: &RoomEvent, authors: &mut Authors<'_>) -> Self {
        let instruction = event.r#type == EventType::Instruction;
        Self {
            role: Role::User,
            images: if instruction {
                Vec::new()
            } else {
                images::collect(&event.content)
            },
            text: text_of(&event.content),
            speaker: if instruction {
                None
            } else {
                speaker(event, authors)
            },
            instruction,
            input: true,
            index: event.index,
            rounds: Vec::new(),
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
        .filter(|turn| !turn.text.trim().is_empty() || !turn.images.is_empty())
        .collect();
    let skip = turns.len().saturating_sub(history);
    turns.into_iter().skip(skip).collect()
}

/// Where the rounds a turn on `event` replays end: before its event. Index
/// 0 is an event the store never numbered: nothing came before it.
fn rounds_before(event: &RoomEvent) -> Option<u64> {
    (event.index > 0).then_some(event.index)
}

/// `rounds`, each read after the last of `turns`, oldest first, that came
/// before it: none older than the first turn, so that the history never
/// opens on the assistant's calls, nor at or after `before`.
fn place(turns: &mut [Turn], rounds: Vec<ToolRound>, before: Option<u64>) {
    for round in rounds {
        if before.is_some_and(|before| round.index >= before) {
            continue;
        }
        if let Some(turn) = turns.iter_mut().rev().find(|turn| turn.index < round.index) {
            turn.rounds.push(round);
        }
    }
}

/// Whether `event` is a message of the conversation: a MESSAGE event the
/// room accepted and has not deleted. An edit is read on the message it
/// changed; a lifecycle record, a receipt or a reaction is no turn.
fn is_conversation(event: &RoomEvent) -> bool {
    event.r#type == EventType::Message && event.status != EventStatus::Blocked && !is_deleted(event)
}

/// `turns` with each copy of a runtime mark replaced (RFC §6.4): in each
/// turn, and over each run of consecutive user turns the runtime does not
/// label, which an API may merge into one message; a replayed round ends a
/// run, and is never rewritten. A turn a copy wholly held is left out, the
/// rounds read after it then read after the turn before it.
fn cleaned(mut turns: Vec<Turn>, labelled: bool) -> Vec<Turn> {
    let mut start = 0;
    while start < turns.len() {
        let rest = turns.get(start..).unwrap_or_default();
        let pieces = rest.iter().take_while(|turn| turn.is_piece(labelled));
        // A run ends with the first of its turns a round is read after.
        let run = pieces
            .clone()
            .position(|turn| !turn.rounds.is_empty())
            .map_or_else(|| pieces.count(), |last| last + 1)
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
    let mut kept: Vec<Turn> = Vec::with_capacity(turns.len());
    for turn in turns {
        if !turn.text.trim().is_empty() || !turn.images.is_empty() {
            kept.push(turn);
        } else if let Some(before) = kept.last_mut() {
            before.rounds.extend(turn.rounds);
        }
    }
    kept
}

/// The conversation's messages: each turn in its role, a participant's
/// turn labelled when the conversation labels them, the instruction marked
/// as the application's, the rounds read after a turn following it, then
/// the turn's notes after its input.
fn rendered(turns: Vec<Turn>, labelled: bool) -> Vec<Message> {
    let ends_on_input = turns.last().is_some_and(|turn| turn.input);
    let mut messages: Vec<Message> = Vec::with_capacity(turns.len());
    for turn in turns {
        let text = match (&turn.speaker, turn.instruction) {
            (_, true) => format!("{INSTRUCTION_MARK}\n{}", turn.text),
            (Some(speaker), false) if labelled && turn.role == Role::User => {
                labelled_lines(&turn.text, &speaker.label)
            }
            (_, false) => turn.text,
        };
        let mut message = Message::new(turn.role, text);
        message.images = turn.images;
        messages.push(message);
        messages.extend(turn.rounds.into_iter().flat_map(|round| round.messages));
    }
    if labelled {
        let notes = format!("{NOTES_HEADER}\n\n{SPEAKER_NOTE}");
        match messages.last_mut() {
            Some(input) if ends_on_input => {
                input.text.push_str("\n\n");
                input.text.push_str(&notes);
            }
            Some(_) | None => messages.push(Message::new(Role::User, notes)),
        }
    }
    messages
}
