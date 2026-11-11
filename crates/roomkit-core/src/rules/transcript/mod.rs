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
//!   as a JSON string; the turn's notes then say how speakers are named,
//!   and, when two people or more take part, name them, silent ones
//!   included. A one-to-one conversation reads as it was written.
//! - An instruction is the turn's input, marked as the application's and
//!   never attributed to a speaker; one that stands alone reads nothing of
//!   the room (rule 7). The agent keeps the one its turn read as a row of
//!   its own, and its later turns replay it where that turn read it, marked
//!   the same way, before what the turn wrote.
//! - A copy of one of the runtime's marks in a text it did not write is
//!   replaced before the runtime places its own.
//! - Text from outside that a channel sets apart in a block of its own, as
//!   the instructions an MCP server publishes, is framed by [`fence`], so
//!   that it cannot close its block.
//!
//! - The channel's earlier rounds of tool calls are replayed at their
//!   place, after the input of the turn that made them and before its
//!   answer (RFC §6.4 "The tools of earlier turns"): the assistant's
//!   message making the calls, the text the model wrote with them first,
//!   then a tool message per result, as the turn itself sent them, read
//!   from the channel's stored rows. A round older than the history's first
//!   message is left out, so that the history never opens on the
//!   assistant's calls.
//!
//! - What a turn's memory adds beside its history (RFC §20.2,
//!   [`TurnMemory`]): the notes it retrieved for the turn alone, after the
//!   runtime's own in the turn's notes, and the labels its messages name
//!   participants by, which count toward labelling the turns.
//!
//! Only what §6.4 requires of every AI channel is built: no tool notes, no
//! cut answers. What each event says is the channel's to read
//! ([`TurnContent`]): an AI channel reads an author's media itself, as an
//! image for a model that sees or a mention in its place (RFC §6.4);
//! [`conversation`] reads each event's text form.
//!
//! Within a turn, the tool loop extends the conversation with its rounds
//! (RFC §6.4, Appendix A.9 `ai_message_parts`): the assistant's message that
//! made calls ([`Message::assistant_calls`]), then a tool message per result
//! ([`Message::tool_result`]). A later turn's history replays them the same
//! way.

use std::collections::BTreeSet;

mod content;
mod data_uri;
mod fence;
mod images;
mod kept;
pub use self::content::{Part, TurnContent};
pub use self::data_uri::{DataUriError, parse_data_uri, to_data_uri};
pub use images::{Image, media_type};
mod marks;
mod quote;
mod rounds;
mod speakers;
/// Whether `text` says nothing, as Python's `not text.strip()` reads it:
/// empty, or Unicode's white space and the information separators `\x1c` to
/// `\x1f` alone. A blank text is no message (RFC §6.4).
pub fn is_blank(text: &str) -> bool {
    text.chars().all(rounds::is_python_space)
}

#[cfg(test)]
mod tests;

pub use self::fence::fence;
#[doc(hidden)]
pub use self::fence::named_blocks;
pub use self::fence::without_tags;
use self::kept::{KeptInstruction, kept_instructions};
#[doc(hidden)]
pub use self::marks::COMPACTION_HEADER;
pub use self::marks::SPEAKER_NOTE as SPEAKER_ATTRIBUTION_NOTE;
pub use self::marks::SUMMARY_HEADER;
use self::marks::{
    INSTRUCTION_MARK, NOTES_HEADER, SPEAKER_NOTE, without_copies, without_header_copies,
};
#[doc(hidden)]
pub use self::quote::FENCED_TAGS;
pub use self::quote::quoted;
use self::rounds::{ToolRound, tool_rounds};
use self::speakers::{Speaker, labelled_lines, labels_turns, speaker, tell_apart};
pub use self::speakers::{people_note, people_taking_part};
use super::authors::Authors;
use super::edits::{is_deleted, opens_no_turn};
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
    /// The application's words within the conversation: a message a host
    /// steers into a turn as `system` (RFC §21.3).
    System,
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
    /// Images, read after its text: what a provider reads of a message
    /// it rewrites from its parts. The transcript writes a message with
    /// images as `parts`.
    pub images: Vec<Image>,
    /// The message's content as parts, in order, when it is parts: an
    /// author's media as a model that sees reads it (RFC §6.4), each image
    /// at its place. `text` then holds its text parts joined and `images`
    /// its images, for a reader taking them apart. Empty for a text alone.
    pub parts: Vec<Part>,
    /// The calls an assistant's message makes, in the order its round made
    /// them; none on any other.
    pub tool_calls: Vec<ToolCall>,
    /// The call a tool's result answers; `None` on any other message.
    pub tool_call_id: Option<String>,
    /// Whether the model reads the call a tool's result answers as failed:
    /// Python's `AIToolResultPart.is_error`, true for every outcome but
    /// served (RFC §6.4, §9.3); false on any other message.
    pub is_error: bool,
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
            parts: Vec::new(),
            tool_calls: Vec::new(),
            tool_call_id: None,
            is_error: false,
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

    /// A message of `role` saying `content`: a text, or parts, its text
    /// parts joined in `text` and its images in `images`.
    fn of_content(role: Role, content: TurnContent) -> Self {
        match content {
            TurnContent::Text(text) => Self::new(role, text),
            TurnContent::Parts(parts) => {
                let mut text = String::new();
                let mut images = Vec::new();
                for part in &parts {
                    match part {
                        Part::Text(part) => text.push_str(part),
                        Part::Image(image) => images.push(image.clone()),
                    }
                }
                Self {
                    images,
                    parts,
                    ..Self::new(role, text)
                }
            }
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
/// Each event reads as its text form (RFC §6.6); [`conversation_with_memory`]
/// takes the channel's reading.
///
/// The rounds replayed are read from the channel's own tool rows among the
/// context's events (RFC §6.4 "The tools of earlier turns"), each as its
/// turn sent it: every call the turn ended, whatever its tool or its
/// outcome, but one cancelled before it answered and one refused under a
/// name vendors do not take, under the id the model
/// gave it, its arguments as the model wrote them (its start row's) and the
/// result it read whole (its end row's). A round right after a message of
/// its own turn, the text the model wrote with its calls, reads with it as
/// the one message the turn sent. A round at or after `event`'s place, or
/// before the history's first message, is left out; an instruction that
/// stands alone replays none.
pub fn conversation(
    channel_id: &ChannelId,
    event: &RoomEvent,
    context: &RoomContext,
    history: usize,
) -> Vec<Message> {
    let memory = TurnMemory::default();
    let read = |said: &RoomEvent| TurnContent::Text(text_of(&said.content));
    conversation_with_memory(channel_id, event, context, history, &memory, read)
}

/// The metadata key, on an event or a message a memory builds, naming what
/// the runtime wrote (`summary`, `handoff`): its marks are the runtime's
/// and are kept, where a copy of one anywhere else is replaced (RFC §6.4).
/// Only the runtime writes it: the inbound pipeline drops it from what a
/// sender supplies. Python's `RUNTIME_RECORD`.
pub const RUNTIME_RECORD: &str = "runtime_record";

/// What a turn's memory adds to the conversation beside the history it
/// retrieved (RFC §20.2): Python's `MemoryResult.notes` and `.speakers`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TurnMemory {
    /// What the memory retrieved for this turn alone (knowledge passages):
    /// blocks of the turn's notes, after the runtime's own, a copy of a
    /// runtime mark in them replaced.
    pub notes: Vec<String>,
    /// The labels the memory's messages name participants by (a summary's
    /// lines, `Alice`, `@sms1`): each counts as a speaker toward labelling
    /// the turns, unless a turn's speaker already has it.
    pub speakers: Vec<String>,
    /// The runtime's own notes for this turn alone, a discussion's (RFC
    /// §19.7.5 rule 14): blocks of the turn's notes, after the memory's, a
    /// copy of a runtime mark in them replaced.
    pub turn_notes: Vec<String>,
    /// A discussion's turn: its event reads at its place in the history,
    /// before what was said after it, rounds of tool calls included, rather
    /// than as the conversation's last message (RFC §19.7.5).
    pub in_place: bool,
}

/// [`conversation`], with what the turn's memory adds beside its history
/// (RFC §20.2): its `notes` ride the turn's notes, after the speaker note,
/// and its `speakers` count toward labelling the turns (RFC §6.4). Each
/// event says what `read` reads of it, history and input alike, an event
/// that reads as nothing left out: an AI channel's reading of its
/// authors' media (RFC §6.4). An instruction reads as its text form
/// (RFC §10.1.1).
pub fn conversation_with_memory(
    channel_id: &ChannelId,
    event: &RoomEvent,
    context: &RoomContext,
    history: usize,
    memory: &TurnMemory,
    read: impl Fn(&RoomEvent) -> TurnContent,
) -> Vec<Message> {
    let mut authors = Authors::new(context);
    let mut turns = past_turns(channel_id, event, context, history, &mut authors, &read);
    let input = Turn::input(event, &mut authors, &read);
    let in_place = memory.in_place && event.index > 0;
    // A standalone turn reads nothing of the room (RFC §10.1.1 rule 7). An input
    // read at its place, saying something, may be all the history a round reads
    // after; else a round with no turn before it opens no history.
    let earlier =
        if is_standalone(event) || (turns.is_empty() && (!in_place || input.content.is_blank())) {
            Earlier::default()
        } else {
            Earlier {
                rounds: tool_rounds(&context.recent_events, channel_id),
                instructions: kept_instructions(&context.recent_events, channel_id),
            }
        };
    if in_place {
        turns.push(input);
        // Told apart in the order spoken: the input read last, whatever its
        // place.
        tell_apart(turns.iter_mut().filter_map(|turn| turn.speaker.as_mut()));
        read_at_its_place(&mut turns, event.index);
        place(&mut turns, earlier, None);
    } else {
        place(&mut turns, earlier, rounds_before(event));
        turns.push(input);
        tell_apart(turns.iter_mut().filter_map(|turn| turn.speaker.as_mut()));
    }
    let speakers: Vec<&Speaker> = turns
        .iter()
        .filter(|turn| turn.role == Role::User && !turn.content.is_blank())
        .filter_map(|turn| turn.speaker.as_ref())
        .collect();
    let named = remembered_speakers(&speakers, &memory.speakers);
    let sources = speakers
        .iter()
        .map(|speaker| speaker.source.as_str())
        .chain(named.iter().map(String::as_str));
    // An instruction that stands alone reads nothing of the room, its roster
    // included (RFC §10.1.1 rule 7).
    let people = if is_standalone(event) {
        Vec::new()
    } else {
        people_taking_part(context, channel_id)
    };
    let labelled = labels_turns(sources, people.len());
    let turns = cleaned(turns, labelled);
    let notes = Notes {
        labelled,
        people: &people,
        discussion: &memory.turn_notes,
        remembered: &memory.notes,
    };
    rendered(turns, &notes)
}

/// A turn of the conversation before it is rendered: its role, what its
/// event says, who said it, whether it is the application's instruction,
/// its event's place in the timeline, the turn of the channel's own that
/// wrote it (its `correlation_id`), the rounds of tool calls read after it,
/// and whether the first of them is the round the turn's text was sent
/// with.
#[derive(Clone, Debug)]
struct Turn {
    role: Role,
    content: TurnContent,
    speaker: Option<Speaker>,
    instruction: bool,
    index: u64,
    written_in: Option<String>,
    rounds: Vec<ToolRound>,
    said_with_round: bool,
}

impl Turn {
    /// A turn of the room's history, saying what `read` reads of it.
    fn past(
        event: &RoomEvent,
        channel_id: &ChannelId,
        authors: &mut Authors<'_>,
        read: &impl Fn(&RoomEvent) -> TurnContent,
    ) -> Self {
        let own = &event.source.channel_id == channel_id;
        Self {
            role: if own { Role::Assistant } else { Role::User },
            content: read(event),
            speaker: if own { None } else { speaker(event, authors) },
            instruction: false,
            index: event.index,
            written_in: own.then(|| event.correlation_id.clone()).flatten(),
            rounds: Vec::new(),
            said_with_round: false,
        }
    }

    /// The turn's input: `event`, saying what `read` reads of it; an
    /// instruction, its text form, never attributed to a speaker.
    fn input(
        event: &RoomEvent,
        authors: &mut Authors<'_>,
        read: &impl Fn(&RoomEvent) -> TurnContent,
    ) -> Self {
        let instruction = event.r#type == EventType::Instruction;
        Self {
            role: Role::User,
            content: if instruction {
                TurnContent::Text(text_of(&event.content))
            } else {
                read(event)
            },
            speaker: if instruction {
                None
            } else {
                speaker(event, authors)
            },
            instruction,
            index: event.index,
            written_in: None,
            rounds: Vec::new(),
            said_with_round: false,
        }
    }

    /// A kept instruction an earlier turn read, as a turn of the history:
    /// the application's, marked as the turn's input was, attributed to no
    /// one (RFC §10.1.1).
    fn kept(instruction: &KeptInstruction) -> Self {
        Self {
            role: Role::User,
            content: instruction.content(),
            speaker: None,
            instruction: true,
            index: instruction.index,
            written_in: None,
            rounds: Vec::new(),
            said_with_round: false,
        }
    }

    /// Whether `round`, read right after this turn, is the round its text
    /// was sent with: the channel's own text of the round's turn (Python's
    /// `_spoken_with`).
    fn speaks_with(&self, round: &ToolRound) -> bool {
        self.rounds.is_empty()
            && self.role == Role::Assistant
            && matches!(self.content, TurnContent::Text(_))
            && self.written_in.is_some()
            && self.written_in == round.turn
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
    read: &impl Fn(&RoomEvent) -> TurnContent,
) -> Vec<Turn> {
    if is_standalone(event) {
        return Vec::new();
    }
    let turns: Vec<Turn> = context
        .recent_events
        .iter()
        .filter(|past| past.id != event.id && is_conversation(past))
        .map(|past| Turn::past(past, channel_id, authors, read))
        .filter(|turn| !turn.content.is_blank())
        .collect();
    let skip = turns.len().saturating_sub(history);
    turns.into_iter().skip(skip).collect()
}

/// `turns`, the input last, with the input at its place, before the first
/// turn that came after it, one at its index `index` too: an instruction,
/// never stored, carries the index of the message that follows it. Python's
/// `_answered_position`.
fn read_at_its_place(turns: &mut Vec<Turn>, index: u64) {
    let Some(input) = turns.pop() else {
        return;
    };
    let at = turns
        .iter()
        .position(|turn| turn.index >= index)
        .unwrap_or(turns.len());
    turns.insert(at, input);
}

/// Where the rounds a turn on `event` replays end: before its event. Index
/// 0 is an event the store never numbered: nothing came before it.
fn rounds_before(event: &RoomEvent) -> Option<u64> {
    (event.index > 0).then_some(event.index)
}

/// What a turn's history replays of the channel's earlier turns beside
/// their messages, read from its own rows: their rounds of tool calls and
/// the instructions they read. Python's `EarlierTurns`.
#[derive(Default)]
struct Earlier {
    rounds: Vec<ToolRound>,
    instructions: Vec<KeptInstruction>,
}

/// `earlier`'s rounds and instructions placed among `turns`, oldest first,
/// each after the last turn that came before it, none at or after `before`
/// (Python's `with_tool_rounds`). A round older than the first turn is left
/// out, so that the history never opens on the assistant's calls, unless
/// its turn's instruction is placed. An instruction reads where its turn
/// read it, before what the turn wrote: one after the first turn, or one
/// whose turn has a message or a round in the history, which may then open
/// the history (an instruction that opened the room). A round read right
/// after the text the model wrote with its calls is sent with it.
fn place(turns: &mut Vec<Turn>, earlier: Earlier, before: Option<u64>) {
    let Some(first) = turns.first() else {
        return;
    };
    // An instruction holds the index of what follows it, a round at that
    // index among them: it reads before that round.
    let (first_index, first_instruction) = (first.index, first.instruction);
    let after_first =
        |index: u64| first_index < index || (first_instruction && first_index == index);
    let in_time = |index: u64| before.is_none_or(|before| index < before);
    let (mut placed, older): (Vec<ToolRound>, Vec<ToolRound>) = earlier
        .rounds
        .into_iter()
        .filter(|round| in_time(round.index))
        .partition(|round| after_first(round.index));
    let held: BTreeSet<String> = turns
        .iter()
        .filter_map(|turn| turn.written_in.clone())
        .chain(placed.iter().filter_map(|round| round.turn.clone()))
        .collect();
    let read: Vec<KeptInstruction> = earlier
        .instructions
        .into_iter()
        .filter(|kept| in_time(kept.index))
        .filter(|kept| {
            after_first(kept.index) || kept.turn.as_ref().is_some_and(|turn| held.contains(turn))
        })
        .collect();
    // The turns whose instruction opens the history: their rounds read
    // after it, older than the first turn as they are.
    let opened: BTreeSet<&String> = read
        .iter()
        .filter(|kept| !after_first(kept.index))
        .filter_map(|kept| kept.turn.as_ref())
        .collect();
    placed.extend(older.into_iter().filter(|round| {
        round
            .turn
            .as_ref()
            .is_some_and(|turn| opened.contains(turn))
    }));
    placed.sort_by_key(|round| round.index);
    for kept in &read {
        let at = turns
            .iter()
            .rposition(|turn| turn.index < kept.index)
            .map_or(0, |at| at + 1);
        turns.insert(at, Turn::kept(kept));
    }
    for round in placed {
        let before_it = |turn: &&mut Turn| {
            turn.index < round.index || (turn.instruction && turn.index == round.index)
        };
        if let Some(turn) = turns.iter_mut().rev().find(before_it) {
            turn.said_with_round = turn.said_with_round || turn.speaks_with(&round);
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
/// turn, over its text parts, and over each run of consecutive user turns
/// the runtime does not label, which an API may merge into one message; a
/// replayed round ends a run, and is never rewritten. A turn a copy wholly
/// held is left out, the rounds read after it then read after the turn
/// before it.
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
            let counts: Vec<usize> = group
                .iter()
                .map(|turn| turn.content.texts().len())
                .collect();
            let texts: Vec<String> = group.iter().flat_map(|turn| turn.content.texts()).collect();
            let mut cleaned = without_copies(&texts).into_iter();
            for (turn, count) in group.iter_mut().zip(counts) {
                let own: Vec<String> = cleaned.by_ref().take(count).collect();
                let content = std::mem::replace(&mut turn.content, TurnContent::Parts(Vec::new()));
                turn.content = content.with_texts(own);
            }
        }
        start = end;
    }
    let mut kept: Vec<Turn> = Vec::with_capacity(turns.len());
    for turn in turns {
        if !turn.content.is_blank() {
            kept.push(turn);
        } else if let Some(before) = kept.last_mut() {
            before.rounds.extend(turn.rounds);
        }
    }
    kept
}

/// The speakers a memory's messages name that no turn's speaker is, each
/// as a source of its own: a memory names its speakers by label, and a
/// label a turn already has is that turn's speaker (RFC §6.4), as Python's
/// `several_speakers` counts them.
fn remembered_speakers(speakers: &[&Speaker], named: &[String]) -> Vec<String> {
    named
        .iter()
        .filter(|label| !label.is_empty())
        .filter(|label| speakers.iter().all(|speaker| &speaker.label != *label))
        .map(|label| format!("\u{0}{label}"))
        .collect()
}

/// The turn's notes (RFC §6.4): `blocks` after the runtime's header, a
/// blank line between, each copy of the header in them replaced; `None`
/// with no block. Python's `turn_notes`.
pub fn turn_notes(blocks: &[String]) -> Option<String> {
    let blocks: Vec<String> = blocks
        .iter()
        .map(|block| without_header_copies(block))
        .collect();
    joined_notes(&blocks)
}

/// `blocks` others wrote, each copy of a runtime mark replaced, one that
/// runs over two blocks too, as the notes join them, and a block left
/// blank dropped.
#[doc(hidden)]
pub fn cleaned_notes(blocks: &[String]) -> Vec<String> {
    without_copies(blocks)
        .into_iter()
        .filter(|block| !block.trim().is_empty())
        .collect()
}

/// `blocks` under the notes' header, a blank line between; `None` with no
/// block.
fn joined_notes(blocks: &[String]) -> Option<String> {
    if blocks.is_empty() {
        return None;
    }
    Some(format!("{NOTES_HEADER}\n\n{}", blocks.join("\n\n")))
}

/// The metadata key the kit's history read writes on its copy of a tool-call
/// row it hands a turn for the channel's tool memory alone (one of its
/// latest 60, which Python's usage loader reads), older than the room's
/// message before the history's first: no round replays it and the turn's
/// footprint measures none of it, as Python's history reads its rows from
/// that message on (RFC §6.4, RMK-1370). The read clears it from every row
/// it reads first, so that no stored row carries it into a turn.
#[doc(hidden)]
pub const TOOL_MEMORY_ONLY: &str = "_tool_memory_only";

/// Whether `event` is a row a turn holds for its channel's tool memory alone
/// ([`TOOL_MEMORY_ONLY`]).
#[doc(hidden)]
pub fn is_tool_memory_only(event: &RoomEvent) -> bool {
    event.metadata.get(TOOL_MEMORY_ONLY) == Some(&serde_json::Value::Bool(true))
}

/// A piece of the channel's earlier turns its history replays beside their
/// messages (RFC §6.4, §10.1.1): a round of tool calls, or an instruction a
/// turn read, as a memory charges it to the history event it reads with
/// (RFC §20).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Replayed {
    /// The timeline index of its first row.
    pub index: u64,
    /// The turn it belongs to: its rows' `correlation_id`.
    pub turn: Option<String>,
    /// The messages it is replayed as.
    pub messages: Vec<Message>,
}

/// What the channel `channel_id`'s own rows among `events` replay of its
/// earlier turns, each piece as the messages [`conversation`] replays it as,
/// every one its rows hold, wherever the history it is placed in starts:
/// each round of tool calls (RFC §6.4 "The tools of earlier turns"), its
/// assistant's message making the calls then a tool message per result,
/// then each instruction a turn read (RFC §10.1.1), the user message marked
/// exactly as the turn's input was. Python's `EarlierTurns`, as the turn's
/// footprint measures it (`_replayed_costs`).
pub fn earlier_turns(events: &[RoomEvent], channel_id: &ChannelId) -> Vec<Replayed> {
    let rounds = tool_rounds(events, channel_id)
        .into_iter()
        .map(|round| Replayed {
            index: round.index,
            turn: round.turn,
            messages: round.messages,
        });
    let instructions = kept_instructions(events, channel_id)
        .into_iter()
        .map(|kept| Replayed {
            messages: vec![Message::new(
                Role::User,
                mark_instruction(&without_mark_copies(&kept.text)),
            )],
            index: kept.index,
            turn: kept.turn,
        });
    rounds.chain(instructions).collect()
}

/// What a turn's notes say (RFC §6.4): how speakers are named when the
/// turns are `labelled`, who takes part when two `people` or more do, then
/// what the memory retrieved for the turn (`remembered`).
struct Notes<'a> {
    labelled: bool,
    people: &'a [String],
    discussion: &'a [String],
    remembered: &'a [String],
}

impl Notes<'_> {
    /// The blocks a copy of a runtime mark is replaced in: the memory's,
    /// then the discussion's, whose notes quote what people wrote.
    fn after_the_runtime_s(&self) -> Vec<String> {
        let (memory, discussion) = (self.remembered.iter(), self.discussion.iter());
        memory.chain(discussion).cloned().collect()
    }

    /// The notes' blocks, in order: the runtime's own, then the memory's,
    /// each copy of a runtime mark in them replaced.
    fn blocks(&self) -> Vec<String> {
        let speaker_note = self.labelled.then(|| SPEAKER_NOTE.to_owned());
        speaker_note
            .into_iter()
            .chain(people_note(self.people))
            .chain(cleaned_notes(&self.after_the_runtime_s()))
            .collect()
    }
}

/// The conversation's messages: each turn in its role, a participant's
/// turn labelled when the conversation labels them, the instruction marked
/// as the application's, the rounds read after a turn following it, the
/// first of them in the turn's own message when the turn's text was sent
/// with it, then
/// the turn's `notes` after the last message when it is a user's, the
/// input or not, else as a user message of their own (Python's
/// `with_turn_notes`): some chat formats refuse two user messages in a row.
fn rendered(turns: Vec<Turn>, notes: &Notes<'_>) -> Vec<Message> {
    let mut messages: Vec<Message> = Vec::with_capacity(turns.len());
    for turn in turns {
        let content = match (&turn.speaker, turn.content) {
            (_, TurnContent::Text(text)) if turn.instruction => {
                TurnContent::Text(format!("{INSTRUCTION_MARK}\n{text}"))
            }
            (Some(speaker), content) if notes.labelled && turn.role == Role::User => {
                content.labelled(&speaker.label)
            }
            (_, content) => content,
        };
        let mut rounds = turn.rounds.into_iter().flat_map(|round| round.messages);
        match (turn.said_with_round, content) {
            (true, TurnContent::Text(text)) => {
                // The text, then the calls: the one message its turn sent.
                messages.extend(rounds.next().map(|calls| Message { text, ..calls }));
            }
            (_, content) => messages.push(Message::of_content(turn.role, content)),
        }
        messages.extend(rounds);
    }
    if let Some(notes) = joined_notes(&notes.blocks()) {
        match messages.pop() {
            Some(last) if last.role == Role::User => messages.push(with_notes(last, &notes)),
            Some(other) => messages.extend([other, Message::new(Role::User, notes)]),
            None => messages.push(Message::new(Role::User, notes)),
        }
    }
    messages
}

/// The turn's `input` with `notes` after it (RFC §6.4): a text input a
/// blank line after its text, an input of parts a part of its own.
/// Python's `with_turn_notes`.
fn with_notes(input: Message, notes: &str) -> Message {
    let Message {
        role,
        text,
        parts,
        metadata,
        ..
    } = input;
    let content = if parts.is_empty() {
        TurnContent::Text(text)
    } else {
        TurnContent::Parts(parts)
    };
    Message {
        metadata,
        ..Message::of_content(role, content.followed_by(notes))
    }
}

/// The label each of `events` opens with where a model reads it (RFC §6.4),
/// in their order: its author's name, ranked when an earlier source's name
/// reads like it, or its channel (`@sms`) when its author has none, a
/// second nameless source of a channel reading `@sms (2)` in the order
/// they first speak in `events`; `None` for the runtime's system events.
/// Python's `turn_labels`, which a delegation's conversation block labels
/// its lines with (RFC §23.3 step 4).
pub fn turn_labels<'a>(
    events: impl IntoIterator<Item = &'a RoomEvent>,
    context: &RoomContext,
) -> Vec<Option<String>> {
    let mut authors = Authors::new(context);
    let mut speakers: Vec<Option<Speaker>> = events
        .into_iter()
        .map(|event| speaker(event, &mut authors))
        .collect();
    tell_apart(speakers.iter_mut().flatten());
    speakers
        .into_iter()
        .map(|said| said.map(|said| said.label))
        .collect()
}

/// Who each of `events` comes from (RFC §6.4), in their order: the source
/// speakers are counted by, two turns of one speaker having the same one
/// whatever their labels; `None` for the runtime's system events. Python's
/// `TurnSpeaker.source`.
pub fn turn_sources<'a>(
    events: impl IntoIterator<Item = &'a RoomEvent>,
    context: &RoomContext,
) -> Vec<Option<String>> {
    let mut authors = Authors::new(context);
    events
        .into_iter()
        .map(|event| speaker(event, &mut authors).map(|said| said.source))
        .collect()
}

/// The text `event` reads as for the running turn of `channel_id` it is
/// steered into (RFC §6.4 "A message for the running turn", §21.3): its
/// content's text form (§6.6), each of its lines under its speaker's label
/// when the room has several speakers, as the turn's history labels them;
/// `None` for an event that reads as no text, which waits a turn of its
/// own, and for an event that opens no turn (a notice, a reaction: RFC
/// §6.4; an edit or a delete: RFC §10.3), whose own turn the agent does
/// not take. Python's `steered_message`: its speakers are the context's
/// events' and `event`'s, nameless ones told apart and counted by source,
/// as the conversation counts them.
pub fn steered_text(
    event: &RoomEvent,
    context: &RoomContext,
    channel_id: &ChannelId,
) -> Option<String> {
    if opens_no_turn(event) {
        return None;
    }
    let text = text_of(&event.content);
    if text.is_empty() {
        return None;
    }
    let mut authors = Authors::new(context);
    // The turns the conversation counts: the agent's own and the
    // application's instructions aside, and those that read as no text.
    let others = context.recent_events.iter().filter(|recent| {
        recent.id != event.id
            && recent.source.channel_id != *channel_id
            && !matches!(
                recent.r#type,
                EventType::Instruction | EventType::ToolCallStart | EventType::ToolCallEnd
            )
            && !text_of(&recent.content).trim().is_empty()
    });
    let mut speakers: Vec<Option<Speaker>> = others
        .chain([event])
        .map(|said| speaker(said, &mut authors))
        .collect();
    tell_apart(speakers.iter_mut().flatten());
    let sources = speakers.iter().flatten().map(|said| said.source.as_str());
    let several = labels_turns(sources, people_taking_part(context, channel_id).len());
    match speakers.last().cloned().flatten() {
        Some(said) if several => Some(labelled_lines(&text, &said.label)),
        Some(_) | None => Some(text),
    }
}

/// How many people take part besides the agent of `channel_id` (RFC §6.4),
/// silent ones included: the room's active participants who are neither
/// bots nor observers, nor on the agent's own channel, and whose messages
/// reach the agent; none for an agent the room does not bind. Python's
/// `len(people_taking_part(context, channel_id))`, which a summary's lines
/// count toward labelling.
pub fn people_count(context: &RoomContext, channel_id: &ChannelId) -> usize {
    people_taking_part(context, channel_id).len()
}

/// The application's instruction as a model reads it (RFC §10.1.1 rule
/// 6): marked, never attributed. Python's `mark_instruction`.
pub fn mark_instruction(text: &str) -> String {
    format!("{INSTRUCTION_MARK}\n{text}")
}

/// `text` with each copy of a runtime mark it holds replaced (RFC §6.4):
/// a message a host steers into a turn (RFC §21.3) reads as no mark of
/// the runtime's. Python's `without_mark_copies`.
pub fn without_mark_copies(text: &str) -> String {
    without_copies(&[text.to_owned()]).pop().unwrap_or_default()
}

/// `line` as a JSON string a model does not read as ending early (RFC
/// §6.4): Python's `json_text_line`.
pub fn json_text_line(line: &str) -> String {
    speakers::json_text_line(line)
}

/// `text` with every double quote mark but `"` made a single one and the
/// marks that reorder text dropped, as inside a quote (RFC §6.4): what
/// [`json_text_line`] reads a line as before it writes it, for a string
/// written as part of a larger JSON value. Python's `_QUOTE_LOOKALIKES`.
#[must_use]
pub fn without_quote_lookalikes(text: &str) -> String {
    speakers::without_quote_lookalikes(text)
}

/// The label of a turn whose author has no name (RFC §6.4): its channel as
/// the room addresses it (`@sms1`), a form no name takes. Python's
/// `channel_label`.
pub(crate) fn channel_label(channel_id: &ChannelId) -> String {
    speakers::channel_label(channel_id)
}

/// `value` as the runtime gives an identifier outside a quote (RFC §6.4):
/// letters with their marks, digits and `_ . @ + -`, every other character
/// a `-`, within 64 characters, composed first; `fallback` when nothing is
/// left. Python's `_text.identifier`.
pub fn identifier(value: &str, fallback: &str) -> String {
    speakers::identifier(value, fallback)
}
