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

mod content;
mod fence;
mod images;
pub use self::content::{Part, TurnContent};
pub use images::Image;
mod marks;
mod quote;
mod rounds;
mod speakers;
#[cfg(test)]
mod tests;

pub use self::fence::fence;
#[doc(hidden)]
pub use self::fence::named_blocks;
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
    let memory = TurnMemory::default();
    let read = |said: &RoomEvent| TurnContent::Text(text_of(&said.content));
    conversation_with_memory(channel_id, event, context, history, replayed, &memory, read)
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
    replayed: impl Fn(&str) -> bool,
    memory: &TurnMemory,
    read: impl Fn(&RoomEvent) -> TurnContent,
) -> Vec<Message> {
    let mut authors = Authors::new(context);
    let mut turns = past_turns(channel_id, event, context, history, &mut authors, &read);
    if !turns.is_empty() {
        let rounds = tool_rounds(&context.recent_events, channel_id, &replayed);
        place(&mut turns, rounds, rounds_before(event));
    }
    turns.push(Turn::input(event, &mut authors, &read));
    tell_apart(turns.iter_mut().filter_map(|turn| turn.speaker.as_mut()));
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
        remembered: &memory.notes,
    };
    rendered(turns, &notes)
}

/// A turn of the conversation before it is rendered: its role, what its
/// event says, who said it, whether it is the application's instruction,
/// its event's place in the timeline, and the rounds of tool calls read
/// after it.
#[derive(Clone, Debug)]
struct Turn {
    role: Role,
    content: TurnContent,
    speaker: Option<Speaker>,
    instruction: bool,
    index: u64,
    rounds: Vec<ToolRound>,
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
            rounds: Vec::new(),
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
/// as a source of its own: Python counts speakers by label, and a label a
/// turn already has is that turn's speaker (RFC §6.4).
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
fn cleaned_notes(blocks: &[String]) -> Vec<String> {
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

/// The channel `channel_id`'s earlier rounds of tool calls among `events`,
/// each as the messages it is replayed as (RFC §6.4 "The tools of earlier
/// turns"): its assistant's message making the calls, then a tool message
/// per result, as [`conversation`] replays them, every round its rows hold,
/// wherever the history it is placed in starts.
pub fn replayed_rounds(
    events: &[RoomEvent],
    channel_id: &ChannelId,
    replayed: impl Fn(&str) -> bool,
) -> Vec<Vec<Message>> {
    tool_rounds(events, channel_id, &replayed)
        .into_iter()
        .map(|round| round.messages)
        .collect()
}

/// What a turn's notes say (RFC §6.4): how speakers are named when the
/// turns are `labelled`, who takes part when two `people` or more do, then
/// what the memory retrieved for the turn (`remembered`).
struct Notes<'a> {
    labelled: bool,
    people: &'a [String],
    remembered: &'a [String],
}

impl Notes<'_> {
    /// The notes' blocks, in order: the runtime's own, then the memory's,
    /// each copy of a runtime mark in them replaced.
    fn blocks(&self) -> Vec<String> {
        let speaker_note = self.labelled.then(|| SPEAKER_NOTE.to_owned());
        speaker_note
            .into_iter()
            .chain(people_note(self.people))
            .chain(cleaned_notes(self.remembered))
            .collect()
    }
}

/// The conversation's messages: each turn in its role, a participant's
/// turn labelled when the conversation labels them, the instruction marked
/// as the application's, the rounds read after a turn following it, then
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
        messages.push(Message::of_content(turn.role, content));
        messages.extend(turn.rounds.into_iter().flat_map(|round| round.messages));
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
/// reads like it, or its channel (`@sms`) when its author has none; `None`
/// for the runtime's system events. Python's `turn_labels`, which a
/// delegation's conversation block labels its lines with (RFC §23.3 step
/// 4).
pub fn turn_labels<'a>(
    events: impl IntoIterator<Item = &'a RoomEvent>,
    context: &RoomContext,
) -> Vec<Option<String>> {
    let mut authors = Authors::new(context);
    events
        .into_iter()
        .map(|event| speaker(event, &mut authors).map(|speaker| speaker.label))
        .collect()
}

/// The text `event` reads as for the running turn of `channel_id` it is
/// steered into (RFC §6.4 "A message for the running turn", §21.3): its
/// content's text form (§6.6), each of its lines under its speaker's label
/// when the room has several speakers, as the turn's history labels them;
/// `None` for an event that reads as no text, which waits a turn of its
/// own. Python's `steered_message`, its speakers counted by label among the
/// context's events and `event`.
pub fn steered_text(
    event: &RoomEvent,
    context: &RoomContext,
    channel_id: &ChannelId,
) -> Option<String> {
    let text = text_of(&event.content);
    if text.is_empty() {
        return None;
    }
    let mut authors = Authors::new(context);
    let others = context
        .recent_events
        .iter()
        .filter(|recent| recent.id != event.id);
    let labels: Vec<Option<String>> = others
        .chain([event])
        .map(|said| speaker(said, &mut authors).map(|speaker| speaker.label))
        .collect();
    let distinct: std::collections::BTreeSet<&str> =
        labels.iter().flatten().map(String::as_str).collect();
    let several = people_taking_part(context, channel_id).len() >= 2 || distinct.len() >= 2;
    match labels.last().cloned().flatten() {
        Some(label) if several => Some(labelled_lines(&text, &label)),
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

/// `value` as the runtime gives an identifier outside a quote (RFC §6.4):
/// letters with their marks, digits and `_ . @ + -`, every other character
/// a `-`, within 64 characters, composed first; `fallback` when nothing is
/// left. Python's `_text.identifier`.
pub fn identifier(value: &str, fallback: &str) -> String {
    speakers::identifier(value, fallback)
}
