//! Who wrote a turn, and the rank that tells apart a room's sources whose
//! names read alike (RFC §6.4, §10.1 step 12, §14.1, gap G-089).
//!
//! A turn's author is the name its sender stamped on it
//! (`metadata.sender_name`), else the name of the room's participant behind
//! it. Two sources whose names read alike are told apart by a rank the room
//! fixes when the turn is committed: the room's register of authors keeps,
//! for each thing a name reads as, the sources whose names read so and the
//! rank of each, as digests salted with the room's id. The turn carries its
//! author's name, rank and source (`metadata.author`), so every reader names
//! it as it was named then.
//!
//! Everything here follows the Python implementation (`core/_authors.py`),
//! digest for digest and rank for rank, so that the two implementations
//! record the same authors: the conformance suite compares them.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::lookalike::{person_name, skeletons};
use crate::model::{ChannelType, EventStatus, Metadata, Participant, RoomContext, RoomEvent};

/// The event metadata key holding the record of its author, fixed when the
/// event is committed: `{"name": "Alice", "rank": 2, "source": "<digest>"}`.
pub(crate) const AUTHOR: &str = "author";

/// The key of the room's private state holding its register of authors
/// (RFC §10.1 step 12, §14.1), and the room metadata key it was kept under
/// before.
pub(crate) const AUTHOR_REGISTER: &str = "author_register";

/// The event metadata key a transport, a host or a diarized voice stamps a
/// sender's name under.
const SENDER_NAME: &str = "sender_name";

/// The register's version, the Python implementation's.
const VERSION: i64 = 3;

/// The room's participants as a turn's `source.participant_id` names them:
/// a participant's id, or an identity's, names its participant only on a
/// channel they are reached through (RFC §5.5). An identity is one person
/// however many records name it: the record that joined first stands for
/// them all.
struct People<'a> {
    by_id: BTreeMap<&'a str, &'a Participant>,
    by_identity: BTreeMap<&'a str, Vec<&'a Participant>>,
    /// One record per person, in the order they joined.
    persons: Vec<&'a Participant>,
}

impl<'a> People<'a> {
    fn new(participants: &'a [Participant]) -> Self {
        let mut records: Vec<&Participant> = participants.iter().collect();
        records
            .sort_by(|left, right| (left.joined_at, &left.id).cmp(&(right.joined_at, &right.id)));
        let mut by_id = BTreeMap::new();
        let mut by_identity: BTreeMap<&str, Vec<&Participant>> = BTreeMap::new();
        for record in &records {
            by_id.insert(record.id.as_str(), *record);
            if let Some(identity) = &record.identity_id {
                by_identity
                    .entry(identity.as_str())
                    .or_default()
                    .push(*record);
            }
        }
        let mut people = Self {
            by_id,
            by_identity,
            persons: Vec::new(),
        };
        people.persons = records
            .into_iter()
            .filter(|record| std::ptr::eq(people.standing_for(record), *record))
            .collect();
        people
    }

    /// The participant behind `event`, if any.
    fn of(&self, event: &RoomEvent) -> Option<&'a Participant> {
        let record = self.named_on(
            event
                .source
                .participant_id
                .as_ref()
                .map(crate::model::ParticipantId::as_str),
            event.source.channel_id.as_str(),
        )?;
        Some(self.standing_for(record))
    }

    /// The record `id` names on `channel`: the record of that id when its
    /// participant is reached through the channel, else one of the
    /// identity's records reached through it.
    fn named_on(&self, id: Option<&str>, channel: &str) -> Option<&'a Participant> {
        let id = id.filter(|id| !id.is_empty())?;
        if let Some(record) = self.by_id.get(id).filter(|record| reached(record, channel)) {
            return Some(*record);
        }
        self.by_identity
            .get(id)?
            .iter()
            .find(|record| reached(record, channel))
            .copied()
    }

    /// The record standing for `record`'s person: its identity's first.
    fn standing_for(&self, record: &'a Participant) -> &'a Participant {
        let Some(identity) = &record.identity_id else {
            return record;
        };
        self.by_identity
            .get(identity.as_str())
            .and_then(|records| records.first().copied())
            .unwrap_or(record)
    }
}

/// Whether the room reached `record`'s participant through `channel`.
fn reached(record: &Participant, channel: &str) -> bool {
    record
        .connected_via
        .iter()
        .any(|reached| reached.as_str() == channel)
}

/// `event`'s author: the sender's name it carries, else the name of the
/// room's participant behind it, each kept to a name's characters.
fn author_name(event: &RoomEvent, people: &People<'_>) -> Option<String> {
    if let Some(Value::String(name)) = event.metadata.get(SENDER_NAME) {
        let kept = person_name(name);
        if !kept.is_empty() {
            return Some(kept);
        }
    }
    let person = people.of(event)?;
    let kept = person_name(person.display_name.as_deref().unwrap_or_default());
    (!kept.is_empty()).then_some(kept)
}

/// Who `event` comes from: the room's participant, whichever channel
/// reached them; else its sender on its channel; else its channel.
fn source_of(event: &RoomEvent, people: &People<'_>) -> Vec<String> {
    if let Some(person) = people.of(event) {
        return vec!["participant".to_owned(), person.id.to_string()];
    }
    let channel = event.source.channel_id.to_string();
    match &event.source.participant_id {
        Some(sender) if !sender.as_str().is_empty() => {
            vec!["sender".to_owned(), channel, sender.to_string()]
        }
        Some(_) | None => vec!["channel".to_owned(), channel],
    }
}

/// `parts` hashed with `salt`, the room's id: the first 16 hexadecimal
/// digits of the SHA-256 of them joined by U+001F.
fn digest(salt: &str, parts: &[String]) -> String {
    let mut joined = salt.to_owned();
    for part in parts {
        joined.push('\u{1f}');
        joined.push_str(part);
    }
    let hash = Sha256::digest(joined.as_bytes());
    let mut hex = String::with_capacity(16);
    for byte in hash.iter().take(8) {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

/// What `name` reads as, hashed with `salt`.
fn name_keys(salt: &str, name: &str) -> BTreeSet<String> {
    skeletons(name)
        .into_iter()
        .map(|key| digest(salt, &[key]))
        .collect()
}

/// The name, rank and source `event` was committed with, when it carries a
/// well-formed record.
fn recorded_author(event: &RoomEvent) -> Option<(String, u32, String)> {
    let Some(Value::Object(record)) = event.metadata.get(AUTHOR) else {
        return None;
    };
    let name = record.get("name")?.as_str()?;
    let rank = record.get("rank").and_then(Value::as_u64)?;
    let source = record.get("source")?.as_str()?;
    let rank = u32::try_from(rank).ok().filter(|rank| *rank > 0)?;
    Some((name.to_owned(), rank, source.to_owned()))
}

/// A room's register of authors: for each thing a name reads as (hashed),
/// the sources whose names read so and the rank of each, kept as the
/// Python implementation keeps it: `{"version": 3, "names": {key:
/// "source:rank source:rank"}}`. A rank once given is never given to
/// another source, since a turn was recorded with it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AuthorRegister {
    data: Metadata,
    names: Map<String, Value>,
    changed: bool,
}

impl AuthorRegister {
    /// An empty register, written once it is used.
    fn empty() -> Self {
        Self {
            data: Metadata::new(),
            names: Map::new(),
            changed: true,
        }
    }

    /// The register `value` holds, when it holds one of this version.
    fn stored(value: &Metadata) -> Option<Self> {
        let version = value.get("version")?;
        if !version.is_i64() && !version.is_u64() || version.as_i64() != Some(VERSION) {
            return None;
        }
        let Value::Object(names) = value.get("names")? else {
            return None;
        };
        let mut data = value.clone();
        data.remove("names");
        Some(Self {
            data,
            names: names.clone(),
            changed: false,
        })
    }

    /// The register as the store keeps it.
    fn written(&self) -> Metadata {
        let mut data = self.data.clone();
        data.insert("version".to_owned(), VERSION.into());
        data.insert("names".to_owned(), Value::Object(self.names.clone()));
        data
    }

    /// `source`'s rank under `names`: the rank it holds when no other
    /// source under them holds it, else one more than the highest held;
    /// filed under each of `names`.
    fn rank(&mut self, source: &str, names: &BTreeSet<String>) -> u32 {
        let (mine, others) = self.ranks(source, names);
        let rank = mine.difference(&others).next().copied().unwrap_or_else(|| {
            mine.union(&others)
                .max()
                .copied()
                .unwrap_or_default()
                .saturating_add(1)
        });
        self.file(source, names, rank);
        rank
    }

    /// `source`'s rank under `names` as a registered participant's: the
    /// rank it holds when no other source under them holds it, else the
    /// lowest none of them holds.
    fn seat(&mut self, source: &str, names: &BTreeSet<String>) -> u32 {
        let (mine, others) = self.ranks(source, names);
        let rank = mine
            .difference(&others)
            .next()
            .copied()
            .unwrap_or_else(|| lowest_free(&others));
        self.file(source, names, rank);
        rank
    }

    /// Files every rank `other` holds here: a register only adds.
    fn merge(&mut self, other: &Self) {
        for name in other.names.keys() {
            for (source, rank) in other.entries(name) {
                self.file(&source, &BTreeSet::from([name.clone()]), rank);
            }
        }
    }

    /// Records `source` at `rank` under `names`, as a turn's record says it
    /// was, unless another source under them holds the rank.
    fn claim(&mut self, source: &str, names: &BTreeSet<String>, rank: u32) {
        let (_, others) = self.ranks(source, names);
        if !others.contains(&rank) {
            self.file(source, names, rank);
        }
    }

    /// The room's named participants take their seats, in the order they
    /// joined.
    fn seed(&mut self, people: &People<'_>, salt: &str) {
        for person in &people.persons {
            let name = person_name(person.display_name.as_deref().unwrap_or_default());
            if !name.is_empty() {
                let source = digest(salt, &["participant".to_owned(), person.id.to_string()]);
                self.seat(&source, &name_keys(salt, &name));
            }
        }
    }

    /// The ranks `source` holds under `names`, and those the other sources
    /// under them hold.
    fn ranks(&self, source: &str, names: &BTreeSet<String>) -> (BTreeSet<u32>, BTreeSet<u32>) {
        let mut mine = BTreeSet::new();
        let mut others = BTreeSet::new();
        for name in names {
            for (holder, rank) in self.entries(name) {
                if holder == source {
                    mine.insert(rank);
                } else {
                    others.insert(rank);
                }
            }
        }
        (mine, others)
    }

    /// The ranks filed under `name`, each with its source, the malformed
    /// pairs left out.
    fn entries(&self, name: &str) -> Vec<(String, u32)> {
        let Some(Value::String(held)) = self.names.get(name) else {
            return Vec::new();
        };
        held.split_whitespace().filter_map(pair).collect()
    }

    /// `rank` filed for `source` under each of `names`, beside any rank it
    /// held there before.
    fn file(&mut self, source: &str, names: &BTreeSet<String>, rank: u32) {
        for name in names {
            let entry = (source.to_owned(), rank);
            if self.entries(name).contains(&entry) {
                continue;
            }
            let pair = format!("{source}:{rank}");
            let value = match self.names.get(name) {
                Some(Value::String(held)) => format!("{held} {pair}"),
                Some(_) | None => pair,
            };
            self.names.insert(name.clone(), Value::String(value));
            self.changed = true;
        }
    }
}

/// The lowest rank `taken` does not hold.
fn lowest_free(taken: &BTreeSet<u32>) -> u32 {
    let mut rank = 1;
    while taken.contains(&rank) {
        rank = rank.saturating_add(1);
    }
    rank
}

/// A `source:rank` pair: 16 lowercase hexadecimal digits, a rank of one to
/// nine digits that does not start with 0.
fn pair(text: &str) -> Option<(String, u32)> {
    let (source, rank) = text.split_once(':')?;
    let hex = |c: char| c.is_ascii_digit() || ('a'..='f').contains(&c);
    let well_formed = source.chars().count() == 16
        && source.chars().all(hex)
        && (1..=9).contains(&rank.len())
        && rank.chars().all(|c| c.is_ascii_digit())
        && !rank.starts_with('0');
    if !well_formed {
        return None;
    }
    Some((source.to_owned(), rank.parse().ok()?))
}

/// The register a room's timeline rebuilds, for a room that holds none
/// (RFC §10.1 step 12): the ranks its turns' records hold, then its named
/// participants' seats, then the authors of its turns that hold no record,
/// in index order.
fn rebuilt(context: &RoomContext, timeline: &[RoomEvent]) -> AuthorRegister {
    let mut register = AuthorRegister::empty();
    let people = People::new(&context.participants);
    let salt = context.room.id.as_str();
    let mut unrecorded = Vec::new();
    for event in timeline {
        if event.status == EventStatus::Blocked || event.source.channel_type == ChannelType::SYSTEM
        {
            continue;
        }
        let recorded = recorded_author(event).and_then(|(name, rank, source)| {
            let name = person_name(&name);
            (!name.is_empty()).then_some((name, rank, source))
        });
        if let Some((name, rank, source)) = recorded {
            register.claim(&source, &name_keys(salt, &name), rank);
        } else if let Some(name) = author_name(event, &people) {
            let source = digest(salt, &source_of(event, &people));
            unrecorded.push((source, name_keys(salt, &name)));
        }
    }
    register.seed(&people, salt);
    for (source, names) in unrecorded {
        register.rank(&source, &names);
    }
    register
}

/// What committing an event needs of the room's timeline to record its
/// author: the room holds no register, and the context's window does not
/// hold its whole timeline to rebuild one from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NeedsTimeline;

/// Records on `event`, about to be committed to the room of `context`,
/// the record of its author (RFC §10.1 step 12), and returns the register
/// to write in the commit's transaction when it changed. A record the
/// event came with is dropped, the runtime's alone; a `BLOCKED` record and
/// an event that names no sender take none. A room with no register has it
/// rebuilt from its timeline: the context's window when it holds the whole
/// timeline, else `timeline`, which the caller reads when this answers
/// [`NeedsTimeline`], and the event is recorded again then. A register the
/// room's metadata still holds is merged into the store's. The event is
/// written in place: a commit copies it no more than it did.
///
/// # Errors
///
/// Returns [`NeedsTimeline`] when the room's whole timeline is needed and
/// `timeline` is `None`.
pub(crate) fn with_author(
    event: &mut RoomEvent,
    context: &RoomContext,
    timeline: Option<&[RoomEvent]>,
) -> Result<Option<Metadata>, NeedsTimeline> {
    event.metadata.remove(AUTHOR);
    let named = event.source.participant_id.is_some()
        || event.metadata.get(SENDER_NAME).is_some_and(truthy);
    if !named || event.status == EventStatus::Blocked {
        return Ok(None);
    }
    let (held, moved) = held_register(context);
    let mut register = match held {
        Some(register) => register,
        None => rebuilt(context, history(context, timeline)?),
    };
    let record = author_record(event, context, &mut register);
    let write = (register.changed || moved).then(|| register.written());
    if let Some((name, rank, source)) = record {
        let mut author = Map::new();
        author.insert("name".to_owned(), name.into());
        author.insert("rank".to_owned(), rank.into());
        author.insert("source".to_owned(), source.into());
        event
            .metadata
            .insert(AUTHOR.to_owned(), Value::Object(author));
    }
    Ok(write)
}

/// The timeline a register is rebuilt from: the context's window when it
/// holds every event the room committed, else `timeline`.
fn history<'a>(
    context: &'a RoomContext,
    timeline: Option<&'a [RoomEvent]>,
) -> Result<&'a [RoomEvent], NeedsTimeline> {
    let whole = u64::try_from(context.recent_events.len())
        .is_ok_and(|held| held >= context.room.event_count);
    match timeline {
        Some(timeline) => Ok(timeline),
        None if whole => Ok(&context.recent_events),
        None => Err(NeedsTimeline),
    }
}

/// The register the room holds, and whether its metadata holds one to move
/// to the store: a room stored before the store kept it apart, or a copy a
/// process from before wrote there since. The store's takes every rank the
/// metadata's gave. A copy is moved even when it is the store's own: a
/// store that keeps the register apart then drops it from the metadata, as
/// the Python implementation's does; one that keeps it in the metadata,
/// which hands the same register twice, writes it back unchanged.
fn held_register(context: &RoomContext) -> (Option<AuthorRegister>, bool) {
    let held = context
        .author_register
        .as_ref()
        .and_then(AuthorRegister::stored);
    let Some(legacy) = context.room.metadata.get(AUTHOR_REGISTER) else {
        return (held, false);
    };
    let legacy = legacy.as_object().and_then(AuthorRegister::stored);
    match (held, legacy) {
        (None, legacy) => (legacy, true),
        (Some(mut held), Some(legacy)) => {
            held.merge(&legacy);
            (Some(held), true)
        }
        (Some(held), None) => (Some(held), true),
    }
}

/// The record `event` is committed with: its author's name, their rank in
/// `register` (extended with the room's named participants, and with the
/// source or its name when new), its source; `None` for a system event or
/// an author with no name.
fn author_record(
    event: &RoomEvent,
    context: &RoomContext,
    register: &mut AuthorRegister,
) -> Option<(String, u32, String)> {
    if event.source.channel_type == ChannelType::SYSTEM {
        return None;
    }
    let people = People::new(&context.participants);
    let name = author_name(event, &people)?;
    let salt = context.room.id.as_str();
    register.seed(&people, salt);
    let source = digest(salt, &source_of(event, &people));
    let rank = register.rank(&source, &name_keys(salt, &name));
    Some((name, rank, source))
}

/// Whether `value` reads as true, as Python's truth test reads it.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(number) => number.as_f64().is_some_and(|number| number != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(map) => !map.is_empty(),
    }
}

/// `metadata`, a direct update's of `stored`, with the record of the
/// author `stored` was committed with, and none when it was committed with
/// none: the record is the room's alone, fixed at the commit (RFC §10.1
/// step 12), so a direct update never writes one (gap G-089).
pub(crate) fn keep_record(metadata: &mut Metadata, stored: &RoomEvent) {
    match stored.metadata.get(AUTHOR) {
        Some(record) => {
            metadata.insert(AUTHOR.to_owned(), record.clone());
        }
        None => {
            metadata.remove(AUTHOR);
        }
    }
}

/// A turn's author as a reader names them (RFC §6.4): their name, and its
/// rank among the room's sources whose names read alike.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Author {
    pub(crate) name: String,
    pub(crate) rank: u32,
}

/// The authors of a room's turns as a reader names them (RFC §6.4), the
/// Python implementation's (`channels/_speaker.py`, `_label`): the record
/// a turn was committed with, while the turn's source is the one recorded;
/// else its author's name, ranked against a copy of the room's register
/// extended with its named participants, then with each new source in the
/// order the turns are read. The copy is made on first use, which a window
/// of recorded turns never makes, and never written back.
pub(crate) struct Authors<'a> {
    people: People<'a>,
    salt: &'a str,
    stored: Option<&'a Metadata>,
    register: Option<AuthorRegister>,
}

impl<'a> Authors<'a> {
    /// The authors of the turns of `context`'s room.
    pub(crate) fn new(context: &'a RoomContext) -> Self {
        Self {
            people: People::new(&context.participants),
            salt: context.room.id.as_str(),
            stored: context.author_register.as_ref(),
            register: None,
        }
    }

    /// Who `event` comes from, as a digest salted with the room's id: the
    /// room's participant, whichever channel reached them; else its sender
    /// on its channel; else its channel.
    pub(crate) fn source(&self, event: &RoomEvent) -> String {
        digest(self.salt, &source_of(event, &self.people))
    }

    /// `event`'s author, or `None` when they have no name.
    pub(crate) fn of(&mut self, event: &RoomEvent) -> Option<Author> {
        let source = self.source(event);
        if let Some((name, rank, recorded)) = recorded_author(event)
            && recorded == source
        {
            let name = person_name(&name);
            if !name.is_empty() {
                return Some(Author { name, rank });
            }
        }
        let name = author_name(event, &self.people)?;
        let names = name_keys(self.salt, &name);
        let rank = self.register().rank(&source, &names);
        Some(Author { name, rank })
    }

    /// One participant record per person, in the order they joined: an
    /// identity's first record stands for all of its records.
    pub(crate) fn persons(&self) -> &[&'a Participant] {
        &self.people.persons
    }

    /// The copy of the room's register the turns that hold no record are
    /// ranked against.
    fn register(&mut self) -> &mut AuthorRegister {
        self.register.get_or_insert_with(|| {
            let mut register = self
                .stored
                .and_then(AuthorRegister::stored)
                .unwrap_or_else(AuthorRegister::empty);
            register.seed(&self.people, self.salt);
            register
        })
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::model::{
        ChannelId, EventId, EventSource, ParticipantId, Room, RoomId, TextContent, Timestamp,
    };

    fn room() -> RoomContext {
        RoomContext::new(Room::at(
            RoomId::new("room_1").unwrap(),
            Timestamp::UNIX_EPOCH,
        ))
    }

    /// A message from `sender` on `channel`, named `name` when given.
    fn message(channel: &str, sender: &str, name: Option<&str>) -> RoomEvent {
        let channel_type = if channel == "sms" {
            ChannelType::SMS
        } else {
            ChannelType::WEBSOCKET
        };
        let mut source = EventSource::new(ChannelId::new(channel).unwrap(), channel_type);
        source.participant_id = Some(ParticipantId::new(sender).unwrap());
        let mut event = RoomEvent::at(
            EventId::new("evt_1").unwrap(),
            RoomId::new("room_1").unwrap(),
            crate::model::EventType::Message,
            source,
            TextContent::new("hi"),
            Timestamp::UNIX_EPOCH,
        );
        if let Some(name) = name {
            event
                .metadata
                .insert(SENDER_NAME.to_owned(), Value::String(name.to_owned()));
        }
        event
    }

    /// The records of `sent`, committed one after the other to `room_1`,
    /// each with the register the one before left, and that register.
    fn committed(sent: &[(&str, &str, Option<&str>)]) -> (Vec<Option<Value>>, Option<Metadata>) {
        let mut context = room();
        let mut records = Vec::new();
        for (channel, sender, name) in sent {
            let mut event = message(channel, sender, *name);
            let written = with_author(&mut event, &context, None).unwrap();
            records.push(event.metadata.get(AUTHOR).cloned());
            if written.is_some() {
                context.author_register = written;
            }
        }
        (records, context.author_register)
    }

    /// The Python implementation's records and register for the same
    /// messages (roomkit 2d6e6267, probe 9 of docs/steps/step-03.md).
    #[test]
    fn records_and_ranks_as_the_python_implementation_does() {
        let (records, register) = committed(&[
            ("sms", "+15550001", Some("Alice")),
            ("ws", "u-2", Some("ALICE")),
            ("sms", "+15550003", Some("\u{410}lice")),
            ("sms", "+15550001", Some("Alice")),
            ("ws", "u-5", Some("Bob")),
            ("ws", "u-6", Some("Lan")),
            ("ws", "u-7", Some("Ian")),
            ("ws", "u-8", Some("ian")),
            ("ws", "u-9", Some("  Jean-Luc\u{200b}  ")),
            ("ws", "u-10", Some("Jean Luc")),
            ("sms", "+15550011", None),
        ]);
        let author = |name: &str, rank: u32, source: &str| {
            Some(json!({ "name": name, "rank": rank, "source": source }))
        };
        assert_eq!(
            records,
            vec![
                author("Alice", 1, "b53db63d2136fd6f"),
                author("ALICE", 2, "f0a09d682358e721"),
                author("\u{410}lice", 3, "ca20ab0afe7d2a54"),
                author("Alice", 1, "b53db63d2136fd6f"),
                author("Bob", 1, "b8e5274fc03bd858"),
                author("Lan", 1, "9ee799f0eaf8ebc3"),
                author("Ian", 2, "e651944b10264c12"),
                author("ian", 3, "eefc94fb4355ccab"),
                author("Jean-Luc", 1, "0aa75761374fd7f9"),
                author("Jean Luc", 2, "9f511ab940ace609"),
                None,
            ]
        );
        assert_eq!(
            register.map(Value::Object),
            Some(json!({
                "names": {
                    "0342973908f267c0": "b53db63d2136fd6f:1 f0a09d682358e721:2 ca20ab0afe7d2a54:3",
                    "3d5f15e1b15907af": "e651944b10264c12:2 eefc94fb4355ccab:3",
                    "87bd064c9cadb523": "f0a09d682358e721:2",
                    "af42c677a85cf601": "b8e5274fc03bd858:1",
                    "cdb9781382facb1e": "9ee799f0eaf8ebc3:1 e651944b10264c12:2",
                    "d3477034863f2409": "0aa75761374fd7f9:1 9f511ab940ace609:2"
                },
                "version": 3
            }))
        );
    }

    /// A record the event came with is the runtime's alone: dropped.
    #[test]
    fn drops_a_record_the_event_came_with() {
        let mut event = message("sms", "+15550001", None);
        event
            .metadata
            .insert(AUTHOR.to_owned(), json!({ "name": "Admin" }));
        with_author(&mut event, &room(), None).unwrap();
        assert_eq!(event.metadata.get(AUTHOR), None);
    }

    /// A room with no register, whose window does not hold its timeline,
    /// needs the timeline read.
    #[test]
    fn needs_the_timeline_of_a_room_whose_window_does_not_hold_it() {
        let mut context = room();
        context.room.event_count = 3;
        let mut event = message("sms", "+15550001", Some("Alice"));
        assert_eq!(with_author(&mut event, &context, None), Err(NeedsTimeline));
    }
}
