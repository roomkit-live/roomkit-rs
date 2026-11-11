//! Who is who in a discussion: people, their labels, what an agent may read
//! (RFC §19.7.5): Python's `_people.py`.
//!
//! A person is told apart by the label the transcript gives them (§6.4):
//! their name kept to a name's characters, with the rank a look-alike of an
//! earlier source carries (`Alice (2)`), or the channel of a sender with no
//! name (`@sms1`), a form no name takes.

use std::collections::BTreeSet;
use std::sync::OnceLock;

use crate::model::{
    Access, ChannelCategory, ChannelId, Participant, ParticipantId, ParticipantRole,
    ParticipantStatus, RoomContext, RoomEvent,
};
use crate::rules::addresses::SYSTEM_SENDER;
use crate::rules::authors::{People, source_of};
use crate::rules::discussion::names::{EVERYONE, NAME_LIMIT, name_key};
use crate::rules::lookalike::{person_name, skeletons};
use crate::rules::permissions::visibility_allows;
use crate::rules::transcript::{channel_label, turn_labels};
use crate::rules::unicode::casefold;

/// What follows a name in a label when it reads like an agent's.
const A_PARTICIPANT: &str = " (a participant)";

/// What the labels a model reads as its own turns, or a summarizer as the
/// agent's and a one-to-one user's, read as. Python's `_agent_labels`.
fn agent_labels() -> &'static BTreeSet<String> {
    static LABELS: OnceLock<BTreeSet<String>> = OnceLock::new();
    LABELS.get_or_init(|| {
        ["You", "you (in a separate session)", "assistant", "user"]
            .into_iter()
            .flat_map(skeletons)
            .collect()
    })
}

/// A turn's label as a discussion reads it: with `(a participant)` after a
/// name reading as one of an agent's own labels (Python's `_ranked`), which
/// the transcript leaves out (RFC §6.4) and which keeps such a
/// sender from being addressed. A nameless sender's label (`@sms`) has no
/// name to read.
#[must_use]
pub fn labelled(label: String) -> String {
    if !label.starts_with('@') && !skeletons(label_name(&label)).is_disjoint(agent_labels()) {
        return format!("{label}{A_PARTICIPANT}");
    }
    label
}

/// Each name people are addressed by, ignoring case, and the people behind
/// it: their label and their source, in the order first met. Two sources
/// behind one name make it ambiguous.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PeopleIndex {
    named: Vec<(String, Vec<Person>)>,
}

/// One person behind a name: their label and who their turns come from.
type Person = (String, Vec<String>);

impl PeopleIndex {
    /// The people behind `key`, a name case-folded.
    fn get(&self, key: &str) -> &[Person] {
        self.named
            .iter()
            .find(|(known, _)| known == key)
            .map_or(&[], |(_, people)| people.as_slice())
    }

    /// Adds a person under `key`: one person once, whatever their label's
    /// case; another source behind the same name (a sender stamping a
    /// participant's name) is another.
    fn add(&mut self, key: String, label: String, source: Vec<String>) {
        if let Some((_, people)) = self.named.iter_mut().find(|(known, _)| *known == key) {
            if !people.iter().any(|(_, known)| *known == source) {
                people.push((label, source));
            }
            return;
        }
        self.named.push((key, vec![(label, source)]));
    }
}

/// Whether `agent` may read `event`, as the router's delivery set says
/// (§10.1 step 12): a binding that reads, and the event's visibility. A name
/// is no way around it.
#[must_use]
pub fn sees(agent: &ChannelId, event: &RoomEvent, context: &RoomContext) -> bool {
    context.get_binding(agent).is_some_and(|binding| {
        matches!(binding.access, Access::ReadWrite | Access::ReadOnly)
            && visibility_allows(&event.visibility, binding)
    })
}

/// Whether `role` takes part as a person (RFC §5, §6.4): a bot is no person,
/// and an observer reads without taking part.
fn is_person_role(role: ParticipantRole) -> bool {
    !matches!(role, ParticipantRole::Bot | ParticipantRole::Observer)
}

/// A transport's event from a participant that is neither an agent nor a
/// bot, or with no participant record behind it on its channel (rule 2, RFC
/// §5.5); never the framework's own sender of a delivery (§22).
#[must_use]
pub fn is_person(event: &RoomEvent, context: &RoomContext) -> bool {
    is_person_among(event, context, &People::new(&context.participants))
}

fn is_person_among(event: &RoomEvent, context: &RoomContext, people: &People<'_>) -> bool {
    if event
        .source
        .participant_id
        .as_ref()
        .is_some_and(|sender| sender.as_str() == SYSTEM_SENDER)
    {
        return false;
    }
    let Some(binding) = context.get_binding(&event.source.channel_id) else {
        return false;
    };
    binding.category == ChannelCategory::Transport
        && people
            .named(event)
            .is_none_or(|participant| is_person_role(participant.role))
}

/// Whether the room records a person among its participants.
#[must_use]
pub fn records_people(context: &RoomContext) -> bool {
    context.participants.iter().any(active_person)
}

/// The person a message is from, as the transcript labels them: ranked among
/// the room's recent turns, so a second nameless sender of a channel reads
/// `@chat (2)` here too (RFC §6.4).
#[must_use]
pub fn person_label(event: &RoomEvent, context: &RoomContext) -> String {
    turn_labels(context.recent_events.iter().chain([event]), context)
        .pop()
        .flatten()
        .filter(|label| !label.is_empty())
        .map_or_else(|| channel_label(&event.source.channel_id), labelled)
}

/// The name a label gives, without the rank a look-alike name carries nor
/// the `(a participant)` an agent-like name does: `ALICE (2)` gives `ALICE`.
/// No name holds a parenthesis, so the tail is the runtime's. Python's
/// `label_name`.
fn label_name(label: &str) -> &str {
    let label = label.strip_suffix(A_PARTICIPANT).unwrap_or(label);
    let label = without_rank(label);
    label.strip_suffix(A_PARTICIPANT).unwrap_or(label)
}

/// `label` without a final ` (2)`.
fn without_rank(label: &str) -> &str {
    label
        .strip_suffix(')')
        .and_then(|open| open.rsplit_once(" ("))
        .filter(|(_, rank)| !rank.is_empty() && rank.chars().all(|c| c.is_ascii_digit()))
        .map_or(label, |(name, _)| name)
}

/// Each name agents address people by, ignoring case, and the people it
/// names, as the transcript labels them (§6.4): the room's active people and
/// the speakers of its recent messages; with `listed` (`people`), only
/// those. A name is kept to a name's characters (`@AliceMartin`), never an
/// agent's handle (one of `handles`) nor one reading like it or like `all` (a
/// Cyrillic `а` in `@аlice`: an agent writing it believes it asks the agent),
/// nor longer than [`NAME_LIMIT`]; two labels behind one name (`Alice Martin`,
/// `AliceMartin`) make it ambiguous,
/// a look-alike's ranked label (`Alice (2)`) is addressed by no name, and
/// two nameless senders of a channel (`@chat`, `@chat (2)`) make its name
/// ambiguous.
#[must_use]
pub fn people_index(
    context: &RoomContext,
    handles: &[String],
    listed: Option<&[String]>,
) -> PeopleIndex {
    let allowed: Option<Vec<String>> = listed.map(|listed| {
        listed
            .iter()
            .map(|person| casefold(&name_key(person)))
            .collect()
    });
    let taken: Vec<String> = handles.iter().map(|handle| casefold(handle)).collect();
    let reads_as: BTreeSet<String> = handles
        .iter()
        .map(String::as_str)
        .chain([EVERYONE])
        .flat_map(skeletons)
        .collect();
    let mut index = PeopleIndex::default();
    for (label, source) in addressable(context) {
        let name = name_key(label.strip_prefix('@').unwrap_or(&label));
        let key = casefold(&name);
        if name.is_empty()
            || name.chars().count() > NAME_LIMIT
            || taken.contains(&key)
            || !skeletons(&name).is_disjoint(&reads_as)
        {
            continue;
        }
        if allowed
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&key))
        {
            continue;
        }
        index.add(key, label, source);
    }
    index
}

/// The people agents may address, each label and source: the room's active
/// people, then the speakers of its recent messages.
fn addressable(context: &RoomContext) -> Vec<Person> {
    let people = People::new(&context.participants);
    let mut entries: Vec<Person> = context
        .participants
        .iter()
        .filter(|participant| active_person(participant))
        .map(|participant| (label_of(participant), participant_source(&participant.id)))
        .collect();
    let spoken: Vec<&RoomEvent> = context
        .recent_events
        .iter()
        .filter(|event| is_person_among(event, context, &people))
        .collect();
    let labels = turn_labels(spoken.iter().copied(), context);
    for (event, label) in spoken.iter().copied().zip(labels) {
        // A nameless sender's rank (`@chat (2)`) is filed under its channel's
        // name, which then names neither alone; a look-alike's ranked name is
        // addressed by none.
        let Some(label) = label.filter(|label| !label.is_empty()).map(labelled) else {
            continue;
        };
        let name = label_name(&label);
        if label == name || label.starts_with('@') {
            entries.push((name.to_owned(), source_of(event, &people)));
        }
    }
    entries
}

/// The names an index addresses people by, each once.
#[must_use]
pub fn names_of(index: &PeopleIndex) -> Vec<String> {
    index
        .named
        .iter()
        .filter_map(|(_, people)| people.first())
        .map(|(label, _)| name_key(label.strip_prefix('@').unwrap_or(label)))
        .collect()
}

/// The label of the one person `name` addresses, or `None` when no one or
/// several sources answer to it: neither may then answer for the other.
#[must_use]
pub fn one_person<'a>(index: &'a PeopleIndex, name: &str) -> Option<&'a str> {
    match index.get(&casefold(name)) {
        [(label, _)] => Some(label.as_str()),
        _ => None,
    }
}

/// Whether `participant` is an active person of the room.
fn active_person(participant: &Participant) -> bool {
    participant.status == ParticipantStatus::Active && is_person_role(participant.role)
}

/// The source of a participant's turns.
fn participant_source(id: &ParticipantId) -> Vec<String> {
    vec!["participant".to_owned(), id.to_string()]
}

/// The label the transcript gives a participant's turns: their name, else
/// their channel's (`@sms1`).
fn label_of(participant: &Participant) -> String {
    let name = person_name(participant.display_name.as_deref().unwrap_or_default());
    if name.is_empty() {
        channel_label(&participant.channel_id)
    } else {
        name
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::Value;

    use super::*;
    use crate::model::{
        ChannelBinding, ChannelDirection, ChannelType, EventId, EventSource, EventType, Room,
        RoomId, TextContent, Timestamp, Visibility,
    };

    /// A room binding the transport `sms` and the agent `dev`.
    fn room() -> RoomContext {
        let mut context = RoomContext::new(Room::at(room_id(), Timestamp::UNIX_EPOCH));
        for (channel, channel_type, category) in [
            ("sms", ChannelType::SMS, ChannelCategory::Transport),
            ("dev", ChannelType::AI, ChannelCategory::Intelligence),
        ] {
            context.bindings.push(ChannelBinding::at(
                channel_id(channel),
                room_id(),
                channel_type,
                category,
                ChannelDirection::Bidirectional,
                Timestamp::UNIX_EPOCH,
            ));
        }
        context
    }

    fn room_id() -> RoomId {
        RoomId::new("r1").unwrap()
    }

    fn channel_id(channel: &str) -> ChannelId {
        ChannelId::new(channel).unwrap()
    }

    /// A participant reached on `sms`.
    fn person(id: &str, name: Option<&str>) -> Participant {
        let mut person = Participant::at(
            ParticipantId::new(id).unwrap(),
            room_id(),
            channel_id("sms"),
            Timestamp::UNIX_EPOCH,
        );
        person.display_name = name.map(ToOwned::to_owned);
        person
    }

    /// A message on `channel`, from `sender` signed `name` when given.
    fn said(id: &str, channel: &str, sender: Option<&str>, name: Option<&str>) -> RoomEvent {
        let mut source = EventSource::new(channel_id(channel), ChannelType::SMS);
        source.participant_id = sender.map(|sender| ParticipantId::new(sender).unwrap());
        let mut event = RoomEvent::at(
            EventId::new(id).unwrap(),
            room_id(),
            EventType::Message,
            source,
            TextContent::new("hi"),
            Timestamp::UNIX_EPOCH,
        );
        if let Some(name) = name {
            event
                .metadata
                .insert("sender_name".to_owned(), Value::String(name.to_owned()));
        }
        event
    }

    fn index_of(context: &RoomContext) -> PeopleIndex {
        people_index(context, &[], None)
    }

    // Python: tests/orchestration/test_discussion_edges.py::test_a_delivery_is_not_a_persons_answer
    #[test]
    fn a_delivery_is_not_a_persons_answer() {
        let context = room();
        assert!(!is_person(
            &said("e1", "sms", Some(SYSTEM_SENDER), None),
            &context
        ));
        assert!(is_person(&said("e2", "sms", Some("u1"), None), &context));
    }

    #[test]
    fn a_person_is_a_transport_s_sender_neither_an_agent_nor_a_bot() {
        let mut context = room();
        let mut bot = person("b1", Some("Robot"));
        bot.role = ParticipantRole::Bot;
        let mut observer = person("o1", Some("Auditor"));
        observer.role = ParticipantRole::Observer;
        context
            .participants
            .extend([bot, observer, person("p1", Some("Ana"))]);
        assert!(!is_person(&said("e1", "sms", Some("b1"), None), &context));
        assert!(!is_person(&said("e2", "sms", Some("o1"), None), &context));
        assert!(is_person(&said("e3", "sms", Some("p1"), None), &context));
        // No record behind the sender: a person.
        assert!(is_person(&said("e4", "sms", Some("u9"), None), &context));
        assert!(is_person(&said("e5", "sms", None, None), &context));
        // An agent's channel, or a channel the room does not bind: no person.
        assert!(!is_person(&said("e6", "dev", Some("p1"), None), &context));
        assert!(!is_person(&said("e7", "web", Some("p1"), None), &context));
    }

    #[test]
    fn the_room_records_a_person_only_while_one_is_active() {
        let mut context = room();
        assert!(!records_people(&context));
        let mut left = person("p1", Some("Ana"));
        left.status = ParticipantStatus::Left;
        context.participants.push(left);
        assert!(!records_people(&context));
        context.participants.push(person("p2", None));
        assert!(records_people(&context));
    }

    #[test]
    fn an_agent_sees_what_its_binding_reads_and_the_event_s_visibility_admits() {
        let mut context = room();
        let dev = channel_id("dev");
        let mut event = said("e1", "sms", Some("u1"), None);
        assert!(sees(&dev, &event, &context));
        assert!(!sees(&channel_id("sre"), &event, &context));
        event.visibility = Visibility::Transport;
        assert!(!sees(&dev, &event, &context));
        event.visibility = Visibility::All;
        for binding in &mut context.bindings {
            binding.access = Access::WriteOnly;
        }
        assert!(!sees(&dev, &event, &context));
    }

    // Python: tests/orchestration/test_discussion_edges.py::test_a_person_with_an_accented_name_can_be_addressed
    #[test]
    fn a_person_with_an_accented_name_can_be_addressed() {
        let mut context = room();
        context.participants.push(person("p1", Some("Hélène Roy")));
        let index = index_of(&context);
        assert_eq!(names_of(&index), ["HélèneRoy"]);
        assert_eq!(one_person(&index, "hélèneROY"), Some("Hélène Roy"));
    }

    // Python: tests/orchestration/test_discussion_edges.py::test_a_name_two_people_answer_to_records_no_ask
    #[test]
    fn a_name_two_people_answer_to_records_no_ask() {
        let mut context = room();
        context.participants.extend([
            person("p1", Some("Alice Martin")),
            person("p2", Some("AliceMartin")),
        ]);
        let index = index_of(&context);
        assert_eq!(names_of(&index), ["AliceMartin"]);
        assert_eq!(one_person(&index, "alicemartin"), None);
    }

    #[test]
    fn one_person_once_whatever_their_label_s_case() {
        let mut context = room();
        context.participants.push(person("p1", Some("Ana")));
        context
            .recent_events
            .push(said("e1", "sms", Some("p1"), Some("ANA")));
        let index = index_of(&context);
        assert_eq!(one_person(&index, "ana"), Some("Ana"));
    }

    #[test]
    fn a_look_alike_s_ranked_label_is_addressed_by_no_name() {
        let mut context = room();
        context.recent_events.extend([
            said("e1", "sms", Some("u1"), Some("Alice")),
            said("e2", "sms", Some("u2"), Some("ALICE")),
        ]);
        let index = index_of(&context);
        assert_eq!(one_person(&index, "alice"), Some("Alice"));
    }

    #[test]
    fn two_nameless_senders_of_a_channel_make_its_name_ambiguous() {
        let mut context = room();
        context
            .recent_events
            .push(said("e1", "sms", Some("u1"), None));
        assert_eq!(one_person(&index_of(&context), "sms"), Some("@sms"));
        context
            .recent_events
            .push(said("e2", "sms", Some("u2"), None));
        let index = index_of(&context);
        assert_eq!(names_of(&index), ["sms"]);
        assert_eq!(one_person(&index, "sms"), None);
    }

    #[test]
    fn a_person_s_label_is_ranked_among_the_room_s_recent_turns() {
        let mut context = room();
        let first = said("e1", "sms", Some("u1"), None);
        context.recent_events.push(first.clone());
        assert_eq!(person_label(&first, &context), "@sms");
        let second = said("e2", "sms", Some("u2"), None);
        assert_eq!(person_label(&second, &context), "@sms (2)");
    }

    #[test]
    fn a_name_is_never_an_agent_s_handle_nor_past_the_limit_and_keeps_to_the_list() {
        let mut context = room();
        let long = "x".repeat(NAME_LIMIT + 1);
        let longest = "y".repeat(NAME_LIMIT);
        context.participants.extend([
            person("p1", Some("Ana")),
            person("p2", Some("DEV")),
            person("p3", Some(long.as_str())),
            person("p4", Some(longest.as_str())),
        ]);
        let handles = ["dev".to_owned()];
        assert_eq!(
            names_of(&people_index(&context, &handles, None)),
            ["Ana", longest.as_str()]
        );
        let listed = ["ana".to_owned(), "Bob".to_owned()];
        assert_eq!(
            names_of(&people_index(&context, &handles, Some(&listed))),
            ["Ana"]
        );
    }

    #[test]
    fn a_participant_without_a_name_is_addressed_by_its_channel() {
        let mut context = room();
        context.participants.push(person("p1", None));
        let index = index_of(&context);
        assert_eq!(names_of(&index), ["sms"]);
        assert_eq!(one_person(&index, "sms"), Some("@sms"));
    }

    // Python: tests/orchestration/test_discussion_edges.py::test_a_person_named_like_a_handle_is_addressed_by_no_name
    #[test]
    fn test_a_person_named_like_a_handle_is_addressed_by_no_name() {
        let lookalike = "\u{430}lice";
        let mut context = room();
        context
            .participants
            .extend([person("p1", Some(lookalike)), person("p2", Some("Ana"))]);
        let index = people_index(&context, &["alice".to_owned()], None);
        assert_eq!(names_of(&index), ["Ana"]);
        assert_eq!(one_person(&index, lookalike), None);
        // Their label does not change: it has no @, so it stays apart from an agent's.
        context
            .recent_events
            .push(said("e1", "sms", Some("p1"), None));
        assert_eq!(
            person_label(&said("e2", "sms", Some("p1"), None), &context),
            lookalike
        );
    }

    // Python: tests/orchestration/test_discussion_edges.py::test_a_person_named_like_all_is_addressed_by_no_name
    #[test]
    fn test_a_person_named_like_all_is_addressed_by_no_name() {
        for name in ["\u{430}ll", "ALL", "all"] {
            for handles in [Vec::new(), vec!["alice".to_owned()]] {
                let mut context = room();
                context
                    .participants
                    .extend([person("p1", Some(name)), person("p2", Some("Ana"))]);
                let index = people_index(&context, &handles, None);
                assert_eq!(names_of(&index), ["Ana"], "{name:?} {handles:?}");
                assert_eq!(one_person(&index, name), None, "{name:?} {handles:?}");
            }
        }
    }

    /// `metadata` as a room stores a discussion's handles.
    fn with_handles(context: &mut RoomContext, channel: &str, handle: &str) {
        context.room.metadata = serde_json::from_value(serde_json::json!({
            "_discussion": {"agents": [channel], "handles": {channel: handle}, "max_depth": 5}
        }))
        .unwrap();
    }

    // Python's channels/_speaker.py _Unrecorded.handles, what its tests read
    // through the transcript at a later lot:
    // tests/orchestration/test_discussion_handles.py::test_notes_and_the_transcript_present_agents_by_their_handles
    #[test]
    fn a_nameless_label_is_the_handle_the_stored_discussion_gives() {
        let mut context = room();
        with_handles(&mut context, "dev", "ops");
        assert_eq!(
            person_label(&said("e1", "dev", None, None), &context),
            "@ops"
        );
        assert_eq!(
            person_label(&said("e2", "sms", None, None), &context),
            "@sms"
        );

        with_handles(&mut context, "sms", "desk");
        context.participants.push(person("p1", None));
        assert_eq!(
            crate::rules::transcript::people_taking_part(&context, &channel_id("dev")),
            ["@desk"]
        );
        context.participants.clear();
        assert_eq!(
            person_label(&said("e3", "sms", None, None), &context),
            "@desk"
        );
        context
            .recent_events
            .push(said("e4", "sms", Some("u1"), None));
        assert_eq!(names_of(&index_of(&context)), ["desk"]);
    }

    // What Python's `channels/_speaker.py` `_ranked` gives a name reading as
    // one of an agent's own labels, and `_people.py` `people_index` of it.
    #[test]
    fn a_sender_named_like_an_agent_s_own_label_is_addressed_by_no_name() {
        for (name, label) in [
            ("You", "You (a participant)"),
            ("assistant", "assistant (a participant)"),
            ("user", "user (a participant)"),
            ("Y\u{43e}u", "Y\u{43e}u (a participant)"),
        ] {
            let mut context = room();
            context
                .recent_events
                .push(said("e1", "sms", Some("u1"), Some(name)));
            assert_eq!(
                person_label(&said("e2", "sms", Some("u1"), Some(name)), &context),
                label
            );
            let index = index_of(&context);
            assert_eq!(names_of(&index), Vec::<String>::new(), "{name:?}");
            assert_eq!(one_person(&index, name), None, "{name:?}");
        }
        // A rank comes before the tail.
        let mut context = room();
        context
            .recent_events
            .push(said("e1", "sms", Some("u1"), Some("You")));
        context
            .recent_events
            .push(said("e2", "sms", Some("u2"), Some("YOU")));
        assert_eq!(
            person_label(&said("e3", "sms", Some("u2"), Some("YOU")), &context),
            "YOU (2) (a participant)"
        );
        // A participant is addressed by the name it records, whatever its turns
        // are labelled.
        let mut context = room();
        context.participants.push(person("p1", Some("assistant")));
        let spoken = said("e1", "sms", Some("p1"), None);
        context.recent_events.push(spoken.clone());
        assert_eq!(person_label(&spoken, &context), "assistant (a participant)");
        assert_eq!(
            one_person(&index_of(&context), "assistant"),
            Some("assistant")
        );
    }

    // What Python's `channels/_speaker.py` `label_name` answers.
    #[test]
    fn a_label_s_name_leaves_the_runtime_s_tail_out() {
        for (label, name) in [
            ("ALICE (2)", "ALICE"),
            ("Alice (a participant)", "Alice"),
            ("Alice (2) (a participant)", "Alice"),
            ("Alice (a participant) (2)", "Alice"),
            ("Alice (a participant) (a participant)", "Alice"),
            ("@chat (2)", "@chat"),
            ("Alice (x)", "Alice (x)"),
            ("(2)", "(2)"),
            (" (2)", ""),
            ("Alice ()", "Alice ()"),
        ] {
            assert_eq!(label_name(label), name, "{label:?}");
        }
    }
}
