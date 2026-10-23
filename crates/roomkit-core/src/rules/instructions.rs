//! Instructions (RFC §10.1.1): what an instruction must be before anything
//! is read, what the pipeline writes on it and keeps of it through a
//! hook's modify, what the turn it opens reads, and the fingerprint its
//! responses carry.
//!
//! An instruction is the application directing an agent for one turn. It
//! goes through the inbound pipeline's hooks, source check and lane, and
//! never into the timeline: the inbound operation decides it without
//! committing it (gap G-065). These functions answer what the RFC fixes
//! about it.

use std::fmt::Write as _;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::model::{ChannelId, EventType, Metadata, RoomContext, RoomEvent, Visibility};
use crate::rules::transcoding;

/// The metadata key under which a response records the instruction that
/// produced it (RFC §10.1.1 rule 6).
pub const INSTRUCTION: &str = "instruction";

/// The metadata key under which an instruction carries its `standalone`
/// word, the typed field's (RFC §10.1.1 rules 4 and 7).
pub const STANDALONE: &str = "standalone";

/// Why an instruction is refused before anything is written (RFC §10.1.1
/// rules 1 and 2). The caller's result reports the reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum InstructionRefusal {
    /// It names no registered intelligence channel: no address, an empty
    /// one, or one naming only channels that are not agents (gap G-065).
    Unaddressed,
    /// It carries an idempotency key, which step 7 could never find: an
    /// instruction is never stored.
    NotIdempotent,
}

impl InstructionRefusal {
    /// The reason the refusal reports (RFC §10.1.1).
    pub fn reason(self) -> &'static str {
        match self {
            Self::Unaddressed => "instruction_unaddressed",
            Self::NotIdempotent => "instruction_not_idempotent",
        }
    }
}

/// Why an instruction addressed to `addressed_to`, carrying
/// `idempotency_key`, is refused, in the order of the RFC's rules; `None`
/// when it is not (RFC §10.1.1 rules 1 and 2).
///
/// `is_intelligence` says whether a channel id names a registered
/// intelligence channel. An address must name at least one; the other names
/// it holds ask nobody, as an address naming a channel the room does not
/// bind (RFC §19.3 rule 2, gap G-065).
pub fn refusal(
    addressed_to: Option<&[ChannelId]>,
    idempotency_key: Option<&str>,
    is_intelligence: impl Fn(&ChannelId) -> bool,
) -> Option<InstructionRefusal> {
    if !addressed_to.is_some_and(|names| names.iter().any(is_intelligence)) {
        return Some(InstructionRefusal::Unaddressed);
    }
    idempotency_key.map(|_| InstructionRefusal::NotIdempotent)
}

/// Whether an event of `event_type` may stand alone: an instruction only
/// (RFC §10.1.1 rule 7).
pub fn may_stand_alone(event_type: EventType) -> bool {
    event_type == EventType::Instruction
}

/// The metadata of an instruction its channel parsed with `parsed`: the
/// parser's keys keep their values, the caller's `caller` metadata fills
/// the others, and `standalone` is the typed field's word whatever either
/// wrote (RFC §10.1.1 rule 4).
pub fn metadata(mut parsed: Metadata, caller: &Metadata, standalone: bool) -> Metadata {
    for (key, value) in caller {
        if !parsed.contains_key(key) {
            parsed.insert(key.clone(), value.clone());
        }
    }
    parsed.insert(STANDALONE.to_owned(), Value::Bool(standalone));
    parsed
}

/// Writes on `instruction`, as its channel parsed it, what the pipeline
/// owns: the visibility `intelligence`, whatever the caller asked, and the
/// caller's metadata with the `standalone` word (RFC §10.1.1 rules 3 and
/// 4).
pub fn stamp(instruction: &mut RoomEvent, caller: &Metadata, standalone: bool) {
    instruction.visibility = Visibility::Intelligence;
    let parsed = std::mem::take(&mut instruction.metadata);
    instruction.metadata = metadata(parsed, caller, standalone);
}

/// Whether `event` is an instruction that stands alone (RFC §10.1.1 rule
/// 7).
pub fn is_standalone(event: &RoomEvent) -> bool {
    event.event_type == EventType::Instruction
        && event.metadata.get(STANDALONE) == Some(&Value::Bool(true))
}

/// Puts back on `modified`, a hook's replacement of `original`, what the
/// pipeline checked and wrote for an instruction (RFC §9.4, §10.1.1):
///
/// - on an instruction, its visibility, its address and its `standalone`
///   word (gap G-020);
/// - on a response to one, the fingerprint it records, which no hook
///   removes or replaces with the instruction's text (gap G-065).
///
/// The rest, an instruction's text and its other metadata included, is the
/// hook's to change.
pub fn keep_checked(original: &RoomEvent, modified: &mut RoomEvent) {
    if let Some(fingerprint) = original.metadata.get(INSTRUCTION) {
        modified
            .metadata
            .insert(INSTRUCTION.to_owned(), fingerprint.clone());
    }
    if original.event_type != EventType::Instruction {
        return;
    }
    modified.visibility = original.visibility.clone();
    modified.addressed_to.clone_from(&original.addressed_to);
    match original.metadata.get(STANDALONE) {
        Some(word) => {
            modified
                .metadata
                .insert(STANDALONE.to_owned(), word.clone());
        }
        None => {
            modified.metadata.remove(STANDALONE);
        }
    }
}

/// The context of the turn `event` opens: `context` as the reader may see
/// it, without any history when `event` is an instruction that stands
/// alone (RFC §10.1.1 rule 7). The room, its bindings and its participants
/// stay: they are not the room's past.
pub fn turn_context(event: &RoomEvent, context: RoomContext) -> RoomContext {
    if !is_standalone(event) {
        return context;
    }
    RoomContext {
        recent_events: Vec::new(),
        ..context
    }
}

/// What a response records of the instruction that produced it, never its
/// text (RFC §10.1.1 rule 6).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Fingerprint {
    /// The SHA-256 of the text's UTF-8 bytes, in lowercase hexadecimal.
    pub sha256: String,
    /// The text's length in Unicode code points.
    pub length: usize,
}

impl Fingerprint {
    /// The fingerprint of `text`.
    pub fn of(text: &str) -> Self {
        let digest = Sha256::digest(text.as_bytes());
        let sha256 = digest
            .iter()
            .fold(String::with_capacity(64), |mut hex, byte| {
                // Writing to a `String` cannot fail.
                let _ = write!(hex, "{byte:02x}");
                hex
            });
        Self {
            sha256,
            length: text.chars().count(),
        }
    }

    /// The fingerprint of `instruction`: of the text its content reads as,
    /// its body for a text (RFC §6.6, gap G-065).
    pub fn of_instruction(instruction: &RoomEvent) -> Self {
        Self::of(&transcoding::text_of(&instruction.content))
    }

    /// The fingerprint as a response's metadata holds it:
    /// `{"sha256": ..., "length": ...}`.
    pub fn to_value(&self) -> Value {
        let mut fields = Metadata::new();
        fields.insert("sha256".to_owned(), Value::from(self.sha256.clone()));
        fields.insert("length".to_owned(), Value::from(self.length));
        Value::Object(fields)
    }
}

/// Writes on `response`, an event the delivery set of `trigger` returned,
/// the fingerprint of `trigger` when it is an instruction (RFC §10.1.1
/// rule 6, decision 9 of step 2). It replaces whatever the channel wrote
/// under the key, so the instruction's text never rides there. Leaves the
/// response as it is for any other trigger.
pub fn mark_response(response: &mut RoomEvent, trigger: &RoomEvent) {
    if trigger.event_type != EventType::Instruction {
        return;
    }
    let fingerprint = Fingerprint::of_instruction(trigger);
    response
        .metadata
        .insert(INSTRUCTION.to_owned(), fingerprint.to_value());
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::model::{
        ChannelType, EventContent, EventDirection, EventId, EventSource, Room, RoomId, Timestamp,
    };

    fn channel(id: &str) -> ChannelId {
        ChannelId::new(id).unwrap()
    }

    fn instruction(text: &str) -> RoomEvent {
        RoomEvent::new(
            EventId::new("evt_1").unwrap(),
            RoomId::new("room_1").unwrap(),
            EventType::Instruction,
            EventSource::new(
                channel("app"),
                ChannelType::Websocket,
                EventDirection::Inbound,
            ),
            EventContent::text(text),
            Timestamp::UNIX_EPOCH,
        )
    }

    fn metadata_of(value: Value) -> Metadata {
        serde_json::from_value(value).unwrap()
    }

    /// RFC §10.1.1 rules 1 and 2.
    mod refusal {
        use super::*;
        use pretty_assertions::assert_eq;

        /// `ai` is the one registered intelligence channel.
        fn refused(addressed_to: Option<&[ChannelId]>, key: Option<&str>) -> Option<&'static str> {
            refusal(addressed_to, key, |id| id.as_str() == "ai").map(InstructionRefusal::reason)
        }

        #[test]
        fn an_instruction_addressed_to_an_agent_is_accepted() {
            assert_eq!(refused(Some(&[channel("ai")]), None), None);
        }

        #[test]
        fn an_unaddressed_instruction_is_refused() {
            assert_eq!(refused(None, None), Some("instruction_unaddressed"));
        }

        #[test]
        fn an_instruction_addressed_to_nobody_is_refused() {
            assert_eq!(refused(Some(&[]), None), Some("instruction_unaddressed"));
        }

        #[test]
        fn an_instruction_naming_no_intelligence_channel_is_refused() {
            assert_eq!(
                refused(Some(&[channel("sms"), channel("ghost")]), None),
                Some("instruction_unaddressed")
            );
        }

        #[test]
        fn an_instruction_naming_an_agent_beside_other_channels_is_accepted() {
            assert_eq!(refused(Some(&[channel("sms"), channel("ai")]), None), None);
        }

        #[test]
        fn an_instruction_carrying_a_key_is_refused() {
            assert_eq!(
                refused(Some(&[channel("ai")]), Some("k1")),
                Some("instruction_not_idempotent")
            );
        }

        /// Rule 1 comes first.
        #[test]
        fn an_unaddressed_instruction_carrying_a_key_is_refused_as_unaddressed() {
            assert_eq!(refused(None, Some("k1")), Some("instruction_unaddressed"));
        }
    }

    /// RFC §10.1.1 rule 7.
    #[test]
    fn only_an_instruction_may_stand_alone() {
        assert_eq!(
            (
                may_stand_alone(EventType::Instruction),
                may_stand_alone(EventType::Message)
            ),
            (true, false)
        );
    }

    /// RFC §10.1.1 rule 4.
    mod metadata {
        use super::*;
        use pretty_assertions::assert_eq;

        #[test]
        fn the_caller_s_metadata_rides_the_instruction() {
            let caller = metadata_of(json!({ "task_id": "t_1" }));
            assert_eq!(
                Value::Object(metadata(Metadata::new(), &caller, false)),
                json!({ "task_id": "t_1", "standalone": false })
            );
        }

        #[test]
        fn a_key_the_parser_set_keeps_its_value() {
            let parsed = metadata_of(json!({ "task_id": "parsed" }));
            let caller = metadata_of(json!({ "task_id": "caller" }));
            assert_eq!(
                metadata(parsed, &caller, false).get("task_id"),
                Some(&json!("parsed"))
            );
        }

        #[test]
        fn standalone_is_the_typed_field_s_word() {
            let parsed = metadata_of(json!({ "standalone": true }));
            let caller = metadata_of(json!({ "standalone": true }));
            assert_eq!(
                metadata(parsed, &caller, false).get(STANDALONE),
                Some(&json!(false))
            );
        }
    }

    /// RFC §10.1.1 rule 3.
    #[test]
    fn a_stamped_instruction_is_visible_to_intelligence_channels_only() {
        let mut event = instruction("Introduce yourself.");
        event.visibility = Visibility::All;
        stamp(&mut event, &Metadata::new(), false);
        assert_eq!(event.visibility, Visibility::Intelligence);
    }

    #[test]
    fn a_stamped_standalone_instruction_stands_alone() {
        let mut event = instruction("Summarize the call.");
        stamp(&mut event, &Metadata::new(), true);
        assert!(is_standalone(&event));
    }

    #[test]
    fn a_message_never_stands_alone_whatever_its_metadata() {
        let mut event = instruction("Hi");
        event.event_type = EventType::Message;
        event.metadata.insert(STANDALONE.to_owned(), json!(true));
        assert!(!is_standalone(&event));
    }

    /// RFC §9.4, gap G-020.
    mod keep_checked {
        use super::*;
        use pretty_assertions::assert_eq;

        fn original() -> RoomEvent {
            let mut event = instruction("Introduce yourself.");
            event.addressed_to = Some(vec![channel("ai")]);
            stamp(&mut event, &Metadata::new(), true);
            event
        }

        fn replaced(change: impl FnOnce(&mut RoomEvent)) -> RoomEvent {
            let mut modified = original();
            change(&mut modified);
            keep_checked(&original(), &mut modified);
            modified
        }

        #[test]
        fn a_modify_keeps_the_instruction_s_visibility() {
            let kept = replaced(|event| event.visibility = Visibility::All);
            assert_eq!(kept.visibility, Visibility::Intelligence);
        }

        #[test]
        fn a_modify_keeps_the_instruction_s_address() {
            let kept = replaced(|event| event.addressed_to = Some(vec![channel("billing")]));
            assert_eq!(kept.addressed_to, Some(vec![channel("ai")]));
        }

        #[test]
        fn a_modify_keeps_the_instruction_s_standalone_word() {
            let kept = replaced(|event| {
                event.metadata.remove(STANDALONE);
            });
            assert!(is_standalone(&kept));
        }

        #[test]
        fn a_modify_changes_the_instruction_s_text_and_other_metadata() {
            let kept = replaced(|event| {
                event.content = EventContent::text("Introduce yourself briefly.");
                event.metadata.insert("tone".to_owned(), json!("brief"));
            });
            assert_eq!(
                (kept.content, kept.metadata.get("tone")),
                (
                    EventContent::text("Introduce yourself briefly."),
                    Some(&json!("brief"))
                )
            );
        }

        /// Gap G-065: the fingerprint is the pipeline's.
        #[test]
        fn a_modify_keeps_a_response_s_fingerprint() {
            let mut response = instruction("Hello, I am the assistant.");
            response.event_type = EventType::Message;
            mark_response(&mut response, &original());
            let mut modified = response.clone();
            modified
                .metadata
                .insert(INSTRUCTION.to_owned(), json!("Introduce yourself."));
            keep_checked(&response, &mut modified);
            assert_eq!(
                modified.metadata.get(INSTRUCTION),
                response.metadata.get(INSTRUCTION)
            );
        }

        #[test]
        fn a_modify_cannot_remove_a_response_s_fingerprint() {
            let mut response = instruction("Hello, I am the assistant.");
            response.event_type = EventType::Message;
            mark_response(&mut response, &original());
            let mut modified = response.clone();
            modified.metadata.clear();
            keep_checked(&response, &mut modified);
            assert_eq!(
                modified.metadata.get(INSTRUCTION),
                Some(&Fingerprint::of("Introduce yourself.").to_value())
            );
        }

        #[test]
        fn a_modify_of_a_message_keeps_what_the_hook_wrote() {
            let mut message = instruction("Hi");
            message.event_type = EventType::Message;
            let mut modified = message.clone();
            modified.visibility = Visibility::Transport;
            keep_checked(&message, &mut modified);
            assert_eq!(modified.visibility, Visibility::Transport);
        }
    }

    /// RFC §10.1.1 rule 7.
    mod turn_context {
        use super::*;
        use pretty_assertions::assert_eq;

        fn context() -> RoomContext {
            RoomContext {
                recent_events: vec![instruction("an earlier summary")],
                ..RoomContext::new(Room::new(
                    RoomId::new("room_1").unwrap(),
                    Timestamp::UNIX_EPOCH,
                ))
            }
        }

        #[test]
        fn a_standalone_instruction_s_turn_reads_no_history() {
            let mut event = instruction("Summarize the call.");
            stamp(&mut event, &Metadata::new(), true);
            assert_eq!(turn_context(&event, context()).recent_events, []);
        }

        #[test]
        fn an_instruction_s_turn_reads_the_history() {
            let mut event = instruction("Introduce yourself.");
            stamp(&mut event, &Metadata::new(), false);
            assert_eq!(turn_context(&event, context()), context());
        }
    }

    /// RFC §10.1.1 rule 6, against the FIPS 180-4 examples of the NIST
    /// Cryptographic Standards and Guidelines.
    mod fingerprint {
        use super::*;
        use pretty_assertions::assert_eq;

        fn sha256(text: &str) -> String {
            Fingerprint::of(text).sha256
        }

        #[test]
        fn of_the_empty_text() {
            assert_eq!(
                sha256(""),
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            );
        }

        /// FIPS 180-4 SHA-256 example, one block.
        #[test]
        fn of_abc() {
            assert_eq!(
                sha256("abc"),
                "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            );
        }

        /// FIPS 180-4 SHA-256 example, two blocks (448 bits).
        #[test]
        fn of_a_two_block_text() {
            assert_eq!(
                sha256("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
                "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
            );
        }

        /// The 896-bit example of the NIST test vectors.
        #[test]
        fn of_an_896_bit_text() {
            assert_eq!(
                sha256(
                    "abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmn\
                     hijklmnoijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu"
                ),
                "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1"
            );
        }

        /// The one-million-`a` example of the NIST test vectors.
        #[test]
        fn of_a_million_a() {
            assert_eq!(
                sha256(&"a".repeat(1_000_000)),
                "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
            );
        }

        /// The bytes hashed are the text's UTF-8: `ë` is two of them.
        #[test]
        fn of_a_text_beyond_ascii() {
            assert_eq!(
                sha256("Greet Zoë"),
                "787fd18c3b96e8509d2430e948a26a7857c002869826bb0b8f189d62e7a03b3e"
            );
        }

        /// `ë` is one code point in two bytes, the wave one in four.
        #[test]
        fn counts_code_points_not_bytes() {
            assert_eq!(
                (
                    Fingerprint::of("Greet Zoë").length,
                    Fingerprint::of("Greet the customer \u{1F44B}").length
                ),
                (9, 20)
            );
        }

        #[test]
        fn is_written_as_its_two_fields() {
            assert_eq!(
                Fingerprint::of("abc").to_value(),
                json!({
                    "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                    "length": 3,
                })
            );
        }

        #[test]
        fn of_an_instruction_is_of_its_text() {
            assert_eq!(
                Fingerprint::of_instruction(&instruction("abc")),
                Fingerprint::of("abc")
            );
        }
    }

    /// RFC §10.1.1 rule 6.
    mod mark_response {
        use super::*;
        use pretty_assertions::assert_eq;

        fn answer() -> RoomEvent {
            let mut event = instruction("Hello, I am the assistant.");
            event.event_type = EventType::Message;
            event
        }

        #[test]
        fn a_response_to_an_instruction_records_its_fingerprint() {
            let mut response = answer();
            mark_response(&mut response, &instruction("abc"));
            assert_eq!(
                response.metadata.get(INSTRUCTION),
                Some(&Fingerprint::of("abc").to_value())
            );
        }

        #[test]
        fn a_channel_s_own_record_is_replaced() {
            let mut response = answer();
            response
                .metadata
                .insert(INSTRUCTION.to_owned(), json!("abc"));
            mark_response(&mut response, &instruction("abc"));
            assert_eq!(
                response.metadata.get(INSTRUCTION),
                Some(&Fingerprint::of("abc").to_value())
            );
        }

        #[test]
        fn a_response_to_a_message_records_nothing() {
            let mut response = answer();
            mark_response(&mut response, &answer());
            assert_eq!(response.metadata, Metadata::new());
        }
    }
}
