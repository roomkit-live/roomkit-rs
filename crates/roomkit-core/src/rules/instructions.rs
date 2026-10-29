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
/// produced it (RFC §10.1.1 rule 6). The pipeline's alone: it writes the key
/// on a response to an instruction and removes it from every other event
/// (gap G-065).
pub(crate) const INSTRUCTION: &str = "instruction";

/// The metadata key under which an instruction carries its `standalone`
/// word, the typed field's (RFC §10.1.1 rules 4 and 7).
pub(crate) const STANDALONE: &str = "standalone";

/// Why an instruction is refused before anything is written (RFC §10.1.1
/// rules 1 and 2). The caller's result reports the reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum InstructionRefusal {
    /// It names no registered intelligence channel: no address, an empty
    /// one, or one naming only channels that are not agents (gap G-065).
    Unaddressed,
    /// It carries an idempotency key, which step 7 could never find: an
    /// instruction is never stored.
    NotIdempotent,
}

impl InstructionRefusal {
    /// The reason the refusal reports (RFC §10.1.1).
    pub(crate) fn reason(self) -> &'static str {
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
pub(crate) fn refusal(
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
pub(crate) fn may_stand_alone(event_type: EventType) -> bool {
    event_type == EventType::Instruction
}

/// The metadata of an instruction its channel parsed with `parsed`: the
/// parser's keys keep their values, the caller's `caller` metadata fills
/// the others, and `standalone` is the typed field's word whatever either
/// wrote (RFC §10.1.1 rule 4).
pub(crate) fn metadata(mut parsed: Metadata, caller: &Metadata, standalone: bool) -> Metadata {
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
pub(crate) fn stamp(instruction: &mut RoomEvent, caller: &Metadata, standalone: bool) {
    instruction.visibility = Visibility::Intelligence;
    let parsed = std::mem::take(&mut instruction.metadata);
    instruction.metadata = metadata(parsed, caller, standalone);
}

/// Whether `event` is an instruction that stands alone (RFC §10.1.1 rule
/// 7).
pub(crate) fn is_standalone(event: &RoomEvent) -> bool {
    event.event_type == EventType::Instruction
        && event.metadata.get(STANDALONE) == Some(&Value::Bool(true))
}

/// Puts back on `modified`, a hook's replacement of `original`, what the
/// pipeline checked and wrote for an instruction (RFC §9.4, §10.1.1):
///
/// - on an instruction, its visibility, its address and its `standalone`
///   word (gap G-020);
/// - under `instruction`, the original's fingerprint, which no hook removes
///   or replaces with the instruction's text, and nothing else: a key the
///   original holds that is not a fingerprint is not the pipeline's, and a
///   key the hook writes on an event the pipeline does not fingerprint is
///   removed (gap G-065).
///
/// The rest, an instruction's text and its other metadata included, is the
/// hook's to change.
pub(crate) fn keep_checked(original: &RoomEvent, modified: &mut RoomEvent) {
    keep_fingerprint(original, modified);
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

/// The key `instruction` of `modified` as `original` holds it: the
/// pipeline's fingerprint, or none (gap G-065). The stamps let a
/// fingerprint written by the pipeline only reach a pass (`mark_response`,
/// `clear_fingerprint`); this keeps a hook from undoing either, and never
/// restores a value that is not a fingerprint, which could hold text.
fn keep_fingerprint(original: &RoomEvent, modified: &mut RoomEvent) {
    match original.metadata.get(INSTRUCTION) {
        Some(value) if Fingerprint::from_value(value).is_some() => {
            modified
                .metadata
                .insert(INSTRUCTION.to_owned(), value.clone());
        }
        Some(_) | None => {
            modified.metadata.remove(INSTRUCTION);
        }
    }
}

/// The context of the turn `event` opens: `context` as the reader may see
/// it, without any history when `event` is an instruction that stands
/// alone (RFC §10.1.1 rule 7). The room, its bindings and its participants
/// stay: they are not the room's past. The pipeline hands every target the
/// context whole; reading none of it is the turn's duty, which a channel
/// applies with this rule to what it is handed.
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
pub(crate) struct Fingerprint {
    /// The SHA-256 of the text's UTF-8 bytes, in lowercase hexadecimal.
    pub sha256: String,
    /// The text's length in Unicode code points.
    pub length: usize,
}

impl Fingerprint {
    /// The fingerprint of `text`.
    pub(crate) fn of(text: &str) -> Self {
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
    pub(crate) fn of_instruction(instruction: &RoomEvent) -> Self {
        Self::of(&transcoding::text_of(&instruction.content))
    }

    /// The fingerprint as a response's metadata holds it:
    /// `{"sha256": ..., "length": ...}`.
    pub(crate) fn to_value(&self) -> Value {
        let mut fields = Metadata::new();
        fields.insert("sha256".to_owned(), Value::from(self.sha256.clone()));
        fields.insert("length".to_owned(), Value::from(self.length));
        Value::Object(fields)
    }

    /// The fingerprint `value` holds in the form [`to_value`](Self::to_value)
    /// writes: those two fields only, the digest as 64 lowercase hexadecimal
    /// digits. `None` for any other value, a text among them.
    pub(crate) fn from_value(value: &Value) -> Option<Self> {
        let fields = value.as_object().filter(|fields| fields.len() == 2)?;
        let sha256 = fields.get("sha256")?.as_str().filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })?;
        let length = usize::try_from(fields.get("length")?.as_u64()?).ok()?;
        Some(Self {
            sha256: sha256.to_owned(),
            length,
        })
    }
}

/// Writes on `response`, an event the delivery set of `trigger` returned,
/// the fingerprint of `trigger` when it is an instruction (RFC §10.1.1
/// rule 6, decision 9 of step 2). It replaces whatever the channel wrote
/// under the key, so the instruction's text never rides there. For any
/// other trigger the key is removed: a response to a message records no
/// instruction (gap G-065).
pub(crate) fn mark_response(response: &mut RoomEvent, trigger: &RoomEvent) {
    if trigger.event_type != EventType::Instruction {
        return clear_fingerprint(response);
    }
    let fingerprint = Fingerprint::of_instruction(trigger);
    response
        .metadata
        .insert(INSTRUCTION.to_owned(), fingerprint.to_value());
}

/// Removes the key `instruction` from an event the pipeline does not
/// fingerprint, whoever wrote it: a channel's parser, the caller of an
/// instruction, a hook's injected event (gap G-065). The RFC defines the
/// key on a response to an instruction only, where the pipeline writes it.
pub(crate) fn clear_fingerprint(event: &mut RoomEvent) {
    event.metadata.remove(INSTRUCTION);
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::model::{
        ChannelType, CompositeContent, EventContent, EventDirection, EventId, EventSource,
        Latitude, LocationContent, Longitude, Room, RoomId, SystemContent, TemplateContent,
        TextContent, Timestamp, ToolCallContent, ToolCallStatus,
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
                ChannelType::WEBSOCKET,
                EventDirection::Inbound,
            ),
            TextContent::new(text).into(),
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
                event.content = TextContent::new("Introduce yourself briefly.").into();
                event.metadata.insert("tone".to_owned(), json!("brief"));
            });
            assert_eq!(
                (kept.content, kept.metadata.get("tone")),
                (
                    TextContent::new("Introduce yourself briefly.").into(),
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

        /// Gap G-065: a key that is not a fingerprint is not the
        /// pipeline's, so a hook's redaction of it stands.
        #[test]
        fn a_modify_removing_a_key_that_is_no_fingerprint_stays_removed() {
            let mut message = instruction("Hi");
            message.event_type = EventType::Message;
            message
                .metadata
                .insert(INSTRUCTION.to_owned(), json!("Offer a refund."));
            let mut modified = message.clone();
            modified.metadata.remove(INSTRUCTION);
            keep_checked(&message, &mut modified);
            assert_eq!(modified.metadata.get(INSTRUCTION), None);
        }

        /// Gap G-065: the key is the pipeline's, on an event it does not
        /// fingerprint too.
        #[test]
        fn a_modify_cannot_write_the_key_on_an_event_holding_none() {
            let mut message = instruction("Hi");
            message.event_type = EventType::Message;
            let mut modified = message.clone();
            modified
                .metadata
                .insert(INSTRUCTION.to_owned(), json!("Offer a refund."));
            keep_checked(&message, &mut modified);
            assert_eq!(modified.metadata.get(INSTRUCTION), None);
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
        fn is_read_back_from_the_value_it_writes() {
            let fingerprint = Fingerprint::of("abc");
            assert_eq!(
                Fingerprint::from_value(&fingerprint.to_value()),
                Some(fingerprint)
            );
        }

        /// Gap G-065: only the form the pipeline writes reads as one.
        #[test]
        fn is_not_read_from_another_value() {
            let digest = Fingerprint::of("abc").sha256;
            let values = [
                json!("Introduce yourself."),
                json!({ "sha256": digest.to_uppercase(), "length": 3 }),
                json!({ "sha256": "abc", "length": 3 }),
                json!({ "sha256": digest, "length": -1 }),
                json!({ "sha256": digest, "length": 3, "text": "abc" }),
                json!({ "sha256": digest }),
            ];
            assert_eq!(
                values
                    .iter()
                    .filter_map(Fingerprint::from_value)
                    .collect::<Vec<_>>(),
                []
            );
        }

        #[test]
        fn of_an_instruction_is_of_its_text() {
            assert_eq!(
                Fingerprint::of_instruction(&instruction("abc")),
                Fingerprint::of("abc")
            );
        }

        /// Gap G-072: a tool call reads as its name, a system notice as its
        /// body, a composite as its parts' texts one per line, an empty one
        /// skipped. The Python implementation pins the same cases and the
        /// same texts, so the two fingerprint an instruction alike.
        #[test]
        fn of_an_instruction_is_of_the_text_its_content_reads_as() {
            let tool_call = || {
                EventContent::ToolCall(ToolCallContent {
                    tool_name: "lookup".to_owned(),
                    tool_id: "call_1".to_owned(),
                    arguments: Metadata::new(),
                    result: Value::Null,
                    status: ToolCallStatus::Pending,
                    duration_ms: None,
                    error: None,
                    structured_content: None,
                    outcome: None,
                    refused_but_ran: false,
                })
            };
            let notice = || EventContent::System(SystemContent::new("Call transferred."));
            let composite = |parts| EventContent::Composite(CompositeContent::new(parts).unwrap());
            let cases = [
                (tool_call(), "[Tool call: lookup]"),
                (notice(), "Call transferred."),
                (
                    composite(vec![
                        TextContent::new("First.").into(),
                        TextContent::new("").into(),
                        TextContent::new("Then.").into(),
                    ]),
                    "First.\nThen.",
                ),
                (
                    composite(vec![notice(), tool_call()]),
                    "Call transferred.\n[Tool call: lookup]",
                ),
                (
                    composite(vec![
                        EventContent::Location(LocationContent {
                            latitude: Latitude::new(45.5).unwrap(),
                            longitude: Longitude::new(-73.6).unwrap(),
                            label: Some("Office".to_owned()),
                            address: None,
                        }),
                        EventContent::Template(TemplateContent {
                            template_id: "welcome".to_owned(),
                            language: "en".to_owned(),
                            parameters: BTreeMap::new(),
                            body: None,
                        }),
                    ]),
                    "[Location: Office (45.5, -73.6)]\n[Template: welcome]",
                ),
                (
                    EventContent::Location(LocationContent {
                        latitude: Latitude::new(45.0).unwrap(),
                        longitude: Longitude::new(-73.0).unwrap(),
                        label: None,
                        address: None,
                    }),
                    "[Location: (45.0, -73.0)]",
                ),
            ];
            for (content, text) in cases {
                let mut event = instruction("");
                event.content = content;
                assert_eq!(
                    Fingerprint::of_instruction(&event),
                    Fingerprint::of(text),
                    "{text:?}"
                );
            }
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

        /// Gap G-065.
        #[test]
        fn a_response_to_a_message_loses_what_its_channel_wrote_under_the_key() {
            let mut response = answer();
            response
                .metadata
                .insert(INSTRUCTION.to_owned(), json!("Offer a refund."));
            mark_response(&mut response, &answer());
            assert_eq!(response.metadata.get(INSTRUCTION), None);
        }
    }

    /// Gap G-065.
    #[test]
    fn clearing_the_fingerprint_removes_the_key_and_keeps_the_rest() {
        let mut event = instruction("Hi");
        event.metadata = metadata_of(json!({ "instruction": "Offer a refund.", "tone": "brief" }));
        clear_fingerprint(&mut event);
        assert_eq!(Value::Object(event.metadata), json!({ "tone": "brief" }));
    }
}
