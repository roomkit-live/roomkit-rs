//! The fields the pipeline owns on the events it commits.
//!
//! A channel parses a message into an event and produces response events; a
//! hook replaces a payload or injects events. The pipeline then writes the
//! fields that decide where an event goes and what it may do, so that no
//! channel or hook can, for example, commit an event under another channel's
//! name or at another depth.

use crate::model::{
    ChannelBinding, ChannelCategory, DeleteContent, DirectEvent, EditContent, ErrorRecord,
    EventContent, EventDirection, EventId, EventSource, EventStatus, EventType, InboundMessage,
    InjectedEvent, Metadata, ParticipantId, RoomEvent, RoomId, Timestamp, Visibility,
};
use crate::ops::ChannelDeclaration;
use crate::rules::{chain_depth, edits, instructions};

/// The event a channel that keeps no data of its own parses from `message`:
/// its content, its provider data, and its sender, in the channel's form
/// (RFC §10.1 step 1), named on the source as its `participant_id`, as the
/// binding a room is created for names it (RFC §5.7, §10.1 step 2). A
/// driver's default `handle_inbound()` (RFC §6.1).
pub fn default_inbound_event(
    message: &InboundMessage,
    channel: &ChannelDeclaration,
    room_id: RoomId,
    id: EventId,
    now: Timestamp,
) -> RoomEvent {
    let mut source = EventSource::new(
        message.channel_id.clone(),
        channel.channel_type,
        EventDirection::Inbound,
    );
    source.provider.clone_from(&channel.provider_name);
    source.participant_id = ParticipantId::new(message.sender_id.clone()).ok();
    source.external_id.clone_from(&message.external_id);
    source.raw_payload.clone_from(&message.raw_payload);
    source
        .provider_message_id
        .clone_from(&message.provider_message_id);
    let mut event = RoomEvent::new(
        id.clone(),
        room_id,
        message.event_type,
        source,
        message.content.clone(),
        now,
    );
    event.metadata.clone_from(&message.metadata);
    stamp_trigger(event, message, channel, id)
}

/// Writes the pipeline's fields on the event a channel parsed from
/// `message`: the id the pipeline gives it (RFC §8.1), the type, depth,
/// visibility, address, thread and idempotency key the message asked for,
/// and the receiving channel as its inbound source. An instruction is
/// visible to intelligence channels only, and carries the caller's
/// metadata and its `standalone` word (RFC §10.1.1 rules 3 and 4). No
/// trigger carries the key a response to an instruction records, whoever
/// wrote it (gap G-065).
pub(crate) fn stamp_trigger(
    mut event: RoomEvent,
    message: &InboundMessage,
    channel: &ChannelDeclaration,
    id: EventId,
) -> RoomEvent {
    event.id = id;
    event.event_type = message.event_type;
    event.chain_depth = message.chain_depth;
    event.visibility = message.visibility.clone();
    event.addressed_to.clone_from(&message.addressed_to);
    event.parent_event_id.clone_from(&message.parent_event_id);
    event.idempotency_key.clone_from(&message.idempotency_key);
    event.responds_to = None;
    event.source.channel_id = message.channel_id.clone();
    event.source.channel_type = channel.channel_type;
    event.source.direction = EventDirection::Inbound;
    if event.source.provider.is_none() {
        event.source.provider.clone_from(&channel.provider_name);
    }
    if message.event_type == EventType::Instruction {
        instructions::stamp(&mut event, &message.metadata, message.standalone);
    }
    instructions::clear_fingerprint(&mut event);
    reset_commit_fields(event)
}

/// The event the integrator injects directly as `direct` into `room_id`
/// (RFC §10.5), with the id the pipeline gives it and the fields it owns:
/// its source is the registered `channel`, `INBOUND` for a transport and
/// `OUTBOUND` for an intelligence channel, whose event is its own words
/// travelling out (gap G-069). An instruction is stamped as a message's is
/// (RFC §10.1.1 rules 3 and 4).
pub(crate) fn stamp_direct(
    direct: DirectEvent,
    channel: &ChannelDeclaration,
    room_id: RoomId,
    id: EventId,
    now: Timestamp,
) -> RoomEvent {
    let direction = if channel.category == ChannelCategory::Intelligence {
        EventDirection::Outbound
    } else {
        EventDirection::Inbound
    };
    let mut source = EventSource::new(direct.channel_id, channel.channel_type, direction);
    source.provider.clone_from(&channel.provider_name);
    source.participant_id = direct.participant_id;
    let mut event = RoomEvent::new(id, room_id, direct.event_type, source, direct.content, now);
    event.chain_depth = direct.chain_depth;
    event.visibility = direct.visibility;
    event.addressed_to = direct.addressed_to;
    event.parent_event_id = direct.parent_event_id;
    event.idempotency_key = direct.idempotency_key;
    event.metadata.clone_from(&direct.metadata);
    if event.event_type == EventType::Instruction {
        instructions::stamp(&mut event, &direct.metadata, direct.standalone);
    }
    instructions::clear_fingerprint(&mut event);
    reset_commit_fields(event)
}

/// Writes the pipeline's fields on a response of `responder` to `trigger`:
/// a fresh id (gap G-024), the responder as its outbound source, one hop
/// deeper than the trigger (RFC §8.3), answering the trigger unless the
/// channel named the event it answers (RFC §8.5), and the fingerprint of
/// the instruction that produced it (RFC §10.1.1 rule 6), or no
/// `instruction` key at all when the trigger is not one (gap G-065).
///
/// A response under its own source keeps the source's other fields. One
/// that claims another channel's source keeps nothing of it, its
/// participant, provider, ids, raw payload and verification included: its
/// source is the responder's alone (gap G-035), so another channel's
/// provider message id is never acted on as the response's. A response
/// keeps the visibility its channel gave it, except `internal`
/// ([`without_internal`]).
pub(crate) fn stamp_response(
    mut response: RoomEvent,
    responder: &ChannelBinding,
    trigger: &RoomEvent,
    id: EventId,
) -> RoomEvent {
    if response.source.channel_id == responder.channel_id {
        response.source.channel_type = responder.channel_type;
        response.source.direction = EventDirection::Outbound;
    } else {
        tracing::warn!(
            target: "roomkit::core::inbound",
            room_id = %trigger.room_id,
            responder = %responder.channel_id,
            claimed = %response.source.channel_id,
            "a response named another channel as its source; the responder is its source, \
             and nothing of the claimed one is kept (gaps G-022, G-035)"
        );
        response.source = EventSource::new(
            responder.channel_id.clone(),
            responder.channel_type,
            EventDirection::Outbound,
        );
    }
    response.id = id;
    response.room_id = trigger.room_id.clone();
    response.chain_depth = chain_depth::response_depth(trigger);
    if response.responds_to.is_none() {
        response.responds_to = Some(trigger.id.clone());
    }
    // Only an inbound message carries an idempotency key (RFC §13.4, gap
    // G-023): a response's key could otherwise shadow a later message's.
    response.idempotency_key = None;
    without_internal(&mut response, responder);
    instructions::mark_response(&mut response, trigger);
    reset_commit_fields(response)
}

/// Puts `none` in place of the visibility `internal` on a response of
/// `responder` (gap G-041): only the framework writes `internal`, on the
/// records it writes itself, and a channel never forges that mark. The
/// response is stored and shown to no channel, which is what the channel
/// asked for.
fn without_internal(response: &mut RoomEvent, responder: &ChannelBinding) {
    if response.visibility != Visibility::Internal {
        return;
    }
    tracing::warn!(
        target: "roomkit::core::inbound",
        room_id = %response.room_id,
        responder = %responder.channel_id,
        "a response carried the visibility internal, which only the framework writes; \
         it is committed with none (gap G-041)"
    );
    response.visibility = Visibility::None;
}

/// Writes the pipeline's fields on an event a hook injected while deciding
/// on `trigger` (gap G-021): a fresh id (gap G-024), the trigger's room and
/// depth, and a visibility naming its targets only, `none` when it is stored
/// only, which its commit narrows to the targets the room binds (gap
/// G-031). It records no instruction (gap G-065).
pub(crate) fn stamp_injected(
    injected: InjectedEvent,
    trigger: &RoomEvent,
    id: EventId,
) -> RoomEvent {
    let mut event = injected.event;
    event.id = id;
    event.room_id = trigger.room_id.clone();
    event.chain_depth = trigger.chain_depth;
    event.visibility = injected
        .target_channel_ids
        .and_then(|targets| Visibility::channels(targets).ok())
        .unwrap_or(Visibility::None);
    event.idempotency_key = None;
    instructions::clear_fingerprint(&mut event);
    reset_commit_fields(event)
}

/// Keeps the identity and the bookkeeping of `original` on a payload a
/// `BEFORE_BROADCAST` hook replaced (RFC §9.4): a hook may change what an
/// event says, where it is addressed and who may see it, never which event it
/// is, where it came from, how deep it is or what became of it. Nor what step
/// 8a checked on an edit or a delete ([`kept_change`]), nor an instruction's
/// visibility, address and `standalone` word, nor the key `instruction`,
/// the fingerprint a response to one records and no other event holds (RFC
/// §10.1.1, gaps G-020 and G-065). Nor may it write the visibility
/// `internal`, which only the framework writes (gap G-041).
pub(crate) fn keep_identity(original: &RoomEvent, mut modified: RoomEvent) -> RoomEvent {
    modified.content = kept_change(&original.content, modified.content);
    instructions::keep_checked(original, &mut modified);
    if modified.visibility == Visibility::Internal {
        // Only the framework writes `internal`, on its own records: a hook's
        // modify that writes it is put back to the original's (gap G-041).
        modified.visibility.clone_from(&original.visibility);
    }
    modified.id = original.id.clone();
    modified.room_id = original.room_id.clone();
    modified.event_type = original.event_type;
    modified.source = original.source.clone();
    modified.index = original.index;
    modified.chain_depth = original.chain_depth;
    modified.responds_to.clone_from(&original.responds_to);
    modified
        .idempotency_key
        .clone_from(&original.idempotency_key);
    modified.created_at = original.created_at;
    modified.status = original.status;
    modified.blocked_by.clone_from(&original.blocked_by);
    modified.consumption.clone_from(&original.consumption);
    modified
        .delivery_results
        .clone_from(&original.delivery_results);
    modified
}

/// The content a hook's replacement of `original` keeps (RFC §9.4, §10.1
/// step 8a, gap G-062): an edit keeps its target and its `edit_source`, a
/// delete its target and its `delete_type`, which step 8a checked; the
/// hook's new content of an edit and reason of a delete stand. A
/// replacement that drops an edit's or a delete's content, or that makes an
/// edit or a delete appear anywhere else (another event's content, a
/// composite's part, an edit's new content), leaves the original's: an
/// event stays the change it was checked as, and no change gets past the
/// check.
fn kept_change(original: &EventContent, modified: EventContent) -> EventContent {
    match (original, modified) {
        (EventContent::Edit(original), EventContent::Edit(modified)) => {
            let new_content = if edits::holds_change(&modified.new_content) {
                original.new_content.clone()
            } else {
                modified.new_content
            };
            EventContent::Edit(EditContent {
                target_event_id: original.target_event_id.clone(),
                new_content,
                edit_source: original.edit_source,
            })
        }
        (EventContent::Delete(original), EventContent::Delete(modified)) => {
            EventContent::Delete(DeleteContent {
                target_event_id: original.target_event_id.clone(),
                delete_type: original.delete_type,
                reason: modified.reason,
            })
        }
        (EventContent::Edit(_) | EventContent::Delete(_), _) => original.clone(),
        (_, modified) if edits::holds_change(&modified) => original.clone(),
        (_, modified) => modified,
    }
}

/// The payload of `ON_ERROR` for a turn of `channel` that failed on
/// `trigger` (RFC §9.2): never stored, one hop deeper than the trigger, its
/// text the error's message.
pub(crate) fn error_event(
    id: EventId,
    channel: &ChannelBinding,
    trigger: &RoomEvent,
    error: &ErrorRecord,
    now: Timestamp,
) -> RoomEvent {
    let source = EventSource::new(
        channel.channel_id.clone(),
        channel.channel_type,
        EventDirection::Outbound,
    );
    let mut event = RoomEvent::new(
        id,
        trigger.room_id.clone(),
        EventType::System,
        source,
        EventContent::text(error.message.clone()),
        now,
    );
    event.chain_depth = chain_depth::response_depth(trigger);
    event.responds_to = Some(trigger.id.clone());
    let mut metadata = Metadata::new();
    metadata.insert("error".to_owned(), error.message.clone().into());
    metadata.insert("error_type".to_owned(), error.error_type.clone().into());
    metadata.insert("error_category".to_owned(), "generation".into());
    event.metadata = metadata;
    event
}

/// The event a target receives: the committed event with its content
/// transcoded for the target.
pub(crate) fn for_target(event: &RoomEvent, content: EventContent) -> RoomEvent {
    RoomEvent {
        content,
        ..event.clone()
    }
}

fn reset_commit_fields(mut event: RoomEvent) -> RoomEvent {
    event.status = EventStatus::Pending;
    event.blocked_by = None;
    event.index = 0;
    event
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{ChannelCategory, ChannelDirection, ChannelId, ChannelType, EventId};

    fn sms() -> ChannelDeclaration {
        ChannelDeclaration {
            provider_name: Some("TwilioSMS".to_owned()),
            ..ChannelDeclaration::new(
                ChannelId::new("sms").unwrap(),
                ChannelType::Sms,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
            )
        }
    }

    fn ai() -> ChannelBinding {
        ChannelBinding::new(
            ChannelId::new("ai").unwrap(),
            RoomId::new("room_1").unwrap(),
            ChannelType::Ai,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        )
    }

    fn message() -> InboundMessage {
        let mut message = InboundMessage::new(
            ChannelId::new("sms").unwrap(),
            "+15551234567",
            EventContent::text("Bonjour"),
        );
        message.chain_depth = 2;
        message.idempotency_key = Some("SM1".to_owned());
        message
    }

    fn fresh() -> EventId {
        EventId::new("evt_9").unwrap()
    }

    #[test]
    fn a_response_takes_the_id_the_pipeline_gives_it() {
        let response = stamp_response(trigger(), &ai(), &trigger(), fresh());
        assert_eq!(response.id, fresh());
    }

    fn trigger() -> RoomEvent {
        default_inbound_event(
            &message(),
            &sms(),
            RoomId::new("room_1").unwrap(),
            EventId::new("evt_1").unwrap(),
            Timestamp::UNIX_EPOCH,
        )
    }

    #[test]
    fn the_default_inbound_event_carries_the_message_s_depth_and_key() {
        let event = trigger();
        assert_eq!(
            (event.chain_depth, event.idempotency_key.as_deref()),
            (2, Some("SM1"))
        );
    }

    #[test]
    fn the_default_inbound_event_names_its_sender_as_the_source_s_participant() {
        assert_eq!(
            trigger().source.participant_id.map(String::from),
            Some("+15551234567".to_owned())
        );
    }

    #[test]
    fn the_default_inbound_event_names_the_channel_s_provider() {
        assert_eq!(trigger().source.provider.as_deref(), Some("TwilioSMS"));
    }

    #[test]
    fn a_parsed_event_is_sourced_from_the_receiving_channel() {
        let mut parsed = trigger();
        parsed.source.channel_id = ChannelId::new("other").unwrap();
        let stamped = stamp_trigger(parsed, &message(), &sms(), fresh());
        assert_eq!(stamped.source.channel_id.as_str(), "sms");
    }

    #[test]
    fn a_parsed_event_takes_the_id_the_pipeline_gives_it() {
        let mut parsed = trigger();
        parsed.id = EventId::new("SM1").unwrap();
        let stamped = stamp_trigger(parsed, &message(), &sms(), fresh());
        assert_eq!(stamped.id, fresh());
    }

    /// RFC §10.1.1 rule 4: the caller's metadata rides the instruction,
    /// whatever its parser kept.
    #[test]
    fn a_parsed_instruction_carries_the_caller_s_metadata() {
        let mut instruction = message();
        instruction.event_type = EventType::Instruction;
        instruction.idempotency_key = None;
        instruction
            .metadata
            .insert("task_id".to_owned(), "t_7".into());
        let parsed = RoomEvent {
            metadata: Metadata::new(),
            ..trigger()
        };
        let stamped = stamp_trigger(parsed, &instruction, &sms(), fresh());
        assert_eq!(
            serde_json::Value::Object(stamped.metadata),
            serde_json::json!({ "task_id": "t_7", "standalone": false })
        );
    }

    /// Gap G-065: the key a response to an instruction records.
    fn writing_the_instruction_key(mut event: RoomEvent) -> RoomEvent {
        event.metadata.insert(
            instructions::INSTRUCTION.to_owned(),
            "Offer a refund.".into(),
        );
        event
    }

    #[test]
    fn a_parsed_event_loses_the_instruction_key_its_parser_wrote() {
        let parsed = writing_the_instruction_key(trigger());
        let stamped = stamp_trigger(parsed, &message(), &sms(), fresh());
        assert_eq!(stamped.metadata.get(instructions::INSTRUCTION), None);
    }

    #[test]
    fn a_parsed_instruction_loses_the_instruction_key_its_caller_wrote() {
        let mut instruction = message();
        instruction.event_type = EventType::Instruction;
        instruction.idempotency_key = None;
        instruction.metadata.insert(
            instructions::INSTRUCTION.to_owned(),
            "Offer a refund.".into(),
        );
        let stamped = stamp_trigger(trigger(), &instruction, &sms(), fresh());
        assert_eq!(stamped.metadata.get(instructions::INSTRUCTION), None);
    }

    #[test]
    fn a_response_to_a_message_loses_the_instruction_key_its_channel_wrote() {
        let response = writing_the_instruction_key(trigger());
        let stamped = stamp_response(response, &ai(), &trigger(), fresh());
        assert_eq!(stamped.metadata.get(instructions::INSTRUCTION), None);
    }

    #[test]
    fn an_injected_event_loses_the_instruction_key_its_hook_wrote() {
        let injected = InjectedEvent::stored_only(writing_the_instruction_key(trigger()));
        let stamped = stamp_injected(injected, &trigger(), fresh());
        assert_eq!(stamped.metadata.get(instructions::INSTRUCTION), None);
    }

    #[test]
    fn a_response_is_one_hop_deeper_than_its_trigger() {
        let response = stamp_response(trigger(), &ai(), &trigger(), fresh());
        assert_eq!(response.chain_depth, 3);
    }

    #[test]
    fn a_response_answers_its_trigger() {
        let response = stamp_response(trigger(), &ai(), &trigger(), fresh());
        assert_eq!(response.responds_to, Some(trigger().id));
    }

    #[test]
    fn a_response_keeps_the_event_its_channel_says_it_answers() {
        let mut response = trigger();
        response.responds_to = Some(EventId::new("evt_0").unwrap());
        let stamped = stamp_response(response, &ai(), &trigger(), fresh());
        assert_eq!(
            stamped.responds_to.map(|id| id.to_string()),
            Some("evt_0".to_owned())
        );
    }

    #[test]
    fn a_response_is_sourced_from_its_responder_whatever_it_claims() {
        let stamped = stamp_response(trigger(), &ai(), &trigger(), fresh());
        assert_eq!(
            (stamped.source.channel_id.as_str(), stamped.source.direction),
            ("ai", EventDirection::Outbound)
        );
    }

    /// A response of the `ai` channel claiming the SMS sender's source, with
    /// all its fields.
    fn claiming_the_sender_s_source() -> RoomEvent {
        let mut response = trigger();
        response.source.verification = Some(crate::model::VerificationStatus::Verified);
        response.source.external_id = Some("crm_7".to_owned());
        response.source.provider_message_id = Some("SM42".to_owned());
        response
            .source
            .raw_payload
            .insert("MessageSid".to_owned(), "SM42".into());
        response
    }

    /// Gap G-035.
    #[test]
    fn a_response_claiming_another_source_keeps_nothing_of_it() {
        let stamped = stamp_response(claiming_the_sender_s_source(), &ai(), &trigger(), fresh());
        assert_eq!(
            stamped.source,
            EventSource::new(
                ChannelId::new("ai").unwrap(),
                ChannelType::Ai,
                EventDirection::Outbound
            )
        );
    }

    /// Gap G-035.
    #[test]
    fn a_response_under_its_own_source_keeps_its_other_fields() {
        let mut response = claiming_the_sender_s_source();
        response.source.channel_id = ChannelId::new("ai").unwrap();
        let stamped = stamp_response(response.clone(), &ai(), &trigger(), fresh());
        let expected = EventSource {
            channel_type: ChannelType::Ai,
            direction: EventDirection::Outbound,
            ..response.source
        };
        assert_eq!(stamped.source, expected);
    }

    #[test]
    fn an_injected_event_is_visible_to_its_targets_only() {
        let injected = InjectedEvent::to_channels(trigger(), vec![ChannelId::new("sms").unwrap()]);
        assert_eq!(
            stamp_injected(injected, &trigger(), fresh())
                .visibility
                .to_string(),
            "sms"
        );
    }

    #[test]
    fn a_stored_only_injected_event_is_visible_to_no_channel() {
        let injected = InjectedEvent::stored_only(trigger());
        assert_eq!(
            stamp_injected(injected, &trigger(), fresh()).visibility,
            Visibility::None
        );
    }

    #[test]
    fn a_modified_payload_keeps_the_original_s_source() {
        let mut modified = trigger();
        modified.source.channel_id = ChannelId::new("ws_admin").unwrap();
        modified.content = EventContent::text("[redacted]");
        let kept = keep_identity(&trigger(), modified);
        assert_eq!(
            (kept.source.channel_id.as_str(), kept.content),
            ("sms", EventContent::text("[redacted]"))
        );
    }

    #[test]
    fn a_modified_payload_keeps_the_original_s_delivery_results() {
        let mut modified = trigger();
        modified
            .delivery_results
            .insert("sms".to_owned(), "delivered".into());
        let kept = keep_identity(&trigger(), modified);
        assert!(kept.delivery_results.is_empty());
    }

    fn edit_of(target: &str, text: &str) -> RoomEvent {
        let mut edit = trigger();
        edit.event_type = EventType::Edit;
        edit.content = EventContent::Edit(EditContent {
            target_event_id: EventId::new(target).unwrap(),
            new_content: Box::new(EventContent::text(text)),
            edit_source: Some(crate::model::EditSource::Sender),
        });
        edit
    }

    /// RFC §9.4: the event an edit targets is kept; its new content is the
    /// hook's to change.
    #[test]
    fn a_modified_edit_keeps_its_target_and_takes_the_new_content() {
        let kept = keep_identity(
            &edit_of("evt_3", "I need 50000$"),
            edit_of("evt_1", "I need [redacted]"),
        );
        assert_eq!(kept.content, edit_of("evt_3", "I need [redacted]").content);
    }

    #[test]
    fn a_modified_edit_keeps_its_edit_source() {
        let mut system = edit_of("evt_3", "x");
        if let EventContent::Edit(edit) = &mut system.content {
            edit.edit_source = Some(crate::model::EditSource::System);
        }
        let kept = keep_identity(&edit_of("evt_3", "x"), system);
        assert_eq!(kept.content, edit_of("evt_3", "x").content);
    }

    #[test]
    fn a_modified_delete_keeps_its_target_and_its_type_and_takes_the_reason() {
        let delete = |target: &str, delete_type, reason: Option<&str>| {
            let mut event = trigger();
            event.event_type = EventType::Delete;
            event.content = EventContent::Delete(DeleteContent {
                target_event_id: EventId::new(target).unwrap(),
                delete_type,
                reason: reason.map(str::to_owned),
            });
            event
        };
        let kept = keep_identity(
            &delete("evt_3", crate::model::DeleteType::Sender, None),
            delete("evt_1", crate::model::DeleteType::Admin, Some("spam")),
        );
        assert_eq!(
            kept.content,
            delete("evt_3", crate::model::DeleteType::Sender, Some("spam")).content
        );
    }

    #[test]
    fn a_replacement_dropping_an_edit_s_content_keeps_the_original() {
        let mut text = edit_of("evt_3", "x");
        text.content = EventContent::text("plain");
        let kept = keep_identity(&edit_of("evt_3", "x"), text);
        assert_eq!(kept.content, edit_of("evt_3", "x").content);
    }

    #[test]
    fn a_modified_edit_whose_new_content_is_a_delete_keeps_the_original_new_content() {
        let mut nested = edit_of("evt_3", "x");
        if let EventContent::Edit(edit) = &mut nested.content {
            *edit.new_content = EventContent::Delete(DeleteContent {
                target_event_id: EventId::new("evt_9").unwrap(),
                delete_type: crate::model::DeleteType::Sender,
                reason: None,
            });
        }
        let kept = keep_identity(&edit_of("evt_3", "x"), nested);
        assert_eq!(kept.content, edit_of("evt_3", "x").content);
    }

    #[test]
    fn a_replacement_never_hides_an_edit_in_a_composite() {
        let mut smuggled = trigger();
        smuggled.content = EventContent::Composite(
            crate::model::CompositeContent::new(vec![
                EventContent::text("Hi"),
                edit_of("evt_3", "x").content,
            ])
            .unwrap(),
        );
        let kept = keep_identity(&trigger(), smuggled);
        assert_eq!(kept.content, trigger().content);
    }

    #[test]
    fn a_replacement_never_turns_a_message_into_an_edit() {
        let mut smuggled = trigger();
        smuggled.content = edit_of("evt_3", "x").content;
        let kept = keep_identity(&trigger(), smuggled);
        assert_eq!(kept.content, trigger().content);
    }

    #[test]
    fn a_modified_payload_keeps_its_new_address() {
        let mut modified = trigger();
        modified.addressed_to = Some(vec![ChannelId::new("billing").unwrap()]);
        let kept = keep_identity(&trigger(), modified.clone());
        assert_eq!(kept.addressed_to, modified.addressed_to);
    }
}
