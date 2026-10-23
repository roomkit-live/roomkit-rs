//! The fields the pipeline owns on the events it commits.
//!
//! A channel parses a message into an event and produces response events; a
//! hook replaces a payload or injects events. The pipeline then writes the
//! fields that decide where an event goes and what it may do, so that no
//! channel or hook can, for example, commit an event under another channel's
//! name or at another depth.

use crate::model::{
    ChannelBinding, ErrorRecord, EventContent, EventDirection, EventId, EventSource, EventStatus,
    EventType, InboundMessage, InjectedEvent, Metadata, ParticipantId, RoomEvent, RoomId,
    Timestamp, Visibility,
};
use crate::ops::ChannelDeclaration;
use crate::rules::chain_depth;

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
/// and the receiving channel as its inbound source.
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
    reset_commit_fields(event)
}

/// Writes the pipeline's fields on a response of `responder` to `trigger`:
/// a fresh id (gap G-024), the responder as its outbound source, one hop
/// deeper than the trigger (RFC §8.3), answering the trigger unless the
/// channel named the event it answers (RFC §8.5).
pub(crate) fn stamp_response(
    mut response: RoomEvent,
    responder: &ChannelBinding,
    trigger: &RoomEvent,
    id: EventId,
) -> RoomEvent {
    if response.source.channel_id != responder.channel_id {
        tracing::warn!(
            target: "roomkit::core::inbound",
            room_id = %trigger.room_id,
            responder = %responder.channel_id,
            claimed = %response.source.channel_id,
            "a response named another channel as its source; the responder is its source (gap G-022)"
        );
    }
    response.id = id;
    response.room_id = trigger.room_id.clone();
    response.source.channel_id = responder.channel_id.clone();
    response.source.channel_type = responder.channel_type;
    response.source.direction = EventDirection::Outbound;
    response.chain_depth = chain_depth::response_depth(trigger);
    if response.responds_to.is_none() {
        response.responds_to = Some(trigger.id.clone());
    }
    // Only an inbound message carries an idempotency key (RFC §13.4, gap
    // G-023): a response's key could otherwise shadow a later message's.
    response.idempotency_key = None;
    reset_commit_fields(response)
}

/// Writes the pipeline's fields on an event a hook injected while deciding
/// on `trigger` (gap G-021): a fresh id (gap G-024), the trigger's room and
/// depth, and a visibility naming its targets only, `none` when it is stored
/// only.
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
    reset_commit_fields(event)
}

/// Keeps the identity and the bookkeeping of `original` on a payload a
/// `BEFORE_BROADCAST` hook replaced (RFC §9.4): a hook may change what an
/// event says, where it is addressed and who may see it, never which event it
/// is, where it came from, how deep it is or what became of it.
pub(crate) fn keep_identity(original: &RoomEvent, mut modified: RoomEvent) -> RoomEvent {
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

    #[test]
    fn a_modified_payload_keeps_its_new_address() {
        let mut modified = trigger();
        modified.addressed_to = Some(vec![ChannelId::new("billing").unwrap()]);
        let kept = keep_identity(&trigger(), modified.clone());
        assert_eq!(kept.addressed_to, modified.addressed_to);
    }
}
