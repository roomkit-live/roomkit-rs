//! A message into a room its caller names, effect by effect (RFC §10.1
//! steps 1 to 3, §5.7, §10.2 step 3d, §17.2): the sender in its channel's
//! form, the room created or attached to, the recipient a delivery needs,
//! the room's participants in the context, and the room's lane, held for
//! the whole call.
//!
//! The simulated driver checks on every run that the operation ended with
//! the lane left and the room lock released.

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelType, DeliveryStatus, ErrorRecord,
    EventContent, InboundMessage, Participant, ParticipantId, Room, RoomId, RoomStatus, Timestamp,
    Visibility,
};
use roomkit_core::ops::{ChannelDeclaration, FrameworkEvent, InboundError};
use roomkit_core::rules::addresses::CountryCode;
use roomkit_core::rules::hook_registry::HookTrigger;
use serde_json::json;

use crate::support::{Sim, after_start, channel, room_id};

/// A message from `sender` on `channel_id`.
fn message_from(channel_id: &str, sender: &str, text: &str) -> InboundMessage {
    InboundMessage::new(channel(channel_id), sender, EventContent::text(text))
}

/// A store with no room, an SMS channel and an agent no room binds.
fn no_room_yet() -> Sim {
    let mut sim = Sim::without_rooms();
    sim.unbound_transport("sms", ChannelType::SMS);
    sim.unbound_agent("ai", "Bonjour Jean!");
    sim
}

/// The binding of `channel_id` in `room_1`.
fn binding_of<'sim>(sim: &'sim Sim, channel_id: &str) -> Option<&'sim ChannelBinding> {
    sim.bindings
        .iter()
        .find(|binding| binding.channel_id.as_str() == channel_id)
}

/// RFC §10.1 step 2: a room the caller names that does not exist.
mod a_missing_room {
    use super::*;
    use pretty_assertions::assert_eq;

    /// RFC §10.1 step 2, Appendix B.1, gap G-050: the creation's hooks are
    /// awaited, the creation is reported, then the channel is attached and
    /// the session starts.
    #[test]
    fn is_created_its_hooks_awaited_and_reported_then_the_channel_attached_and_a_session_started() {
        let mut sim = no_room_yet();
        sim.observer_attaching("welcome", HookTrigger::OnRoomCreated, "ai");
        sim.observer("greeter", HookTrigger::OnChannelAttached);
        sim.observer("audit", HookTrigger::OnSessionStarted);
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let until_parse = sim
            .journal
            .iter()
            .position(|line| line == "handle_inbound sms")
            .unwrap();
        assert_eq!(
            sim.journal[..=until_parse],
            [
                "load_context room_1",
                "store room room_1 Active",
                "await on_room_created welcome",
                "emit room_created",
                "lock room_1",
                "load_context room_1",
                "store binding sms",
                "commit #0 system delivered \"Channel sms attached (read_write, all)\"",
                "release room_1",
                "emit room_channel_attached",
                "fire on_channel_attached",
                "emit session_started",
                "fire on_session_started",
                "handle_inbound sms",
            ]
        );
    }

    /// RFC §10.1 step 2, §13.6, gap G-076: a caller that stops waiting
    /// while the room's hooks are due expires the call, and the room is
    /// completed all the same: its hooks run, its channel is attached and
    /// its session starts, none of it within the call's bound. The message
    /// stops before its parse, nothing of it written.
    #[test]
    fn is_completed_when_its_caller_stops_waiting_before_its_hooks() {
        let mut sim = no_room_yet();
        sim.observer("welcome", HookTrigger::OnRoomCreated);
        sim.leaves_before = Some("await on_room_created");
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.reason,
                binding_of(&sim, "sms").map(|binding| binding.channel_id.to_string()),
                sim.timeline_lines(),
                sim.traced("handle_inbound"),
                sim.framework_event_names(),
            ),
            (
                Some("process_timeout".to_owned()),
                Some("sms".to_owned()),
                // The attach's record (gap G-046), nothing of the message.
                vec!["#0 system delivered \"Channel sms attached (read_write, all)\"".to_owned()],
                Vec::<&str>::new(),
                ["room_created", "room_channel_attached", "session_started"]
                    .map(str::to_owned)
                    .to_vec()
            )
        );
    }

    /// Gap G-050: hooks the driver could not run are logged, and step 2
    /// goes on.
    #[test]
    fn whose_hooks_could_not_be_run_still_has_the_channel_attached() {
        let mut sim = no_room_yet();
        sim.observer("welcome", HookTrigger::OnRoomCreated);
        sim.failing_effect = Some("await on_room_created");
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.blocked,
                binding_of(&sim, "sms").map(|binding| binding.channel_id.to_string())
            ),
            (false, Some("sms".to_owned()))
        );
    }

    #[test]
    fn hands_its_hooks_the_room_before_the_channel_is_attached() {
        let mut sim = no_room_yet();
        sim.observer("welcome", HookTrigger::OnRoomCreated);
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.awaited_contexts[0].bindings, []);
    }

    /// RFC Appendix B.1: the agent a hook attaches answers the first
    /// message, and its answer reaches the sender at their number.
    #[test]
    fn has_the_agent_its_hook_attached_answer_the_first_message() {
        let mut sim = no_room_yet();
        sim.observer_attaching("welcome", HookTrigger::OnRoomCreated, "ai");
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (sim.timeline_lines(), sim.traced("deliver")),
            (
                vec![
                    "#0 system delivered \"Channel sms attached (read_write, all)\"".to_owned(),
                    "#1 sms delivered \"Bonjour\"".to_owned(),
                    "#2 ai delivered \"Bonjour Jean!\"".to_owned(),
                ],
                vec!["deliver sms <- evt_6"]
            )
        );
    }

    #[test]
    fn binds_the_channel_to_the_sender_as_its_correspondent_and_recipient() {
        let mut sim = no_room_yet();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let binding = binding_of(&sim, "sms").unwrap();
        assert_eq!(
            (
                binding.participant_id.as_ref().map(ParticipantId::as_str),
                serde_json::Value::Object(binding.metadata.clone())
            ),
            (
                Some("+15551234567"),
                json!({ "phone_number": "+15551234567" })
            )
        );
    }

    #[test]
    fn is_created_in_the_organization_the_caller_acts_for() {
        let mut sim = no_room_yet();
        sim.send_to("room_1", Some("bank"), Sim::message("sms", "Bonjour"))
            .unwrap();
        assert_eq!(sim.room.organization_id.as_deref(), Some("bank"));
    }

    #[test]
    fn emits_session_started_for_a_text_channel() {
        let mut sim = no_room_yet();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.framework_events()[2],
            &FrameworkEvent::SessionStarted {
                room_id: room_id(),
                channel_id: channel("sms"),
                channel_type: ChannelType::SMS,
            }
        );
    }

    /// RFC §26.1: a voice session starts with its media, not with its room.
    #[test]
    fn starts_no_session_for_a_voice_channel() {
        let mut sim = Sim::without_rooms();
        sim.unbound_transport("phone", ChannelType::VOICE);
        sim.observer("audit", HookTrigger::OnSessionStarted);
        sim.send(Sim::message("phone", "Allo")).unwrap();
        assert_eq!(
            (sim.framework_event_names(), sim.async_triggers()),
            (
                ["room_created", "room_channel_attached", "event_processed"]
                    .map(str::to_owned)
                    .to_vec(),
                Vec::new()
            )
        );
    }

    #[test]
    fn starts_no_session_for_a_realtime_voice_channel() {
        let mut sim = Sim::without_rooms();
        sim.unbound_transport("live", ChannelType::REALTIME_VOICE);
        sim.send(Sim::message("live", "Allo")).unwrap();
        assert!(
            !sim.framework_event_names()
                .contains(&"session_started".to_owned())
        );
    }

    /// RFC §5.7, gap G-057: on a chat channel the correspondent and the
    /// recipient are the chat the message was posted in; the message's
    /// author is neither.
    #[test]
    fn names_the_chat_as_correspondent_and_recipient_on_a_chat_channel() {
        let mut sim = Sim::without_rooms();
        sim.unbound_transport("telegram", ChannelType::TELEGRAM);
        let mut message = message_from("telegram", "4242", "Hi");
        message
            .metadata
            .insert("chat_id".to_owned(), json!(-100_777));
        sim.send(message).unwrap();
        let binding = binding_of(&sim, "telegram").unwrap();
        assert_eq!(
            (
                binding.participant_id.as_ref().map(ParticipantId::as_str),
                serde_json::Value::Object(binding.metadata.clone())
            ),
            (Some("-100777"), json!({ "telegram_chat_id": "-100777" }))
        );
    }

    /// Gap G-057: a chat message that does not carry its chat names
    /// neither, and its deliveries are refused `no_recipient` until the
    /// integrator names one.
    #[test]
    fn names_neither_correspondent_nor_recipient_for_a_chat_message_without_its_chat() {
        let mut sim = Sim::without_rooms();
        sim.unbound_transport("telegram", ChannelType::TELEGRAM);
        sim.send(message_from("telegram", "4242", "Hi")).unwrap();
        let binding = binding_of(&sim, "telegram").unwrap();
        assert_eq!(
            (binding.participant_id.clone(), binding.metadata.len()),
            (None, 0)
        );
    }

    /// RFC §10.4: the framework's own sender names no correspondent.
    #[test]
    fn records_no_correspondent_for_the_framework_s_own_sender() {
        let mut sim = no_room_yet();
        sim.send(message_from("sms", "system", "Reminder")).unwrap();
        let binding = binding_of(&sim, "sms").unwrap();
        assert_eq!(
            (binding.participant_id.clone(), binding.metadata.len()),
            (None, 0)
        );
    }

    /// RFC §7.5 rule 7: an `ON_ROOM_CREATED` hook that attached the channel
    /// itself keeps its binding.
    #[test]
    fn keeps_the_binding_a_hook_made_for_the_channel() {
        let mut sim = no_room_yet();
        sim.observer_attaching("concierge", HookTrigger::OnRoomCreated, "sms");
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let binding = binding_of(&sim, "sms").unwrap();
        assert_eq!(
            (sim.traced("store binding"), binding.participant_id.clone()),
            (Vec::<&str>::new(), None)
        );
    }

    #[test]
    fn whose_creation_fails_fails_the_message_with_nothing_attached() {
        let mut sim = no_room_yet();
        sim.failing_effect = Some("store room");
        let sent = sim.send(Sim::message("sms", "Bonjour"));
        assert_eq!(
            (sent, sim.trace),
            (
                Err(InboundError::Store(ErrorRecord::new(
                    "StoreUnavailable",
                    "the store is down"
                ))),
                [
                    "load_context room_1",
                    "store room room_1 failed",
                    "load_context room_1",
                ]
                .map(str::to_owned)
                .to_vec()
            )
        );
    }

    /// Gap G-053: another call created the room between the read and the
    /// creation; the message goes on into it as into any room that exists.
    #[test]
    fn created_meanwhile_is_taken_as_a_room_that_exists() {
        let mut sim = no_room_yet();
        sim.created_meanwhile = Some(Room::new(room_id(), after_start(0)));
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.framework_event_names(),
                binding_of(&sim, "sms").unwrap().participant_id.clone()
            ),
            (
                ["room_channel_attached", "event_processed"]
                    .map(str::to_owned)
                    .to_vec(),
                None
            )
        );
    }

    #[test]
    fn created_meanwhile_by_another_organization_fails_the_message() {
        let mut sim = no_room_yet();
        let mut clinic = Room::new(room_id(), after_start(0));
        clinic.organization_id = Some("clinic".to_owned());
        sim.created_meanwhile = Some(clinic);
        let sent = sim.send_to("room_1", Some("bank"), Sim::message("sms", "Bonjour"));
        assert_eq!(
            (sent, sim.bindings.len()),
            (Err(InboundError::OtherOrganization(room_id())), 0)
        );
    }

    /// The store keeps several rooms: one created beside another leaves the
    /// other as it was.
    #[test]
    fn is_created_beside_the_rooms_that_exist() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.send_to("room_2", None, Sim::message("sms", "Bonjour"))
            .unwrap();
        let created: Vec<String> = sim
            .other_bindings("room_2")
            .iter()
            .map(|binding| binding.channel_id.to_string())
            .collect();
        assert_eq!(
            (
                sim.other_room("room_2").map(|room| room.event_count),
                created,
                sim.bindings.len(),
                sim.timeline.len()
            ),
            // The attach's record and the message (gap G-046).
            (Some(2), vec!["sms".to_owned()], 0, 0)
        );
    }
}

/// RFC §17.2, gap G-051: the organization a caller acts for.
mod another_organization {
    use super::*;
    use pretty_assertions::assert_eq;

    fn bank_room() -> Sim {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.room.organization_id = Some("bank".to_owned());
        sim
    }

    #[test]
    fn s_room_fails_the_message_before_any_write() {
        let mut sim = bank_room();
        let sent = sim.send_to("room_1", Some("clinic"), Sim::message("sms", "Bonjour"));
        assert_eq!(
            (sent, sim.trace, sim.notices.len()),
            (
                Err(InboundError::OtherOrganization(room_id())),
                ["load_context room_1"].map(str::to_owned).to_vec(),
                0
            )
        );
    }

    #[test]
    fn is_not_that_of_a_room_of_no_organization() {
        let mut sim = bank_room();
        sim.room.organization_id = None;
        let sent = sim.send_to("room_1", Some("bank"), Sim::message("sms", "Bonjour"));
        assert_eq!(sent, Err(InboundError::OtherOrganization(room_id())));
    }

    #[test]
    fn is_checked_in_whichever_room_the_message_names() {
        let mut sim = bank_room();
        let mut clinic = Room::new(RoomId::new("room_2").unwrap(), Timestamp::UNIX_EPOCH);
        clinic.organization_id = Some("clinic".to_owned());
        sim.seed_room(clinic);
        let sent = sim.send_to("room_2", Some("bank"), Sim::message("sms", "Bonjour"));
        assert_eq!(
            (sent, sim.other_bindings("room_2").len()),
            (
                Err(InboundError::OtherOrganization(
                    RoomId::new("room_2").unwrap()
                )),
                0
            )
        );
    }

    #[test]
    fn is_not_checked_for_a_caller_naming_none() {
        let mut sim = bank_room();
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert!(!result.blocked);
    }

    #[test]
    fn is_not_met_in_the_caller_s_own_room() {
        let mut sim = bank_room();
        let result = sim
            .send_to("room_1", Some("bank"), Sim::message("sms", "Bonjour"))
            .unwrap();
        assert_eq!(
            (result.blocked, sim.timeline_lines()),
            (false, vec!["#0 sms delivered \"Bonjour\"".to_owned()])
        );
    }
}

/// RFC §10.1 step 2, §7.5 rule 7, gaps G-015 and G-053: a room that exists
/// and a channel it does not bind.
mod an_existing_room {
    use super::*;
    use pretty_assertions::assert_eq;

    fn with_unbound_sms() -> Sim {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim
    }

    #[test]
    fn attaches_an_unbound_channel_under_its_lock_before_the_parse() {
        let mut sim = with_unbound_sms();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.trace[..7],
            [
                "load_context room_1",
                "lock room_1",
                "load_context room_1",
                "store binding sms",
                "commit #0 system delivered \"Channel sms attached (read_write, all)\"",
                "release room_1",
                "handle_inbound sms",
            ]
        );
    }

    #[test]
    fn attaches_an_unbound_channel_at_its_declaration_s_defaults() {
        let mut sim = with_unbound_sms();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        let declared = sim.config.channel(&channel("sms")).unwrap();
        assert_eq!(
            binding_of(&sim, "sms"),
            Some(&declared.binding(room_id(), after_start(0)))
        );
    }

    #[test]
    fn reports_the_attach_and_nothing_of_a_creation() {
        let mut sim = with_unbound_sms();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.framework_event_names(),
            ["room_channel_attached", "event_processed"]
        );
    }

    #[test]
    fn delivers_the_message_of_a_channel_it_attached() {
        let mut sim = with_unbound_sms();
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (result.blocked, sim.timeline_lines()),
            (
                false,
                vec![
                    "#0 system delivered \"Channel sms attached (read_write, all)\"".to_owned(),
                    "#1 sms delivered \"Bonjour\"".to_owned()
                ]
            )
        );
    }

    /// Gap G-015: a detached channel is the one step 2 leaves unbound.
    #[test]
    fn leaves_a_detached_channel_unbound_and_stores_its_event_source_unbound() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.detach("sms");
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.reason.as_deref(),
                sim.trace[..2].to_vec(),
                sim.timeline_lines()
            ),
            (
                Some("source_unbound"),
                ["load_context room_1", "handle_inbound sms"]
                    .map(str::to_owned)
                    .to_vec(),
                vec!["#0 sms blocked:source_unbound \"Bonjour\"".to_owned()]
            )
        );
    }

    #[test]
    fn never_replaces_a_binding() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        let binding = sim.attach("sms");
        binding.visibility = Visibility::None;
        binding
            .metadata
            .insert("phone_number".to_owned(), json!("+15559999999"));
        let before = binding.clone();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (sim.traced("store binding"), binding_of(&sim, "sms")),
            (Vec::<&str>::new(), Some(&before))
        );
    }

    /// Gap G-053: a room that refuses the message takes no attach for it.
    #[test]
    fn that_is_closed_attaches_nothing_and_refuses_the_message() {
        let mut sim = with_unbound_sms();
        sim.room.status = RoomStatus::Closed;
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.reason.as_deref(),
                sim.traced("store binding"),
                sim.bindings.len()
            ),
            (Some("room_closed"), Vec::<&str>::new(), 0)
        );
    }
}

/// RFC §5.7, §10.1 step 1, gap G-052: the sender is written in its
/// channel's form before anything reads it.
mod the_sender {
    use super::*;
    use pretty_assertions::assert_eq;

    fn on(channel_type: ChannelType) -> Sim {
        let mut sim = Sim::new();
        sim.unbound_transport("inbox", channel_type);
        sim.attach("inbox");
        sim
    }

    #[test]
    fn of_a_phone_channel_reaches_the_channel_in_e164() {
        let mut sim = on(ChannelType::WHATSAPP);
        sim.send(message_from("inbox", "whatsapp:+1 (555) 000-0001", "Hi"))
            .unwrap();
        assert_eq!(sim.parsed_messages[0].sender_id, "+15550000001");
    }

    #[test]
    fn is_named_on_the_event_s_source_in_the_channel_s_form() {
        let mut sim = on(ChannelType::SMS);
        let result = sim
            .send(message_from("inbox", "+1 555 000 0001", "Hi"))
            .unwrap();
        assert_eq!(
            result
                .event
                .and_then(|event| event.source.participant_id)
                .map(String::from),
            Some("+15550000001".to_owned())
        );
    }

    /// Gap G-052: with no country code declared, digits without `+` are
    /// kept as written.
    #[test]
    fn without_a_plus_is_kept_as_written_where_its_channel_declares_no_country_code() {
        let mut sim = on(ChannelType::SMS);
        sim.send(message_from("inbox", "15550000001", "Hi"))
            .unwrap();
        assert_eq!(sim.parsed_messages[0].sender_id, "15550000001");
    }

    #[test]
    fn keeps_the_provider_s_spelling_in_the_raw_payload() {
        let mut sim = on(ChannelType::SMS);
        let mut message = message_from("inbox", "whatsapp:+15550000001", "Hi");
        message
            .raw_payload
            .insert("From".to_owned(), json!("whatsapp:+15550000001"));
        let result = sim.send(message).unwrap();
        assert_eq!(
            result.event.unwrap().source.raw_payload.get("From"),
            Some(&json!("whatsapp:+15550000001"))
        );
    }

    #[test]
    fn takes_the_country_code_its_channel_declares() {
        let mut sim = Sim::new();
        sim.declare(ChannelDeclaration {
            country_code: Some(CountryCode::new("33").unwrap()),
            ..ChannelDeclaration::new(
                channel("inbox"),
                ChannelType::SMS,
                ChannelCategory::Transport,
                ChannelDirection::Bidirectional,
            )
        });
        sim.send(message_from("inbox", "06 12 34 56 78", "Salut"))
            .unwrap();
        assert_eq!(sim.parsed_messages[0].sender_id, "+33612345678");
    }

    #[test]
    fn of_an_email_channel_loses_its_display_name_and_its_case() {
        let mut sim = Sim::without_rooms();
        sim.unbound_transport("mail", ChannelType::EMAIL);
        sim.send(message_from("mail", "Alice <Alice@Example.com>", "Hello"))
            .unwrap();
        let binding = binding_of(&sim, "mail").unwrap();
        assert_eq!(
            (
                sim.parsed_messages[0].sender_id.as_str(),
                binding.metadata.get("email_address")
            ),
            ("alice@example.com", Some(&json!("alice@example.com")))
        );
    }

    #[test]
    fn of_another_channel_is_kept_as_written() {
        let mut sim = on(ChannelType::WEBSOCKET);
        sim.send(message_from("inbox", "+1 (555) 000-0001", "Hi"))
            .unwrap();
        assert_eq!(sim.parsed_messages[0].sender_id, "+1 (555) 000-0001");
    }
}

/// RFC §10.2 step 3d, gap G-051: a transport delivers to the recipient its
/// binding names.
mod a_recipient {
    use super::*;
    use pretty_assertions::assert_eq;

    /// A room where a WebSocket writes and an SMS binding reads, without a
    /// recipient.
    fn sms_without_recipient() -> Sim {
        let mut sim = Sim::new();
        sim.transport("ws");
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.attach("sms");
        sim
    }

    #[test]
    fn missing_refuses_the_delivery_before_deliver_is_called() {
        let mut sim = sms_without_recipient();
        let result = sim.send(Sim::message("ws", "Hello")).unwrap();
        let refused = result.delivery_results.get("sms").unwrap();
        assert_eq!(
            (
                sim.traced("on_event sms").len(),
                sim.traced("deliver sms"),
                refused.status,
                refused.error.as_ref().map(|error| error.code.as_str())
            ),
            (
                1,
                Vec::<&str>::new(),
                DeliveryStatus::Failed,
                Some("no_recipient")
            )
        );
    }

    #[test]
    fn missing_is_reported_as_a_failed_delivery() {
        let mut sim = sms_without_recipient();
        sim.send(Sim::message("ws", "Hello")).unwrap();
        assert_eq!(
            sim.framework_events()[0],
            &FrameworkEvent::DeliveryFailed {
                room_id: room_id(),
                event_id: "evt_1".parse().unwrap(),
                channel_id: channel("sms"),
                error: "no_recipient".to_owned(),
            }
        );
    }

    #[test]
    fn named_on_the_binding_is_delivered_to() {
        let mut sim = sms_without_recipient();
        sim.bindings[1]
            .metadata
            .insert("phone_number".to_owned(), json!("+15551234567"));
        sim.send(Sim::message("ws", "Hello")).unwrap();
        assert_eq!(sim.traced("deliver"), ["deliver sms <- evt_1"]);
    }

    /// RFC §10.1 step 2: with a room id given, only a room step 2 creates
    /// records its sender; an existing room's SMS channel attached at step
    /// 2 names no recipient, and the agent's answer is refused.
    #[test]
    fn is_not_named_by_an_existing_room_s_attach_so_the_answer_is_refused() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.echo_agent("ai", "Bonjour Jean!");
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                binding_of(&sim, "sms").map(|binding| binding.metadata.len()),
                sim.timeline_lines(),
                sim.traced("on_event sms").len(),
                sim.traced("deliver sms"),
            ),
            (
                Some(0),
                vec![
                    "#0 system delivered \"Channel sms attached (read_write, all)\"".to_owned(),
                    "#1 sms delivered \"Bonjour\"".to_owned(),
                    "#2 ai delivered \"Bonjour Jean!\"".to_owned(),
                ],
                1,
                Vec::<&str>::new()
            )
        );
    }

    #[test]
    fn named_by_the_integrator_s_binding_receives_the_answer() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.attach("sms")
            .metadata
            .insert("phone_number".to_owned(), json!("+15551234567"));
        sim.echo_agent("ai", "Bonjour Jean!");
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.traced("deliver sms").len(), 1);
    }

    #[test]
    fn is_not_needed_by_a_channel_that_delivers_to_its_own_destination() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.unbound_transport("hook", ChannelType::WEBHOOK);
        sim.attach("hook");
        sim.send(Sim::message("sms", "Hello")).unwrap();
        assert_eq!(sim.traced("deliver"), ["deliver hook <- evt_1"]);
    }
}

/// RFC §10.1 step 3: the context holds the room's participants.
mod the_participants {
    use super::*;
    use pretty_assertions::assert_eq;

    fn with_jean() -> (Sim, Participant) {
        let mut sim = Sim::new();
        sim.transport("sms");
        let jean = Participant::new(
            ParticipantId::new("p_jean").unwrap(),
            room_id(),
            channel("sms"),
            after_start(0),
        );
        sim.participants.push(jean.clone());
        (sim, jean)
    }

    #[test]
    fn are_in_the_context_the_channel_parses_with() {
        let (mut sim, jean) = with_jean();
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.parse_contexts[0].participants, [jean]);
    }

    #[test]
    fn are_in_the_context_hooks_read() {
        let (mut sim, jean) = with_jean();
        sim.observer("audit", HookTrigger::AfterBroadcast);
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.async_contexts()[0].participants, [jean]);
    }
}

/// RFC §10.2: nothing of the room's delivery lane comes before the commit;
/// every place taken is left.
mod the_lane {
    use super::*;
    use pretty_assertions::assert_eq;

    /// RFC §10.2: each event delivered takes its place at its commit; none
    /// is taken before the first commit, and the last is left last.
    #[test]
    fn holds_a_place_per_event_delivered_from_its_commit_to_its_set_s_end() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.echo_agent("ai", "Hello back");
        sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.trace.first().map(String::as_str),
                sim.trace.last().map(String::as_str),
                sim.traced("enter lane")
            ),
            (
                Some("load_context room_1"),
                Some("leave lane room_1 evt_2"),
                vec!["enter lane room_1 evt_1", "enter lane room_1 evt_2"]
            )
        );
    }

    #[test]
    fn is_never_entered_for_a_message_refused_before_anything_is_read() {
        let mut sim = Sim::new();
        sim.transport("sms");
        let mut message = Sim::message("sms", "Bonjour");
        message.standalone = true;
        let sent = sim.send(message);
        assert_eq!((sent.is_err(), sim.trace.len()), (true, 0));
    }

    #[test]
    fn is_never_entered_when_step_2_s_attach_fails() {
        let mut sim = no_room_yet();
        sim.failing_effect = Some("store binding");
        let sent = sim.send(Sim::message("sms", "Bonjour"));
        assert_eq!(
            (
                sent.is_err(),
                sim.trace.last().map(String::as_str),
                sim.traced("enter lane")
            ),
            (true, Some("release room_1"), vec![])
        );
    }

    #[test]
    fn is_never_entered_when_step_2_s_attach_cannot_take_the_room_lock() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.failing_effect = Some("lock room_1");
        let sent = sim.send(Sim::message("sms", "Bonjour"));
        assert_eq!(
            (sent, sim.trace),
            (
                Err(InboundError::Lock(ErrorRecord::new(
                    "LockUnavailable",
                    "the lock manager is down"
                ))),
                ["load_context room_1", "lock room_1 failed"]
                    .map(str::to_owned)
                    .to_vec()
            )
        );
    }

    /// A place that fails to be left is logged: what the set delivered and
    /// what the call decided stand, and the result reads as it would.
    #[test]
    fn that_fails_to_be_left_changes_nothing_the_call_decided() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.leave_fails = true;
        let result = sim.send(Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.index),
                result.error.map(|error| error.error_type)
            ),
            (Some(0), None)
        );
    }

    #[test]
    fn is_the_named_room_s_own() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.send_to("room_2", None, Sim::message("sms", "Bonjour"))
            .unwrap();
        assert_eq!(
            sim.traced("enter lane")
                .into_iter()
                .chain(sim.traced("leave lane"))
                .collect::<Vec<_>>(),
            ["enter lane room_2 evt_5", "leave lane room_2 evt_5"]
        );
    }
}
