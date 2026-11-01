//! A message that names no room, effect by effect (RFC §10.4, §10.1 step 2,
//! §5.7): the identity its address resolves to, the router asked, the
//! sender recorded on the binding under the room lock before step 3, a
//! binding taken meanwhile routing the message once more, a room paused by
//! its lock kept as the sender's conversation, a new room when none is
//! found, and a failing router failing the message before any write.
//!
//! The router is scripted (`Sim::routes_to`). The simulated driver checks on
//! every run that the router is asked holding no lock and no lane, that the
//! operation never holds two lanes, and that it ended with every lane left
//! and every lock released.

use roomkit_core::model::{
    Access, ChannelBinding, ChannelType, ErrorRecord, Identity, IdentityId, InboundMessage,
    Participant, ParticipantId, Room, RoomId, RoomStatus, RoomTimers, TextContent, Timestamp,
};
use roomkit_core::ops::{FrameworkEvent, InboundError, InboundResult};
use roomkit_core::rules::hook_registry::HookTrigger;
use serde_json::json;

use crate::support::{Sim, channel, room_id};

const SENDER: &str = "+15551234567";
const OTHER: &str = "+15550000009";

/// A message from `sender` on `channel_id`.
fn message_from(channel_id: &str, sender: &str, text: &str) -> InboundMessage {
    InboundMessage::new(channel(channel_id), sender, TextContent::new(text))
}

/// `room_1` with an SMS channel bound naming no one, which the router
/// returns.
fn routed_to_room_1() -> Sim {
    let mut sim = Sim::new();
    sim.unbound_transport("sms", ChannelType::SMS);
    sim.attach("sms");
    sim.routes_to(Some("room_1"));
    sim
}

/// A store with no room, an SMS channel and an agent no room binds; the
/// router finds no room.
fn no_room_yet() -> Sim {
    let mut sim = Sim::without_rooms();
    sim.unbound_transport("sms", ChannelType::SMS);
    sim.unbound_agent("ai", "Bonjour!");
    sim
}

/// The binding of `channel_id` in `room_1`.
fn binding_of<'sim>(sim: &'sim Sim, channel_id: &str) -> Option<&'sim ChannelBinding> {
    sim.bindings
        .iter()
        .find(|binding| binding.channel_id.as_str() == channel_id)
}

/// The correspondent and the metadata of `binding`.
fn named(binding: Option<&ChannelBinding>) -> (Option<String>, serde_json::Value) {
    binding.map_or((None, json!(null)), |binding| {
        (
            binding.participant_id.as_ref().map(ToString::to_string),
            serde_json::Value::Object(binding.metadata.clone()),
        )
    })
}

/// The trace up to and including the channel's parse.
fn until_parse(sim: &Sim) -> Vec<&str> {
    let end = sim
        .trace
        .iter()
        .position(|line| line.starts_with("handle_inbound"))
        .map_or(sim.trace.len(), |at| at + 1);
    sim.trace.iter().take(end).map(String::as_str).collect()
}

/// RFC §10.1 step 2: the sender of a routed message, recorded on the
/// binding under the room lock, before step 3.
mod a_routed_sender {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn is_recorded_under_the_room_lock_before_the_parse() {
        let mut sim = routed_to_room_1();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            until_parse(&sim),
            [
                "resolve_identity sms +15551234567",
                "route sms +15551234567",
                "load_context room_1",
                "lock room_1",
                "load_context room_1",
                // The record reads the room's whole timeline under the lock
                // for another sender on the channel (RMK-1071).
                "list_events room_1",
                "store binding sms",
                "release room_1",
                "handle_inbound sms",
            ]
        );
    }

    #[test]
    fn is_named_as_correspondent_and_recipient_on_the_binding() {
        let mut sim = routed_to_room_1();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            named(binding_of(&sim, "sms")),
            (Some(SENDER.to_owned()), json!({ "phone_number": SENDER }))
        );
    }

    /// RFC §10.1 step 3: the context of the parse is what step 2 left.
    #[test]
    fn is_on_the_binding_the_channel_parses_with() {
        let mut sim = routed_to_room_1();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        let parsed_with = sim.parse_contexts[0]
            .bindings
            .iter()
            .find(|binding| binding.channel_id.as_str() == "sms")
            .and_then(|binding| binding.participant_id.clone());
        assert_eq!(parsed_with, Some(ParticipantId::new(SENDER).unwrap()));
    }

    /// RFC §7.5 rule 6: the record carries every other field over.
    #[test]
    fn is_recorded_keeping_the_binding_s_permissions() {
        let mut sim = routed_to_room_1();
        sim.bindings[0].access = Access::ReadOnly;
        sim.bindings[0].muted = true;
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        let binding = binding_of(&sim, "sms").unwrap();
        assert_eq!(
            (
                binding.access,
                binding.muted,
                binding.participant_id.clone(),
                sim.timeline_lines()
            ),
            (
                Access::ReadOnly,
                true,
                Some(ParticipantId::new(SENDER).unwrap()),
                vec!["#0 sms blocked:source_read_only \"Bonjour\"".to_owned()]
            )
        );
    }

    /// RFC §10.4, §5.7: a group's binding names no one.
    #[test]
    fn is_never_recorded_on_a_group_binding() {
        let mut sim = routed_to_room_1();
        sim.bindings[0].group = true;
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.traced("store binding"),
                named(binding_of(&sim, "sms")).0
            ),
            (Vec::<&str>::new(), None)
        );
    }

    /// RFC §10.4: the framework's own sender names no correspondent, and
    /// has no address to resolve.
    #[test]
    fn is_never_the_framework_s_own_sender() {
        let mut sim = routed_to_room_1();
        sim.bindings[0].group = true;
        sim.route(None, message_from("sms", "system", "Reminder"))
            .unwrap();
        assert_eq!(
            until_parse(&sim),
            [
                "route sms system",
                "load_context room_1",
                "lock room_1",
                "load_context room_1",
                "release room_1",
                "handle_inbound sms",
            ]
        );
    }

    /// RFC §5.7: a binding found naming the sender without a recipient gets
    /// their address as its recipient.
    #[test]
    fn named_by_the_binding_already_gives_it_their_recipient() {
        let mut sim = routed_to_room_1();
        sim.bindings[0].participant_id = Some(ParticipantId::new(SENDER).unwrap());
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            named(binding_of(&sim, "sms")),
            (Some(SENDER.to_owned()), json!({ "phone_number": SENDER }))
        );
    }

    #[test]
    fn named_by_the_binding_with_their_recipient_writes_nothing() {
        let mut sim = routed_to_room_1();
        sim.bindings[0].participant_id = Some(ParticipantId::new(SENDER).unwrap());
        sim.bindings[0]
            .metadata
            .insert("phone_number".to_owned(), json!(SENDER));
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.traced("store binding"), Vec::<&str>::new());
    }

    /// RFC §10.1 step 2: a routed room's unbound channel is attached, its
    /// binding naming the sender.
    #[test]
    fn in_a_room_the_channel_is_not_bound_to_is_attached_naming_them() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.routes_to(Some("room_1"));
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                named(binding_of(&sim, "sms")),
                sim.framework_event_names()[0].clone()
            ),
            (
                (Some(SENDER.to_owned()), json!({ "phone_number": SENDER })),
                "room_channel_attached".to_owned()
            )
        );
    }

    /// RFC §7.5 rule 7: a detached channel stays unbound, and its event is
    /// stored `source_unbound`.
    #[test]
    fn in_a_room_the_channel_was_detached_from_is_not_attached() {
        let mut sim = routed_to_room_1();
        sim.detach("sms");
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (sim.bindings.len(), sim.timeline_lines()),
            (
                0,
                vec!["#0 sms blocked:source_unbound \"Bonjour\"".to_owned()]
            )
        );
    }

    /// RFC §10.4: a binding naming a member who is the sender, by the
    /// identity their address resolves to, is theirs; the message lands in
    /// their room, which records their address as its recipient.
    #[test]
    fn named_by_the_binding_through_their_membership_lands_in_their_room() {
        let mut sim = routed_to_room_1();
        sim.bindings[0].participant_id = Some(ParticipantId::new("p_jean").unwrap());
        let mut jean = Participant::at(
            ParticipantId::new("p_jean").unwrap(),
            room_id(),
            channel("sms"),
            Timestamp::UNIX_EPOCH,
        );
        jean.identity_id = Some(IdentityId::new("id_jean").unwrap());
        sim.participants.push(jean);
        let mut identity = Identity::new(IdentityId::new("id_jean").unwrap());
        identity.link(ChannelType::SMS, SENDER);
        sim.identities.push(identity);
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.timeline_lines(),
                named(binding_of(&sim, "sms")),
                sim.traced("route").len()
            ),
            (
                vec!["#0 sms delivered \"Bonjour\"".to_owned()],
                (Some("p_jean".to_owned()), json!({ "phone_number": SENDER })),
                1
            )
        );
    }

    /// RFC §10.4, gap G-057: a chat message that does not carry its chat
    /// has no address: no identity is read, and nothing is recorded.
    #[test]
    fn with_no_address_reads_no_identity_and_is_not_recorded() {
        let mut sim = Sim::new();
        sim.unbound_transport("telegram", ChannelType::TELEGRAM);
        sim.attach("telegram");
        sim.routes_to(Some("room_1"));
        sim.route(None, message_from("telegram", "4242", "Hi"))
            .unwrap();
        assert_eq!(
            (
                sim.trace[..2].to_vec(),
                sim.route_queries[0].sender.address.clone(),
                named(binding_of(&sim, "telegram")),
                sim.timeline_lines()
            ),
            (
                ["route telegram 4242", "load_context room_1"]
                    .map(str::to_owned)
                    .to_vec(),
                None,
                (None, json!({})),
                vec!["#0 telegram delivered \"Hi\"".to_owned()]
            )
        );
    }

    /// Gap G-055: the framework's own sender, routed to a room whose
    /// binding names someone, finds it someone else's.
    #[test]
    fn under_the_framework_s_own_name_into_a_binding_naming_someone_is_routed_once_more() {
        let mut sim = routed_to_room_1();
        sim.bindings[0].participant_id = Some(ParticipantId::new(OTHER).unwrap());
        let result = sim
            .route(None, message_from("sms", "system", "Reminder"))
            .unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.room_id.to_string()),
                sim.traced("route").len(),
                named(binding_of(&sim, "sms")).0
            ),
            (Some("new_room_1".to_owned()), 2, Some(OTHER.to_owned()))
        );
    }

    /// Gap G-061: recording a sender fires no trigger.
    #[test]
    fn is_recorded_without_a_hook_or_a_framework_event() {
        let mut sim = routed_to_room_1();
        sim.observer("attached", HookTrigger::OnChannelAttached);
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (sim.async_triggers(), sim.framework_event_names()),
            (Vec::new(), vec!["event_processed".to_owned()])
        );
    }
}

/// RFC §10.1 step 2, gap G-055: a binding found naming another sender by
/// the room lock routes the message once more.
mod a_binding_taken_meanwhile {
    use super::*;
    use pretty_assertions::assert_eq;

    /// `room_1` the router returns, whose binding another message records
    /// `OTHER` on before this one takes the lock.
    fn taken() -> Sim {
        let mut sim = routed_to_room_1();
        sim.recorded_meanwhile = Some((
            room_id(),
            channel("sms"),
            ParticipantId::new(OTHER).unwrap(),
        ));
        sim
    }

    #[test]
    fn routes_the_message_once_more() {
        let mut sim = taken();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            until_parse(&sim),
            [
                "resolve_identity sms +15551234567",
                "route sms +15551234567",
                "load_context room_1",
                "lock room_1",
                "load_context room_1",
                "release room_1",
                "route sms +15551234567",
                "store room new_room_1 Active",
                "lock new_room_1",
                "load_context new_room_1",
                "store binding sms",
                "commit #0 system delivered \"Channel sms attached (read_write, all)\"",
                "release new_room_1",
                "handle_inbound sms",
            ]
        );
    }

    #[test]
    fn writes_nothing_in_the_room_taken() {
        let mut sim = taken();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (named(binding_of(&sim, "sms")).0, sim.timeline.len()),
            (Some(OTHER.to_owned()), 0)
        );
    }

    #[test]
    fn lands_in_the_room_routing_finds_the_second_time() {
        let mut sim = taken();
        let mut second = Room::at(RoomId::new("room_2").unwrap(), Timestamp::UNIX_EPOCH);
        second.metadata.insert("crm".to_owned(), json!("c_2"));
        sim.seed_room(second);
        sim.routes_to(Some("room_2"));
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        let recorded = sim
            .other_bindings("room_2")
            .first()
            .and_then(|binding| binding.participant_id.clone());
        assert_eq!(
            (
                sim.other_timeline_lines("room_2"),
                recorded,
                sim.other_room("new_room_1").is_none()
            ),
            (
                vec![
                    "#0 system delivered \"Channel sms attached (read_write, all)\"".to_owned(),
                    "#1 sms delivered \"Bonjour\"".to_owned()
                ],
                Some(ParticipantId::new(SENDER).unwrap()),
                true
            )
        );
    }

    /// Gap G-055: once more, then a new room.
    #[test]
    fn found_taken_again_gives_the_message_a_new_room() {
        let mut sim = taken();
        sim.routes_to(Some("room_1"));
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.traced("route"),
                sim.traced("enter lane"),
                sim.other_timeline_lines("new_room_1")
            ),
            (
                vec!["route sms +15551234567"; 2],
                vec!["enter lane new_room_1 evt_5"],
                vec![
                    "#0 system delivered \"Channel sms attached (read_write, all)\"".to_owned(),
                    "#1 sms delivered \"Bonjour\"".to_owned()
                ]
            )
        );
    }

    #[test]
    fn resolves_the_sender_s_identity_once() {
        let mut sim = taken();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.traced("resolve_identity").len(), 1);
    }
}

/// RFC §10.4 step 3, gap G-055: a room the sender was admitted into while
/// its binding named no one, which by its lock has heard someone else on
/// the channel, is no longer the message's.
mod a_room_that_heard_another_sender_meanwhile {
    use super::*;
    use pretty_assertions::assert_eq;

    /// `room_1`, its SMS binding naming no one, which the router returns for
    /// a message from `SENDER` having read the room before `OTHER`'s message,
    /// sent naming the room (RFC §10.1 step 2, gap G-053), was committed
    /// there: by the lock, the room has heard `OTHER` on the channel.
    fn heard_meanwhile() -> Sim {
        let mut sim = routed_to_room_1();
        sim.send(message_from("sms", OTHER, "Hello")).unwrap();
        sim.trace.clear();
        sim.notices.clear();
        sim
    }

    #[test]
    fn routes_the_message_once_more() {
        let mut sim = heard_meanwhile();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            until_parse(&sim),
            [
                "resolve_identity sms +15551234567",
                "route sms +15551234567",
                "load_context room_1",
                "lock room_1",
                "load_context room_1",
                "list_events room_1",
                "release room_1",
                "route sms +15551234567",
                "store room new_room_1 Active",
                "lock new_room_1",
                "load_context new_room_1",
                "store binding sms",
                "commit #0 system delivered \"Channel sms attached (read_write, all)\"",
                "release new_room_1",
                "handle_inbound sms",
            ]
        );
    }

    /// The binding is not recorded for the sender, so the answers to the
    /// one heard are not sent to them.
    #[test]
    fn writes_nothing_in_the_room() {
        let mut sim = heard_meanwhile();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (named(binding_of(&sim, "sms")), sim.timeline_lines()),
            (
                (None, json!({})),
                vec!["#0 sms delivered \"Hello\"".to_owned()]
            )
        );
    }

    /// RFC §10.4 step 1: a room where the sender is a member reached through
    /// the channel is their conversation, whoever else was heard there; the
    /// router would choose it again, and the sender is recorded.
    #[test]
    fn whose_member_is_the_sender_is_their_conversation_and_records_them() {
        let mut sim = heard_meanwhile();
        sim.participants.push(Participant::at(
            ParticipantId::new(SENDER).unwrap(),
            room_id(),
            channel("sms"),
            Timestamp::UNIX_EPOCH,
        ));
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (named(binding_of(&sim, "sms")).0, sim.traced("route").len()),
            (Some(SENDER.to_owned()), 1)
        );
    }

    /// RFC §10.4 step 3: a member other than the sender reached through the
    /// channel by the lock closes the room to them too.
    #[test]
    fn having_another_member_joined_through_the_channel_routes_the_message_once_more() {
        let mut sim = routed_to_room_1();
        sim.participants.push(Participant::at(
            ParticipantId::new(OTHER).unwrap(),
            room_id(),
            channel("sms"),
            Timestamp::UNIX_EPOCH,
        ));
        let result = sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.event.map(|event| event.room_id.to_string()),
                named(binding_of(&sim, "sms")).0
            ),
            (Some("new_room_1".to_owned()), None)
        );
    }

    /// The sender's own earlier message on the channel keeps the room
    /// theirs.
    #[test]
    fn having_heard_the_sender_only_records_them() {
        let mut sim = routed_to_room_1();
        sim.send(Sim::message("sms", "Hello")).unwrap();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (named(binding_of(&sim, "sms")).0, sim.traced("route").len()),
            (Some(SENDER.to_owned()), 1)
        );
    }
}

/// Gap G-055: a routed room CLOSED by its lock is no longer the message's.
mod a_routed_room_closed_by_its_lock {
    use super::*;
    use pretty_assertions::assert_eq;

    /// RFC §5.1, gap G-045: the room's pause and close fell due before the
    /// message came; its lock's gate applies them, and the message goes to
    /// a room of its own.
    #[test]
    fn whose_close_fell_due_routes_the_message_once_more() {
        let mut sim = routed_to_room_1();
        sim.room.timers = RoomTimers {
            inactive_after_seconds: Some(60),
            closed_after_seconds: Some(60),
            last_activity_at: None,
        };
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.room.status,
                sim.framework_event_names()[..3].to_vec(),
                sim.other_timeline_lines("new_room_1")
            ),
            (
                RoomStatus::Closed,
                ["room_paused", "room_closed", "room_created"]
                    .map(str::to_owned)
                    .to_vec(),
                vec![
                    "#0 system delivered \"Channel sms attached (read_write, all)\"".to_owned(),
                    "#1 sms delivered \"Bonjour\"".to_owned()
                ]
            )
        );
    }

    #[test]
    fn closed_meanwhile_routes_the_message_once_more() {
        let mut sim = routed_to_room_1();
        sim.room.status = RoomStatus::Closed;
        let result = sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                result.blocked,
                result.event.map(|event| event.room_id.to_string()),
                named(binding_of(&sim, "sms")).0
            ),
            (false, Some("new_room_1".to_owned()), None)
        );
    }
}

/// Gaps G-045, G-055: a routed room PAUSED by its lock is the message's when
/// it is the sender's conversation by step 1 (RFC §10.4), which the message
/// resumes; otherwise the message is routed once more.
mod a_routed_room_paused_by_its_lock {
    use super::*;
    use pretty_assertions::assert_eq;

    /// `room_1` with an SMS channel bound naming the sender, which the
    /// router returns.
    fn routed_to_the_sender_s_room() -> Sim {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.attach("sms").participant_id = Some(ParticipantId::new(SENDER).unwrap());
        sim.routes_to(Some("room_1"));
        sim
    }

    /// `sim` with `room_1` paused.
    fn paused(mut sim: Sim) -> Sim {
        sim.room.status = RoomStatus::Paused;
        sim.room.paused_at = Some(Timestamp::UNIX_EPOCH);
        sim
    }

    /// The room the message landed in, `room_1`'s status and pause, and how
    /// often the router was asked.
    fn landing(
        sim: &Sim,
        result: &InboundResult,
    ) -> (Option<String>, RoomStatus, Option<Timestamp>, usize) {
        (
            result.event.as_ref().map(|event| event.room_id.to_string()),
            sim.room.status,
            sim.room.paused_at,
            sim.traced("route").len(),
        )
    }

    #[test]
    fn whose_binding_names_the_sender_takes_the_message_and_is_resumed() {
        let mut sim = paused(routed_to_the_sender_s_room());
        let result = sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (landing(&sim, &result), sim.timeline_lines()),
            (
                (Some("room_1".to_owned()), RoomStatus::Active, None, 1),
                vec!["#0 sms delivered \"Bonjour\"".to_owned()]
            )
        );
    }

    /// RFC §10.4 step 1: a member reached through the channel makes the room
    /// the sender's conversation, and the sender is recorded on its binding.
    #[test]
    fn whose_member_reached_through_the_channel_is_the_sender_takes_the_message() {
        let mut sim = paused(routed_to_room_1());
        sim.participants.push(Participant::at(
            ParticipantId::new(SENDER).unwrap(),
            room_id(),
            channel("sms"),
            Timestamp::UNIX_EPOCH,
        ));
        let result = sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (landing(&sim, &result), named(binding_of(&sim, "sms")).0),
            (
                (Some("room_1".to_owned()), RoomStatus::Active, None, 1),
                Some(SENDER.to_owned())
            )
        );
    }

    /// RFC §5.1, gap G-045: the room was ACTIVE when the router read it, and
    /// its lock's gate applies the pause that fell due meanwhile.
    #[test]
    fn by_its_timer_since_the_router_read_it_takes_the_sender_s_message() {
        let mut sim = routed_to_the_sender_s_room();
        sim.room.timers = RoomTimers {
            inactive_after_seconds: Some(60),
            closed_after_seconds: None,
            last_activity_at: None,
        };
        let result = sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                landing(&sim, &result),
                sim.framework_event_names().first().cloned()
            ),
            (
                (Some("room_1".to_owned()), RoomStatus::Active, None, 1),
                Some("room_paused".to_owned())
            )
        );
    }

    /// Gap G-045: a paused room is step 3's only through a `group` binding.
    /// Whatever strategy chose this one, a record would name the sender on a
    /// binding naming no one, or attach the channel: the room is left
    /// paused, and the message gets a room of its own.
    #[test]
    fn that_is_not_the_sender_s_conversation_routes_the_message_once_more() {
        let mut unbound = paused(routed_to_room_1());
        unbound.bindings.clear();
        let landed = [paused(routed_to_room_1()), unbound].map(|mut sim| {
            let result = sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
            (landing(&sim, &result), named(binding_of(&sim, "sms")).0)
        });
        let rerouted = (
            (
                Some("new_room_1".to_owned()),
                RoomStatus::Paused,
                Some(Timestamp::UNIX_EPOCH),
                2,
            ),
            None,
        );
        assert_eq!(landed, [rerouted.clone(), rerouted]);
    }

    /// Gap G-045: a group binding makes the room one conversation, whoever
    /// writes; any strategy's paused group room takes the message, which
    /// resumes it, and records no one.
    #[test]
    fn bound_as_a_group_takes_the_message_and_is_resumed() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::SMS);
        sim.attach("sms").group = true;
        sim.routes_to(Some("room_1"));
        let mut sim = paused(sim);
        let result = sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (landing(&sim, &result), named(binding_of(&sim, "sms")).0),
            (
                (Some("room_1".to_owned()), RoomStatus::Active, None, 1),
                None
            )
        );
    }
}

/// RFC §10.1 step 2: "Router returns null → create new room".
mod no_room_found {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn creates_one_bound_to_the_sender() {
        let mut sim = no_room_yet();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        let binding = sim.other_bindings("new_room_1").into_iter().next();
        assert_eq!(
            (
                until_parse(&sim),
                named(binding.as_ref()),
                sim.other_timeline_lines("new_room_1")
            ),
            (
                vec![
                    "resolve_identity sms +15551234567",
                    "route sms +15551234567",
                    "store room new_room_1 Active",
                    "lock new_room_1",
                    "load_context new_room_1",
                    "store binding sms",
                    "commit #0 system delivered \"Channel sms attached (read_write, all)\"",
                    "release new_room_1",
                    "handle_inbound sms",
                ],
                (Some(SENDER.to_owned()), json!({ "phone_number": SENDER })),
                vec![
                    "#0 system delivered \"Channel sms attached (read_write, all)\"".to_owned(),
                    "#1 sms delivered \"Bonjour\"".to_owned()
                ]
            )
        );
    }

    /// RFC Appendix B.1, gap G-050: the agent an `ON_ROOM_CREATED` hook
    /// attaches answers the first message, at the sender's number.
    #[test]
    fn has_the_agent_its_hook_attached_answer_at_the_sender_s_number() {
        let mut sim = no_room_yet();
        sim.observer_attaching("welcome", HookTrigger::OnRoomCreated, "ai");
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            (
                sim.other_timeline_lines("new_room_1"),
                sim.traced("deliver").len()
            ),
            (
                vec![
                    "#0 system delivered \"Channel sms attached (read_write, all)\"".to_owned(),
                    "#1 sms delivered \"Bonjour\"".to_owned(),
                    "#2 ai delivered \"Bonjour!\"".to_owned(),
                ],
                1
            )
        );
    }

    #[test]
    fn reports_the_creation_the_attach_and_the_session() {
        let mut sim = no_room_yet();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(
            sim.framework_event_names(),
            [
                "room_created",
                "room_channel_attached",
                "session_started",
                "event_processed"
            ]
        );
    }

    #[test]
    fn creates_it_in_the_caller_s_organization() {
        let mut sim = no_room_yet();
        sim.route(Some("bank"), Sim::message("sms", "Bonjour"))
            .unwrap();
        assert_eq!(
            sim.other_room("new_room_1")
                .and_then(|room| room.organization_id.clone()),
            Some("bank".to_owned())
        );
    }

    /// A fresh id another room holds is someone else's room: the message is
    /// not carried into it.
    #[test]
    fn whose_fresh_id_another_room_holds_fails_the_message() {
        let mut sim = no_room_yet();
        sim.seed_room(Room::at(
            RoomId::new("new_room_1").unwrap(),
            Timestamp::UNIX_EPOCH,
        ));
        let sent = sim.route(None, Sim::message("sms", "Bonjour"));
        assert_eq!(
            (sent.map(|result| result.blocked), sim.traced("enter lane")),
            (
                Err(InboundError::Store(ErrorRecord::new(
                    "RoomAlreadyExists",
                    "a room with the id exists already"
                ))),
                vec![]
            )
        );
    }
}

/// RFC §10.4: a router that fails fails the message before any write.
mod a_failing_router {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn fails_the_message_before_any_write() {
        let mut sim = routed_to_room_1();
        sim.routes.clear();
        sim.routes.push_back(Err(ErrorRecord::new(
            "RouterError",
            "the CRM is unreachable",
        )));
        let sent = sim.route(None, Sim::message("sms", "Bonjour"));
        assert_eq!(
            (sent, sim.trace, sim.notices.len()),
            (
                Err(InboundError::Routing(ErrorRecord::new(
                    "RouterError",
                    "the CRM is unreachable"
                ))),
                [
                    "resolve_identity sms +15551234567",
                    "route sms +15551234567"
                ]
                .map(str::to_owned)
                .to_vec(),
                0
            )
        );
    }

    #[test]
    fn whose_identity_read_fails_fails_the_message_before_the_router_is_asked() {
        let mut sim = routed_to_room_1();
        sim.failing_effect = Some("resolve_identity");
        let sent = sim.route(None, Sim::message("sms", "Bonjour"));
        assert_eq!(
            (sent.map(|result| result.blocked), sim.trace),
            (
                Err(InboundError::Store(ErrorRecord::new(
                    "StoreUnavailable",
                    "the store is down"
                ))),
                vec!["resolve_identity +15551234567 failed".to_owned()]
            )
        );
    }
}

/// RFC §10.4, §17.2, gap G-056: what the router is asked.
mod the_router_s_query {
    use super::*;
    use pretty_assertions::assert_eq;

    fn identity(id: &str, organization: Option<&str>) -> Identity {
        let mut identity = Identity {
            organization_id: organization.map(str::to_owned),
            ..Identity::new(IdentityId::new(id).unwrap())
        };
        identity.link(ChannelType::SMS, SENDER);
        identity
    }

    #[test]
    fn names_the_channel_the_sender_their_address_and_their_identity() {
        let mut sim = no_room_yet();
        sim.identities.push(identity("id_jean", None));
        sim.route(None, message_from("sms", "+1 (555) 123-4567", "Bonjour"))
            .unwrap();
        let query = &sim.route_queries[0];
        assert_eq!(
            (
                query.channel_id.as_str(),
                query.channel_type.clone(),
                query.sender_id.as_str(),
                query.sender.address.as_deref(),
                query.sender.identity_id.as_ref().map(IdentityId::as_str)
            ),
            (
                "sms",
                ChannelType::SMS,
                SENDER,
                Some(SENDER),
                Some("id_jean")
            )
        );
    }

    /// RFC §17.2: identity resolution is scoped to the organization.
    #[test]
    fn carries_the_identity_of_the_caller_s_organization() {
        let mut sim = no_room_yet();
        sim.identities.push(identity("id_bank", Some("bank")));
        sim.identities.push(identity("id_clinic", Some("clinic")));
        sim.route(Some("clinic"), Sim::message("sms", "Bonjour"))
            .unwrap();
        let query = &sim.route_queries[0];
        assert_eq!(
            (
                query.organization_id.as_deref(),
                query.sender.identity_id.as_ref().map(IdentityId::as_str)
            ),
            (Some("clinic"), Some("id_clinic"))
        );
    }

    /// Gap G-060: a caller naming no organization resolves the identities
    /// of no organization only.
    #[test]
    fn carries_no_identity_of_an_organization_for_a_caller_naming_none() {
        let mut sim = no_room_yet();
        sim.identities.push(identity("id_bank", Some("bank")));
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        assert_eq!(sim.route_queries[0].sender.identity_id, None);
    }
}

/// RFC §10.1 step 2, §17.2: a room a router returns is checked as a room
/// the caller names.
mod a_routed_room {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn of_another_organization_fails_the_message_before_any_write() {
        let mut sim = routed_to_room_1();
        sim.room.organization_id = Some("bank".to_owned());
        let sent = sim.route(Some("clinic"), Sim::message("sms", "Bonjour"));
        assert_eq!(
            (sent, sim.trace),
            (
                Err(InboundError::RoomNotFound(room_id())),
                [
                    "resolve_identity sms +15551234567",
                    "route sms +15551234567",
                    "load_context room_1",
                ]
                .map(str::to_owned)
                .to_vec()
            )
        );
    }

    /// RFC §10.1 step 2: a room id in hand that names no room is created
    /// with it, whoever gave it.
    #[test]
    fn that_does_not_exist_is_created_with_the_router_s_id() {
        let mut sim = no_room_yet();
        sim.routes_to(Some("room_7"));
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        let binding = sim.other_bindings("room_7").into_iter().next();
        assert_eq!(
            (
                sim.other_timeline_lines("room_7"),
                named(binding.as_ref()).0
            ),
            (
                vec![
                    "#0 system delivered \"Channel sms attached (read_write, all)\"".to_owned(),
                    "#1 sms delivered \"Bonjour\"".to_owned()
                ],
                Some(SENDER.to_owned())
            )
        );
    }
}

/// RFC §5.7, gap G-057: on a chat channel, the chat is the conversation.
mod a_chat_message {
    use super::*;
    use pretty_assertions::assert_eq;

    /// A message from `4242` posted in the chat `-100777`.
    fn in_the_chat() -> InboundMessage {
        let mut message = message_from("telegram", "4242", "Hi");
        message
            .metadata
            .insert("chat_id".to_owned(), json!("-100777"));
        message
    }

    fn telegram_without_rooms() -> Sim {
        let mut sim = Sim::without_rooms();
        sim.unbound_transport("telegram", ChannelType::TELEGRAM);
        sim.unbound_agent("ai", "Hello!");
        sim
    }

    #[test]
    fn is_routed_by_its_chat() {
        let mut sim = telegram_without_rooms();
        sim.route(None, in_the_chat()).unwrap();
        assert_eq!(
            (
                sim.traced("resolve_identity"),
                sim.route_queries[0].sender.address.as_deref()
            ),
            (vec!["resolve_identity telegram -100777"], Some("-100777"))
        );
    }

    #[test]
    fn opens_a_room_bound_to_the_chat_as_correspondent_and_recipient() {
        let mut sim = telegram_without_rooms();
        sim.route(None, in_the_chat()).unwrap();
        let binding = sim.other_bindings("new_room_1").into_iter().next();
        assert_eq!(
            named(binding.as_ref()),
            (
                Some("-100777".to_owned()),
                json!({ "telegram_chat_id": "-100777" })
            )
        );
    }

    /// RFC §10.2 step 3d: the answer goes to the chat, and is not refused
    /// `no_recipient`.
    #[test]
    fn is_answered_in_its_chat() {
        let mut sim = telegram_without_rooms();
        sim.observer_attaching("welcome", HookTrigger::OnRoomCreated, "ai");
        sim.route(None, in_the_chat()).unwrap();
        let failed = sim
            .framework_events()
            .into_iter()
            .any(|event| matches!(event, FrameworkEvent::DeliveryFailed { .. }));
        assert_eq!((sim.traced("deliver telegram").len(), failed), (1, false));
    }

    /// The message's author stays its sender.
    #[test]
    fn keeps_its_author_as_the_event_s_sender() {
        let mut sim = telegram_without_rooms();
        let result = sim.route(None, in_the_chat()).unwrap();
        assert_eq!(
            result.event.and_then(|event| event.source.participant_id),
            Some(ParticipantId::new("4242").unwrap())
        );
    }

    /// Gap G-051: a Teams message carries its conversation under
    /// `conversation_id` by default.
    #[test]
    fn of_a_teams_conversation_is_routed_by_it() {
        let mut sim = Sim::without_rooms();
        sim.unbound_transport("teams", ChannelType::TEAMS);
        let mut message = message_from("teams", "29:alice", "Hi");
        message
            .metadata
            .insert("conversation_id".to_owned(), json!("19:abc@thread.v2"));
        sim.route(None, message).unwrap();
        assert_eq!(
            sim.route_queries[0].sender.address.as_deref(),
            Some("19:abc@thread.v2")
        );
    }

    /// RFC §5.7, gap G-051: WhatsApp Personal takes a group message's
    /// group, carried under `group_jid` by default, as its conversation.
    #[test]
    fn of_a_whatsapp_personal_group_is_routed_by_its_group() {
        let mut sim = Sim::without_rooms();
        sim.unbound_transport("whatsapp", ChannelType::WHATSAPP_PERSONAL);
        let mut message = message_from("whatsapp", SENDER, "Hi");
        message
            .metadata
            .insert("group_jid".to_owned(), json!("120363@g.us"));
        sim.route(None, message).unwrap();
        assert_eq!(
            sim.route_queries[0].sender.address.as_deref(),
            Some("120363@g.us")
        );
    }

    #[test]
    fn records_its_chat_on_a_room_opened_naming_no_one() {
        let mut sim = Sim::new();
        sim.unbound_transport("telegram", ChannelType::TELEGRAM);
        sim.attach("telegram");
        sim.routes_to(Some("room_1"));
        sim.route(None, in_the_chat()).unwrap();
        assert_eq!(
            named(binding_of(&sim, "telegram")),
            (
                Some("-100777".to_owned()),
                json!({ "telegram_chat_id": "-100777" })
            )
        );
    }
}

/// RFC §10.2: a routed message takes no place in a room's lane before its
/// commit, and holds nothing of the room once it fails.
mod the_lane {
    use super::*;
    use pretty_assertions::assert_eq;

    /// Its place is taken under the lock of its commit, in the room it
    /// lands in: none while it is routed and recorded.
    #[test]
    fn of_the_room_the_message_lands_in_takes_its_place_at_the_commit() {
        let mut sim = routed_to_room_1();
        sim.route(None, Sim::message("sms", "Bonjour")).unwrap();
        let placed: Vec<&str> = sim
            .trace
            .iter()
            .map(String::as_str)
            .skip_while(|line| !line.starts_with("commit"))
            .take(3)
            .collect();
        assert_eq!(
            (sim.traced("enter lane"), placed),
            (
                vec!["enter lane room_1 evt_1"],
                vec![
                    "commit #0 sms delivered \"Bonjour\"",
                    "enter lane room_1 evt_1",
                    "release room_1"
                ]
            )
        );
    }

    #[test]
    fn holds_nothing_when_the_record_cannot_take_the_room_lock() {
        let mut sim = routed_to_room_1();
        sim.failing_effect = Some("lock room_1");
        let sent = sim.route(None, Sim::message("sms", "Bonjour"));
        assert_eq!(
            (sent.map(|result| result.blocked), sim.trace.last().cloned()),
            (
                Err(InboundError::Lock(ErrorRecord::new(
                    "LockUnavailable",
                    "the lock manager is down"
                ))),
                Some("lock room_1 failed".to_owned())
            )
        );
    }

    #[test]
    fn holds_nothing_when_the_record_cannot_be_stored() {
        let mut sim = routed_to_room_1();
        sim.failing_effect = Some("store binding sms");
        let sent = sim.route(None, Sim::message("sms", "Bonjour"));
        assert_eq!(
            (sent.map(|result| result.blocked), sim.trace.last().cloned()),
            (
                Err(InboundError::Store(ErrorRecord::new(
                    "StoreUnavailable",
                    "the store is down"
                ))),
                Some("release room_1".to_owned())
            )
        );
    }
}
