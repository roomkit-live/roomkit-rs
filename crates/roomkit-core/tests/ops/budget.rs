//! A message a delivery's code sends, and the reentry budget it draws on
//! (RFC §8.3, §9.5.1 "The room's work outside the lock" and "Past the cap",
//! §10.1 steps 1 and 2, gap G-070): one that names its room is refused past
//! the budget first thing; a routed one is charged once its final room is
//! known, on the delivering budget only when it lands in the delivering
//! room; a room another call created first keeps the charge but leaves the
//! message's cascade a budget of its own; and no message is charged twice.
//!
//! Each test runs the message as the runtime runs a delivery's call: with
//! the room the delivery set delivers in and its cascade's budget
//! (`Inbound::drawing_on_from_delivery`), or, into that room by name, on
//! the budget at once (`Inbound::drawing_on`).

use roomkit_core::model::{
    ChannelType, EventContent, HookResult, InboundMessage, InjectedEvent, ParticipantId, Room,
    RoomId, Timestamp,
};
use roomkit_core::ops::{Inbound, InboundError, InboundResult, InboundTarget};
use roomkit_core::rules::chain_depth::ReentryBudget;
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::transcoding::text_of;

use crate::support::{Sim, after_start, channel, notice_event, room_id};

const SENDER: &str = "+15551234567";
const OTHER: &str = "+15550000009";

/// A budget with `passes` passes left.
fn budget_of(passes: u32) -> ReentryBudget {
    let budget = ReentryBudget::new(1);
    while budget.remaining() > passes {
        budget.take();
    }
    budget
}

fn room_2() -> RoomId {
    RoomId::new("room_2").unwrap()
}

/// Runs `message`, which names no room, as the code of a delivery set in
/// `delivering` sends it, drawing on `budget` once it lands there.
fn route_from_delivery(
    sim: &mut Sim,
    delivering: RoomId,
    budget: &ReentryBudget,
    message: InboundMessage,
) -> Result<InboundResult, InboundError> {
    let operation = Inbound::new(sim.shared_config(), InboundTarget::Routed, None, message)
        .drawing_on_from_delivery(delivering, budget.clone());
    sim.run(operation)
}

/// The sender recorded on the SMS binding of `room_2`.
fn recorded_in_room_2(sim: &Sim) -> Option<ParticipantId> {
    sim.other_bindings("room_2")
        .into_iter()
        .find(|binding| binding.channel_id == channel("sms"))
        .and_then(|binding| binding.participant_id)
}

/// `room_1` binding SMS, whose binding another message records `OTHER` on
/// before this one takes the lock, and `room_2`, which binds nothing yet.
fn room_1_taken_meanwhile() -> Sim {
    let mut sim = Sim::new();
    sim.unbound_transport("sms", ChannelType::Sms);
    sim.attach("sms");
    sim.recorded_meanwhile = Some((
        room_id(),
        channel("sms"),
        ParticipantId::new(OTHER).unwrap(),
    ));
    sim.seed_room(Room::new(room_2(), Timestamp::UNIX_EPOCH));
    sim
}

/// RFC §10.1 step 2: a routed message is charged once the locked check
/// kept its room for it, after any re-route.
mod a_routed_message {
    use super::*;
    use pretty_assertions::assert_eq;

    /// Routed first into the delivering room, which is no longer the
    /// message's by its lock, then into another existing room: it has a
    /// budget of its own there, whatever the delivering one has left.
    #[test]
    fn re_routed_out_of_the_delivering_room_is_not_charged() {
        let mut sim = room_1_taken_meanwhile();
        sim.routes_to(Some("room_1"));
        sim.routes_to(Some("room_2"));
        let spent = budget_of(0);
        let result = route_from_delivery(&mut sim, room_id(), &spent, Sim::message("sms", "Hi"));
        assert_eq!(
            (
                result.map(|result| result.event.map(|event| event.room_id)),
                sim.other_timeline_lines("room_2")
            ),
            (
                Ok(Some(room_2())),
                vec!["#0 sms delivered \"Hi\"".to_owned()]
            )
        );
    }

    /// Routed first into another room, no longer the message's, then into
    /// the delivering room: charged there, refused past the budget, the
    /// sender's record kept and nothing of the message stored.
    #[test]
    fn re_routed_into_the_delivering_room_is_charged_there() {
        let mut sim = room_1_taken_meanwhile();
        sim.routes_to(Some("room_1"));
        sim.routes_to(Some("room_2"));
        let spent = budget_of(0);
        let result = route_from_delivery(&mut sim, room_2(), &spent, Sim::message("sms", "Hi"));
        assert_eq!(
            (
                result.map(|result| (result.blocked, result.reason, result.event.is_none())),
                recorded_in_room_2(&sim),
                sim.other_timeline_lines("room_2")
            ),
            (
                Ok((true, Some("reentry_loop_cap".to_owned()), true)),
                Some(ParticipantId::new(SENDER).unwrap()),
                Vec::<String>::new()
            )
        );
    }

    /// Past the budget, the message routed into the delivering room is
    /// refused after the locked check: what step 2 wrote to route it, the
    /// sender's record, stays.
    #[test]
    fn past_the_budget_in_the_delivering_room_keeps_the_sender_s_record() {
        let mut sim = Sim::new();
        sim.unbound_transport("sms", ChannelType::Sms);
        sim.attach("sms");
        sim.routes_to(Some("room_1"));
        let spent = budget_of(0);
        let result = route_from_delivery(&mut sim, room_id(), &spent, Sim::message("sms", "Hi"));
        let recorded = sim
            .bindings
            .iter()
            .find(|binding| binding.channel_id == channel("sms"))
            .and_then(|binding| binding.participant_id.clone());
        assert_eq!(
            (
                result.map(|result| result.reason),
                recorded,
                sim.timeline.len()
            ),
            (
                Ok(Some("reentry_loop_cap".to_owned())),
                Some(ParticipantId::new(SENDER).unwrap()),
                0
            )
        );
    }

    /// Charged for the room step 2 read absent and another call created,
    /// then routed once more into the delivering room: never charged a
    /// second time.
    #[test]
    fn charged_for_a_room_is_not_charged_again_once_re_routed() {
        let mut sim = Sim::without_rooms();
        sim.unbound_transport("sms", ChannelType::Sms);
        sim.attach("sms");
        sim.created_meanwhile = Some(Room::new(room_id(), after_start(0)));
        sim.recorded_meanwhile = Some((
            room_id(),
            channel("sms"),
            ParticipantId::new(OTHER).unwrap(),
        ));
        sim.seed_room(Room::new(room_2(), Timestamp::UNIX_EPOCH));
        sim.routes_to(Some("room_1"));
        sim.routes_to(Some("room_2"));
        let one = budget_of(1);
        let result = route_from_delivery(&mut sim, room_2(), &one, Sim::message("sms", "Hi"));
        assert_eq!(
            (
                result.map(|result| result.event.map(|event| event.room_id)),
                one.remaining()
            ),
            (Ok(Some(room_2())), 0)
        );
    }
}

/// RFC §9.5.1, §10.1 step 2: a room step 2 reads absent and another call
/// creates first.
mod a_room_created_meanwhile {
    use super::*;
    use pretty_assertions::assert_eq;

    /// The message stays charged and goes on into the room as into one
    /// that exists, its cascade on a budget of its own: what its hook
    /// injects is committed, though the delivering budget is spent.
    #[test]
    fn keeps_the_charge_and_leaves_the_cascade_a_budget_of_its_own() {
        let mut sim = Sim::without_rooms();
        sim.unbound_transport("sms", ChannelType::Sms);
        sim.created_meanwhile = Some(Room::new(room_id(), after_start(0)));
        sim.before_broadcast(
            "notify",
            Box::new(|event| {
                let notice = InjectedEvent::stored_only(notice_event("inj_1", "Heads up"));
                if text_of(&event.content) == "Bonjour" {
                    return HookOutcome::Returned(HookResult::allow().inject(notice));
                }
                HookOutcome::Returned(HookResult::allow())
            }),
        );
        let one = budget_of(1);
        let operation = Inbound::new(
            sim.shared_config(),
            InboundTarget::Room(room_id()),
            None,
            Sim::message("sms", "Bonjour"),
        )
        .drawing_on_from_delivery(room_2(), one.clone());
        sim.run(operation).unwrap();
        assert_eq!(
            (one.remaining(), sim.timeline_lines()),
            (
                0,
                vec![
                    "#0 sms delivered \"Bonjour\"".to_owned(),
                    "#1 system delivered \"Heads up\"".to_owned()
                ]
            )
        );
    }
}

/// RFC §10.1 step 1: a message that names its room and draws on that
/// room's work's budget is refused past it first thing.
mod a_named_message {
    use super::*;
    use pretty_assertions::assert_eq;

    /// Before the channel lookup: an unregistered channel past the budget
    /// is refused `reentry_loop_cap`, nothing asked of the driver.
    #[test]
    fn past_the_budget_is_refused_before_its_channel_is_looked_up() {
        let mut sim = Sim::new();
        let message = InboundMessage::new(channel("fax"), SENDER, EventContent::text("Hi"));
        let operation = Inbound::new(
            sim.shared_config(),
            InboundTarget::Room(room_id()),
            None,
            message,
        )
        .drawing_on(budget_of(0));
        let result = sim.run(operation);
        assert_eq!(
            (
                result.map(|result| (result.blocked, result.reason)),
                sim.trace.len()
            ),
            (Ok((true, Some("reentry_loop_cap".to_owned()))), 0)
        );
    }
}
