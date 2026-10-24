//! What a message the code of a delivery set sent pays on that delivery's
//! reentry budget, as its arrival goes (RFC §8.3, §9.5.1, §10.1 step 2, gap
//! G-070).
//!
//! The message is charged one pass, once its final room is known:
//!
//! - a room step 2 creates, routed to none, or to a room the store lacks,
//!   or named and missing, is the delivery's own work: charged before it is
//!   created, and the message's cascade then draws on the delivery's budget;
//!   when another call created it first, the charge stays and the cascade
//!   has a budget of its own;
//! - the room the delivery delivers in, once step 2's locked check kept it
//!   for the message, after any re-route: past the budget, the message is
//!   refused, what step 2 wrote to route it kept, and the cascade draws on
//!   that budget;
//! - another room that exists, nothing.
//!
//! A message is charged once, wherever routing places it once more. A
//! message no delivery's code sent pays nothing.

use super::named_room::Mode;
use crate::model::RoomId;
use crate::rules::chain_depth::ReentryBudget;

/// The charge of one message on the budget of the delivery whose code sent
/// it, if any.
#[derive(Debug)]
pub(super) struct DeliveryCharge {
    /// The room the delivery delivers in, and its cascade's budget.
    delivery: Option<(RoomId, ReentryBudget)>,
    /// Whether the message drew its one pass on that budget: it is never
    /// charged twice, wherever routing places it once more.
    charged: bool,
    /// Whether its cascade draws on that budget too: step 2 created its
    /// room, or it came into the delivering room. A room another call
    /// created first leaves it a budget of its own.
    draws: bool,
}

impl DeliveryCharge {
    /// The charge of a message the code of a delivery set in `delivery`'s
    /// room sent, `None` for any other message.
    pub(super) fn new(delivery: Option<(RoomId, ReentryBudget)>) -> Self {
        Self {
            delivery,
            charged: false,
            draws: false,
        }
    }

    /// The delivering cascade's budget the message's cascade draws on: the
    /// one it was charged on, when step 2 created its room or it came into
    /// the room that cascade delivers in.
    pub(super) fn drawn_on(&self) -> Option<&ReentryBudget> {
        self.delivery
            .as_ref()
            .filter(|_| self.draws)
            .map(|(_, budget)| budget)
    }

    /// The budget step 2, in `mode`, charges before it creates a room it
    /// reads absent: the delivery's, unless the message was charged
    /// already; a fresh room was charged before step 2.
    pub(super) fn opening(&self, mode: Mode) -> Option<ReentryBudget> {
        match mode {
            Mode::Routed | Mode::Named if !self.charged => {
                self.delivery.as_ref().map(|(_, budget)| budget.clone())
            }
            Mode::Routed | Mode::Named | Mode::Fresh => None,
        }
    }

    /// What step 2 did: whether it `charged` its opening budget, for a room
    /// it read absent, and whether it `created` the room.
    pub(super) fn after_step_two(&mut self, charged: bool, created: bool) {
        self.charged |= charged;
        self.draws |= created;
    }

    /// Charges the message its one pass on the budget of the delivery whose
    /// code sent it, unless it was charged already. `false` once that
    /// budget is spent. A message no delivery's code sent charges nothing.
    pub(super) fn charge_once(&mut self) -> bool {
        let Some((_, budget)) = &self.delivery else {
            return true;
        };
        if self.charged {
            return true;
        }
        if !budget.take() {
            return false;
        }
        self.charged = true;
        true
    }

    /// Charges a message that came into the room its delivery delivers in,
    /// its final room, once step 2 kept that room for it, after any
    /// re-route (RFC §10.1 step 2): its cascade then draws on that
    /// delivery's budget. `false` past that budget: the message is refused,
    /// what step 2 wrote to route it kept.
    pub(super) fn arrived_within_budget(&mut self, room_id: &RoomId) -> bool {
        let delivering = self.delivery.as_ref().map(|(room, _)| room);
        if delivering != Some(room_id) {
            return true;
        }
        if !self.charge_once() {
            return false;
        }
        self.draws = true;
        true
    }
}
