//! What a call holds besides the room lock, and lets go of on every path:
//! the room's admission ticket, from its off-lock check to the end of its
//! trigger's pass (RFC §9.5.1), and the room's delivery lane, from the
//! moment the room is known to the call's end (RFC §10.2).
//!
//! The ticket is released once the trigger's pass is over, the room lock
//! released already: committed, blocked, refused, a duplicate, an edit
//! refused, failed or past the call's deadline. A call that ends before its
//! pass releases it before it leaves the lane. A failed release changes
//! nothing the call decided: it is logged, and the result carries it when
//! no earlier error was met, as a failed release of the lock (gap G-025).

use super::job::Finished;
use super::{InboundError, InboundResult, Machine, Stage};
use crate::model::ErrorRecord;
use crate::ops::Cx;
use crate::ops::protocol::{Outcome, Request};

/// What a call does once its ticket is released.
#[derive(Debug)]
pub(super) enum AfterTicket {
    /// It settles its trigger's pass.
    Settle(Box<Finished>),
    /// It ends with this outcome, once the lane is left.
    End(Box<Result<InboundResult, InboundError>>),
}

impl Machine {
    /// Ends the operation with `outcome`, once the ticket the call holds is
    /// released and the room's lane is left: every path that took them lets
    /// them go.
    pub(super) fn end(&mut self, cx: &mut Cx<'_>, outcome: Result<InboundResult, InboundError>) {
        self.lane.clear();
        self.active = None;
        if self.ticket.is_some() {
            return self.release_ticket(cx, AfterTicket::End(Box::new(outcome)));
        }
        self.leave_lane(cx, outcome);
    }

    /// Releases the ticket the call holds, then goes on as `then` says.
    pub(super) fn release_ticket(&mut self, cx: &mut Cx<'_>, then: AfterTicket) {
        let Some(room_id) = self.ticket.take() else {
            return self.after_ticket(cx, then, None);
        };
        let effect = cx.io.issue(Request::ReleaseTicket { room_id });
        self.stage = Stage::ReleasingTicket {
            effect,
            then: Box::new(then),
        };
    }

    /// The ticket is released, or its release failed.
    pub(super) fn on_ticket_released(
        &mut self,
        cx: &mut Cx<'_>,
        then: AfterTicket,
        released: Outcome,
    ) {
        let failure = released.into_ticket_released().err();
        if let Some(error) = &failure {
            tracing::warn!(
                target: "roomkit::core::inbound",
                room_id = ?self.room_id,
                error_type = %error.error_type,
                message = %error.message,
                "releasing the room's admission ticket failed; what the call decided stands"
            );
        }
        self.after_ticket(cx, then, failure);
    }

    fn after_ticket(&mut self, cx: &mut Cx<'_>, then: AfterTicket, failure: Option<ErrorRecord>) {
        match then {
            AfterTicket::Settle(finished) => {
                self.stage = Stage::Cascade;
                self.note_error(failure);
                self.settle(cx, *finished);
            }
            AfterTicket::End(outcome) => {
                let outcome = outcome.map(|mut result| {
                    if let Some(error) = failure {
                        result.error.get_or_insert(error);
                    }
                    result
                });
                self.leave_lane(cx, outcome);
            }
        }
    }

    /// Ends the operation with `outcome` once the room's lane is left, when
    /// the operation holds it.
    fn leave_lane(&mut self, cx: &mut Cx<'_>, outcome: Result<InboundResult, InboundError>) {
        let held = self.room_id.clone().filter(|_| self.holds_lane);
        let Some(room_id) = held else {
            self.stage = Stage::Done;
            self.outcome = Some(outcome);
            return;
        };
        let effect = cx.io.issue(Request::LeaveLane { room_id });
        self.stage = Stage::LeavingLane {
            effect,
            outcome: Box::new(outcome),
        };
    }

    /// The lane is left. A failed leave changes nothing the call decided: it
    /// is logged, and a result carries it when no earlier error was met, as
    /// a failed release of the room lock (gap G-025).
    pub(super) fn on_left(&mut self, outcome: Result<InboundResult, InboundError>, left: Outcome) {
        self.holds_lane = false;
        self.stage = Stage::Done;
        let outcome = match (left.into_left(), outcome) {
            (Ok(()), outcome) => outcome,
            (Err(error), outcome) => {
                tracing::warn!(
                    target: "roomkit::core::inbound",
                    room_id = ?self.room_id,
                    error_type = %error.error_type,
                    message = %error.message,
                    "leaving the room's delivery lane failed; what the call decided stands"
                );
                outcome.map(|mut result| {
                    result.error.get_or_insert(error);
                    result
                })
            }
        };
        self.outcome = Some(outcome);
    }
}
