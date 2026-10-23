//! What a call holds besides the room lock, and lets go of on every path:
//! the room's admission ticket, from its off-lock check to the end of its
//! trigger's pass (RFC §9.5.1). Its events' places in the room's delivery
//! lane are their sets' to leave (`delivery`).
//!
//! A call ends once every entry it adopted is settled and every event it
//! committed delivered (gap G-070): an adopted entry's caller is answered
//! exactly once, and the call does not end while an entry still works in
//! the room or a committed event waits for its delivery set.
//!
//! The ticket is released once the trigger's pass is over, the room lock
//! released already: committed, blocked, refused, a duplicate, an edit
//! refused, failed or past the call's deadline. `Io` holds the release
//! back while another section of the call, an entry it adopted, holds the
//! room's lock or waits for it (RFC §9.5.1). A call that ends before its
//! pass releases it as it ends, before it waits for the sets of the entries
//! it adopted: those may wait in the room's lane behind another call's set,
//! whose turn may wait for a later ticket of the room. A failed release
//! changes nothing the call decided: it is logged, and the result carries
//! it when no earlier error was met, as a failed release of the lock (gap
//! G-025).

use super::job::Finished;
use super::{InboundError, InboundResult, Machine, Stage};
use crate::model::ErrorRecord;
use crate::ops::Cx;
use crate::ops::protocol::{HoldRequest, Outcome};

/// What a call does once its ticket is released.
#[derive(Debug)]
pub(super) enum AfterTicket {
    /// It settles its trigger's pass.
    Settle(Box<Finished>),
    /// It ends with this outcome.
    End(Box<Result<InboundResult, InboundError>>),
}

impl Machine {
    /// Ends the operation with `outcome`, published as its decision when
    /// none was: the ticket the call holds is released at once, and the
    /// operation is over once every entry it adopted is settled and every
    /// event it committed delivered.
    pub(super) fn end(&mut self, cx: &mut Cx<'_>, outcome: Result<InboundResult, InboundError>) {
        self.publish(cx, &outcome);
        self.stop_cascade();
        if self.ticket.is_some() {
            return self.release_ticket(cx, AfterTicket::End(Box::new(outcome)));
        }
        self.end_after_the_cascade(outcome);
    }

    /// Ends with `outcome` once the last adopted entry is settled and the
    /// last committed event delivered.
    fn end_after_the_cascade(&mut self, outcome: Result<InboundResult, InboundError>) {
        if !self.adopted.is_idle() || !self.is_idle() {
            self.stage = Stage::Ending(Box::new(outcome));
            return;
        }
        self.finish(outcome);
    }

    /// Goes on ending once the last adopted entry is settled and the last
    /// committed event delivered.
    pub(super) fn end_once_settled(&mut self) {
        let over = self.adopted.is_idle() && self.is_idle();
        if !matches!(self.stage, Stage::Ending(_)) || !over {
            return;
        }
        if let Stage::Ending(outcome) = std::mem::replace(&mut self.stage, Stage::Done) {
            self.finish(*outcome);
        }
    }

    /// Releases the ticket the call holds, then goes on as `then` says.
    pub(super) fn release_ticket(&mut self, cx: &mut Cx<'_>, then: AfterTicket) {
        let Some(room_id) = self.ticket.take() else {
            return self.after_ticket(cx, then, None);
        };
        let effect = cx.io.issue(HoldRequest::ReleaseTicket { room_id });
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
                self.end_after_the_cascade(outcome);
            }
        }
    }

    /// The operation is over with `outcome`.
    fn finish(&mut self, outcome: Result<InboundResult, InboundError>) {
        self.stage = Stage::Done;
        self.outcome = Some(outcome);
    }
}
