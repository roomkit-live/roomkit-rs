//! What a call holds besides its sections' room lock, and lets go of on
//! every path: the room's admission ticket, from its off-lock check to the
//! end of its trigger's pass (RFC §9.5.1), and the room's lock its caller
//! handed it, from its start to the end of that pass (`Inbound::under_lock`,
//! RFC §22.1). Its events' places in the room's delivery lane are their
//! sets' to leave (`delivery`).
//!
//! A call ends once every entry it adopted is settled and every event it
//! committed delivered (gap G-070): an adopted entry's caller is answered
//! exactly once, and the call does not end while an entry still works in
//! the room or a committed event waits for its delivery set.
//!
//! The ticket is released once the trigger's pass is over, the room lock
//! released already: committed, blocked, refused, a duplicate, an edit
//! refused, failed or past the call's deadline. A pass that is over has
//! published the call's decision and queued its delivery set by then: the
//! release waits while an entry the call adopted works under the room's
//! lock, and neither the decision nor the set's place in the lane waits
//! with it. `Io` holds the release
//! back while another section of the call, an entry it adopted, holds the
//! room's lock or waits for it (RFC §9.5.1). A call that ends before its
//! pass releases it as it ends, before it waits for the sets of the entries
//! it adopted: those may wait in the room's lane behind another call's set,
//! whose turn may wait for a later ticket of the room. A failed release
//! changes nothing the call decided: it is logged, and the result carries
//! it when no earlier error was met, as a failed release of the lock (gap
//! G-025).

use super::job::Finished;
use super::queues::Lineage;
use super::{InboundError, InboundResult, Machine, Stage};
use crate::model::ErrorRecord;
use crate::ops::Cx;
use crate::ops::protocol::{HoldRequest, Notice, Outcome};

/// What a call does once its ticket is released.
#[derive(Debug)]
pub(super) enum AfterTicket {
    /// It goes on with its cascade, its trigger's pass settled already; a
    /// failed release is folded into its final result.
    Resume,
    /// It settles its trigger's pass, which failed.
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
        self.release_hand(cx);
        self.publish(cx, &outcome);
        self.cascade.stop();
        if self.trigger.ticket.is_some() {
            return self.release_ticket(cx, AfterTicket::End(Box::new(outcome)));
        }
        self.end_after_the_cascade(cx, outcome);
    }

    /// [`end`](Self::end) for a call past its deadline (RFC §13.6): only the
    /// call's own cascade stops. An entry its admission adopted runs its
    /// cascade to its end, the rows of its stream committed, and the call's
    /// caller is answered once the ticket is released, without waiting for
    /// it, as the Python implementation's adopted call runs a cascade of its
    /// own (RFC §9.5.1 Reentrance).
    pub(super) fn end_past_the_deadline(
        &mut self,
        cx: &mut Cx<'_>,
        outcome: Result<InboundResult, InboundError>,
    ) {
        self.release_hand(cx);
        self.publish(cx, &outcome);
        self.cascade.stop_lineage(Lineage::Call);
        self.answers_before_its_entries = true;
        if self.trigger.ticket.is_some() {
            return self.release_ticket(cx, AfterTicket::End(Box::new(outcome)));
        }
        self.end_after_the_cascade(cx, outcome);
    }

    /// Ends with `outcome` once the last adopted entry is settled and the
    /// last committed event delivered; a call that answers before its
    /// entries answers its caller now.
    fn end_after_the_cascade(
        &mut self,
        cx: &mut Cx<'_>,
        outcome: Result<InboundResult, InboundError>,
    ) {
        if !self.is_settled() {
            // Only an entry's work answers before the end, never a handed
            // lock's release alone.
            let entries_work = !self.adopted.is_idle() || !self.cascade.is_idle();
            if self.answers_before_its_entries && entries_work {
                let answered = self.for_caller(outcome.clone());
                cx.io.notify(Notice::Answered(Box::new(answered)));
            }
            self.stage.enter(Stage::Ending(Box::new(outcome)));
            return;
        }
        self.finish(outcome);
    }

    /// Goes on ending once the last adopted entry is settled and the last
    /// committed event delivered.
    pub(super) fn end_once_settled(&mut self) {
        if !matches!(self.stage.current(), Stage::Ending(_)) || !self.is_settled() {
            return;
        }
        if let Stage::Ending(outcome) = self.stage.take() {
            self.finish(*outcome);
        }
    }

    /// Whether every entry the call adopted is settled, every event it
    /// committed delivered, and the lock its caller handed it released.
    fn is_settled(&self) -> bool {
        self.adopted.is_idle() && self.cascade.is_idle() && self.trigger.releasing_hand.is_none()
    }

    /// Lets go of the room's lock the call's caller handed it
    /// (`Inbound::under_lock`), once its trigger's pass is over or as it
    /// ends before: what the call asks from then on is asked outside that
    /// lock, and its later sections take the lock as any section does.
    pub(super) fn release_hand(&mut self, cx: &mut Cx<'_>) {
        let Some(room_id) = self.trigger.handed.take() else {
            return;
        };
        cx.io.nest(None);
        let effect = cx.io.issue(HoldRequest::ReleaseLock { room_id });
        self.trigger.releasing_hand = Some(effect);
    }

    /// The handed lock is released, or its release failed, which changes
    /// nothing the call decided: it is logged, and the result carries it
    /// when no earlier error was met (gap G-025).
    pub(super) fn on_hand_released(&mut self, released: Outcome) {
        self.trigger.releasing_hand = None;
        if let Err(error) = released.into_released() {
            tracing::warn!(
                target: "roomkit::core::inbound",
                room_id = ?self.call.room_id,
                error_type = %error.error_type,
                message = %error.message,
                "releasing the room lock its caller handed the call failed; what the call decided stands"
            );
            self.trigger.note_error(Some(error));
        }
    }

    /// Releases the ticket the call holds, then goes on as `then` says.
    pub(super) fn release_ticket(&mut self, cx: &mut Cx<'_>, then: AfterTicket) {
        let Some(room_id) = self.trigger.ticket.take() else {
            return self.after_ticket(cx, then, None);
        };
        let effect = cx.io.issue(HoldRequest::ReleaseTicket { room_id });
        self.stage.enter(Stage::ReleasingTicket {
            effect,
            then: Box::new(then),
        });
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
                room_id = ?self.call.room_id,
                error_type = %error.error_type,
                message = %error.message,
                "releasing the room's admission ticket failed; what the call decided stands"
            );
        }
        self.after_ticket(cx, then, failure);
    }

    fn after_ticket(&mut self, cx: &mut Cx<'_>, then: AfterTicket, failure: Option<ErrorRecord>) {
        match then {
            AfterTicket::Resume => {
                self.stage.enter(Stage::Cascade);
                self.trigger.note_error(failure);
            }
            AfterTicket::Settle(finished) => {
                self.stage.enter(Stage::Cascade);
                self.trigger.note_error(failure);
                self.settle(cx, *finished);
            }
            AfterTicket::End(outcome) => {
                let outcome = outcome.map(|mut result| {
                    if let Some(error) = failure {
                        result.error.get_or_insert(error);
                    }
                    result
                });
                self.end_after_the_cascade(cx, outcome);
            }
        }
    }

    /// The operation is over with `outcome`.
    fn finish(&mut self, outcome: Result<InboundResult, InboundError>) {
        self.stage.enter(Stage::Done);
        self.outcome = Some(outcome);
    }
}
