//! Where an inbound call stands, and the one place it changes stage.
//!
//! A call goes through its stages in this order (RFC §10.1, §10.5):
//!
//! - from its start, a message arrives (`Arriving`, steps 2 to 4); a direct
//!   injection has its room read for its off-lock check (`Reading`), or its
//!   pass is queued at once (`Cascade`);
//! - arrived, the trigger's off-lock check runs (`Checking`, step 5a) when an
//!   off-lock hook fires for it, then its pass is queued (`Cascade`);
//! - the trigger's pass over, the call releases the admission ticket it
//!   holds (`ReleasingTicket`) and goes back to its cascade, as it releases
//!   it first when it ends holding it;
//! - once nothing is queued or running, step 15's writes (`Persisting`);
//! - the call ends from any stage: it waits for the entries it adopted and
//!   its delivery sets (`Ending`), then is over (`Done`).
//!
//! [`follows`] is that order, written once: every change of stage goes
//! through [`CallStage::enter`], which checks it, so a transition the order
//! does not list fails a debug assertion.

use std::mem;

use super::arrival::Arrival;
use super::direct::DirectRead;
use super::holds::AfterTicket;
use super::off_lock::OffLockCheck;
use super::{Call, InboundError, InboundResult};
use crate::model::Task;
use crate::ops::protocol::EffectId;

/// A stage of the call, with what it holds.
#[derive(Debug)]
pub(super) enum Stage {
    Start(Box<Call>),
    /// A message's steps 2 to 4.
    Arriving(Box<Arrival>),
    /// A direct injection's read of its room for its off-lock check.
    Reading(Box<DirectRead>),
    /// Step 5a: the trigger's off-lock check, holding the room's ticket.
    Checking(Box<OffLockCheck>),
    Cascade,
    /// The ticket's release, before the call goes on.
    ReleasingTicket {
        effect: EffectId,
        then: Box<AfterTicket>,
    },
    /// Step 15's writes, each outstanding until answered.
    Persisting {
        side_effects: Option<(EffectId, Vec<Task>)>,
        metadata: Option<EffectId>,
    },
    /// The call's outcome waits for every entry it adopted to be settled,
    /// and every event committed to be delivered.
    Ending(Box<Result<InboundResult, InboundError>>),
    Done,
}

/// A stage's name, without what it holds: what the order is written in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StageName {
    Start,
    Arriving,
    Reading,
    Checking,
    Cascade,
    ReleasingTicket,
    Persisting,
    Ending,
    Done,
}

impl Stage {
    fn name(&self) -> StageName {
        match self {
            Self::Start(_) => StageName::Start,
            Self::Arriving(_) => StageName::Arriving,
            Self::Reading(_) => StageName::Reading,
            Self::Checking(_) => StageName::Checking,
            Self::Cascade => StageName::Cascade,
            Self::ReleasingTicket { .. } => StageName::ReleasingTicket,
            Self::Persisting { .. } => StageName::Persisting,
            Self::Ending(_) => StageName::Ending,
            Self::Done => StageName::Done,
        }
    }
}

/// Whether a call in stage `from` may go into stage `to`: the order of the
/// stages (module docs). A call ends from any stage, into `Ending` while
/// work it started still runs, else into `Done`.
fn follows(from: StageName, to: StageName) -> bool {
    use StageName::{
        Arriving, Cascade, Checking, Done, Ending, Persisting, Reading, ReleasingTicket, Start,
    };
    match from {
        // A message arrives; a direct injection has its room read, or its
        // pass is queued at once.
        Start => matches!(to, Arriving | Reading | Cascade | Ending | Done),
        // Arrived: the off-lock check when an off-lock hook fires for the
        // trigger, else its pass. Each stays while it waits for more
        // answers.
        Arriving => matches!(to, Arriving | Checking | Cascade | Ending | Done),
        Reading => matches!(to, Reading | Checking | Cascade | Ending | Done),
        // The check over, the pass; a call that ends holding the ticket
        // releases it first.
        Checking => matches!(to, Checking | Cascade | ReleasingTicket | Ending | Done),
        // The trigger's pass over, its ticket is released; once nothing is
        // queued or running, step 15's writes.
        Cascade => matches!(to, ReleasingTicket | Persisting | Ending | Done),
        ReleasingTicket => matches!(to, Cascade | Ending | Done),
        Persisting => matches!(to, Persisting | Ending | Done),
        Ending => to == Done,
        Done => false,
    }
}

/// The stage the call is in, and the one place that changes it.
#[derive(Debug)]
pub(super) struct CallStage {
    stage: Stage,
    /// The name of the stage a handler took out to work on ([`take`]), the
    /// one the call is in until the handler enters the next.
    ///
    /// [`take`]: Self::take
    taken: Option<StageName>,
}

impl CallStage {
    /// The first stage of `call`.
    pub(super) fn new(call: Call) -> Self {
        Self {
            stage: Stage::Start(Box::new(call)),
            taken: None,
        }
    }

    /// The stage the call is in; `Done` while a handler holds it ([`take`]).
    ///
    /// [`take`]: Self::take
    pub(super) fn current(&self) -> &Stage {
        &self.stage
    }

    /// Takes the stage out for a handler to work on: the call stays in it
    /// until the handler enters the next stage, or puts it back.
    pub(super) fn take(&mut self) -> Stage {
        let stage = mem::replace(&mut self.stage, Stage::Done);
        self.taken = Some(stage.name());
        stage
    }

    /// Moves the call into `next`, which must follow the stage it is in
    /// ([`follows`]).
    pub(super) fn enter(&mut self, next: Stage) {
        let from = self.taken.take().unwrap_or_else(|| self.stage.name());
        debug_assert!(
            follows(from, next.name()),
            "the inbound call went from {from:?} to {:?}",
            next.name()
        );
        self.stage = next;
    }

    /// Puts back the stage a handler took out, as it was: an answer it does
    /// not wait for changes nothing.
    pub(super) fn put_back(&mut self, stage: Stage) {
        let taken = self.taken.take();
        debug_assert_eq!(taken, Some(stage.name()));
        self.stage = stage;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A transition the order does not list is caught: a call over never
    /// goes back to its cascade.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "the inbound call went from Done to Cascade")]
    fn a_transition_the_order_does_not_list_is_caught() {
        let mut stage = CallStage {
            stage: Stage::Done,
            taken: None,
        };
        stage.enter(Stage::Cascade);
    }
}
