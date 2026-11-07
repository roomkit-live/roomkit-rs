//! Where an inbound call stands, and the one place it changes stage.
//!
//! A call goes through its stages in this order (RFC §10.1, §10.5):
//!
//! - from its start, a message arrives (`Arriving`, steps 2 to 4); a post
//!   has its room read and its host channel attached first (`Posting`); a
//!   direct injection from the room's work has its room and its key read
//!   before it draws on the budget (`ReadingWork`), its off-lock check run
//!   on the room as read; any other has its room read for its off-lock
//!   check (`Reading`), or its pass queued at once (`Cascade`);
//! - arrived, the trigger's off-lock check runs (`Checking`, step 5a) when an
//!   off-lock hook fires for it, then its pass is queued (`Cascade`);
//! - a rerun takes the room's lock, reads its event and plans its agents'
//!   set (`Rerunning`), then goes on with its cascade (`Cascade`);
//! - the trigger's pass over, the call releases the admission ticket it
//!   holds (`ReleasingTicket`) and goes back to its cascade, as it releases
//!   it first when it ends holding it;
//! - once nothing is queued or running, step 15's write of what no delivery
//!   set stored: a record's side effects, a discussion's stopped turn's
//!   observation (`Persisting`);
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
use super::host_post::HostPost;
use super::off_lock::OffLockCheck;
use super::rerun::RerunWork;
use super::work_read::WorkRead;
use super::{Call, InboundError, InboundResult};
use crate::model::Task;
use crate::ops::protocol::EffectId;
use crate::ops::section::Section;

/// A stage of the call, with what it holds.
#[derive(Debug)]
pub(super) enum Stage {
    Start(Box<Call>),
    /// A message's steps 2 to 4.
    Arriving(Box<Arrival>),
    /// A post's read of its room, its speaker's check and its host
    /// channel's first attach (RFC §10.5).
    Posting(Box<HostPost>),
    /// A direct injection from the room's work reads its room, then its
    /// key, before it draws on the budget (RFC §9.5.1 Past the cap).
    ReadingWork(Box<WorkRead>),
    /// A direct injection's read of its room for its off-lock check.
    Reading(Box<DirectRead>),
    /// Step 5a: the trigger's off-lock check, holding the room's ticket.
    Checking(Box<OffLockCheck>),
    Cascade,
    /// A rerun's section: the room's lock, its event read again, its
    /// agents' set planned and placed (RFC §11.7.6).
    Rerunning(Box<Section<RerunWork>>),
    /// The ticket's release, before the call goes on.
    ReleasingTicket {
        effect: EffectId,
        then: Box<AfterTicket>,
    },
    /// Step 15's write of what no set stored, outstanding until answered.
    Persisting {
        side_effects: (EffectId, Vec<Task>),
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
    Posting,
    ReadingWork,
    Reading,
    Checking,
    Cascade,
    Rerunning,
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
            Self::Posting(_) => StageName::Posting,
            Self::ReadingWork(_) => StageName::ReadingWork,
            Self::Reading(_) => StageName::Reading,
            Self::Checking(_) => StageName::Checking,
            Self::Cascade => StageName::Cascade,
            Self::Rerunning(_) => StageName::Rerunning,
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
        Arriving, Cascade, Checking, Done, Ending, Persisting, Posting, Reading, ReadingWork,
        ReleasingTicket, Rerunning, Start,
    };
    match from {
        // A message arrives; a direct injection from the room's work has
        // its key read, then any has its room read, or its pass queued at
        // once.
        Start => matches!(
            to,
            Arriving | Posting | ReadingWork | Reading | Rerunning | Cascade | Ending | Done
        ),
        // A rerun's section, then its cascade.
        Rerunning => matches!(to, Rerunning | Cascade | Ending | Done),
        // A post's room read and its first attach, then its injection.
        Posting => matches!(
            to,
            Posting | ReadingWork | Reading | Cascade | Ending | Done
        ),
        // Read before the budget: the off-lock check on the room as read,
        // or the pass.
        ReadingWork => matches!(to, ReadingWork | Checking | Cascade | Ending | Done),
        // Arrived: the off-lock check when an off-lock hook fires for the
        // trigger, else its pass. Each stays while it waits for more
        // answers.
        Arriving => matches!(to, Arriving | Checking | Cascade | Ending | Done),
        Reading => matches!(to, Reading | Checking | Cascade | Ending | Done),
        // The check over, the pass; a call that ends holding the ticket
        // releases it first.
        Checking => matches!(to, Checking | Cascade | ReleasingTicket | Ending | Done),
        // The trigger's pass over, its ticket is released; once nothing is
        // queued or running, step 15's write of what no set stored.
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
