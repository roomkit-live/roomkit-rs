//! The operation's work in its room (RFC §10.1 steps 6 to 14, §10.2): its
//! passes through the room lock, one at a time in the order they are
//! queued, and its delivery sets, one at a time, each run once its place's
//! turn in the room's delivery lane comes.
//!
//! The two queues run beside each other: a pass never waits behind a set.
//! A set may wait in the room's lane behind another call's set, whose turn
//! may wait for this call's decision, as an agent's turn waits for its call
//! into the room: the trigger's pass, queued behind the set of an entry its
//! off-lock hook made, would never run. Passes run once the trigger's pass
//! is queued, at their head; a set runs as soon as it is queued, whatever
//! the call is doing, the set of an entry its parse or its off-lock hook
//! made included, so that nothing behind its place waits for the call's
//! own steps.
//!
//! An event's place is taken at its commit, under the lock (`pass`), so the
//! places follow the commits; its set joins `sets` once its pass is over,
//! in the same order. The events its pass's hooks injected are held with
//! the set and join `passes` once it is over, before the records and the
//! responses the set produced (RFC §9.4, §10.1 step 14). A cascade that
//! stops, failed or past its deadline, runs no pass any more, and still
//! delivers every event committed (RFC §13.6).

use super::delivery::{DeliverySet, SetOutcome};
use super::direct::DirectCall;
use super::job::{Job, RecordNote, Run};
use super::pass::{CommitPass, PassOrigin};
use super::record::RecordPass;
use super::{Machine, Stage};
use crate::model::RoomEvent;
use crate::ops::protocol::{EffectId, Outcome};
use crate::ops::{Cx, Progress, RefusedOperation};
use crate::rules::chain_depth;
use crate::rules::hook_chain::HookChain;

/// A committed event's delivery set, and the passes that follow it.
#[derive(Debug)]
pub(super) struct Delivery {
    pub(super) set: Box<DeliverySet>,
    /// The events the event's hooks injected, which join the passes once
    /// the set is over (RFC §9.4).
    pub(super) then: Vec<Job>,
}

impl Machine {
    /// Queues the trigger's pass at the head of the passes, ahead of what
    /// the sets of its adopted entries queued meanwhile, bounded by the
    /// call's deadline and going on with the chain its off-lock check left,
    /// if any.
    pub(super) fn start_trigger(
        &mut self,
        event: RoomEvent,
        checked: Option<HookChain<RoomEvent>>,
    ) {
        let pass = CommitPass::trigger(event, self.bound, checked).checking(self.direct.clone());
        self.passes
            .push_front(Job::Pass(Box::new(pass), PassOrigin::Trigger));
        self.stage = Stage::Cascade;
    }

    /// Queues the trigger, past the reentry budget it draws on, as its
    /// record at the head of the passes: stored `BLOCKED` with
    /// `reentry_loop_cap`, checked first for the refusals `direct` names,
    /// with no hook run for it (RFC §8.3, gap G-070).
    pub(super) fn start_capped_trigger(&mut self, mut event: RoomEvent, direct: DirectCall) {
        event.blocked_by = Some(chain_depth::REENTRY_LOOP_CAP.to_owned());
        let record = RecordPass::refusing(event, RefusedOperation::Inbound).checking(Some(direct));
        self.passes
            .push_front(Job::Record(Box::new(record), RecordNote::Trigger));
        self.stage = Stage::Cascade;
    }

    /// Queues `job` behind the cascade's passes; once the cascade stops, no
    /// pass is queued.
    pub(super) fn queue(&mut self, job: Job) {
        if !self.stopping {
            self.passes.push_back(job);
        }
    }

    /// Queues a committed event's delivery set, which runs whatever comes
    /// next; once the cascade stops, the passes it holds are dropped.
    pub(super) fn queue_set(&mut self, mut delivery: Delivery) {
        if self.stopping {
            delivery.then.clear();
        }
        self.sets.push_back(delivery);
    }

    /// Stops the cascade: no pass nor record runs any more, but the
    /// delivery set of every event committed already still runs, in index
    /// order, so an event that is stored is delivered (RFC §13.6), an
    /// adopted entry's caller having been told it is committed.
    pub(super) fn stop_cascade(&mut self) {
        self.stopping = true;
        self.passes.clear();
        for delivery in self.sets.iter_mut().chain(self.delivery.as_mut()) {
            delivery.then.clear();
        }
    }

    /// Whether the cascade has nothing queued nor running.
    pub(super) fn is_idle(&self) -> bool {
        self.passes.is_empty()
            && self.sets.is_empty()
            && self.pass.is_none()
            && self.delivery.is_none()
    }

    /// Hands the running pass the answer to one of its effects.
    pub(super) fn on_pass_outcome(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        let Some(job) = self.pass.take() else {
            debug_assert!(false, "inbound received {effect:?} with no pass running");
            return;
        };
        match job.accept(cx, effect, outcome) {
            Run::Running(job) => self.pass = Some(job),
            Run::Finished(finished) => self.settle(cx, *finished),
        }
    }

    /// Hands the running set the answer to one of its effects.
    pub(super) fn on_set_outcome(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        let Some(mut delivery) = self.delivery.take() else {
            debug_assert!(false, "inbound received {effect:?} with no set running");
            return;
        };
        match delivery.set.accept(cx, effect, outcome) {
            Progress::Waiting => self.delivery = Some(delivery),
            Progress::Done(outcome) => self.set_over(cx, outcome, delivery),
        }
    }

    /// A set is over: the passes it held join the queue, then what the set
    /// produced (RFC §10.1 step 14).
    fn set_over(&mut self, cx: &mut Cx<'_>, outcome: SetOutcome, delivery: Delivery) {
        let Delivery { set, then } = delivery;
        for job in then {
            self.queue(job);
        }
        self.on_set(cx, outcome, &set);
    }

    /// Starts the next pass, once the trigger's is queued, and the next set
    /// while none of each runs, the call ending included; once both queues
    /// are empty and no adopted entry may still add to them, the cascade
    /// ends.
    pub(super) fn run_queues(&mut self, cx: &mut Cx<'_>) {
        loop {
            let passing = matches!(self.stage, Stage::Cascade | Stage::Ending(_));
            if passing
                && self.pass.is_none()
                && let Some(mut job) = self.passes.pop_front()
            {
                job.start(cx);
                self.pass = Some(job);
                continue;
            }
            if self.delivery.is_none()
                && let Some(mut delivery) = self.sets.pop_front()
            {
                match delivery.set.start(cx) {
                    Progress::Waiting => self.delivery = Some(delivery),
                    Progress::Done(outcome) => self.set_over(cx, outcome, delivery),
                }
                continue;
            }
            break;
        }
        if matches!(self.stage, Stage::Cascade) && self.is_idle() && self.adopted.is_idle() {
            self.persist_side_effects(cx);
        }
    }
}
