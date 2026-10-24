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

use std::collections::VecDeque;

use super::delivery::{DeliverySet, SetOutcome};
use super::direct::DirectCall;
use super::job::{Job, RecordNote, Run};
use super::pass::{CommitPass, PassOrigin};
use super::record::RecordPass;
use super::{Machine, Stage};
use crate::model::{RoomEvent, RoomId};
use crate::ops::protocol::{EffectId, Outcome};
use crate::ops::{Cx, Progress, RefusedOperation};
use crate::rules::chain_depth::{self, ReentryBudget};
use crate::rules::hook_chain::HookChain;

/// A committed event's delivery set, and the passes that follow it.
#[derive(Debug)]
pub(super) struct Delivery {
    pub(super) set: Box<DeliverySet>,
    /// The events the event's hooks injected, which join the passes once
    /// the set is over (RFC §9.4).
    pub(super) then: Vec<Job>,
}

/// The operation's cascades, the call's and its adopted entries' (gap
/// G-070), as two queues: the passes queued and the one running, and the
/// delivery sets queued, in the order of their places, and the one running;
/// whether the cascade stopped; and the reentry budget its passes draw on.
#[derive(Debug)]
pub(super) struct Cascade {
    passes: VecDeque<Job>,
    pass: Option<Job>,
    sets: VecDeque<Delivery>,
    delivery: Option<Delivery>,
    /// Whether the cascade stopped, failed or past its deadline: no pass
    /// runs any more, committed events are still delivered.
    stopping: bool,
    /// The reentry budget every cascade of the operation draws on, an
    /// adopted entry's on its caller's (RFC §8.3), and the calls its
    /// delivery sets' targets make into a room too (gap G-070).
    pub(super) budget: ReentryBudget,
    /// Whether the call draws on the budget of the cascade whose delivery
    /// made it, which charges its trigger as it starts (gap G-070).
    drawn: bool,
    /// For a message a delivery's code sent, the room that delivery
    /// delivers in and its budget, which the message draws on once routed
    /// into that room or once it creates a room (gap G-070).
    from_delivery: Option<(RoomId, ReentryBudget)>,
}

impl Cascade {
    /// An empty cascade drawing on a budget of its own.
    pub(super) fn new(budget: ReentryBudget) -> Self {
        Self {
            passes: VecDeque::new(),
            pass: None,
            sets: VecDeque::new(),
            delivery: None,
            stopping: false,
            budget,
            drawn: false,
            from_delivery: None,
        }
    }

    /// Draws on `budget` only once the call's message is routed into
    /// `room_id`, the room whose delivery made the call, or creates a room
    /// (gap G-070).
    pub(super) fn draw_on_from_delivery(&mut self, room_id: RoomId, budget: ReentryBudget) {
        self.from_delivery = Some((room_id, budget));
    }

    /// The room and budget a delivery's message draws on once routed into
    /// that room or once it creates one, taken by its arrival.
    pub(super) fn take_from_delivery(&mut self) -> Option<(RoomId, ReentryBudget)> {
        self.from_delivery.take()
    }

    /// Draws on `budget`, the budget of the cascade whose delivery made the
    /// call (gap G-070).
    pub(super) fn draw_on(&mut self, budget: ReentryBudget) {
        self.budget = budget;
        self.drawn = true;
    }

    /// The budget of the cascade whose delivery made the call, when it
    /// draws on one, which charges the call's trigger as it starts.
    pub(super) fn drawn_on(&self) -> Option<&ReentryBudget> {
        self.drawn.then_some(&self.budget)
    }

    /// Queues `job` at the head of the passes, ahead of what the sets of
    /// the adopted entries queued meanwhile: the trigger's pass or record.
    fn lead(&mut self, job: Job) {
        self.passes.push_front(job);
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
    pub(super) fn stop(&mut self) {
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

    /// Whether the running pass waits for `effect`.
    pub(super) fn pass_awaits(&self, effect: EffectId) -> bool {
        self.pass.as_ref().is_some_and(|job| job.awaits(effect))
    }

    /// Whether the running set waits for `effect`.
    pub(super) fn set_awaits(&self, effect: EffectId) -> bool {
        self.delivery
            .as_ref()
            .is_some_and(|delivery| delivery.set.awaits(effect))
    }
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
        let pass =
            CommitPass::trigger(event, self.call.bound, checked).checking(self.call.direct.clone());
        self.cascade
            .lead(Job::Pass(Box::new(pass), PassOrigin::Trigger));
        self.stage = Stage::Cascade;
    }

    /// Queues the trigger, past the reentry budget it draws on, as its
    /// record at the head of the passes: stored `BLOCKED` with
    /// `reentry_loop_cap`, checked first for the refusals `direct` names,
    /// with no hook run for it (RFC §8.3, gap G-070).
    pub(super) fn start_capped_trigger(&mut self, mut event: RoomEvent, direct: DirectCall) {
        event.blocked_by = Some(chain_depth::REENTRY_LOOP_CAP.to_owned());
        let record = RecordPass::refusing(event, RefusedOperation::Inbound).checking(Some(direct));
        self.cascade
            .lead(Job::Record(Box::new(record), RecordNote::Trigger));
        self.stage = Stage::Cascade;
    }

    /// Hands the running pass the answer to one of its effects.
    pub(super) fn on_pass_outcome(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        let Some(job) = self.cascade.pass.take() else {
            debug_assert!(false, "inbound received {effect:?} with no pass running");
            return;
        };
        match job.accept(cx, effect, outcome) {
            Run::Running(job) => self.cascade.pass = Some(job),
            Run::Finished(finished) => self.settle(cx, *finished),
        }
    }

    /// Hands the running set the answer to one of its effects.
    pub(super) fn on_set_outcome(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        let Some(mut delivery) = self.cascade.delivery.take() else {
            debug_assert!(false, "inbound received {effect:?} with no set running");
            return;
        };
        match delivery.set.accept(cx, effect, outcome) {
            Progress::Waiting => self.cascade.delivery = Some(delivery),
            Progress::Done(outcome) => self.set_over(cx, outcome, delivery),
        }
    }

    /// A set is over: the passes it held join the queue, then what the set
    /// produced (RFC §10.1 step 14).
    fn set_over(&mut self, cx: &mut Cx<'_>, outcome: SetOutcome, delivery: Delivery) {
        let Delivery { set, then } = delivery;
        for job in then {
            self.cascade.queue(job);
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
                && self.cascade.pass.is_none()
                && let Some(mut job) = self.cascade.passes.pop_front()
            {
                job.start(cx);
                self.cascade.pass = Some(job);
                continue;
            }
            if self.cascade.delivery.is_none()
                && let Some(mut delivery) = self.cascade.sets.pop_front()
            {
                match delivery.set.start(cx) {
                    Progress::Waiting => self.cascade.delivery = Some(delivery),
                    Progress::Done(outcome) => self.set_over(cx, outcome, delivery),
                }
                continue;
            }
            break;
        }
        if matches!(self.stage, Stage::Cascade) && self.cascade.is_idle() && self.adopted.is_idle()
        {
            self.persist_side_effects(cx);
        }
    }
}
