//! The room's delivery lane as the operation runs it (RFC §10.2): a FIFO of
//! the cascades' work, one job at a time. The call's pass joins it once the
//! call arrived, behind the delivery sets its adopted entries queued
//! meanwhile. A cascade that stops, failed or past its deadline, runs no
//! pass any more, and still delivers every event committed.

use super::job::{Job, Run};
use super::pass::{CommitPass, PassOrigin};
use super::{Machine, Stage};
use crate::model::RoomEvent;
use crate::ops::Cx;
use crate::ops::protocol::{EffectId, Outcome};
use crate::rules::hook_chain::HookChain;

impl Machine {
    /// Queues the trigger's pass, bounded by the call's deadline and going
    /// on with the chain its off-lock check left, if any.
    pub(super) fn start_trigger(
        &mut self,
        event: RoomEvent,
        checked: Option<HookChain<RoomEvent>>,
    ) {
        let pass = CommitPass::trigger(event, self.deadline, checked);
        self.queue(Job::Pass(Box::new(pass), PassOrigin::Trigger));
        self.stage = Stage::Cascade;
    }

    /// Queues `job` at the back of the lane; once the cascade stops, only
    /// a committed event's delivery set is queued.
    pub(super) fn queue(&mut self, job: Job) {
        if self.stopping && !matches!(job, Job::Set(_)) {
            return;
        }
        self.lane.push_back(job);
    }

    /// Stops the cascade: no pass nor record runs any more, but the
    /// delivery set of every event committed already still runs, in index
    /// order, so an event that is stored is delivered (RFC §13.6), an
    /// adopted entry's caller having been told it is committed.
    pub(super) fn stop_cascade(&mut self) {
        self.stopping = true;
        self.lane.retain(|job| matches!(job, Job::Set(_)));
    }

    pub(super) fn dispatch_to_job(&mut self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) {
        let Some(job) = self.active.take() else {
            debug_assert!(false, "inbound received {effect:?} with no job running");
            return;
        };
        match job.accept(cx, effect, outcome) {
            Run::Running(job) => self.active = Some(job),
            Run::Finished(finished) => self.settle(cx, *finished),
        }
    }

    /// Starts the lane's next job while none runs, the call ending
    /// included; once the lane is empty and no adopted entry may still add
    /// to it, the cascade ends.
    pub(super) fn run_lane(&mut self, cx: &mut Cx<'_>) {
        while matches!(self.stage, Stage::Cascade | Stage::Ending(_)) && self.active.is_none() {
            let Some(job) = self.lane.pop_front() else {
                if matches!(self.stage, Stage::Cascade) && self.adopted.is_idle() {
                    self.persist_side_effects(cx);
                }
                return;
            };
            match job.start(cx) {
                Run::Running(job) => self.active = Some(job),
                Run::Finished(finished) => self.settle(cx, *finished),
            }
        }
    }
}
