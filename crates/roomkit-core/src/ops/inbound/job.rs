//! The work items of a room's delivery lane, and how one advances.

use super::delivery::{DeliverySet, SetOutcome};
use super::pass::{CommitPass, PassOrigin, PassReport};
use super::record::{RecordPass, RecordResult};
use crate::model::{ChannelBinding, RoomEvent};
use crate::ops::protocol::{EffectId, Outcome};
use crate::ops::{Abort, Cx, Progress};

/// One item of the room's delivery lane.
#[derive(Debug)]
pub(super) enum Job {
    /// An event's pass through the locked section.
    Pass(Box<CommitPass>, PassOrigin),
    /// A refused record's commit.
    Record(Box<RecordPass>, RecordNote),
    /// A committed event's delivery set.
    Set(Box<DeliverySet>),
}

/// What a stored record stands for.
#[derive(Debug)]
pub(super) enum RecordNote {
    /// A channel not asked at the chain-depth limit: it gets an observation
    /// and a `chain_depth_exceeded` event (RFC §8.3).
    NotAsked {
        agent: Box<ChannelBinding>,
        trigger: Box<RoomEvent>,
    },
    /// A response the room refused without asking anyone.
    Refused,
}

/// A job after a start or an answer: still running, or finished.
pub(super) enum Run {
    Running(Job),
    Finished(Box<Finished>),
}

/// What a finished job produced.
pub(super) enum Finished {
    Pass(Result<PassReport, Abort>, PassOrigin),
    Record(Result<RecordResult, Abort>, RecordNote),
    Set(SetOutcome, Box<DeliverySet>),
}

impl Job {
    /// Starts the job; a job that needs nothing comes back finished.
    pub(super) fn start(self, cx: &mut Cx<'_>) -> Run {
        match self {
            Self::Pass(mut pass, origin) => {
                pass.start(cx);
                Run::Running(Self::Pass(pass, origin))
            }
            Self::Record(mut pass, note) => {
                pass.start(cx);
                Run::Running(Self::Record(pass, note))
            }
            Self::Set(mut set) => match set.start(cx) {
                Progress::Waiting => Run::Running(Self::Set(set)),
                Progress::Done(outcome) => Run::Finished(Box::new(Finished::Set(outcome, set))),
            },
        }
    }

    /// Hands the job the outcome of one of its effects; a job that is over
    /// comes back finished.
    pub(super) fn accept(self, cx: &mut Cx<'_>, effect: EffectId, outcome: Outcome) -> Run {
        match self {
            Self::Pass(mut pass, origin) => match pass.accept(cx, effect, outcome) {
                Progress::Waiting => Run::Running(Self::Pass(pass, origin)),
                Progress::Done(result) => Run::Finished(Box::new(Finished::Pass(result, origin))),
            },
            Self::Record(mut pass, note) => match pass.accept(cx, effect, outcome) {
                Progress::Waiting => Run::Running(Self::Record(pass, note)),
                Progress::Done(result) => Run::Finished(Box::new(Finished::Record(result, note))),
            },
            Self::Set(mut set) => match set.accept(cx, effect, outcome) {
                Progress::Waiting => Run::Running(Self::Set(set)),
                Progress::Done(outcome) => Run::Finished(Box::new(Finished::Set(outcome, set))),
            },
        }
    }
}
