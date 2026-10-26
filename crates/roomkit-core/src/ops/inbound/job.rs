//! The passes of the cascade through the room lock, and how one advances.

use super::pass::{CommitPass, PassOrigin, PassReport};
use super::record::{RecordPass, RecordReport};
use crate::model::{ChannelBinding, ChannelId, RoomEvent};
use crate::ops::protocol::{EffectId, EntryId, Outcome};
use crate::ops::{Abort, Cx, Progress};

/// One pass of the cascade.
#[derive(Debug)]
pub(super) enum Job {
    /// An event's pass through the locked section.
    Pass(Box<CommitPass>, PassOrigin),
    /// A refused record's commit.
    Record(Box<RecordPass>, RecordNote),
    /// The pass of a row a turn's stream ended, a response, which its
    /// reader waits for (RFC §12.2 step 13s).
    Row(Box<CommitPass>, RowNote),
}

/// What a row's pass settles once it is over.
#[derive(Clone, Debug)]
pub(super) struct RowNote {
    /// The entry its reader handed it back as.
    pub(super) entry: EntryId,
    /// The channel its delivery skips: the stream's target, which read it
    /// in the stream.
    pub(super) skips: Option<ChannelId>,
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
    /// The call's own event, past the reentry budget it draws on (gap
    /// G-070): the record is the call's result.
    Trigger,
}

/// A pass after an answer: still running, or finished.
pub(super) enum Run {
    Running(Job),
    Finished(Box<Finished>),
}

/// What a finished pass produced.
#[derive(Debug)]
pub(super) enum Finished {
    Pass(Result<PassReport, Abort>, PassOrigin),
    Record(Result<RecordReport, Abort>, RecordNote),
    Row(Result<PassReport, Abort>, RowNote),
}

impl Job {
    /// Starts the pass: it asks for the room lock.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        match self {
            Self::Pass(pass, _) | Self::Row(pass, _) => pass.start(cx),
            Self::Record(pass, _) => pass.start(cx),
        }
    }

    /// Whether the pass waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        match self {
            Self::Pass(pass, _) | Self::Row(pass, _) => pass.awaits(effect),
            Self::Record(pass, _) => pass.awaits(effect),
        }
    }

    /// Hands the pass the outcome of one of its effects; a pass that is
    /// over comes back finished.
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
            Self::Row(mut pass, note) => match pass.accept(cx, effect, outcome) {
                Progress::Waiting => Run::Running(Self::Row(pass, note)),
                Progress::Done(result) => Run::Finished(Box::new(Finished::Row(result, note))),
            },
        }
    }
}
