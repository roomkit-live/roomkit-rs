//! The passes of the cascade through the room lock, and how one advances.

use super::pass::{CommitPass, PassOrigin, PassReport};
use super::queues::Lineage;
use super::record::{RecordPass, RecordReport};
use crate::model::{ChannelBinding, ChannelId, RoomEvent};
use crate::ops::protocol::{EffectId, EntryId, Notice, Outcome, SpanNote};
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
    /// Whether its stream is the call's own: a chained stream's failure, or an
    /// adopted entry's, is not its caller's (Python's `if sr.chained: continue`).
    pub(super) own: bool,
}

/// What a stored record stands for.
#[derive(Debug)]
pub(super) enum RecordNote {
    /// A channel not asked at the chain-depth limit: it gets an observation
    /// and a `chain_depth_exceeded` event (RFC §8.3).
    NotAsked {
        agent: Box<ChannelBinding>,
        trigger: Box<RoomEvent>,
        /// The chain-depth limit it reached: the kit's, or a discussion's
        /// own for its turn (RFC §19.7.5).
        limit: u32,
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

    /// Whose cascade the pass belongs to.
    pub(super) fn lineage(&self) -> Lineage {
        match self {
            Self::Pass(pass, _) | Self::Row(pass, _) => pass.lineage(),
            Self::Record(pass, _) => pass.lineage(),
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
                Progress::Done(result) => {
                    tell_failure(cx, &result, pass.lineage() == Lineage::Call);
                    Run::Finished(Box::new(Finished::Pass(result, origin)))
                }
            },
            Self::Record(mut pass, note) => match pass.accept(cx, effect, outcome) {
                Progress::Waiting => Run::Running(Self::Record(pass, note)),
                Progress::Done(result) => {
                    tell_failure(cx, &result, pass.lineage() == Lineage::Call);
                    Run::Finished(Box::new(Finished::Record(result, note)))
                }
            },
            Self::Row(mut pass, note) => match pass.accept(cx, effect, outcome) {
                Progress::Waiting => Run::Running(Self::Row(pass, note)),
                Progress::Done(result) => {
                    tell_failure(cx, &result, note.own);
                    Run::Finished(Box::new(Finished::Row(result, note)))
                }
            },
        }
    }
}

/// Tells the call's tail of a job of its `own` cascade that failed, as Python's
/// `cascade.record_error`: a failure before the call's decision has no tail to
/// reach yet.
fn tell_failure<T>(cx: &mut Cx<'_>, result: &Result<T, Abort>, own: bool) {
    if let (true, Err(abort)) = (own, result) {
        let error = abort.clone().into_record();
        cx.io
            .notify(Notice::Span(SpanNote::CascadeFailed { error }));
    }
}
