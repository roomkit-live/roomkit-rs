//! The commit of a record the room refused before any hook (RFC §8.3).
//!
//! A channel not asked at the chain-depth limit, a response past its
//! cascade's reentry budget, a response a transport returned at the limit,
//! an entry adopted past it, an injected event past it: each is stored
//! `BLOCKED`, under the room's lock, with the next index, and runs no hook.
//! A channel's record keeps the visibility step 12 would have given it when
//! its source is bound (gap G-036), a hook's injected event the targets the
//! room binds (gap G-031). The
//! room's due timer transitions apply first (gap G-045), and a room that no
//! longer accepts events refuses the record, as it refuses any write, with
//! `room_refused_event`: `reentry` for the framework's records, `inbound`
//! for an adopted entry's (RFC §5.1, gap G-043). A record's thread parent
//! is rooted at its commit, as every committed event's is (RFC §10.1 step
//! 8). A direct injection adopted
//! past the budget is refused first, as its pass would have refused it, when
//! the room is another organization's or binds no channel it names: nothing
//! is stored (gap G-069).

use super::direct::DirectCall;
use super::{cascade, commit, report};
use crate::model::{ChannelId, ErrorRecord, EventStatus, Room, RoomContext, RoomEvent, RoomId};
use crate::ops::protocol::{EffectId, KeyHeld, Outcome, StoreRequest};
use crate::ops::rooting::{Rooted, Rooting};
use crate::ops::section::{Failure, Section, SectionWork};
use crate::ops::{Abort, Cx, Progress, RefusedOperation};
use crate::rules::{authors, permissions};

/// What became of a record.
#[derive(Clone, Debug, PartialEq)]
pub(super) enum RecordResult {
    /// Stored, with its index.
    Committed(Box<RoomEvent>),
    /// A direct injection's record whose key its channel holds in the room:
    /// a replay of the event the key committed, nothing stored (RFC §13.4,
    /// §9.5.1 Past the cap).
    Replayed(Box<RoomEvent>),
    /// The room no longer accepts events: nothing was stored.
    Refused,
}

/// Everything a record's commit produced.
#[derive(Clone, Debug, PartialEq)]
pub(super) struct RecordReport {
    pub(super) result: RecordResult,
    /// The failure of the lock's release, which changes nothing the commit
    /// decided (gap G-025).
    pub(super) release_error: Option<ErrorRecord>,
}

/// The commit of one blocked record, in the locked-section frame
/// (`ops::section`).
#[derive(Debug)]
pub(super) struct RecordPass {
    section: Section<RecordWork>,
}

/// What a record's commit does under the room's lock.
#[derive(Debug)]
struct RecordWork {
    record: RoomEvent,
    /// What a refusal of the record by the room's status reports it as.
    refused_as: RefusedOperation,
    /// What the record's visibility is narrowed to at its commit.
    narrowing: Narrowing,
    /// What a direct injection's record is checked for at the gate (gap
    /// G-069).
    direct: Option<DirectCall>,
    /// Whether the record is the call's own event, whose commit decides the
    /// call (`Effect::decides`).
    decides: bool,
    /// The request outstanding.
    waiting: Waiting,
}

/// What a record's commit waits for under the lock.
#[derive(Debug)]
enum Waiting {
    Nothing,
    /// A direct injection's key, looked up before anything of it is
    /// committed (RFC §14.3, §13.4), then the commit.
    Key(EffectId),
    /// A read of the record's thread parent's rooting (RFC §10.1 step 8),
    /// then the commit.
    Root {
        effect: EffectId,
        rooting: Box<Rooting>,
    },
    Commit(EffectId),
}

/// What a record's visibility is narrowed to at its commit, on the room as
/// the lock finds it.
#[derive(Debug)]
enum Narrowing {
    /// A channel's record: what its source binding allows, as step 12 would
    /// have narrowed it (gap G-036).
    SourceBinding,
    /// A hook's injected event for these targets, `None` stored only: the
    /// targets the room binds (gap G-031).
    Targets(Option<Vec<ChannelId>>),
}

impl RecordPass {
    /// The commit of `record`, a channel's, which already carries its
    /// `blocked_by`, in place of what the framework refused: a turn or a
    /// response (RFC §8.3).
    pub(super) fn new(record: RoomEvent) -> Self {
        Self::refusing(record, RefusedOperation::Reentry)
    }

    /// The commit of `record`, a channel's, which already carries its
    /// `blocked_by`, reported as `refused_as` when the room's status
    /// refuses it.
    pub(super) fn refusing(mut record: RoomEvent, refused_as: RefusedOperation) -> Self {
        record.status = EventStatus::Blocked;
        let room_id = record.room_id.clone();
        let work = RecordWork {
            record,
            refused_as,
            narrowing: Narrowing::SourceBinding,
            direct: None,
            decides: false,
            waiting: Waiting::Nothing,
        };
        Self {
            section: Section::new(room_id, work),
        }
    }

    /// The record of a direct injection for `direct`, which the room refuses
    /// before anything is stored as its pass would have (gap G-069); `None`
    /// for any other record.
    pub(super) fn checking(mut self, direct: Option<DirectCall>) -> Self {
        self.section.work_mut().direct = direct;
        self
    }

    /// The record of the call's own event, whose commit decides the call.
    pub(super) fn deciding(mut self) -> Self {
        self.section.work_mut().decides = true;
        self
    }

    /// The commit of `record`, an event a hook injected for `targets`,
    /// visible to the targets the room binds (gap G-031).
    pub(super) fn injected(record: RoomEvent, targets: Option<Vec<ChannelId>>) -> Self {
        let mut pass = Self::new(record);
        pass.section.work_mut().narrowing = Narrowing::Targets(targets);
        pass
    }

    /// Whether the record's commit waits for `effect`.
    pub(super) fn awaits(&self, effect: EffectId) -> bool {
        self.section.awaits(effect)
    }

    /// Takes the room's lock.
    pub(super) fn start(&mut self, cx: &mut Cx<'_>) {
        self.section.start(cx);
    }

    /// Takes the driver's answer to the pass's outstanding effect.
    pub(super) fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<RecordReport, Abort>> {
        match self.section.accept(cx, effect, outcome) {
            Progress::Waiting => Progress::Waiting,
            Progress::Done(ended) => Progress::Done(ended.result.map(|result| RecordReport {
                result,
                release_error: ended.release_error,
            })),
        }
    }
}

impl RecordWork {
    /// Roots the record's thread parent, when it has one, as every
    /// committed event's is (RFC §10.1 step 8), then commits it.
    fn root_and_commit(&mut self, cx: &mut Cx<'_>) {
        match Rooting::of(&self.record) {
            Some((rooting, request)) => {
                let effect = cx.io.issue_locked(request, self.record.room_id.clone());
                self.waiting = Waiting::Root {
                    effect,
                    rooting: Box::new(rooting),
                };
            }
            None => self.commit(cx),
        }
    }

    /// Commits the record, no activity of the room (RFC §8.3). A record
    /// stored `BLOCKED` takes no author record, one it came with dropped
    /// (RFC §10.1 step 12).
    fn commit(&mut self, cx: &mut Cx<'_>) {
        self.record.metadata.remove(authors::AUTHOR);
        let request = StoreRequest::Commit {
            event: Box::new(self.record.clone()),
            target: None,
            activity: None,
            author_register: None,
        };
        let effect = cx.io.issue_locked(request, self.record.room_id.clone());
        if self.decides {
            cx.io
                .decides(effect, cascade::stored_record(self.record.clone()));
        }
        self.waiting = Waiting::Commit(effect);
    }
}

impl SectionWork for RecordWork {
    type Output = RecordResult;
    type Error = Abort;

    fn failure(failure: Failure) -> Abort {
        match failure {
            Failure::Store(error) => Abort::Store(error),
            Failure::Lock(error) => Abort::Lock(error),
        }
    }

    fn missing(&self, _room_id: &RoomId) -> Result<RecordResult, Abort> {
        Err(Abort::RoomNotFound)
    }

    /// A direct injection's caller that does not reach the room has its
    /// record refused before the room's due timers are applied, nothing
    /// written in it (RFC §17.2, gap G-069).
    fn refuses(&self, room: &Room) -> Option<Result<RecordResult, Abort>> {
        let refusal = self.direct.as_ref()?.foreign(room)?;
        Some(Err(Abort::Refused(refusal)))
    }

    /// The status gate, then the commit with the next index and the
    /// visibility step 12 would have given the record (gap G-036); a direct
    /// injection into a room that binds no channel it names is refused
    /// first (gap G-069), and one whose key its channel holds is a replay,
    /// looked up before anything of it is committed (RFC §14.3, §13.4).
    fn begin(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
    ) -> Progress<Result<RecordResult, Abort>> {
        let refusal = self
            .direct
            .as_ref()
            .and_then(|_| DirectCall::unbound(&self.record, &context));
        if let Some(refusal) = refusal {
            return Progress::Done(Err(Abort::Refused(refusal)));
        }
        let status = context.room.status;
        if !status.accepts_events() {
            report::room_refused_as(cx, &self.record, self.refused_as, status);
            return Progress::Done(Ok(RecordResult::Refused));
        }
        self.record.index = context.room.event_count;
        self.record.visibility = match &self.narrowing {
            Narrowing::SourceBinding => commit::stored_visibility(&self.record, &context),
            Narrowing::Targets(targets) => {
                permissions::targets_visibility(targets.as_deref(), &context.bindings)
            }
        };
        match self.record.idempotency_key.clone() {
            Some(key) if self.direct.is_some() => {
                let request = StoreRequest::GetEventByIdempotencyKey {
                    room_id: self.record.room_id.clone(),
                    channel_id: Some(self.record.source.channel_id.clone()),
                    key,
                };
                let effect = cx.io.issue_locked(request, self.record.room_id.clone());
                self.waiting = Waiting::Key(effect);
            }
            Some(_) | None => self.root_and_commit(cx),
        }
        Progress::Waiting
    }

    fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        _effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<RecordResult, Abort>> {
        match std::mem::replace(&mut self.waiting, Waiting::Nothing) {
            Waiting::Key(_) => match outcome.into_idempotent() {
                Ok(KeyHeld::Event(original)) => {
                    Progress::Done(Ok(RecordResult::Replayed(original)))
                }
                // A key held whose event the store cannot read back: the
                // record itself, as Python's `_replay_of_key` returns it.
                Ok(KeyHeld::Seen) => {
                    let replayed = Box::new(self.record.clone());
                    Progress::Done(Ok(RecordResult::Replayed(replayed)))
                }
                Ok(KeyHeld::Free) => {
                    self.root_and_commit(cx);
                    Progress::Waiting
                }
                Err(error) => Progress::Done(Err(Abort::Store(error))),
            },
            Waiting::Root { mut rooting, .. } => match rooting.on_read(outcome) {
                Ok(Rooted::Read(request)) => {
                    let effect = cx.io.issue_locked(*request, self.record.room_id.clone());
                    self.waiting = Waiting::Root { effect, rooting };
                    Progress::Waiting
                }
                Ok(Rooted::Parent(parent)) => {
                    self.record.parent_event_id = parent;
                    self.commit(cx);
                    Progress::Waiting
                }
                Err(error) => Progress::Done(Err(Abort::Store(error))),
            },
            Waiting::Commit(_) | Waiting::Nothing => match outcome.into_committed() {
                Ok(stored) => Progress::Done(Ok(RecordResult::Committed(stored))),
                Err(error) => Progress::Done(Err(Abort::Store(error))),
            },
        }
    }

    fn awaits(&self, effect: EffectId) -> bool {
        match &self.waiting {
            Waiting::Key(waited)
            | Waiting::Root { effect: waited, .. }
            | Waiting::Commit(waited) => *waited == effect,
            Waiting::Nothing => false,
        }
    }

    /// A record reports its refusal by the room's status under the lock,
    /// and nothing once it is released.
    fn report(&self, _cx: &mut Cx<'_>, _result: &Result<RecordResult, Abort>) {}
}
