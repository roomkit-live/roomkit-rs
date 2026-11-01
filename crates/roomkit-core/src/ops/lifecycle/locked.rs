//! The shape every change of a room shares, take the room's lock, read the
//! room, apply its due timers, write the change, release the lock, then
//! report it, as the work of the locked-section frame (`ops::section`).
//!
//! [`LockedSection`] makes a [`SectionChange`] in that frame: a lifecycle
//! operation runs one as its whole work ([`LockedChange`]), and the inbound
//! pipeline runs one for the attach of its step 2 (RFC §10.1 step 2).

use std::sync::Arc;

use super::LifecycleError;
use super::change::{Decision, SectionChange};
use super::{record, report};
use crate::model::{ErrorRecord, Room, RoomContext, RoomId};
use crate::ops::io::Io;
use crate::ops::protocol::{EffectId, Outcome, ProtocolError, Step, StoreRequest};
use crate::ops::section::{Failure, Section, SectionWork};
use crate::ops::{Cx, Environment, KitConfig, Progress};
use crate::rules::chain_depth::Draw;
use crate::rules::organizations;
use crate::rules::timers::DueTransition;

/// Why a locked section failed: the room, the store or the lock failed it,
/// or its change refused, with nothing written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SectionError<R> {
    RoomNotFound(RoomId),
    Store(ErrorRecord),
    Lock(ErrorRecord),
    Refused(R),
}

impl SectionError<LifecycleError> {
    /// The failure as a lifecycle operation reports it.
    fn into_lifecycle(self) -> LifecycleError {
        match self {
            Self::RoomNotFound(room_id) => LifecycleError::RoomNotFound(room_id),
            Self::Store(error) => LifecycleError::Store(error),
            Self::Lock(error) => LifecycleError::Lock(error),
            Self::Refused(error) => error,
        }
    }
}

/// What a locked section of the change `C` ends with.
pub(crate) type SectionResult<C> =
    Result<<C as SectionChange>::Done, SectionError<<C as SectionChange>::Refusal>>;

/// One change of a room, made under its lock, as an operation's whole work.
#[derive(Debug)]
pub(super) struct LockedChange<C: SectionChange<Refusal = LifecycleError>> {
    config: Arc<KitConfig>,
    io: Io,
    section: LockedSection<C>,
    result: Option<Result<C::Done, LifecycleError>>,
}

impl<C: SectionChange<Refusal = LifecycleError>> LockedChange<C> {
    /// The change of `room_id`, for a caller acting for `organization_id`:
    /// a room of another organization, or of none, is not found (RFC
    /// §17.2); `None` reads the room unscoped.
    pub(super) fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        organization_id: Option<String>,
        change: C,
    ) -> Self {
        Self {
            config,
            io: Io::new(),
            section: LockedSection::changing(room_id, organization_id, change),
            result: None,
        }
    }

    /// A change refused before anything is read: it ends at once with
    /// `error`, taking no lock.
    pub(super) fn refused(
        config: Arc<KitConfig>,
        room_id: RoomId,
        change: C,
        error: LifecycleError,
    ) -> Self {
        Self {
            config,
            io: Io::new(),
            section: Section::ended(room_id, Changing::new(None, change)),
            result: Some(Err(error)),
        }
    }

    /// Advances the change; `output` reads the operation's result from what
    /// the change did and the timer transitions the gate applied.
    pub(super) fn poll<T>(
        &mut self,
        environment: &dyn Environment,
        output: impl Fn(C::Done, &[DueTransition]) -> T,
    ) -> Step<Result<T, LifecycleError>> {
        let Self {
            config,
            io,
            section,
            result,
        } = self;
        let mut cx = Cx {
            config,
            env: environment,
            io,
        };
        section.start(&mut cx);
        while let Some((effect, outcome)) = cx.io.next_outcome() {
            if let Progress::Done(ended) = section.accept(&mut cx, effect, outcome) {
                *result = Some(ended.result.map_err(SectionError::into_lifecycle));
            }
        }
        let ready = result
            .take()
            .map(|result| result.map(|done| output(done, section.transitions())));
        self.io.step(ready)
    }

    pub(super) fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }

    /// The same change, made in the room's work that `draw` names, its
    /// trigger's hook bodies drawing their calls on that work's budget, at
    /// least `floor` deep (RFC §8.3, §10.3).
    pub(super) fn made_in(mut self, draw: Draw, floor: u32) -> Self {
        self.section.work_mut().work = Some((draw, floor));
        self
    }
}

/// One change of a room under its lock: the frame every locked change
/// shares, making a [`SectionChange`].
pub(crate) type LockedSection<C> = Section<Changing<C>>;

impl<C: SectionChange> LockedSection<C> {
    /// The section making `change` in `room_id`, for a caller acting for
    /// `organization_id`; `None` reads the room unscoped. Step 2 checked
    /// the room's organization on its own read already.
    pub(crate) fn changing(room_id: RoomId, organization_id: Option<String>, change: C) -> Self {
        Section::new(room_id, Changing::new(organization_id, change))
    }
}

/// A [`SectionChange`] as the work of a locked section: decided on the room
/// as the gate left it, its writes made in order, reported once the lock
/// is released.
#[derive(Debug)]
pub(crate) struct Changing<C: SectionChange> {
    change: C,
    /// The organization the caller acts for, whose rooms only it reaches
    /// (RFC §17.2); `None` reads the room unscoped.
    organization_id: Option<String>,
    /// The write outstanding, and the decision's writes left after it.
    writing: Option<(EffectId, Box<Decision<C::Done>>)>,
    /// The read of the room's timeline outstanding, and the room as the
    /// section read it, for a change that reads it before deciding.
    reading: Option<(EffectId, Box<RoomContext>)>,
    /// The room's work the change is made in, and the least depth of its
    /// trigger's calls; `None` outside the room's work.
    work: Option<(Draw, u32)>,
}

impl<C: SectionChange> Changing<C> {
    fn new(organization_id: Option<String>, change: C) -> Self {
        Self {
            change,
            organization_id,
            writing: None,
            reading: None,
            work: None,
        }
    }

    /// Goes on with what the change decided: its writes, or its refusal.
    fn decided(
        &mut self,
        cx: &mut Cx<'_>,
        decided: Result<Decision<C::Done>, C::Refusal>,
    ) -> Progress<SectionResult<C>> {
        match decided {
            Ok(decision) => self.write_next(cx, decision),
            Err(refusal) => Progress::Done(Err(SectionError::Refused(refusal))),
        }
    }

    /// Issues the decision's next write, or ends once none is left.
    fn write_next(
        &mut self,
        cx: &mut Cx<'_>,
        mut decision: Decision<C::Done>,
    ) -> Progress<SectionResult<C>> {
        match decision.writes.pop_front() {
            Some(request) => {
                let effect = record::issue_write(cx, request);
                self.writing = Some((effect, Box::new(decision)));
                Progress::Waiting
            }
            None => Progress::Done(Ok(decision.done)),
        }
    }
}

impl<C: SectionChange> SectionWork for Changing<C> {
    type Output = C::Done;
    type Error = SectionError<C::Refusal>;

    fn reads_history(&self) -> bool {
        self.change.reads_history()
    }

    fn failure(failure: Failure) -> Self::Error {
        match failure {
            Failure::Store(error) => SectionError::Store(error),
            Failure::Lock(error) => SectionError::Lock(error),
        }
    }

    fn missing(&self, room_id: &RoomId) -> SectionResult<C> {
        Err(SectionError::RoomNotFound(room_id.clone()))
    }

    /// A room of another organization, or of none, is not found.
    fn refuses(&self, room: &Room) -> Option<SectionResult<C>> {
        let admitted = organizations::admits(self.organization_id.as_deref(), room);
        (!admitted).then(|| self.missing(&room.id))
    }

    /// Decides the change on the room as the gate left it, once the room's
    /// timeline is read for a change that reads it.
    fn begin(&mut self, cx: &mut Cx<'_>, context: Box<RoomContext>) -> Progress<SectionResult<C>> {
        if self.change.reads_timeline(&context) {
            let request = StoreRequest::ListEvents {
                room_id: context.room.id.clone(),
            };
            self.reading = Some((cx.io.issue(request), context));
            return Progress::Waiting;
        }
        let decided = self.change.decide(context, cx);
        self.decided(cx, decided)
    }

    fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        effect: EffectId,
        outcome: Outcome,
    ) -> Progress<SectionResult<C>> {
        if let Some((reading, context)) = self.reading.take() {
            if reading != effect {
                self.reading = Some((reading, context));
                return Progress::Waiting;
            }
            return match outcome.into_events() {
                Ok(timeline) => {
                    let decided = self.change.decide_on(context, &timeline, cx);
                    self.decided(cx, decided)
                }
                Err(error) => Progress::Done(Err(SectionError::Store(error))),
            };
        }
        let Some((_, decision)) = self.writing.take() else {
            return Progress::Waiting;
        };
        // A lifecycle record's commit is answered `Committed`, every other
        // write `Stored`.
        match outcome.into_written() {
            Ok(()) => self.write_next(cx, *decision),
            Err(error) => Progress::Done(Err(SectionError::Store(error))),
        }
    }

    fn awaits(&self, effect: EffectId) -> bool {
        let writes = self
            .writing
            .as_ref()
            .is_some_and(|(writing, _)| *writing == effect);
        writes
            || self
                .reading
                .as_ref()
                .is_some_and(|(reading, _)| *reading == effect)
    }

    /// A failed release changes nothing the change did; a lifecycle change
    /// reports no error beside its result, so the failure is only logged
    /// (gap G-025).
    fn report(&self, cx: &mut Cx<'_>, result: &SectionResult<C>) {
        let Some((reported, context)) = result.as_ref().ok().and_then(C::reported) else {
            return;
        };
        match &self.work {
            Some(work) => report::report_within(cx, reported, context, work),
            None => report::report(cx, reported, context),
        }
    }
}
