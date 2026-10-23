//! A direct call as an operation: its change made in the locked-section
//! frame (`ops::section`), which takes the room's lock, reads the room,
//! refuses a room the caller does not reach, applies its due timers (gap
//! G-045), changes the event or commits the record, releases the lock,
//! then reports (RFC §10.3, §10.5, §13.5).

use std::sync::Arc;

use super::MutationError;
use super::change::EventChange;
use crate::model::{Room, RoomContext, RoomId};
use crate::ops::io::Io;
use crate::ops::protocol::{EffectId, Outcome, ProtocolError, Step};
use crate::ops::section::{Failure, Section, SectionWork};
use crate::ops::{Cx, Environment, KitConfig, Progress};
use crate::rules::organizations;

/// A direct call on a room's timeline, as an operation's whole work.
#[derive(Debug)]
pub(super) struct EventSection<C: EventChange> {
    config: Arc<KitConfig>,
    io: Io,
    section: Section<EventWork<C>>,
    result: Option<Result<C::Output, MutationError>>,
}

impl<C: EventChange> EventSection<C> {
    /// The call making `change` in `room_id`, for a caller acting for
    /// `organization_id`; `None` reads the room unscoped.
    pub(super) fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        organization_id: Option<String>,
        change: C,
    ) -> Self {
        let work = EventWork {
            change,
            organization_id,
        };
        Self {
            config,
            io: Io::new(),
            section: Section::new(room_id, work),
            result: None,
        }
    }

    /// A call refused before anything is read: it ends at once with
    /// `error`, taking no lock.
    pub(super) fn refused(
        config: Arc<KitConfig>,
        room_id: RoomId,
        change: C,
        error: MutationError,
    ) -> Self {
        let work = EventWork {
            change,
            organization_id: None,
        };
        Self {
            config,
            io: Io::new(),
            section: Section::ended(room_id, work),
            result: Some(Err(error)),
        }
    }

    /// Hands the driver's answer to effect `id`.
    pub(super) fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }

    /// Advances the call; `output` reads its result from what it gave.
    pub(super) fn poll<T>(
        &mut self,
        environment: &dyn Environment,
        output: impl FnOnce(C::Output) -> T,
    ) -> Step<Result<T, MutationError>> {
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
                // A failed release is logged, and changes nothing the call
                // did (gap G-025).
                *result = Some(ended.result);
            }
        }
        let ready = result.take().map(|result| result.map(output));
        self.io.step(ready)
    }
}

/// A direct call's change as the work of a locked section.
#[derive(Debug)]
struct EventWork<C: EventChange> {
    change: C,
    /// The organization the caller acts for, whose rooms only it reaches
    /// (RFC §17.2); `None` reads the room unscoped.
    organization_id: Option<String>,
}

impl<C: EventChange> SectionWork for EventWork<C> {
    type Output = C::Output;
    type Error = MutationError;

    fn failure(failure: Failure) -> MutationError {
        match failure {
            Failure::Store(error) => MutationError::Store(error),
            Failure::Lock(error) => MutationError::Lock(error),
        }
    }

    /// A missing room makes a change of a stored event a no-op, as a target
    /// the room does not hold does, and fails a record's commit (gap
    /// G-063).
    fn missing(&self, room_id: &RoomId) -> Result<C::Output, MutationError> {
        self.change.missing(room_id)
    }

    /// A room the caller's organization does not reach is a missing one,
    /// nothing written in it, its due timers included (RFC §17.2).
    fn refuses(&self, room: &Room) -> Option<Result<C::Output, MutationError>> {
        let admitted = organizations::admits(self.organization_id.as_deref(), room);
        (!admitted).then(|| self.change.missing(&room.id))
    }

    fn begin(
        &mut self,
        cx: &mut Cx<'_>,
        context: Box<RoomContext>,
    ) -> Progress<Result<C::Output, MutationError>> {
        let room_id = context.room.id.clone();
        self.change.begin(cx, &room_id, context)
    }

    fn accept(
        &mut self,
        cx: &mut Cx<'_>,
        _effect: EffectId,
        outcome: Outcome,
    ) -> Progress<Result<C::Output, MutationError>> {
        self.change.accept(cx, outcome)
    }

    fn awaits(&self, effect: EffectId) -> bool {
        self.change.awaits(effect)
    }

    /// The change's trigger fires for what it wrote; a call that wrote
    /// nothing fires nothing (gap G-063).
    fn report(&self, cx: &mut Cx<'_>, result: &Result<C::Output, MutationError>) {
        if result.is_ok() {
            self.change.report(cx);
        }
    }
}
