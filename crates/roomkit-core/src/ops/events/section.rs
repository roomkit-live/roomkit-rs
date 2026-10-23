//! The shape the direct calls share: take the room's lock, read the room,
//! apply its due timers (gap G-045), change the event or commit the record,
//! release the lock, then report (RFC §10.3, §10.5, §13.5).

use std::mem;
use std::sync::Arc;

use super::MutationError;
use super::change::{self, Change, Done};
use crate::model::{RoomContext, RoomId};
use crate::ops::io::Io;
use crate::ops::lifecycle::gate::{self, Gate, Opened, Pending};
use crate::ops::protocol::{EffectId, Outcome, ProtocolError, Request, Step};
use crate::ops::{self, Cx, Environment, KitConfig};

/// The shape the direct calls share: take the lock, read the room, apply
/// its due timers, change the event or commit the record, release the lock,
/// then report.
#[derive(Debug)]
pub(super) struct EventSection {
    config: Arc<KitConfig>,
    io: Io,
    room_id: RoomId,
    change: Change,
    state: State,
}

#[derive(Debug)]
pub(super) enum State {
    Start,
    Locking(EffectId),
    Loading(EffectId),
    ApplyingTimers {
        effect: EffectId,
        pending: Box<Pending>,
    },
    /// The update reads the event first.
    Reading {
        effect: EffectId,
        context: Box<RoomContext>,
    },
    /// The update's write, or the delete, is outstanding.
    Writing {
        effect: EffectId,
        done: Box<Done>,
    },
    Releasing {
        effect: EffectId,
        result: Result<Box<Done>, MutationError>,
    },
    Finished(Result<Box<Done>, MutationError>),
    Reported,
}

impl EventSection {
    pub(super) fn new(config: Arc<KitConfig>, room_id: RoomId, change: Change) -> Self {
        Self {
            config,
            io: Io::new(),
            room_id,
            change,
            state: State::Start,
        }
    }

    /// A call refused before anything is read: it ends at once with
    /// `error`, taking no lock.
    pub(super) fn refused(
        config: Arc<KitConfig>,
        room_id: RoomId,
        change: Change,
        error: MutationError,
    ) -> Self {
        Self {
            state: State::Finished(Err(error)),
            ..Self::new(config, room_id, change)
        }
    }

    /// Hands the driver's answer to effect `id`.
    pub(super) fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }

    /// Advances the call; `output` reads its result from what it did.
    pub(super) fn poll<T>(
        &mut self,
        environment: &mut dyn Environment,
        output: impl FnOnce(Done) -> Result<T, MutationError>,
    ) -> Step<Result<T, MutationError>> {
        let Self {
            config,
            io,
            room_id,
            change,
            state,
        } = self;
        let mut cx = Cx {
            config,
            env: environment,
            io,
        };
        if matches!(state, State::Start) {
            let room_id = room_id.clone();
            *state = State::Locking(cx.io.issue(Request::AcquireLock { room_id }));
        }
        while let Some((effect, outcome)) = cx.io.next_outcome() {
            *state = advance(
                &mut cx,
                room_id,
                change,
                mem::replace(state, State::Reported),
                effect,
                outcome,
            );
        }
        let ready = if matches!(state, State::Finished(_)) {
            match mem::replace(state, State::Reported) {
                State::Finished(result) => Some(result.and_then(|done| output(*done))),
                State::Start
                | State::Locking(_)
                | State::Loading(_)
                | State::ApplyingTimers { .. }
                | State::Reading { .. }
                | State::Writing { .. }
                | State::Releasing { .. }
                | State::Reported => None,
            }
        } else {
            None
        };
        self.io.step(ready)
    }
}

/// The section's next state once `effect` is answered with `outcome`.
fn advance(
    cx: &mut Cx<'_>,
    room_id: &RoomId,
    change: &Change,
    state: State,
    effect: EffectId,
    outcome: Outcome,
) -> State {
    match state {
        State::Locking(expected) if expected == effect => match outcome.into_locked() {
            Ok(()) => State::Loading(cx.io.issue(Request::LoadContext {
                room_id: room_id.clone(),
                recent_events: cx.config.context_window,
            })),
            Err(error) => State::Finished(Err(MutationError::Lock(error))),
        },
        State::Loading(expected) if expected == effect => on_loaded(cx, room_id, change, outcome),
        State::ApplyingTimers {
            effect: expected,
            pending,
        } if expected == effect => match pending.stored(cx, outcome) {
            Ok(Opened { context, .. }) => change::act(cx, room_id, change, context),
            Err(error) => release(cx, room_id, Err(MutationError::Store(error))),
        },
        State::Reading {
            effect: expected,
            context,
        } if expected == effect => change::on_read(cx, room_id, change, context, outcome),
        State::Writing {
            effect: expected,
            done,
        } if expected == effect => change::on_written(cx, room_id, change, done, outcome),
        State::Releasing {
            effect: expected,
            result,
        } if expected == effect => on_released(cx, room_id, change, result, outcome),
        state @ (State::Start
        | State::Locking(_)
        | State::Loading(_)
        | State::ApplyingTimers { .. }
        | State::Reading { .. }
        | State::Writing { .. }
        | State::Releasing { .. }
        | State::Finished(_)
        | State::Reported) => {
            debug_assert!(
                false,
                "a direct event change received {effect:?} in {state:?}"
            );
            state
        }
    }
}

/// The room as the lock found it goes through the timer gate first. A
/// missing room makes a change of a stored event a no-op, as a target the
/// room does not hold does, and fails a record's commit (gap G-063).
fn on_loaded(cx: &mut Cx<'_>, room_id: &RoomId, change: &Change, outcome: Outcome) -> State {
    let context = match outcome.into_context() {
        Ok(Some(context)) => context,
        Ok(None) if change.changes_a_stored_event() => {
            return release(cx, room_id, Ok(Box::new(Done::nothing())));
        }
        Ok(None) => {
            return release(
                cx,
                room_id,
                Err(MutationError::RoomNotFound(room_id.clone())),
            );
        }
        Err(error) => return release(cx, room_id, Err(MutationError::Store(error))),
    };
    match gate::open(cx, context) {
        Gate::Clear(context) => change::act(cx, room_id, change, context),
        Gate::Writing { effect, pending } => State::ApplyingTimers { effect, pending },
    }
}

/// Reports the change once the lock is released. A failed release changes
/// nothing the call did, and is only logged (gap G-025).
fn on_released(
    cx: &mut Cx<'_>,
    room_id: &RoomId,
    change: &Change,
    result: Result<Box<Done>, MutationError>,
    outcome: Outcome,
) -> State {
    let (result, _logged) = ops::after_release(room_id, result, outcome.into_released());
    if let Ok(done) = &result
        && done.changed
    {
        change::report(cx, change, done);
    }
    State::Finished(result)
}

/// Releases the room's lock before the call reports `result`.
pub(super) fn release(
    cx: &mut Cx<'_>,
    room_id: &RoomId,
    result: Result<Box<Done>, MutationError>,
) -> State {
    let effect = cx.io.issue(Request::ReleaseLock {
        room_id: room_id.clone(),
    });
    State::Releasing { effect, result }
}
