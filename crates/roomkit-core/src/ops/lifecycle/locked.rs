//! The shape attaching and closing share: take the room's lock, read the
//! room, write the change, release the lock, then report it.

use std::mem;
use std::sync::Arc;

use super::LifecycleError;
use super::report::{self, Reported};
use crate::model::{ChannelBinding, ErrorRecord, Room, RoomContext, RoomId, RoomStatus};
use crate::ops::io::Io;
use crate::ops::protocol::{EffectId, Outcome, ProtocolError, Request, Step};
use crate::ops::{self, Environment, KitConfig};

/// A change made under a room's lock.
#[derive(Clone, Debug)]
pub(super) enum Change {
    Attach(Box<ChannelBinding>),
    Close,
}

/// What a locked change did. A change made carries the room as it left it,
/// which its hooks receive.
#[derive(Clone, Debug)]
pub(super) enum Changed {
    Attached {
        binding: Box<ChannelBinding>,
        context: Box<RoomContext>,
    },
    Closed(Box<RoomContext>),
    /// The room was already closed: nothing was written.
    Unchanged(Box<Room>),
}

/// One change of a room, made under its lock.
#[derive(Debug)]
pub(super) struct LockedChange {
    config: Arc<KitConfig>,
    io: Io,
    room_id: RoomId,
    change: Change,
    state: State,
}

#[derive(Debug)]
enum State {
    Start,
    Locking(EffectId),
    Loading(EffectId),
    Writing {
        effect: EffectId,
        changed: Changed,
    },
    Releasing {
        effect: EffectId,
        result: Result<Changed, LifecycleError>,
    },
    Finished(Result<Changed, LifecycleError>),
    Reported,
}

impl State {
    fn awaits(&self) -> Option<EffectId> {
        match self {
            Self::Locking(effect)
            | Self::Loading(effect)
            | Self::Writing { effect, .. }
            | Self::Releasing { effect, .. } => Some(*effect),
            Self::Start | Self::Finished(_) | Self::Reported => None,
        }
    }
}

impl LockedChange {
    pub(super) fn new(config: Arc<KitConfig>, room_id: RoomId, change: Change) -> Self {
        Self {
            config,
            io: Io::new(),
            room_id,
            change,
            state: State::Start,
        }
    }

    /// Advances the change; `output` reads the operation's result from what
    /// the change did.
    pub(super) fn poll<T>(
        &mut self,
        environment: &mut dyn Environment,
        output: impl Fn(Changed) -> Option<T>,
    ) -> Step<Result<T, LifecycleError>> {
        if matches!(self.state, State::Start) {
            let room_id = self.room_id.clone();
            self.state = State::Locking(self.io.issue(Request::AcquireLock { room_id }));
        }
        while let Some((effect, outcome)) = self.io.next_outcome() {
            self.accept(environment, effect, outcome);
        }
        let result = self.take_result().map(|result| {
            result.and_then(|changed| {
                output(changed).ok_or_else(|| {
                    LifecycleError::Store(ErrorRecord::new(
                        "Internal",
                        "the change produced another kind of result",
                    ))
                })
            })
        });
        self.io.step(result)
    }

    pub(super) fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }

    fn accept(&mut self, environment: &mut dyn Environment, effect: EffectId, outcome: Outcome) {
        if self.state.awaits() != Some(effect) {
            debug_assert!(
                false,
                "lifecycle change received {effect:?} in {:?}",
                self.state
            );
            return;
        }
        self.state = match mem::replace(&mut self.state, State::Reported) {
            State::Locking(_) => self.on_locked(outcome),
            State::Loading(_) => self.on_loaded(environment, outcome),
            State::Writing { changed, .. } => {
                let written = outcome
                    .into_stored()
                    .map(|()| changed)
                    .map_err(LifecycleError::Store);
                self.release(written)
            }
            State::Releasing { result, .. } => self.on_released(environment, result, outcome),
            state @ (State::Start | State::Finished(_) | State::Reported) => state,
        };
    }

    fn take_result(&mut self) -> Option<Result<Changed, LifecycleError>> {
        if !matches!(self.state, State::Finished(_)) {
            return None;
        }
        let State::Finished(result) = mem::replace(&mut self.state, State::Reported) else {
            return None;
        };
        Some(result)
    }

    fn on_locked(&mut self, outcome: Outcome) -> State {
        match outcome.into_locked() {
            // The change's hooks receive the room's recent events, as every
            // hook does (gap G-027).
            Ok(()) => State::Loading(self.io.issue(Request::LoadContext {
                room_id: self.room_id.clone(),
                recent_events: self.config.context_window,
            })),
            Err(error) => State::Finished(Err(LifecycleError::Lock(error))),
        }
    }

    /// Decides the write from the room as the lock found it.
    fn on_loaded(&mut self, environment: &mut dyn Environment, outcome: Outcome) -> State {
        let mut context = match outcome.into_context() {
            Ok(Some(context)) => context,
            Ok(None) => {
                return self.release(Err(LifecycleError::RoomNotFound(self.room_id.clone())));
            }
            Err(error) => return self.release(Err(LifecycleError::Store(error))),
        };
        let (request, changed) = match &self.change {
            Change::Attach(binding) => {
                attached(&mut context, binding);
                let request = Request::StoreBinding {
                    binding: (**binding).clone(),
                };
                let changed = Changed::Attached {
                    binding: binding.clone(),
                    context,
                };
                (request, changed)
            }
            Change::Close if !context.room.status.accepts_events() => {
                return self.release(Ok(Changed::Unchanged(Box::new(context.room))));
            }
            Change::Close => {
                close(&mut context.room, environment);
                let request = Request::UpdateRoom {
                    room: context.room.clone(),
                };
                (request, Changed::Closed(context))
            }
        };
        State::Writing {
            effect: self.io.issue(request),
            changed,
        }
    }

    /// Reports the change once the lock is released. A failed release
    /// changes nothing the change did; a lifecycle operation reports no error
    /// beside its result, so the failure is only logged (gap G-025).
    fn on_released(
        &mut self,
        environment: &mut dyn Environment,
        result: Result<Changed, LifecycleError>,
        outcome: Outcome,
    ) -> State {
        let (result, _logged) = ops::after_release(&self.room_id, result, outcome.into_released());
        let reported = match &result {
            Ok(Changed::Attached { binding, context }) => {
                Some((Reported::ChannelAttached(binding), context))
            }
            Ok(Changed::Closed(context)) => Some((Reported::RoomClosed, context)),
            Ok(Changed::Unchanged(_)) | Err(_) => None,
        };
        if let Some((reported, context)) = reported {
            report::report(&mut self.io, &self.config, environment, reported, context);
        }
        State::Finished(result)
    }

    fn release(&mut self, result: Result<Changed, LifecycleError>) -> State {
        let effect = self.io.issue(Request::ReleaseLock {
            room_id: self.room_id.clone(),
        });
        State::Releasing { effect, result }
    }
}

/// Puts `binding` in `context` in place of the room's binding of the same
/// channel, as storing it does.
fn attached(context: &mut RoomContext, binding: &ChannelBinding) {
    context
        .bindings
        .retain(|bound| bound.channel_id != binding.channel_id);
    context.bindings.push(binding.clone());
}

/// Closes `room` now.
fn close(room: &mut Room, environment: &mut dyn Environment) {
    let now = environment.now();
    room.status = RoomStatus::Closed;
    room.closed_at = Some(now);
    room.updated_at = now;
}
