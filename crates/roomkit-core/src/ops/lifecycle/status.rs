//! Changes of a room's status, by the integrator or by its timers (RFC
//! §5.1, §8.2, §26.1).
//!
//! Each runs under the room's lock and applies a transition of
//! [`rules::lifecycle`](crate::rules::lifecycle) when it leaves from the
//! room's status; otherwise the room is left as it is, as closing a closed
//! room is. Its output is the room as it now stands.

use std::sync::Arc;

use super::LifecycleError;
use super::change::Change;
use super::locked::LockedChange;
use crate::model::{Room, RoomId};
use crate::ops::protocol::{EffectId, Operation, Outcome, ProtocolError, Step};
use crate::ops::{Environment, KitConfig};
use crate::rules::lifecycle::Transition;
use crate::rules::timers::DueTransition;

/// Declares an operation applying one transition to a room.
macro_rules! status_change {
    ($(#[$doc:meta])* $name:ident, $transition:expr) => {
        $(#[$doc])*
        #[derive(Debug)]
        pub struct $name {
            change: LockedChange,
        }

        impl $name {
            /// The change of `room_id`.
            pub fn new(config: Arc<KitConfig>, room_id: RoomId) -> Self {
                Self {
                    change: LockedChange::new(config, room_id, Change::Transition($transition)),
                }
            }
        }

        impl Operation for $name {
            type Output = Result<Room, LifecycleError>;

            fn poll(&mut self, environment: &mut dyn Environment) -> Step<Self::Output> {
                self.change
                    .poll(environment, |changed, _| Some(changed.into_context().room))
            }

            fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
                self.change.complete(id, outcome)
            }
        }
    };
}

status_change!(
    /// Pauses an active room, then fires `ON_ROOM_PAUSED`. The integrator's
    /// pause emits no `room_paused`, which reports the inactivity timer's
    /// (RFC §8.2, §26.1). The close timer counts from the pause (gap G-045).
    PauseRoom,
    Transition::Pause
);

status_change!(
    /// Resumes a paused room, which restarts its inactivity timer (gap
    /// G-045). The RFC names no trigger or framework event for it.
    ResumeRoom,
    Transition::Resume
);

status_change!(
    /// Reopens a closed room: it accepts events again (RFC §5.1), and its
    /// inactivity timer restarts (gap G-045). The RFC names no trigger or
    /// framework event for it.
    ReopenRoom,
    Transition::Reopen
);

status_change!(
    /// Closes an active or paused room, then fires `ON_ROOM_CLOSED` and
    /// emits `room_closed`: the room refuses every new event from now on
    /// (RFC §5.1).
    CloseRoom,
    Transition::Close
);

status_change!(
    /// Archives a room, the terminal state, which is never left; emits
    /// `room_archived` and fires no hook (RFC §26.1).
    ArchiveRoom,
    Transition::Archive
);

/// What [`CheckTimers`] did: the room as it now stands, and the
/// transitions applied, in the order they fell due.
#[derive(Clone, Debug, PartialEq)]
pub struct TimersChecked {
    /// The room.
    pub room: Room,
    /// The transitions applied, each at the instant it fell due; empty when
    /// none was due.
    pub transitions: Vec<DueTransition>,
}

/// Applies the timer transitions that fell due to a room nobody writes to
/// (RFC §5.1, gap G-045), under its lock, as the status gate of every
/// locked section does. A timer's pause emits `room_paused` and its close
/// `room_closed`, each with the reason `timer`, and fires its trigger.
#[derive(Debug)]
pub struct CheckTimers {
    change: LockedChange,
}

impl CheckTimers {
    /// The check of `room_id`'s timers.
    pub fn new(config: Arc<KitConfig>, room_id: RoomId) -> Self {
        Self {
            change: LockedChange::new(config, room_id, Change::CheckTimers),
        }
    }
}

impl Operation for CheckTimers {
    type Output = Result<TimersChecked, LifecycleError>;

    fn poll(&mut self, environment: &mut dyn Environment) -> Step<Self::Output> {
        self.change.poll(environment, |changed, transitions| {
            Some(TimersChecked {
                room: changed.into_context().room,
                transitions: transitions.to_vec(),
            })
        })
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.change.complete(id, outcome)
    }
}
