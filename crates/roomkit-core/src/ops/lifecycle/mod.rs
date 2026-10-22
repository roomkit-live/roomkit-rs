//! Room lifecycle operations: creating a room, attaching a channel, closing
//! a room (RFC §5.1, §5.7, §8.2, §9.2).
//!
//! Attaching and closing change a room that events may be flowing through,
//! so both run under the room's lock: closing under it is what makes the
//! inbound pipeline's status gate exact (RFC §10.1 step 6).

mod locked;
mod report;

use std::sync::Arc;

use thiserror::Error;

use self::locked::{Change, Changed, LockedChange};
use self::report::Reported;
use super::io::Io;
use super::protocol::{EffectId, Operation, Outcome, ProtocolError, Request, Step};
use super::{Environment, KitConfig};
use crate::model::{ChannelBinding, ErrorRecord, Room, RoomId};

/// Why a lifecycle operation failed. Nothing was changed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum LifecycleError {
    /// The room does not exist.
    #[error("room {0} does not exist")]
    RoomNotFound(RoomId),
    /// The store refused or failed the change.
    #[error("the store failed: {0:?}")]
    Store(ErrorRecord),
    /// The room's lock could not be taken or released.
    #[error("the room lock failed: {0:?}")]
    Lock(ErrorRecord),
}

/// Creates a room, then fires `ON_ROOM_CREATED` and emits `room_created`.
/// Its output is the stored room.
#[derive(Debug)]
pub struct CreateRoom {
    config: Arc<KitConfig>,
    io: Io,
    room: Room,
    creating: Option<EffectId>,
    started: bool,
}

impl CreateRoom {
    /// The creation of `room`.
    pub fn new(config: Arc<KitConfig>, room: Room) -> Self {
        Self {
            config,
            io: Io::new(),
            room,
            creating: None,
            started: false,
        }
    }
}

impl Operation for CreateRoom {
    type Output = Result<Room, LifecycleError>;

    fn poll(&mut self, environment: &mut dyn Environment) -> Step<Self::Output> {
        if !self.started {
            self.started = true;
            let room = self.room.clone();
            self.creating = Some(self.io.issue(Request::CreateRoom { room }));
        }
        let mut result = None;
        while let Some((effect, outcome)) = self.io.next_outcome() {
            if self.creating != Some(effect) {
                continue;
            }
            self.creating = None;
            result = Some(match outcome.into_stored() {
                Ok(()) => {
                    let created = Reported::RoomCreated(&self.room);
                    report::report(&mut self.io, &self.config, environment, created);
                    Ok(self.room.clone())
                }
                Err(error) => Err(LifecycleError::Store(error)),
            });
        }
        self.io.step(result)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }
}

/// Attaches a channel to a room with its binding, under the room's lock,
/// then fires `ON_CHANNEL_ATTACHED` and emits `room_channel_attached`. Its
/// output is the stored binding.
#[derive(Debug)]
pub struct AttachChannel {
    change: LockedChange,
}

impl AttachChannel {
    /// The attachment of `binding`'s channel to `binding`'s room.
    pub fn new(config: Arc<KitConfig>, binding: ChannelBinding) -> Self {
        let room_id = binding.room_id.clone();
        Self {
            change: LockedChange::new(config, room_id, Change::Attach(Box::new(binding))),
        }
    }
}

impl Operation for AttachChannel {
    type Output = Result<ChannelBinding, LifecycleError>;

    fn poll(&mut self, environment: &mut dyn Environment) -> Step<Self::Output> {
        self.change.poll(environment, |changed| match changed {
            Changed::Attached(binding) => Some(*binding),
            Changed::Closed(_) | Changed::Unchanged(_) => None,
        })
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.change.complete(id, outcome)
    }
}

/// Closes a room under its lock, then fires `ON_ROOM_CLOSED` and emits
/// `room_closed`. Closing a room that no longer accepts events changes
/// nothing. Its output is the room as it now stands.
#[derive(Debug)]
pub struct CloseRoom {
    change: LockedChange,
}

impl CloseRoom {
    /// The closing of `room_id`.
    pub fn new(config: Arc<KitConfig>, room_id: RoomId) -> Self {
        Self {
            change: LockedChange::new(config, room_id, Change::Close),
        }
    }
}

impl Operation for CloseRoom {
    type Output = Result<Room, LifecycleError>;

    fn poll(&mut self, environment: &mut dyn Environment) -> Step<Self::Output> {
        self.change.poll(environment, |changed| match changed {
            Changed::Closed(room) | Changed::Unchanged(room) => Some(*room),
            Changed::Attached(_) => None,
        })
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.change.complete(id, outcome)
    }
}
