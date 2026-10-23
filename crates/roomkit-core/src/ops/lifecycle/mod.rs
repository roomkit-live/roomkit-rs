//! Room lifecycle operations (RFC §5.1, §5.7, §7.2, §7.5 rules 6 and 7,
//! §8.2, §9.2): creating a room; attaching, detaching and updating a
//! channel's binding; pausing, resuming, reopening, closing and archiving a
//! room; and applying the timer transitions that fell due.
//!
//! Every change but a creation changes a room that events may be flowing
//! through, so it runs under the room's lock, which is what makes the
//! inbound pipeline's status gate exact (RFC §10.1 step 6). Its status gate
//! first applies the timer transitions that fell due (gap G-045), then the
//! change is decided on the room as they left it. A change reports after
//! the release: its framework event and its trigger's hooks, with the room
//! as the change left it (gap G-027).

mod bindings;
mod change;
pub(crate) mod gate;
mod locked;
pub(crate) mod report;
mod status;

use std::sync::Arc;

use thiserror::Error;

pub use self::bindings::{AttachChannel, DetachChannel, UpdateBinding};
pub use self::status::{
    ArchiveRoom, CheckTimers, CloseRoom, PauseRoom, ReopenRoom, ResumeRoom, TimersChecked,
};

use self::report::Reported;
use super::io::Io;
use super::protocol::{EffectId, Operation, Outcome, ProtocolError, Request, Step};
use super::{Cx, Environment, KitConfig};
use crate::model::{ChannelId, ErrorRecord, Room, RoomContext, RoomId};
use crate::rules::attachment;

/// Why a lifecycle operation failed. Nothing was changed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum LifecycleError {
    /// The room does not exist.
    #[error("room {0} does not exist")]
    RoomNotFound(RoomId),
    /// The channel has no binding in the room to detach or update (gap
    /// G-047).
    #[error("channel {channel_id} is not attached to room {room_id}")]
    ChannelNotBound {
        /// The room.
        room_id: RoomId,
        /// The channel.
        channel_id: ChannelId,
    },
    /// Only the framework writes the visibility `internal`, on the records
    /// it writes itself (gap G-041).
    #[error("only the framework writes the visibility internal")]
    InternalVisibility,
    /// The room metadata key belongs to the namespace the implementation
    /// reserves (RFC §10.6.5, gap G-049).
    #[error("room metadata key {0:?} is reserved by the implementation")]
    ReservedMetadataKey(String),
    /// The store refused or failed the change.
    #[error("the store failed: {0:?}")]
    Store(ErrorRecord),
    /// The room's lock could not be taken. A failed release is no error:
    /// what was decided under the lock stands (gap G-025).
    #[error("the room lock failed: {0:?}")]
    Lock(ErrorRecord),
}

/// Creates a room, then fires `ON_ROOM_CREATED` and emits `room_created`.
/// Its output is the stored room.
///
/// A room whose metadata holds a key of the namespace the implementation
/// reserves is refused before anything is written (RFC §10.6.5, gap
/// G-049).
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

    /// Asks for the write, unless the room is refused as it stands.
    fn start(&mut self) -> Option<Result<Room, LifecycleError>> {
        self.started = true;
        if let Some(key) = attachment::reserved_key(&self.room.metadata) {
            return Some(Err(LifecycleError::ReservedMetadataKey(key.to_owned())));
        }
        let room = self.room.clone();
        self.creating = Some(self.io.issue(Request::CreateRoom { room }));
        None
    }
}

impl Operation for CreateRoom {
    type Output = Result<Room, LifecycleError>;

    fn poll(&mut self, environment: &mut dyn Environment) -> Step<Self::Output> {
        let mut result = if self.started { None } else { self.start() };
        while let Some((effect, outcome)) = self.io.next_outcome() {
            if self.creating != Some(effect) {
                continue;
            }
            self.creating = None;
            result = Some(match outcome.into_stored() {
                Ok(()) => {
                    let context = RoomContext::new(self.room.clone());
                    let mut cx = Cx {
                        config: &self.config,
                        env: environment,
                        io: &mut self.io,
                    };
                    report::report(&mut cx, Reported::RoomCreated, &context);
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
