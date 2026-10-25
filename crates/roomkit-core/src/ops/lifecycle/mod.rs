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
pub(crate) mod change;
pub(crate) mod gate;
pub(crate) mod locked;
mod payload;
pub(crate) mod record;
pub(crate) mod report;
mod status;

use std::sync::Arc;

use thiserror::Error;

pub use self::bindings::{AttachChannel, DetachChannel, UpdateBinding};
pub use self::payload::{LifecycleChange, LifecyclePayload};
pub use self::status::{
    ArchiveRoom, CheckTimers, CloseRoom, PauseRoom, ReopenRoom, ResumeRoom, TimersChecked,
};

use self::report::Reported;
use super::io::Io;
use super::protocol::{EffectId, Operation, Outcome, ProtocolError, Step};
use super::{Cx, Environment, KitConfig};
use crate::model::{ChannelId, ErrorRecord, Room, RoomContext, RoomId, RoomStatus};
use crate::ops::protocol::StoreRequest;
use crate::rules::attachment;
use crate::rules::lifecycle::Transition;

/// Why a lifecycle operation failed. Nothing was changed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum LifecycleError {
    /// The room does not exist, or belongs to another organization than
    /// the one the caller acts for, or to none (RFC §17.2).
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
    /// A host channel is bound as a transport only: the attach binds it as
    /// an intelligence channel, by the category it names or the one the
    /// channel declares (RFC §6.2, §10.5). Nothing was read or written.
    #[error("channel {0} is a host channel, never bound as an intelligence channel")]
    HostAsIntelligence(ChannelId),
    /// The room's status is neither one the transition leaves from nor the
    /// one it names: a pause or a resume of a CLOSED or ARCHIVED room, a
    /// reopen of a room that is not CLOSED, a close of an ARCHIVED room
    /// (RFC §5.1, gap G-074). Nothing was written.
    #[error("room {room_id} is {status:?}, which {transition:?} does not leave from")]
    TransitionRefused {
        /// The room.
        room_id: RoomId,
        /// The transition.
        transition: Transition,
        /// The room's status, as the transition found it.
        status: RoomStatus,
    },
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

/// Creates a room, then runs the `ON_ROOM_CREATED` hooks, waiting for them
/// (`RunObservers`) before it emits `room_created` and returns, as the
/// inbound pipeline's creation of a room does (RFC §10.1 step 2): a host
/// that creates a room and then sends into it finds what the hooks set up,
/// the agent they attach for instance (RFC Appendix B.1, gap G-050). What a
/// hook raises is the driver's to log; a driver that cannot run them
/// answers `Failed`, which is logged, and the room is created all the same.
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
    observing: Option<EffectId>,
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
            observing: None,
            started: false,
        }
    }

    /// The room is stored: its hooks run, awaited (gap G-050), its creation
    /// announced once they are over; the output at once when none observes
    /// it.
    fn on_created(
        &mut self,
        environment: &dyn Environment,
        outcome: Outcome,
    ) -> Option<Result<Room, LifecycleError>> {
        if let Err(error) = outcome.into_stored() {
            return Some(Err(LifecycleError::Store(error)));
        }
        let context = RoomContext::new(self.room.clone());
        let mut cx = Cx {
            config: &self.config,
            env: environment,
            io: &mut self.io,
        };
        self.observing = report::report_awaited(&mut cx, Reported::RoomCreated, &context);
        match self.observing {
            Some(_) => None,
            None => Some(Ok(self.room.clone())),
        }
    }

    /// The `ON_ROOM_CREATED` hooks are over, what they raised the driver's
    /// to log: `room_created` is emitted. The room is created whatever they
    /// did.
    fn on_observed(&mut self, environment: &dyn Environment, outcome: Outcome) -> Room {
        if let Err(error) = outcome.into_observed() {
            tracing::warn!(
                target: "roomkit::core::lifecycle",
                room_id = %self.room.id,
                error_type = %error.error_type,
                message = %error.message,
                "the ON_ROOM_CREATED hooks could not be run; the room is created all the same"
            );
        }
        let mut cx = Cx {
            config: &self.config,
            env: environment,
            io: &mut self.io,
        };
        report::announce(&mut cx, Reported::RoomCreated, &self.room.id);
        self.room.clone()
    }

    /// Asks for the write, unless the room is refused as it stands.
    fn start(&mut self) -> Option<Result<Room, LifecycleError>> {
        self.started = true;
        if let Some(key) = attachment::reserved_key(&self.room.metadata) {
            return Some(Err(LifecycleError::ReservedMetadataKey(key.to_owned())));
        }
        let room = self.room.clone();
        self.creating = Some(self.io.issue(StoreRequest::CreateRoom { room }));
        None
    }
}

impl Operation for CreateRoom {
    type Output = Result<Room, LifecycleError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        let mut result = if self.started { None } else { self.start() };
        while let Some((effect, outcome)) = self.io.next_outcome() {
            if self.creating == Some(effect) {
                self.creating = None;
                result = self.on_created(environment, outcome);
            } else if self.observing == Some(effect) {
                self.observing = None;
                result = Some(Ok(self.on_observed(environment, outcome)));
            }
        }
        self.io.step(result)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.io.complete(id, outcome)
    }
}
