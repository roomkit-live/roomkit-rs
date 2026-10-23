//! What a kit's operations fail with.

use roomkit_core::model::{ChannelId, RoomId};
use roomkit_core::ops::{ConfigError, InboundError, LifecycleError, MutationError, ProtocolError};
use roomkit_core::rules::hook_registry::RegistrationError;
use thiserror::Error;

use crate::store::StoreError;

/// Why a kit call failed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum KitError {
    /// The inbound pipeline refused or failed the message.
    #[error(transparent)]
    Inbound(#[from] InboundError),
    /// A room lifecycle change failed.
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    /// A direct update or delete of a stored event failed (RFC §10.3).
    #[error(transparent)]
    Mutation(#[from] MutationError),
    /// The kit's configuration refused a channel.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// A hook registration broke a rule of RFC §9.1.
    #[error(transparent)]
    Registration(#[from] RegistrationError),
    /// No channel is registered under the id.
    #[error("no channel {0} is registered")]
    UnknownChannel(ChannelId),
    /// The store failed a read the kit made itself.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The timer sweeper needs a tokio runtime to run on.
    #[error("the timer sweeper must be started from within a tokio runtime")]
    NoRuntime,
    /// The timer sweeper's period must be longer than zero.
    #[error("the timer sweeper's period must be longer than zero")]
    ZeroPeriod,
    /// A call made from inside a room's own work, under the room's lock or
    /// in its admission order, would have waited for what its own caller
    /// holds: the room's lock or its admission ticket (RFC §13.5, §9.5.1
    /// Reentrance). A call into the room it names is adopted by the
    /// operation whose work made it instead; this one was not, a message
    /// routed back into the room among others, so it fails rather than wait
    /// for itself.
    #[error("{call} from inside room {room_id}'s own work would wait for itself")]
    Reentrant {
        /// The room whose lock or admission order the caller holds.
        room_id: RoomId,
        /// The kit's call.
        call: &'static str,
    },
    /// A call made from inside a room's own work was adopted by the call
    /// whose work made it, and that call was dropped before deciding it:
    /// whether its event was committed is not known.
    #[error("{call} into room {room_id} was adopted by a call dropped before deciding it")]
    Abandoned {
        /// The room.
        room_id: RoomId,
        /// The kit's call.
        call: &'static str,
    },
    /// The driver broke the effect protocol: a defect of this crate.
    #[error("the driver broke the effect protocol: {0}")]
    Protocol(#[from] ProtocolError),
    /// An operation's run ended without its output: it waited for effects
    /// the driver never received, a defect of this crate, or the runtime
    /// cancelled it, as it does when it shuts down.
    #[error("an operation's run ended without its output")]
    Stalled,
}
