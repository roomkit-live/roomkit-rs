//! What a kit's operations fail with.

use roomkit_core::model::{ChannelId, RoomId};
use roomkit_core::ops::{ConfigError, InboundError, LifecycleError, MutationError, ProtocolError};
use roomkit_core::rules::hook_registry::RegistrationError;
use thiserror::Error;

use crate::store::StoreError;

/// Why a kit call failed.
#[derive(Debug, PartialEq, Eq, Error)]
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
    /// in its admission order, could not be adopted: it would have waited
    /// for what its own caller holds, the room's lock or its admission
    /// ticket, or committed outside the order its caller stands in (RFC
    /// §13.5, §9.5.1 Reentrance). A call into the room it names is adopted
    /// by the operation whose work made it instead; this one was not, a
    /// message a hook, an off-lock hook's body or a parse routed back into
    /// the room, whose room is known only once routed, so it fails rather
    /// than wait for itself or recurse.
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
    #[error("the driver broke the effect protocol")]
    Protocol(#[from] ProtocolError),
    /// An operation's run waited for effects the driver never received, or
    /// was answered for a call it never made: a defect of this crate.
    #[error("an operation's run stalled: it waited for effects the driver never received")]
    Stalled,
    /// The runtime cancelled the call's run before it ended, as a tokio
    /// runtime does when it shuts down: what the call wrote by then is not
    /// known.
    #[error("the runtime cancelled the call's run before it ended")]
    Cancelled,
    /// A direct injection's run ended with neither its event nor a refusal
    /// (RFC §10.5): a defect of this crate.
    #[error("a direct injection ended with neither its event nor a refusal")]
    NoEvent,
    /// The kit is shut down ([`RoomKit::shutdown`](crate::RoomKit::shutdown)):
    /// the call was made after it, or its run was running and the shutdown
    /// ended it, what it wrote by then not known.
    #[error("the kit is shut down")]
    ShutDown,
}

/// `error`'s message, then each of its causes', joined by `: `: what a log
/// line or a record shows of an error whose message leaves its cause to its
/// source, a store that could not be reached among them.
pub(crate) fn with_causes(error: &dyn std::error::Error) -> String {
    let mut message = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        message.push_str(": ");
        message.push_str(&next.to_string());
        cause = next.source();
    }
    message
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pretty_assertions::assert_eq;
    use roomkit_core::ops::{
        CheckTimers, HoldOutcome, KitConfig, Next, Operation, SequentialEnvironment,
    };

    use super::*;

    /// A store failure the kit meets shows its message alone; its chain
    /// carries the cause the store met.
    #[test]
    fn a_kit_error_s_chain_carries_the_store_s_cause() {
        let error = KitError::Store(StoreError::unavailable("connection refused"));
        assert_eq!(
            (error.to_string(), with_causes(&error)),
            (
                "the store failed".to_owned(),
                "the store failed: connection refused".to_owned()
            )
        );
    }

    /// A protocol error is shown once: as the source of the kit's error.
    #[test]
    fn a_kit_error_s_chain_shows_a_protocol_error_once() {
        let error = KitError::Protocol(answered_twice());
        assert_eq!(
            with_causes(&error),
            format!("the driver broke the effect protocol: {}", answered_twice())
        );
    }

    /// The error of an effect answered twice.
    fn answered_twice() -> ProtocolError {
        let room_id = RoomId::new("room_1").unwrap();
        let mut check = CheckTimers::new(Arc::new(KitConfig::new()), room_id);
        let step = check.poll(&SequentialEnvironment::default());
        let Next::Effects(effects) = step.next else {
            panic!("the check asks for the room's lock first");
        };
        let lock = effects[0].id;
        check.complete(lock, HoldOutcome::Locked.into()).unwrap();
        check
            .complete(lock, HoldOutcome::Locked.into())
            .unwrap_err()
    }
}
