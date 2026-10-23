//! The pipeline operations, each a state machine run through the effect
//! protocol (docs/architecture.md, "Operations").
//!
//! A driver runs an operation in a loop: it polls it, performs the notices,
//! answers the callers of the entries it settled, executes the effects and
//! completes each with its outcome as it finishes, polling between
//! completions, until the operation is ready. A synchronous driver that
//! executes every effect at once runs it so:
//!
//! ```no_run
//! # use roomkit_core::ops::{Effect, Environment, Next, Notice, Operation, Outcome};
//! # fn execute(effect: &Effect) -> Outcome { unimplemented!() }
//! # fn perform(notice: Notice) {}
//! # fn run<O: Operation>(
//! #     mut operation: O,
//! #     environment: &mut dyn Environment,
//! # ) -> Result<O::Output, roomkit_core::ops::ProtocolError> {
//! let output = loop {
//!     let step = operation.poll(environment);
//!     step.notices.into_iter().for_each(perform);
//!     match step.next {
//!         Next::Ready(output) => break output,
//!         Next::Waiting => continue,
//!         Next::Effects(effects) => {
//!             for effect in effects {
//!                 let outcome = execute(&effect);
//!                 operation.complete(effect.id, outcome)?;
//!             }
//!         }
//!     }
//! };
//! # Ok(output)
//! # }
//! ```
//!
//! An asynchronous driver completes each effect as it finishes, and hands
//! an operation that holds a room's lane the calls its own work makes into
//! that room ([`Operation::adopt`]), answering each caller from
//! [`Step::settled`] (docs/architecture.md, "Reentrancy").

mod config;
mod environment;
mod events;
mod framework_events;
mod inbound;
mod io;
mod lifecycle;
mod protocol;

pub use config::{ChannelDeclaration, ConfigError, DEFAULT_CONTEXT_WINDOW, KitConfig};
pub use environment::{Environment, SequentialEnvironment};
pub use events::{CommitRecord, DeleteEvent, MutationError, UpdateEvent};
pub use framework_events::{ChainDepthData, FrameworkEvent, RefusedOperation, TransitionReason};
pub use inbound::{Inbound, InboundError, InboundResult, InboundTarget, default_inbound_event};
pub use lifecycle::{
    ArchiveRoom, AttachChannel, CheckTimers, CloseRoom, CreateRoom, DetachChannel, LifecycleError,
    PauseRoom, ReopenRoom, ResumeRoom, TimersChecked, UpdateBinding,
};
pub use protocol::{
    CHANNEL_NOT_REGISTERED, Effect, EffectId, Entry, EntryId, Next, Notice, Operation, Outcome,
    ProtocolError, Request, RoomCounters, Settled, Step, TargetUpdate,
};

use crate::model::{ErrorRecord, EventId, RoomId};

/// What a sub-machine reads and writes while it advances.
pub(crate) struct Cx<'op> {
    pub(crate) config: &'op KitConfig,
    pub(crate) env: &'op mut dyn Environment,
    pub(crate) io: &'op mut io::Io,
}

impl Cx<'_> {
    /// Runs `advance` with the requests it issues made in the admission
    /// order of `room_id` (RFC §9.5.1, `Effect::admitted`).
    pub(crate) fn in_admission_order<T>(
        &mut self,
        room_id: &RoomId,
        advance: impl FnOnce(&mut Self) -> T,
    ) -> T {
        let outer = self.io.admit(Some(room_id.clone()));
        let advanced = advance(self);
        self.io.admit(outer);
        advanced
    }
}

/// Where a sub-machine stands after an outcome.
#[derive(Debug)]
pub(crate) enum Progress<T> {
    /// Effects are outstanding.
    Waiting,
    /// It is over.
    Done(T),
}

/// Why a sub-machine could not finish its work.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Abort {
    RoomNotFound,
    Store(ErrorRecord),
    Lock(ErrorRecord),
    /// The call's deadline passed before its pass decided (RFC §13.6),
    /// naming the event when the pass held the room lock by then, which it
    /// released; `None` before the lock.
    DeadlinePassed(Option<EventId>),
    /// A direct injection the room refused before its hooks, nothing
    /// written: the room is another organization's, or binds no channel
    /// the event names (gap G-069).
    Refused(InboundError),
}

impl Abort {
    /// The failure as a result's `error` records it.
    pub(crate) fn into_record(self) -> ErrorRecord {
        match self {
            Self::RoomNotFound => ErrorRecord::new("RoomNotFound", "the room disappeared"),
            Self::Store(error) | Self::Lock(error) => error,
            Self::DeadlinePassed(_) => {
                ErrorRecord::new("ProcessTimeout", "the call passed its process_timeout")
            }
            Self::Refused(error) => ErrorRecord::new("Refused", error.to_string()),
        }
    }
}

/// Settles a locked section once its lock's release answered (gap G-025).
///
/// A failed release changes nothing the section decided or wrote, and an
/// earlier failure stays the one reported, so `result` comes back as it
/// went. The release's failure is logged, and handed back when the section
/// had succeeded, for an operation that reports what failed after its
/// decision.
pub(crate) fn after_release<T, E>(
    room_id: &RoomId,
    result: Result<T, E>,
    released: Result<(), ErrorRecord>,
) -> (Result<T, E>, Option<ErrorRecord>) {
    let Err(error) = released else {
        return (result, None);
    };
    tracing::warn!(
        target: "roomkit::core::locks",
        room_id = %room_id,
        error_type = %error.error_type,
        message = %error.message,
        "releasing the room lock failed; what was decided under it stands"
    );
    let reported = result.is_ok().then_some(error);
    (result, reported)
}
