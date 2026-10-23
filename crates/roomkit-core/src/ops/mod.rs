//! The pipeline operations, each a state machine run through the effect
//! protocol (docs/architecture.md, "Operations").
//!
//! A driver runs an operation in a loop: it polls it, performs the notices,
//! executes the effects and completes each with its outcome, until the
//! operation is ready.
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

mod config;
mod environment;
mod framework_events;
mod inbound;
mod io;
mod lifecycle;
mod protocol;

pub use config::{ChannelDeclaration, ConfigError, DEFAULT_CONTEXT_WINDOW, KitConfig};
pub use environment::{Environment, SequentialEnvironment};
pub use framework_events::{ChainDepthData, FrameworkEvent, RefusedOperation, TransitionReason};
pub use inbound::{Inbound, InboundError, InboundResult, default_inbound_event};
pub use lifecycle::{
    ArchiveRoom, AttachChannel, CheckTimers, CloseRoom, CreateRoom, DetachChannel, LifecycleError,
    PauseRoom, ReopenRoom, ResumeRoom, TimersChecked, UpdateBinding,
};
pub use protocol::{
    Effect, EffectId, Next, Notice, Operation, Outcome, ProtocolError, Request, RoomCounters, Step,
};

use crate::model::{ErrorRecord, RoomId};

/// What a sub-machine reads and writes while it advances.
pub(crate) struct Cx<'op> {
    pub(crate) config: &'op KitConfig,
    pub(crate) env: &'op mut dyn Environment,
    pub(crate) io: &'op mut io::Io,
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
