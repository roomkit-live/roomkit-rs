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
//! #     environment: &dyn Environment,
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
//! an operation the calls into its room that its own work makes under the
//! room's lock or in its admission order ([`Operation::adopt`]), answering
//! each caller from [`Step::settled`] (docs/architecture.md,
//! "Reentrancy").

mod config;
mod environment;
mod events;
mod framework_events;
mod inbound;
mod io;
mod lifecycle;
mod protocol;
mod rooting;
mod section;

pub use config::{ChannelDeclaration, ConfigError, KitConfig};
#[cfg(feature = "system-environment")]
#[doc(hidden)]
pub use environment::SystemEnvironment;
pub use environment::{Environment, SequentialEnvironment};
pub use events::{CommitRecord, DeleteEvent, MutationError, UpdateEvent};
pub use framework_events::{ChainDepthData, FrameworkEvent, RefusedOperation, TransitionReason};
pub use inbound::{Inbound, InboundError, InboundResult, InboundTarget, default_inbound_event};
pub use lifecycle::{
    ArchiveRoom, AttachChannel, CheckTimers, CloseRoom, CreateRoom, DetachChannel, LifecycleChange,
    LifecycleError, LifecyclePayload, PauseRoom, ReopenRoom, ResumeRoom, TimersChecked,
    UpdateBinding,
};
pub use protocol::{
    AfterDelivery, AnswerOf, CHANNEL_NOT_REGISTERED, ChannelOutcome, ChannelRequest, Decided,
    Delivered, Effect, EffectId, Entry, EntryId, HoldOutcome, HoldRequest, HookAnswer, HookRequest,
    KIT_CLOSED, LANE_CANCELLED, Next, Notice, Operation, Outcome, ProtocolError, ROW_HIDDEN,
    ROW_WITHHELD, Request, RoomCounters, Settled, Step, StoreOutcome, StoreRequest, StreamTarget,
    TargetUpdate,
};

use crate::model::{ErrorRecord, EventId, RoomContext, RoomEvent, RoomId};
use crate::rules::chain_depth::ReentryBudget;
use crate::rules::hook_registry::HookTrigger;

/// What a sub-machine reads and writes while it advances.
pub(crate) struct Cx<'op> {
    pub(crate) config: &'op KitConfig,
    pub(crate) env: &'op dyn Environment,
    pub(crate) io: &'op mut io::Io,
}

impl Cx<'_> {
    /// Fires `trigger`'s observers of `payload` (RFC §9.3): the hooks the
    /// registry runs on an event of its source, in one notice, with
    /// `context`, which they read whole. Nothing fires when no hook
    /// observes it.
    pub(crate) fn fire_observers(
        &mut self,
        trigger: HookTrigger,
        payload: &RoomEvent,
        context: &RoomContext,
    ) {
        let hooks = self.config.hooks.observers(trigger, payload);
        if hooks.is_empty() {
            return;
        }
        self.io.notify(Notice::AsyncHooks {
            trigger,
            hooks,
            event: Box::new(payload.clone()),
            context: Box::new(context.clone()),
            after_delivery: None,
            then: None,
        });
    }

    /// Fires `trigger`'s observers of `payload` as [`fire_observers`]
    /// does, their bodies the work of the cascade or the direct change
    /// whose budget is `budget` while they run (RFC §8.3, §9.5.1, §10.1
    /// step 16, §10.3, gap G-087): what they call into the room draws on
    /// its hook calls, at least `floor` deep, one deeper than `payload`
    /// when the caller names no deeper floor. The hooks a cascade fires
    /// once an event's delivery set has run are such work, and so are the
    /// ASYNC hooks of a SYNC trigger, which observe the event its chain
    /// allowed.
    ///
    /// [`fire_observers`]: Self::fire_observers
    pub(crate) fn fire_as_cascade_work(
        &mut self,
        trigger: HookTrigger,
        payload: &RoomEvent,
        context: &RoomContext,
        budget: &ReentryBudget,
        floor: u32,
    ) {
        let floor = floor.max(payload.chain_depth.saturating_add(1));
        self.fire_within(trigger, payload, context, budget, floor);
    }

    /// Fires `trigger`'s observers of `payload` as [`fire_observers`]
    /// does, their bodies the work whose budget is `budget` while they run,
    /// their calls into the room at least `floor` deep: a change of the
    /// room made in the room's work, a turn's tool's attach for one, fires
    /// its trigger so, under the work's floor (RFC §8.3 "A direct change's
    /// own triggers run within it, under the same floor", §10.3).
    ///
    /// [`fire_observers`]: Self::fire_observers
    pub(crate) fn fire_within(
        &mut self,
        trigger: HookTrigger,
        payload: &RoomEvent,
        context: &RoomContext,
        budget: &ReentryBudget,
        floor: u32,
    ) {
        self.fire_within_then(trigger, payload, context, (budget, floor), None);
    }

    /// Fires `trigger`'s observers of `payload` as [`fire_within`] does, in
    /// the work of `budget` at least `floor` deep, then publishes `then`
    /// once every one has ended; at once when no hook observes it, as the
    /// Python implementation's `_fire_async_hooks(then=...)` does.
    ///
    /// [`fire_within`]: Self::fire_within
    pub(crate) fn fire_within_then(
        &mut self,
        trigger: HookTrigger,
        payload: &RoomEvent,
        context: &RoomContext,
        (budget, floor): (&ReentryBudget, u32),
        then: Option<FrameworkEvent>,
    ) {
        let hooks = self.config.hooks.observers(trigger, payload);
        if hooks.is_empty() {
            if let Some(event) = then {
                self.io.notify(Notice::FrameworkEvent(event));
            }
            return;
        }
        self.io.notify(Notice::AsyncHooks {
            trigger,
            hooks,
            event: Box::new(payload.clone()),
            context: Box::new(context.clone()),
            after_delivery: Some(AfterDelivery {
                budget: budget.clone(),
                floor,
            }),
            then,
        });
    }

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
