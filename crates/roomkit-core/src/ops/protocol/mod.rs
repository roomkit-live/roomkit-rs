//! The protocol between an operation and the driver that runs it
//! (docs/architecture.md, "The effect protocol").
//!
//! An operation asks for [`Effect`]s, each answered by exactly one
//! [`Outcome`], and emits [`Notice`]s, which need no answer.

mod answers;

pub(crate) use answers::RequestKind;

use std::time::Duration;

use thiserror::Error;

use super::{Environment, FrameworkEvent};
use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, ErrorRecord, InboundMessage, Observation, Room,
    RoomContext, RoomEvent, RoomId, Task, Timestamp,
};
use crate::rules::hook_chain::HookOutcome;
use crate::rules::hook_registry::{HookId, HookTrigger};

/// Identifies an effect within its operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EffectId(u64);

impl EffectId {
    pub(crate) fn first() -> Self {
        Self(0)
    }

    pub(crate) fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// Something the driver does for an operation.
#[derive(Clone, Debug, PartialEq)]
pub struct Effect {
    /// The id the driver completes the effect with.
    pub id: EffectId,
    /// What to do.
    pub request: Request,
}

/// What an effect asks of the driver, and the outcome it is answered with.
///
/// Every request may also be answered with [`Outcome::Failed`] when the
/// driver could not carry it out (a store error, a channel that raised).
#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    /// Take the room's lock (RFC §13.5). Answer [`Outcome::Locked`] once it
    /// is held.
    AcquireLock {
        /// The room.
        room_id: RoomId,
    },
    /// Release the room's lock. Answer [`Outcome::Released`].
    ReleaseLock {
        /// The room.
        room_id: RoomId,
    },
    /// Read the room with its bindings, its participants and its latest
    /// events, refused ones included (RFC §10.1 step 3). Answer
    /// [`Outcome::Context`], or [`Outcome::RoomNotFound`].
    LoadContext {
        /// The room.
        room_id: RoomId,
        /// How many of the latest events to read.
        recent_events: usize,
    },
    /// Read the room's bindings. Answer [`Outcome::Bindings`].
    LoadBindings {
        /// The room.
        room_id: RoomId,
    },
    /// Find the event the room committed with an idempotency key (RFC
    /// §13.4). Answer [`Outcome::Idempotent`].
    FindByIdempotencyKey {
        /// The room.
        room_id: RoomId,
        /// The key.
        key: String,
    },
    /// Store `event` and set the room's counters, as one atomic commit (RFC
    /// §14.3). The store must refuse an index the room already holds (RFC
    /// §8.1). Answer [`Outcome::Committed`].
    Commit {
        /// The event, with its index and status.
        event: RoomEvent,
        /// The room's counters once the event is stored.
        counters: RoomCounters,
    },
    /// Store side effects (RFC §10.1 step 15). Answer
    /// [`Outcome::Persisted`].
    PersistSideEffects {
        /// The room they belong to.
        room_id: RoomId,
        /// The tasks.
        tasks: Vec<Task>,
        /// The observations.
        observations: Vec<Observation>,
    },
    /// Create a room. Answer [`Outcome::Stored`], or [`Outcome::Failed`]
    /// when the id is taken.
    CreateRoom {
        /// The room to store.
        room: Room,
    },
    /// Replace a room's stored record. Answer [`Outcome::Stored`].
    UpdateRoom {
        /// The room as it is to be stored.
        room: Room,
    },
    /// Store a binding, replacing the room's binding of the same channel.
    /// Answer [`Outcome::Stored`].
    StoreBinding {
        /// The binding.
        binding: ChannelBinding,
    },
    /// Call the channel's `handle_inbound()` (RFC §6.1). Answer
    /// [`Outcome::Parsed`].
    HandleInbound {
        /// The channel.
        channel_id: ChannelId,
        /// The message it received.
        message: InboundMessage,
        /// The room's context.
        context: Box<RoomContext>,
    },
    /// Call the channel's `on_event()` (RFC §6.1). For an intelligence
    /// channel this is a turn (RFC §10.6). Answer [`Outcome::Output`].
    OnEvent {
        /// The channel's binding.
        binding: ChannelBinding,
        /// The event, transcoded for the channel.
        event: RoomEvent,
        /// The room's context, as the channel may see it (RFC §7.5 rule 8).
        context: Box<RoomContext>,
    },
    /// Call the channel's `deliver()` (RFC §6.1). Answer
    /// [`Outcome::Output`].
    Deliver {
        /// The channel's binding.
        binding: ChannelBinding,
        /// The event, transcoded for the channel.
        event: RoomEvent,
        /// The room's context, as the channel may see it.
        context: Box<RoomContext>,
    },
    /// Run one SYNC hook on `event`, within `timeout` (RFC §9.3). Answer
    /// [`Outcome::Hook`] with what the hook produced, `TimedOut` past the
    /// timeout.
    RunHook {
        /// The hook.
        hook: HookId,
        /// The trigger it runs on.
        trigger: HookTrigger,
        /// The payload, as the previous hook left it.
        event: RoomEvent,
        /// The room's context, whole (RFC §7.5 rule 8).
        context: Box<RoomContext>,
        /// The hook's timeout.
        timeout: Duration,
    },
}

/// The room's counters after a commit (RFC §10.1 step 12).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoomCounters {
    /// How many events the timeline holds.
    pub event_count: u64,
    /// The highest index of the timeline.
    pub latest_index: u64,
    /// The room's last activity, moved by a delivered event only.
    pub last_activity_at: Option<Timestamp>,
}

/// The driver's answer to an effect.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// The room's lock is held.
    Locked,
    /// The room's lock is released.
    Released,
    /// The room's context.
    Context(Box<RoomContext>),
    /// No room has the requested id.
    RoomNotFound,
    /// The room's bindings.
    Bindings(Vec<ChannelBinding>),
    /// The event committed with the key, if any.
    Idempotent(Option<Box<RoomEvent>>),
    /// The commit is durable.
    Committed,
    /// The side effects are stored.
    Persisted,
    /// The record is stored.
    Stored,
    /// The event the channel parsed from the message.
    Parsed(Box<RoomEvent>),
    /// What the channel returned.
    Output(Box<ChannelOutput>),
    /// What the hook produced.
    Hook(Box<HookOutcome<RoomEvent>>),
    /// The driver could not carry the request out.
    Failed(ErrorRecord),
}

/// Something the driver performs or reports without answering.
#[derive(Clone, Debug, PartialEq)]
pub enum Notice {
    /// Publish a framework event to its subscribers (RFC §8.2).
    FrameworkEvent(FrameworkEvent),
    /// Run these hooks concurrently as observers of `event`; what they return
    /// is discarded and what they raise is logged (RFC §9.3).
    AsyncHooks {
        /// The trigger.
        trigger: HookTrigger,
        /// The hooks to run.
        hooks: Vec<HookId>,
        /// Their payload.
        event: Box<RoomEvent>,
        /// The room's context, whole.
        context: Box<RoomContext>,
    },
}

/// What an operation produced on one poll.
#[derive(Clone, Debug, PartialEq)]
pub struct Step<T> {
    /// Notices to perform now, in order.
    pub notices: Vec<Notice>,
    /// What comes next.
    pub next: Next<T>,
}

/// What an operation needs next.
#[derive(Clone, Debug, PartialEq)]
pub enum Next<T> {
    /// Execute these effects, in any order or concurrently, and complete
    /// each; then poll again.
    Effects(Vec<Effect>),
    /// Effects are still outstanding: complete them, then poll again.
    Waiting,
    /// The operation is over.
    Ready(T),
}

/// What every operation offers its driver: a step on each poll, and the
/// answer to each effect.
///
/// A driver runs any operation with the same loop: poll, perform the
/// notices, execute the effects and complete each, until the operation is
/// ready (see the [`ops`](crate::ops) module).
pub trait Operation {
    /// What the operation produces once it is over.
    type Output;

    /// Advances the operation with the outcomes completed since the last
    /// poll, and says what it needs next.
    fn poll(&mut self, environment: &mut dyn Environment) -> Step<Self::Output>;

    /// Hands the driver's answer to effect `id`.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] when `id` is not outstanding or `outcome`
    /// does not answer its request; the operation is unchanged then.
    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError>;
}

/// A driver broke the protocol.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum ProtocolError {
    /// The effect is unknown, or already completed.
    #[error("effect {0:?} is not outstanding")]
    UnknownEffect(EffectId),
    /// The outcome does not answer the effect's request.
    #[error("effect {effect:?} asked for {expected}, the driver answered {got}")]
    UnexpectedOutcome {
        /// The effect.
        effect: EffectId,
        /// What its request is answered with.
        expected: &'static str,
        /// What the driver answered.
        got: &'static str,
    },
}
