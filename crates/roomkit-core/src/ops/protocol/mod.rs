//! The protocol between an operation and the driver that runs it
//! (docs/architecture.md, "Operations").
//!
//! Each poll of an [`Operation`] returns a [`Step`]: the [`Notice`]s to
//! perform now, which need no answer, and what comes [`Next`], the
//! [`Effect`]s to execute or the operation's output. An effect carries a
//! [`Request`] of one family, the room's lock, its lane and its admission
//! ticket ([`HoldRequest`]), the store ([`StoreRequest`]), a channel
//! ([`ChannelRequest`]), a hook ([`HookRequest`]) or the router; the driver
//! answers it with exactly one [`Outcome`] of the same family, or
//! `Failed`, or `DeadlinePassed` past the deadline it carries
//! ([`Operation::complete`]). An inbound operation adopts the calls into
//! its room that its own work makes ([`Entry`], [`Operation::adopt`]), and
//! reports each one's result once it is decided ([`Settled`]), and its own
//! once it is ([`Decided`]). An operation whose caller is gone before its
//! decision expires ([`Operation::expire`]): the driver drops the requests
//! it gives up on, and runs it to its end.

mod answers;
mod entries;
mod requests;
mod results;

pub(crate) use answers::RequestKind;
pub use entries::{Decided, Entry, EntryId, Settled};
pub use requests::{
    ChannelOutcome, ChannelRequest, HoldOutcome, HoldRequest, HookAnswer, HookRequest, Outcome,
    Request, StoreOutcome, StoreRequest,
};
pub use results::{InboundError, InboundResult};

use thiserror::Error;

use super::{Environment, FrameworkEvent};
use crate::model::{
    EventId, EventUpdates, Room, RoomContext, RoomEvent, RoomId, RoomUpdates, Timestamp,
};
use crate::rules::chain_depth::ReentryBudget;
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
    /// The instant past which the driver gives up on the request and answers
    /// [`Outcome::DeadlinePassed`]: a call's `process_timeout`, which bounds
    /// what precedes its commit (RFC §13.6). `None` for a request nothing
    /// bounds but its own timeouts. An operation never sets one on a write,
    /// a release or a commit: giving up on one of those midway could leave
    /// half of it done.
    pub deadline: Option<Timestamp>,
    /// The room whose lock the operation holds on the request's behalf: the
    /// request runs within that lock's tenure, as a hook under the lock
    /// does, so code it calls may take the lock again, at once (RFC §13.5:
    /// the lock is reentrant within one execution context). `None` for a
    /// request made outside every lock: code it calls waits for a lock as
    /// anyone does.
    pub locked: Option<RoomId>,
    /// The room in whose admission order the request runs (RFC §9.5.1): an
    /// off-lock hook, a message's parse, or any request of an entry the
    /// operation adopted beside its work. Code it runs stands in that
    /// order: an event it injects into the room takes no ticket, so it
    /// never waits for the ticket its own caller holds (RFC §9.5.1
    /// Reentrance). `None` for any other request.
    pub admitted: Option<RoomId>,
    /// The reentry budget of the cascade whose work the request's code is
    /// (RFC §8.3, §10.3, gaps G-070, G-087): an `OnEvent` or a `Deliver`
    /// carries the budget of the cascade whose delivery set made it, so that
    /// the operation a turn's or a delivery's call starts draws one of its
    /// passes, and a channel that calls into a room on every delivery stops
    /// at the cap; a hook, a parse or a route made under a room's lock or
    /// in its admission order (`locked`, `admitted`) carries the budget of
    /// the cascade its section works for, so that a direct change its code
    /// makes is that cascade's work, its triggers' bodies drawing on its hook
    /// calls. `None` for any other request.
    pub budget: Option<ReentryBudget>,
}

/// The error type of a [`ChannelRequest::OnEvent`] or [`ChannelRequest::Deliver`]
/// answered [`Outcome::Failed`] because the driver holds no channel under
/// the binding's id, unregistered since the operation started: a transport
/// target is then skipped, an intelligence target reported
/// `delivery_failed` with this error (gap G-048).
pub const CHANNEL_NOT_REGISTERED: &str = "channel_not_registered";

/// The room's counters after a commit (RFC §10.1 step 12).
#[derive(Clone, Debug, PartialEq)]
pub struct RoomCounters {
    /// How many indices the room has given out: the events its timeline
    /// holds, and those a hard delete removed (gap G-064).
    pub event_count: u64,
    /// The highest index of the timeline.
    pub latest_index: u64,
    /// The room's last activity, moved by a delivered event only.
    pub last_activity_at: Option<Timestamp>,
    /// What else the commit writes on the room, in the same transaction: a
    /// delivered event resumes a PAUSED room (gap G-045). Empty otherwise.
    pub room: RoomUpdates,
}

impl RoomCounters {
    /// The counters of `room` once an event of index `index` is committed to
    /// it (RFC §10.1 step 12, §14.3): one more index given out, `index` the
    /// latest, the room's activity as it is and nothing else written. So
    /// commit a record stored `BLOCKED` and a record committed outside the
    /// pipeline (gap G-071); a delivered event moves the activity too
    /// ([`moving_activity`](Self::moving_activity)).
    pub(crate) fn after_commit(room: &Room, index: u64) -> Self {
        Self {
            event_count: room.event_count.saturating_add(1),
            latest_index: index,
            last_activity_at: room.timers.last_activity_at,
            room: RoomUpdates::default(),
        }
    }

    /// The same counters, the room's activity moved to `now`, with `room`
    /// written on it in the same commit: a delivered event's, which resumes
    /// a PAUSED room (gap G-045).
    pub(crate) fn moving_activity(self, now: Timestamp, room: RoomUpdates) -> Self {
        Self {
            last_activity_at: Some(now),
            room,
            ..self
        }
    }
}

/// What a commit writes on the event an accepted edit or delete targets
/// (RFC §10.1 step 12, §10.3), in the commit's transaction.
#[derive(Clone, Debug, PartialEq)]
pub struct TargetUpdate {
    /// The target.
    pub event_id: EventId,
    /// What to write on it.
    pub updates: EventUpdates,
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
        /// For the hooks a cascade fires once an event's delivery set has
        /// run, and a direct change's: the work each body is while it runs
        /// (RFC §8.3, §9.5.1, §10.1 step 16, §10.3); `None` for any other.
        after_delivery: Option<AfterDelivery>,
    },
}

/// The work the body of an ASYNC hook is while it runs, when a cascade
/// fires it once an event's delivery set has run (RFC §10.1 step 16's
/// `AFTER_BROADCAST` hooks, a change's `ON_EVENT_UPDATED` and
/// `ON_EVENT_DELETED` hooks, §10.3), or a direct change does (gap G-087).
///
/// While the body runs, a call it makes into the room is a new arrival
/// that draws one of `budget`'s hook calls as it starts, its cascade on
/// that budget's passes; a direct change it makes there draws one too; and
/// a call it makes into any room is at least `floor` deep (RFC §8.3), so
/// that a hook that asks an agent again after each of its answers ends at
/// the chain-depth limit. Work the body starts that calls once the body
/// has returned is work of its own.
#[derive(Clone, Debug, PartialEq)]
pub struct AfterDelivery {
    /// The budget of the cascade, or of the direct change's work, whose
    /// hook calls the hooks' bodies draw on ([`Draw::HookCall`]).
    ///
    /// [`Draw::HookCall`]: crate::rules::chain_depth::Draw::HookCall
    pub budget: ReentryBudget,
    /// The least chain depth of a call the body makes: one deeper than the
    /// event the hook runs for.
    pub floor: u32,
}

/// What an operation produced on one poll.
#[derive(Clone, Debug, PartialEq)]
pub struct Step<T> {
    /// Notices to perform now, in order.
    pub notices: Vec<Notice>,
    /// The adopted entries decided since the last poll, whose callers the
    /// driver answers now (RFC §10.1 step 18).
    pub settled: Vec<Settled>,
    /// An inbound call's decision, published once, on the poll that knows
    /// it: what a caller that does not wait for the call's delivery
    /// receives (RFC §10.1 step 18). `None` on every other poll, and for
    /// every other operation.
    pub decided: Option<Decided>,
    /// What comes next.
    pub next: Next<T>,
}

/// What an operation needs next.
#[derive(Clone, Debug, PartialEq)]
pub enum Next<T> {
    /// Execute these effects, in any order or concurrently, and complete
    /// each as it finishes; poll again after each completion.
    Effects(Vec<Effect>),
    /// Effects or adopted entries are still outstanding: complete the next
    /// effect that finishes, or adopt the next call, then poll again.
    Waiting,
    /// The operation is over.
    Ready(T),
}

/// What every operation offers its driver: a step on each poll, and the
/// answer to each effect.
///
/// A driver runs any operation with the same loop: poll, perform the
/// notices, execute the effects and complete each as it finishes, polling
/// between completions, until the operation is ready (see the
/// [`ops`](crate::ops) module).
pub trait Operation {
    /// What the operation produces once it is over.
    type Output;

    /// Advances the operation with the outcomes completed since the last
    /// poll, and says what it needs next.
    ///
    /// A driver hands every poll of an operation the environment it started
    /// with: the operation's clock and its ids never change source midway,
    /// whatever environment the host sets meanwhile.
    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output>;

    /// Hands the driver's answer to effect `id`.
    ///
    /// # Errors
    ///
    /// Returns [`ProtocolError`] when `id` is not outstanding or `outcome`
    /// does not answer its request; the operation is unchanged then.
    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError>;

    /// Hands the operation a call into its room, made by the code effect
    /// `origin` runs, or by code that code awaits (RFC §13.5, §9.5.1
    /// Reentrance): code under the room's lock or in its admission order.
    /// The operation runs it as an entry of its own and reports its result
    /// in [`Step::settled`]; the next poll starts it.
    ///
    /// # Errors
    ///
    /// Hands `entry` back when the operation does not adopt it: another
    /// room's call, an `origin` no longer outstanding, or one made neither
    /// under the room's lock nor in its admission order. The driver then
    /// runs the call as an operation of its own.
    fn adopt(&mut self, entry: Entry, origin: EffectId) -> Result<EntryId, Box<Entry>> {
        let _ = origin;
        Err(Box::new(entry))
    }

    /// The call's caller stopped waiting before its decision
    /// ([`Step::decided`]): the call expires as one past its
    /// `process_timeout` does (RFC §13.6, gap G-076). Nothing more of its
    /// trigger is written, what it holds is let go, its decision is
    /// published, and every event it committed, its trigger's when the
    /// commit was answered, an adopted entry's, is delivered. A commit not
    /// answered yet, and what completes a room step 2 created, run to their
    /// end first: they are never within the call's bound.
    ///
    /// Returns the effects the operation gives up on, requests within the
    /// call's bound it was waiting for: the driver drops them and never
    /// answers them. The operation goes on to its end through `poll`, the
    /// driver answering its other effects as before. An operation decided
    /// already, or that has no caller to lose, gives up on nothing.
    fn expire(&mut self) -> Vec<EffectId> {
        Vec::new()
    }
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
