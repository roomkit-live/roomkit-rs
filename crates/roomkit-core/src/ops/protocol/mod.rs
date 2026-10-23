//! The protocol between an operation and the driver that runs it
//! (docs/architecture.md, "The effect protocol").
//!
//! An operation asks for [`Effect`]s, each answered by exactly one
//! [`Outcome`], and emits [`Notice`]s, which need no answer. An inbound
//! operation adopts the calls into its room its own work makes ([`Entry`]),
//! and reports each one's result once it is decided ([`Settled`]), and its
//! own once it is ([`Decided`]).

mod answers;
mod entries;

pub(crate) use answers::RequestKind;
pub use entries::{Decided, Entry, EntryId, Settled};

use std::time::Duration;

use thiserror::Error;

use super::{Environment, FrameworkEvent};
use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, ErrorRecord, EventId, EventUpdates,
    IdentityId, InboundMessage, Observation, Room, RoomContext, RoomEvent, RoomId, RoomUpdates,
    Task, Timestamp,
};
use crate::rules::chain_depth::ReentryBudget;
use crate::rules::hook_chain::HookOutcome;
use crate::rules::hook_registry::{HookId, HookTrigger};
use crate::rules::routing::RouteQuery;

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
    /// The reentry budget a call into a room that the request's code makes
    /// draws on (RFC §8.3, gap G-070): an `OnEvent` or a `Deliver` carries
    /// the budget of the cascade whose delivery set made it, so that the
    /// operation a turn's or a delivery's call starts is charged on it, and
    /// a channel that calls into a room on every delivery stops at the cap.
    /// `None` for any other request.
    pub budget: Option<ReentryBudget>,
}

/// The error type of a [`Request::OnEvent`] or [`Request::Deliver`]
/// answered [`Outcome::Failed`] because the driver holds no channel under
/// the binding's id, unregistered since the operation started: a transport
/// target is then skipped, an intelligence target reported
/// `delivery_failed` with this error (gap G-048).
pub const CHANNEL_NOT_REGISTERED: &str = "channel_not_registered";

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
    /// Take the event's place in the room's delivery lane, behind every
    /// place of the room taken before in this process (RFC §10.2): issued
    /// under the room lock once the event is committed, or an instruction
    /// decided, before the lock's release, so that places follow the order
    /// of the commits. It never waits. Answer [`Outcome::Entered`].
    EnterLane {
        /// The room.
        room_id: RoomId,
        /// The event whose delivery set the place is for.
        event_id: EventId,
    },
    /// Wait until every place of the room taken before the event's is
    /// left: its delivery set's turn, so that one event's set completes
    /// before the next one's begins (RFC §10.2). Answer
    /// [`Outcome::LaneTurn`].
    AwaitLane {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
    },
    /// Leave the event's place in the room's delivery lane, wherever it
    /// stands. Answer [`Outcome::Left`].
    LeaveLane {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
    },
    /// Take the room's admission ticket, behind every ticket of the room
    /// taken earlier in this process (RFC §9.5.1): an event whose off-lock
    /// checks run before the room lock keeps its arrival order. It never
    /// waits. An operation holds at most one ticket per room. Answer
    /// [`Outcome::Ticketed`].
    TakeTicket {
        /// The room.
        room_id: RoomId,
    },
    /// Wait until every ticket of the room taken before the operation's
    /// own is released (RFC §9.5.1). Answer [`Outcome::Admitted`].
    AwaitTicket {
        /// The room.
        room_id: RoomId,
    },
    /// Release the operation's ticket of the room, wherever it stands in
    /// the room's order. Answer [`Outcome::TicketReleased`].
    ReleaseTicket {
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
    /// Find the event the room committed from `channel_id` with an
    /// idempotency key (RFC §13.4, scoped to the channel by gap G-023).
    /// Answer [`Outcome::Idempotent`].
    FindByIdempotencyKey {
        /// The room.
        room_id: RoomId,
        /// The channel the event came from.
        channel_id: ChannelId,
        /// The key.
        key: String,
    },
    /// Read the event `event_id`, whichever room holds it, refused ones
    /// included (RFC §14.1 `get_event`). Answer [`Outcome::Event`].
    GetEvent {
        /// The event.
        event_id: EventId,
    },
    /// Store `event` and set the room's counters, as one atomic commit (RFC
    /// §14.3), with the update an edit or a delete makes to its target in
    /// the same transaction (RFC §10.1 step 12). The store must refuse an
    /// index the room has given out already, a hard-deleted one included
    /// (RFC §8.1), and fail the whole commit when the target is gone.
    /// Answer [`Outcome::Committed`].
    Commit {
        /// The event, with its index and status.
        event: RoomEvent,
        /// The room's counters once the event is stored.
        counters: RoomCounters,
        /// What an accepted edit or delete writes on its target.
        target: Option<TargetUpdate>,
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
    /// Write the fields `updates` names on the stored room, keeping every
    /// other one as stored (RFC §14.1 `update_room(room_id, updates)`).
    /// Answer [`Outcome::Stored`].
    UpdateRoom {
        /// The room.
        room_id: RoomId,
        /// What to write.
        updates: RoomUpdates,
    },
    /// Store a binding, replacing the room's binding of the same channel.
    /// Answer [`Outcome::Stored`].
    StoreBinding {
        /// The binding.
        binding: ChannelBinding,
    },
    /// Remove the room's binding of a channel (RFC §14.1
    /// `delete_binding(room_id, channel_id)`). Answer [`Outcome::Stored`].
    DeleteBinding {
        /// The room.
        room_id: RoomId,
        /// The channel.
        channel_id: ChannelId,
    },
    /// Write the fields `updates` names on the stored event, keeping every
    /// other one as stored (RFC §14.1 `update_event(event_id, updates)`).
    /// Answer [`Outcome::Stored`].
    UpdateEvent {
        /// The event.
        event_id: EventId,
        /// What to write.
        updates: EventUpdates,
    },
    /// Remove the room's event `event_id` and, with `cascade_replies`, the
    /// replies of its thread (RFC §14.1 `delete_event(room_id, event_id,
    /// cascade_replies)`): a hard delete, which leaves the room's counters
    /// as they are. Answer [`Outcome::Deleted`] with the events removed,
    /// none when the room holds no such event.
    DeleteEvent {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
        /// Whether the events whose thread it roots go with it.
        cascade_replies: bool,
    },
    /// Find the identity an address belongs to on channels of a type, in an
    /// organization (RFC §14.1 `resolve_identity(channel_type, address,
    /// organization_id)`, §17.2). Answer [`Outcome::Resolved`].
    ResolveIdentity {
        /// The channel type the address is used on.
        channel_type: ChannelType,
        /// The address, in its channel's form.
        address: String,
        /// The organization the caller acts for; `None` for the identities
        /// of no organization (gap G-060).
        organization_id: Option<String>,
    },
    /// Ask the kit's routing strategy which room a message that names none
    /// belongs to (RFC §10.4), as a hook is asked: the default strategy, or
    /// the integrator's. Answer [`Outcome::Routed`].
    Route {
        /// What the router is asked.
        query: Box<RouteQuery>,
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
        /// The id the pipeline gives the event, whatever id the channel
        /// builds it with (RFC §8.1, §10.1 step 4).
        id: EventId,
    },
    /// Call the channel's `on_event()` (RFC §6.1). For an intelligence
    /// channel this is a turn (RFC §10.6). Answer [`Outcome::Output`], or
    /// [`Outcome::Failed`] with the error type [`CHANNEL_NOT_REGISTERED`]
    /// for a channel the driver no longer holds.
    OnEvent {
        /// The channel's binding.
        binding: ChannelBinding,
        /// The event, transcoded for the channel.
        event: RoomEvent,
        /// The room's context, as the channel may see it (RFC §7.5 rule 8).
        context: Box<RoomContext>,
    },
    /// Call the channel's `deliver()` (RFC §6.1). Answer
    /// [`Outcome::Output`], or [`Outcome::Failed`] with the error type
    /// [`CHANNEL_NOT_REGISTERED`] for a channel the driver no longer holds.
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
    /// Run these hooks concurrently as observers of `event`, each within
    /// its timeout, and answer once every one has finished (gap G-050):
    /// what they return is discarded and what they raise is logged, as for
    /// [`Notice::AsyncHooks`]. Answer [`Outcome::Observed`].
    RunObservers {
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

/// The driver's answer to an effect.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// The room's lock is held.
    Locked,
    /// The room's lock is released.
    Released,
    /// The event's place in the room's delivery lane is taken.
    Entered,
    /// Every earlier place of the room's delivery lane is left: the event's
    /// turn.
    LaneTurn,
    /// The event's place in the room's delivery lane is left.
    Left,
    /// The room's admission ticket is taken.
    Ticketed,
    /// Every earlier ticket of the room is released: the operation's turn.
    Admitted,
    /// The room's admission ticket is released.
    TicketReleased,
    /// The room's context.
    Context(Box<RoomContext>),
    /// No room has the requested id.
    RoomNotFound,
    /// The event committed with the key, if any.
    Idempotent(Option<Box<RoomEvent>>),
    /// The event read, if any.
    Event(Option<Box<RoomEvent>>),
    /// The events a delete removed, the target first.
    Deleted(Vec<RoomEvent>),
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
    /// Every observer has finished.
    Observed,
    /// The identity the address belongs to, if any.
    Resolved(Option<IdentityId>),
    /// The room the router chose, `None` for a new room (RFC §10.4).
    Routed(Option<RoomId>),
    /// The driver could not carry the request out.
    Failed(ErrorRecord),
    /// The effect's deadline passed before the request was carried out; it
    /// answers an effect that carried a deadline only (RFC §13.6). What the
    /// request would have taken is not held: a lock or a ticket's turn
    /// waited for is not granted, a hook given up on is not heard.
    DeadlinePassed,
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
    fn poll(&mut self, environment: &mut dyn Environment) -> Step<Self::Output>;

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

    /// Whether the operation is completing a write that a stop would leave
    /// half done: a commit not answered yet, or a room step 2 created, until
    /// its channel is attached (RFC §10.1 step 2, §13.6, gap G-068).
    ///
    /// A driver may stop an operation its caller no longer waits for, as
    /// long as the call is not decided ([`Step::decided`]): a decided call
    /// delivers what it committed whoever waits. It lets the operation run
    /// while this holds, and stops it once it does not, so that a commit
    /// is either answered or never made, and a room created is never left
    /// without the channel it was created for.
    fn completing(&self) -> bool {
        false
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
