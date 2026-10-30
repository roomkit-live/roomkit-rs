//! What an operation asks of its driver, by family ([`Request`]), and what
//! the driver answers, by the same families ([`Outcome`]).

use std::time::Duration;

use super::{EffectId, RoomCounters, TargetUpdate};
use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, ErrorRecord, EventId, EventUpdates,
    Identity, InboundMessage, Observation, ProviderResult, Room, RoomContext, RoomEvent, RoomId,
    RoomUpdates, Task,
};
use crate::rules::hook_chain::HookOutcome;
use crate::rules::hook_registry::{HookId, HookTrigger};
use crate::rules::routing::RouteQuery;

/// What an effect asks of the driver, by family, and the outcome it is
/// answered with: a driver hands each family to one handler. A hold is the
/// driver's own work, carried out at once or waited for; every other family
/// is a call, on the store, a channel, hooks or the router.
///
/// Every request may also be answered with [`Outcome::Failed`] when the
/// driver could not carry it out (a store error, a channel that raised).
#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "a request is built once and moved to its family's handler: a channel's carries its event and binding whole"
)]
pub enum Request {
    /// A room's lock, a place in its lane, its admission ticket.
    Hold(HoldRequest),
    /// The store.
    Store(StoreRequest),
    /// A channel.
    Channel(ChannelRequest),
    /// Hooks.
    Hook(HookRequest),
    /// Ask the kit's routing strategy which room a message that names none
    /// belongs to (RFC §10.4), as a hook is asked: the default strategy, or
    /// the integrator's. Answer [`Outcome::Routed`].
    Route {
        /// What the router is asked.
        query: Box<RouteQuery>,
    },
}

impl From<HoldRequest> for Request {
    fn from(request: HoldRequest) -> Self {
        Self::Hold(request)
    }
}

impl From<StoreRequest> for Request {
    fn from(request: StoreRequest) -> Self {
        Self::Store(request)
    }
}

impl From<ChannelRequest> for Request {
    fn from(request: ChannelRequest) -> Self {
        Self::Channel(request)
    }
}

impl From<HookRequest> for Request {
    fn from(request: HookRequest) -> Self {
        Self::Hook(request)
    }
}

/// What the driver holds of a room for an operation, and carries out
/// itself, never as a call: the room's lock (RFC §13.5), the places of the
/// operation's events in the room's delivery lane (RFC §10.2), and its
/// admission ticket (RFC §9.5.1). Each is answered by its own
/// [`HoldOutcome`].
#[derive(Clone, Debug, PartialEq)]
pub enum HoldRequest {
    /// Take the room's lock (RFC §13.5). Answer [`HoldOutcome::Locked`] once it
    /// is held.
    AcquireLock {
        /// The room.
        room_id: RoomId,
    },
    /// Release the room's lock. Answer [`HoldOutcome::Released`].
    ReleaseLock {
        /// The room.
        room_id: RoomId,
    },
    /// Take the event's place in the room's delivery lane, behind every
    /// place of the room taken before in this process (RFC §10.2): issued
    /// under the room lock once the event is committed, or an instruction
    /// decided, before the lock's release, so that places follow the order
    /// of the commits. It never waits. Answer [`HoldOutcome::Entered`].
    EnterLane {
        /// The room.
        room_id: RoomId,
        /// The event whose delivery set the place is for.
        event_id: EventId,
    },
    /// Wait until every place of the room taken before the event's is
    /// left: its delivery set's turn, so that one event's set completes
    /// before the next one's begins (RFC §10.2). Answer
    /// [`HoldOutcome::LaneTurn`], or [`HoldOutcome::LaneClosed`] once the
    /// kit's close drops the queued sets (RFC §12.10.4).
    AwaitLane {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
    },
    /// Leave the event's place in the room's delivery lane, wherever it
    /// stands. Answer [`HoldOutcome::Left`].
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
    /// [`HoldOutcome::Ticketed`].
    TakeTicket {
        /// The room.
        room_id: RoomId,
    },
    /// Wait until every ticket of the room taken before the operation's
    /// own is released (RFC §9.5.1). Answer [`HoldOutcome::Admitted`].
    AwaitTicket {
        /// The room.
        room_id: RoomId,
    },
    /// Release the operation's ticket of the room, wherever it stands in
    /// the room's order. Answer [`HoldOutcome::TicketReleased`].
    ReleaseTicket {
        /// The room.
        room_id: RoomId,
    },
}

/// A read or a write of the store (RFC §14), answered by a
/// [`StoreOutcome`].
#[derive(Clone, Debug, PartialEq)]
pub enum StoreRequest {
    /// Read the room with its bindings, its participants, its latest
    /// events, refused ones included (RFC §10.1 step 3), and its register of
    /// authors, the room's private state under `author_register` (RFC §14.1
    /// `get_private_state`, §10.1 step 12, gap G-089). Answer
    /// [`StoreOutcome::Context`], or [`StoreOutcome::RoomNotFound`].
    LoadContext {
        /// The room.
        room_id: RoomId,
        /// How many of the latest events to read.
        recent_events: usize,
    },
    /// Read the event the room committed with an idempotency key, a
    /// `BLOCKED` one included (RFC §14.1 `get_event_by_idempotency_key`,
    /// §13.4): the one `channel_id` committed, or for `None` the one any
    /// channel of the room committed (§22.1). Answer
    /// [`StoreOutcome::Idempotent`].
    GetEventByIdempotencyKey {
        /// The room.
        room_id: RoomId,
        /// The channel the event came from; `None` for any of the room's.
        channel_id: Option<ChannelId>,
        /// The key.
        key: String,
    },
    /// Read the event `event_id`, whichever room holds it, refused ones
    /// included (RFC §14.1 `get_event`). Answer [`StoreOutcome::Event`].
    GetEvent {
        /// The event.
        event_id: EventId,
    },
    /// Read the room's whole timeline, in index order, the events stored
    /// `BLOCKED` left out (RFC §14.1 `list_events`): what a register of
    /// authors is rebuilt from, for a room that holds none and whose
    /// timeline the context's window does not cover (RFC §10.1 step 12,
    /// gap G-089). Answer [`StoreOutcome::Events`].
    ListEvents {
        /// The room.
        room_id: RoomId,
    },
    /// Store `event` and set the room's counters, as one atomic commit (RFC
    /// §14.3), with the update an edit or a delete makes to its target in
    /// the same transaction (RFC §10.1 step 12), and what the counters
    /// carry for the room, its register of authors among it (gap G-089). The store must refuse an
    /// index the room has given out already, a hard-deleted one included
    /// (RFC §8.1), and fail the whole commit when the target is gone.
    /// Answer [`StoreOutcome::Committed`].
    Commit {
        /// The event, with its index and status.
        event: Box<RoomEvent>,
        /// The room's counters once the event is stored.
        counters: RoomCounters,
        /// What an accepted edit or delete writes on its target.
        target: Option<TargetUpdate>,
    },
    /// Store side effects (RFC §10.1 step 15). Answer
    /// [`StoreOutcome::Persisted`].
    PersistSideEffects {
        /// The room they belong to.
        room_id: RoomId,
        /// The tasks.
        tasks: Vec<Task>,
        /// The observations.
        observations: Vec<Observation>,
    },
    /// Create a room. Answer [`StoreOutcome::Stored`], or [`Outcome::Failed`]
    /// when the id is taken.
    CreateRoom {
        /// The room to store.
        room: Room,
    },
    /// Write the fields `updates` names on the stored room, keeping every
    /// other one as stored (RFC §14.1 `update_room(room_id, updates)`).
    /// Answer [`StoreOutcome::Stored`].
    UpdateRoom {
        /// The room.
        room_id: RoomId,
        /// What to write.
        updates: RoomUpdates,
    },
    /// Store a binding, replacing the room's binding of the same channel.
    /// Answer [`StoreOutcome::Stored`].
    StoreBinding {
        /// The binding.
        binding: ChannelBinding,
    },
    /// Remove the room's binding of a channel (RFC §14.1
    /// `delete_binding(room_id, channel_id)`). Answer [`StoreOutcome::Stored`].
    DeleteBinding {
        /// The room.
        room_id: RoomId,
        /// The channel.
        channel_id: ChannelId,
    },
    /// Write the fields `updates` names on the stored event, keeping every
    /// other one as stored (RFC §14.1 `update_event(event_id, updates)`).
    /// Answer [`StoreOutcome::Stored`].
    UpdateEvent {
        /// The event.
        event_id: EventId,
        /// What to write.
        updates: EventUpdates,
    },
    /// Remove the room's event `event_id`, with `cascade_replies` its
    /// replies one level deep, and the edits and deletes of what it removes
    /// (RFC §14.1 `delete_event(room_id, event_id, cascade_replies)`,
    /// §10.3): a hard delete, which leaves the room's counters as they are.
    /// Answer [`StoreOutcome::Deleted`] with the events removed, in §10.3's
    /// order: the target, its replies by index, then the changes by index;
    /// none when the room holds no such event.
    DeleteEvent {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
        /// Whether the events whose `parent_event_id` names it go with it.
        cascade_replies: bool,
    },
    /// Find the identity an address belongs to on channels of a type, in an
    /// organization (RFC §14.1 `resolve_identity(channel_type, address,
    /// organization_id)`, §17.2). Answer [`StoreOutcome::Resolved`].
    ResolveIdentity {
        /// The channel type the address is used on.
        channel_type: ChannelType,
        /// The address, in its channel's form.
        address: String,
        /// The organization the caller acts for; `None` for the identities
        /// of no organization (gap G-060).
        organization_id: Option<String>,
    },
}

/// A call of a channel (RFC §6.1), answered by a [`ChannelOutcome`].
#[derive(Clone, Debug, PartialEq)]
pub enum ChannelRequest {
    /// Call the channel's `handle_inbound()` (RFC §6.1). Answer
    /// [`ChannelOutcome::Parsed`].
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
    /// channel this is a turn (RFC §10.6). Answer [`ChannelOutcome::Output`],
    /// [`ChannelOutcome::ProviderDelivery`] for a provider's refusal, or
    /// [`Outcome::Failed`] with the error type [`CHANNEL_NOT_REGISTERED`](super::CHANNEL_NOT_REGISTERED)
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
    /// [`ChannelOutcome::Output`], [`ChannelOutcome::ProviderDelivery`] for a
    /// provider's refusal, or [`Outcome::Failed`] with the error type
    /// [`CHANNEL_NOT_REGISTERED`](super::CHANNEL_NOT_REGISTERED) for a channel the driver no longer holds.
    Deliver {
        /// The channel's binding.
        binding: ChannelBinding,
        /// The event, transcoded for the channel.
        event: RoomEvent,
        /// The room's context, as the channel may see it.
        context: Box<RoomContext>,
    },
    /// Read the stream of a turn's response to its end, or until its reader
    /// stops (RFC §6.1 `response_stream`, §12.2 steps 13s and 14s): the
    /// turn whose `OnEvent` was answered [`ChannelOutcome::Streamed`].
    ///
    /// The reader is `target`'s `deliver_stream()`, handed the text as it
    /// is written and each row once the store holds it, or, with no
    /// target, the driver itself. Each row the stream ends (a text segment,
    /// a call's start, a call's end) is handed to the operation as an
    /// [`Entry::Row`](super::Entry::Row) of this effect, which commits it
    /// as a response to `answers` and settles it with the stored row,
    /// before the reader is handed it. Nothing reaches a reader that the
    /// store does not hold.
    ///
    /// Answer [`ChannelOutcome::Output`] once the stream is over: the
    /// target's output, its `error` the response's failure, or the
    /// target's own (RFC §6.1 `ChannelOutput.error`).
    ReadStream {
        /// The turn whose response it is.
        turn: EffectId,
        /// The intelligence channel whose response it is.
        responder: ChannelBinding,
        /// The event its response answers, as the turn was handed it.
        answers: Box<RoomEvent>,
        /// The channel that reads the stream as it is written, if one does:
        /// one per response.
        target: Option<StreamTarget>,
    },
}

/// The channel that reads a turn's response as it is written (RFC §6.1
/// `supports_streaming_delivery`, `deliver_stream`): a transport the
/// response's rows reach, which declares it streams.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamTarget {
    /// The channel's binding.
    pub binding: ChannelBinding,
    /// The room's context, as the channel may see it (RFC §7.5 rule 8).
    pub context: Box<RoomContext>,
    /// Whether it is handed the tool-call rows (`ChannelDeclaration::receives_tool_calls`).
    pub tool_rows: bool,
}

/// The run of hooks (RFC §9.3), answered by a [`HookAnswer`].
#[derive(Clone, Debug, PartialEq)]
pub enum HookRequest {
    /// Run one SYNC hook on `event`, within `timeout` (RFC §9.3). Answer
    /// [`HookAnswer::Ran`] with what the hook produced, `TimedOut` past the
    /// timeout.
    RunHook {
        /// The hook.
        hook: HookId,
        /// The trigger it runs on.
        trigger: HookTrigger,
        /// The payload, as the previous hook left it.
        event: Box<RoomEvent>,
        /// The room's context, whole (RFC §7.5 rule 8).
        context: Box<RoomContext>,
        /// The hook's timeout.
        timeout: Duration,
    },
    /// Run these hooks concurrently as observers of `event`, each within
    /// its timeout, and answer once every one has finished (gap G-050):
    /// what they return is discarded and what they raise is logged, as for
    /// [`Notice::AsyncHooks`](super::Notice::AsyncHooks). Answer [`HookAnswer::Observed`].
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

/// The driver's answer to an effect, of its request's family, or one of
/// the answers every request may get.
#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    /// A hold's answer.
    Hold(HoldOutcome),
    /// The store's answer.
    Store(StoreOutcome),
    /// A channel's answer.
    Channel(ChannelOutcome),
    /// The hooks' answer.
    Hook(HookAnswer),
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

impl From<HoldOutcome> for Outcome {
    fn from(outcome: HoldOutcome) -> Self {
        Self::Hold(outcome)
    }
}

impl From<StoreOutcome> for Outcome {
    fn from(outcome: StoreOutcome) -> Self {
        Self::Store(outcome)
    }
}

impl From<ChannelOutcome> for Outcome {
    fn from(outcome: ChannelOutcome) -> Self {
        Self::Channel(outcome)
    }
}

impl From<HookAnswer> for Outcome {
    fn from(outcome: HookAnswer) -> Self {
        Self::Hook(outcome)
    }
}

/// The driver's answer to a [`HoldRequest`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoldOutcome {
    /// The room's lock is held.
    Locked,
    /// The room's lock is released.
    Released,
    /// The event's place in the room's delivery lane is taken.
    Entered,
    /// The room's delivery lane takes no more place, nor gives a place its
    /// turn, the kit closing (RFC §10.1 step 18, §12.10.4): the event is
    /// not delivered, its cascade cancelled
    /// [`KIT_CLOSED`](super::KIT_CLOSED); a set that held a place leaves it.
    LaneClosed,
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
}

/// The driver's answer to a [`StoreRequest`].
#[derive(Clone, Debug, PartialEq)]
pub enum StoreOutcome {
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
    /// The room's timeline, in index order.
    Events(Vec<RoomEvent>),
    /// The commit is durable.
    Committed,
    /// The side effects are stored.
    Persisted,
    /// The record is stored.
    Stored,
    /// The identity the address belongs to, if any.
    Resolved(Option<Identity>),
}

/// The driver's answer to a [`ChannelRequest`].
#[derive(Clone, Debug, PartialEq)]
pub enum ChannelOutcome {
    /// The event the channel parsed from the message.
    Parsed(Box<RoomEvent>),
    /// What the channel returned.
    Output(Box<ChannelOutput>),
    /// What a turn returned, the stream of its response following (RFC
    /// §6.1 `response_stream`): its rows are what reading the stream
    /// stores ([`ChannelRequest::ReadStream`]), and the output's own
    /// `response_events` are none.
    Streamed(Box<ChannelOutput>),
    /// The call failed with the provider's explicit refusal, the Python
    /// implementation's `ProviderDeliveryError(result)`: its delivery result
    /// reads the code and retryability the refusal declares, and keeps it
    /// (RFC §5.13). An answer of [`ChannelRequest::OnEvent`] and
    /// [`ChannelRequest::Deliver`].
    ProviderDelivery(Box<ProviderResult>),
}

/// The driver's answer to a [`HookRequest`].
#[derive(Clone, Debug, PartialEq)]
pub enum HookAnswer {
    /// What the hook produced.
    Ran(Box<HookOutcome<RoomEvent>>),
    /// Every observer has finished.
    Observed,
}
