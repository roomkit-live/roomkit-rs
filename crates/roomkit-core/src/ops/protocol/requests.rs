//! What an operation asks of its driver, by family ([`Request`]), and what
//! the driver answers, by the same families ([`Outcome`]).

use std::time::Duration;

use serde_json::Value;

use super::EffectId;
use crate::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, DeliveryError, ErrorRecord, EventId,
    EventUpdates, Identity, IdentityHookResult, IdentityId, IdentityResult, InboundMessage,
    Metadata, Observation, ObservationId, Participant, ProviderResult, Room, RoomContext,
    RoomEvent, RoomId, RoomStatus, RoomUpdates, Task, TaskId, Timestamp, Verification,
};
use crate::ops::RecentEvents;
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
    /// The discussion holding a room (RFC §19.7.5), told under the room's
    /// lock. Answer [`Outcome::Discussion`].
    Discussion(DiscussionRequest),
    /// Identity resolution (RFC §10.1 step 5, §11): the kit's resolver,
    /// and its identity hooks.
    Identity(IdentityRequest),
}

impl From<DiscussionRequest> for Request {
    fn from(request: DiscussionRequest) -> Self {
        Self::Discussion(request)
    }
}

impl From<IdentityRequest> for Request {
    fn from(request: IdentityRequest) -> Self {
        Self::Identity(request)
    }
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

/// What the pipeline asks of the discussion holding a room (RFC §19.7.5),
/// under the room's lock, so that the queue moves in the order of the
/// room's commits. A room whose discussion this process does not run is
/// answered [`DiscussionOutcome::Done`], or no turn.
#[derive(Clone, Debug, PartialEq)]
pub enum DiscussionRequest {
    /// A message was committed and took its place in the room's lane: queue
    /// whom it asks for (rule 8). What goes wrong here is logged: an event
    /// committed is delivered.
    Committed {
        /// The room.
        room_id: RoomId,
        /// The message, committed.
        event: Box<RoomEvent>,
        /// The channels its delivery set reaches (§10.1 step 12).
        reach: Vec<ChannelId>,
        /// The room's context at the commit.
        context: Box<RoomContext>,
    },
    /// An instruction addressed to agents of the discussion: a turn of its
    /// own each, in place of its delivery (rule 7).
    Instruction {
        /// The room.
        room_id: RoomId,
        /// The instruction, never stored.
        event: Box<RoomEvent>,
    },
    /// The turn to give now, if any, its queue edited under the lock.
    Turn {
        /// The room.
        room_id: RoomId,
        /// The room's context, with its history, read under the lock.
        context: Box<RoomContext>,
    },
}

/// The turn a discussion gives: the rerun of the event it answers, for one
/// agent alone (RFC §19.7.5 rules 9 and 11).
#[derive(Clone, Debug, PartialEq)]
pub struct DiscussionTurn {
    /// The agent whose turn it is.
    pub agent: ChannelId,
    /// The event the turn answers, at the depth it was asked at.
    pub answered: Box<RoomEvent>,
    /// The mark its trigger carries, under `rules::discussion::TURN_KEY`.
    pub mark: Value,
    /// The discussion's own chain-depth limit.
    pub max_depth: u32,
}

/// A turn the depth limit stopped: recorded once, in place of the answer it
/// would have made, its agent kept queued (RFC §8.3, §19.7.5 rule 11).
#[derive(Clone, Debug, PartialEq)]
pub struct StoppedTurn {
    /// The agent not asked.
    pub agent: ChannelId,
    /// The event it would have answered.
    pub answered: Box<RoomEvent>,
    /// The discussion's own chain-depth limit.
    pub max_depth: u32,
}

/// What a discussion gives now: at most one turn, and the turns its depth
/// limit stopped on the way.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NextTurn {
    /// The turn to give, `None` for none.
    pub turn: Option<Box<DiscussionTurn>>,
    /// The turns stopped: their records are committed before the turn runs.
    pub stopped: Vec<StoppedTurn>,
}

/// A discussion's answer to a [`DiscussionRequest`].
#[derive(Clone, Debug, PartialEq)]
pub enum DiscussionOutcome {
    /// Told; nothing to answer.
    Done,
    /// What it gives now.
    Turn(NextTurn),
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
    /// Take the room's lock to record a delivery set on its event (RFC
    /// §14.3), or to write the room metadata updates its targets returned
    /// (RFC §10.1 step 15), work past that event's commit: the kit's close
    /// never cuts a call for this wait as it cuts one waiting before its
    /// commit, and past its grace answers it failed. Answer
    /// [`HoldOutcome::Locked`].
    AcquireLockForRecord {
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
    /// Wait for nothing until the effect's deadline, then answer
    /// [`Outcome::DeadlinePassed`]: the deadline of an acquire of the room's
    /// lock the operation holds back behind another of its sections, which
    /// the driver never sees otherwise (RFC §13.6). Issued with a deadline
    /// only; the operation may stop caring for it before it passes.
    Expiry {
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
        /// How many of the latest events to read, which the room's
        /// bindings, read with it, settle.
        recent_events: RecentEvents,
    },
    /// Read whether the room holds an idempotency key, then the event it
    /// committed with it, a `BLOCKED` one included (RFC §14.1
    /// `check_idempotency`, `get_event_by_idempotency_key`, §13.4): the
    /// one `channel_id` committed, or for `None` the one any channel of
    /// the room committed (§22.1). Answer [`StoreOutcome::Idempotent`], or
    /// [`StoreOutcome::KeySeen`] for a key the store holds and whose event
    /// it cannot read back.
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
    /// Commit `event` as one atomic write (RFC §10.1 step 12, §14.3,
    /// Python's `commit_event`): the store gives it its index, the room's
    /// count of indices given out, bumps the room's counters, writes the
    /// change's `target` in the same transaction, failing the whole commit
    /// when the target is gone, and, for the room's `activity`, moves its
    /// last activity and resumes it when paused (RFC §5.1). The register
    /// of authors the event's record changed is written first, as the
    /// Python implementation writes it: a commit that fails after it leaves
    /// a rank unused, never one held twice (gap G-089). Answer
    /// [`StoreOutcome::Committed`] with the event as stored.
    Commit {
        /// The event, with its status and the index its room's count gave
        /// it under the lock; the index the store gives it is the one it
        /// takes.
        event: Box<RoomEvent>,
        /// An accepted edit's or delete's target, as the change leaves it.
        target: Option<Box<RoomEvent>>,
        /// The instant of the room's activity the commit is: an event the
        /// pipeline stores delivered. `None` for any other commit, which
        /// moves the counters only (gap G-071).
        activity: Option<Timestamp>,
        /// The room's register of authors as the event's record left it,
        /// when the record changed it (RFC §14.1 `set_private_state` under
        /// `author_register`).
        author_register: Option<Metadata>,
    },
    /// Store side effects (RFC §10.1 step 15), each written on its own:
    /// one the store fails to write leaves the others to write, as
    /// Python's `_persist_side_effects` does. Answer
    /// [`StoreOutcome::Persisted`], naming those not stored.
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
    /// Store a participant of a room (RFC §14.1 `add_participant`). Answer
    /// [`StoreOutcome::Stored`].
    AddParticipant {
        /// The participant, its room named.
        participant: Box<Participant>,
    },
    /// Write a participant of a room whole (RFC §14.1
    /// `update_participant`). Answer [`StoreOutcome::Stored`].
    UpdateParticipant {
        /// The participant, its room named.
        participant: Box<Participant>,
    },
    /// Write `results` as the event's `delivery_results`, nothing else of
    /// it, under the room's lock (RFC §14.1 `record_delivery_results`,
    /// §14.3): a delivery set with a failure. Answer
    /// [`StoreOutcome::Stored`].
    RecordDeliveryResults {
        /// The room.
        room_id: RoomId,
        /// The event.
        event_id: EventId,
        /// Each target's outcome, in the serialized form, by its channel.
        results: Metadata,
    },
    /// Read a page of the events the room received on `channel_id`, in index
    /// order, the events stored `BLOCKED` left out (RFC §14.1 `list_events`,
    /// §10.4): what step 2 reads of who the room heard, up to the first
    /// other sender. Answer [`StoreOutcome::Events`].
    ListEventsPage {
        /// The room.
        room_id: RoomId,
        /// The channel the events came from.
        channel_id: ChannelId,
        /// How many of them the page skips.
        offset: u64,
        /// How many it holds at most.
        limit: u64,
    },
    /// Write what an event the kit's persistence policy keeps out of the
    /// timeline changes, with no row of it and the room's counters unmoved
    /// (RFC §14.3): the `target` of an edit or a delete whole, a delete's
    /// target written deleted marking its edits deleted with it (RFC §10.3,
    /// Python's `write_change_target`), then the room's register of authors
    /// as the event's author record left it (RFC §10.1 step 12, Python's
    /// `set_private_state` under `author_register`). Answer
    /// [`StoreOutcome::Stored`], or [`Outcome::Failed`] when the room holds
    /// no such target.
    WriteUnstored {
        /// The room.
        room_id: RoomId,
        /// An accepted edit's or delete's target, as the change leaves it.
        target: Option<Box<RoomEvent>>,
        /// The room's register of authors, when the event's record changed
        /// it.
        author_register: Option<Metadata>,
    },
    /// Conditionally write a room whose timer transition was computed from
    /// these fields. Answer `RoomUpdated(None)` if a concurrent commit moved
    /// them or removed the room; otherwise return the room actually stored,
    /// including its preserved counters (RFC §5.1, §14.1 `update_room_if`).
    UpdateRoomIf {
        /// The room after every due transition.
        room: Box<Room>,
        /// The status the transition read.
        status: RoomStatus,
        /// The pause the transition read.
        paused_at: Option<Timestamp>,
        /// The activity the transition read.
        last_activity_at: Option<Timestamp>,
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
    /// target's own (RFC §6.1 `ChannelOutput.error`); an `error` marked
    /// reported (`ErrorRecord::marked_reported`) was reported to `ON_ERROR`
    /// and logged by the reader, and is only handed on to the caller.
    /// Answer [`ChannelOutcome::Raised`] when the turn raised once its
    /// stream was read, what follows a strategy's first pass (RFC §19.7.3):
    /// the turn failed as an `OnEvent` that raised, never its stream.
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
        /// The response's own id, which each of its rows carries, as the
        /// payload of `ON_ERROR` for its failure does (RFC §9.2): the
        /// Python implementation's `correlation_id`, one per read.
        correlation_id: String,
        /// Whether its reader commits the turn as a delegated task's trace,
        /// crossing no hook (RFC §23.3 step 5): only such a turn's answer
        /// text is published (RFC §6.4).
        as_trace: bool,
    },
    /// Ask which of `channel_ids`, the transports a response's rows reach,
    /// take a streamed response in `room_id` (RFC §6.1
    /// `supports_streaming_delivery_for`): a channel may hold streaming
    /// clients for one room and none for another. Answer
    /// [`ChannelOutcome::StreamsIn`] with those that do.
    StreamsIn {
        /// The room the response is delivered in.
        room_id: RoomId,
        /// The channels asked, in the order the response's rows reach them.
        channel_ids: Vec<ChannelId>,
    },
    /// Call the channel's `on_room_attached()` (RFC §6.1) with the binding
    /// an attach stored, under the room's lock, before the attach is
    /// recorded or announced; an intelligence channel's runs on its
    /// executor, which logs what it raises (RFC §10.6.3). Answer
    /// [`ChannelOutcome::Told`], also for a channel the driver no longer
    /// holds, or [`Outcome::Failed`] with what the channel raised, which
    /// refuses the attach.
    RoomAttached {
        /// The binding the attach stored.
        binding: ChannelBinding,
    },
    /// Call the channel's `on_room_detached()` (RFC §6.1) once a detach
    /// removed its binding and recorded it, under the room's lock, before
    /// the detach is announced; an intelligence channel's runs on its
    /// executor, which logs what it raises (RFC §10.6.3). Answer
    /// [`ChannelOutcome::Told`], also for a channel the driver no longer
    /// holds, or [`Outcome::Failed`] with what the channel raised, which the
    /// detach returns once announced.
    RoomDetached {
        /// The binding the detach removed.
        binding: ChannelBinding,
    },
    /// Ask the channel to write `binding`'s addresses in its own form, as an
    /// attach does (RFC §5.7, §10.4): its recipient by `recipient_address`
    /// and `recipient_metadata`, its correspondent by `normalize_address`,
    /// a binding naming no one taking its recipient. Answer
    /// [`ChannelOutcome::Named`], the binding as given for a channel the
    /// driver no longer holds.
    NamedCorrespondent {
        /// The binding, as the update leaves it.
        binding: ChannelBinding,
    },
    /// Read who `binding` delivers to as routing reads it (RFC §5.7, §10.4,
    /// Python's `routed_recipient`): the channel's `recipient_address`, else
    /// the value under its recipient key in its form. Answer
    /// [`ChannelOutcome::Recipient`], `None` for a channel the driver no
    /// longer holds, whose declared key the core still reads.
    RoutedRecipient {
        /// The binding, as the lock finds it.
        binding: ChannelBinding,
    },
}

/// The channel that reads a turn's response as it is written (RFC §6.1
/// `supports_streaming_delivery_for`, `deliver_stream`): a transport the
/// response's rows reach, which says it streams in the room.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamTarget {
    /// The channel's binding.
    pub binding: ChannelBinding,
    /// The room's context, as the channel may see it (RFC §7.5 rule 8).
    pub context: Box<RoomContext>,
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

/// What identity resolution asks of the driver (RFC §10.1 step 5, §11),
/// answered by an [`IdentityAnswer`].
#[derive(Clone, Debug, PartialEq)]
pub enum IdentityRequest {
    /// Ask the kit's identity resolver who sent `message` (RFC §11.1), with
    /// the room's context, within `timeout` (RFC §11.5). Answer
    /// [`IdentityAnswer::Resolved`], [`IdentityAnswer::TimedOut`] past the
    /// timeout, or [`Outcome::Failed`] for a resolver that raised.
    Resolve {
        /// The message, its sender in its channel's form.
        message: Box<InboundMessage>,
        /// The room's context, whole.
        context: Box<RoomContext>,
        /// How long the resolver may take.
        timeout: Duration,
    },
    /// Run the kit's identity hooks of `trigger` whose filters admit
    /// `event`, in registration order, until one answers (RFC §11.2): one
    /// that raises is logged and the next one runs. Answer
    /// [`IdentityAnswer::Hooked`] with the first answer, `None` when none
    /// answered.
    RunHooks {
        /// `ON_IDENTITY_AMBIGUOUS` or `ON_IDENTITY_UNKNOWN`.
        trigger: HookTrigger,
        /// The event the message parsed to.
        event: Box<RoomEvent>,
        /// The room's context, whole.
        context: Box<RoomContext>,
        /// What the resolver said of the sender, its address and channel
        /// type filled in.
        result: Box<IdentityResult>,
    },
    /// Read the verification of `identity_id` in `organization_id` as it
    /// stands now (RFC §10.1 step 5b, §11.7.1): the kit's
    /// `verification_of`, UNVERIFIED for an identity with none. Answer
    /// [`IdentityAnswer::Verification`].
    Verification {
        /// The sender's identity.
        identity_id: IdentityId,
        /// The room's organization (RFC §11.7.7).
        organization_id: Option<String>,
    },
    /// Attempt `code`, a message's whole text, against the request
    /// `request_id` when it is an open code request of `room_id` (RFC
    /// §11.7.4): the kit's `complete_verification`, a refusal (a wrong code,
    /// counted) kept from the message. Answer [`IdentityAnswer::CodeTried`]
    /// with whether the message replied to the request, attempted; no
    /// deadline bounds it, as none bounds a write (RFC §13.6).
    TryCode {
        /// The identity's open request.
        request_id: String,
        /// The message's room.
        room_id: RoomId,
        /// The code the message replies with.
        code: String,
    },
}

/// The driver's answer to an [`IdentityRequest`].
#[derive(Clone, Debug, PartialEq)]
pub enum IdentityAnswer {
    /// What the resolver said of the sender.
    Resolved(Box<IdentityResult>),
    /// The resolver did not answer within its timeout (RFC §11.5).
    TimedOut,
    /// The first identity hook's answer, `None` when none answered.
    Hooked(Option<Box<IdentityHookResult>>),
    /// The sender's verification as it stands (RFC §11.7.1).
    Verification(Box<Verification>),
    /// Whether the message replied to the open code request, attempted
    /// (RFC §11.7.4).
    CodeTried(bool),
}

/// The driver's answer to an effect, of its request's family, or one of
/// the answers every request may get.
#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "an outcome is moved once, from the driver to the operation that asked: the store's carries its record whole"
)]
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
    /// Identity resolution's answer.
    Identity(IdentityAnswer),
    /// A discussion's answer.
    Discussion(DiscussionOutcome),
}

impl From<DiscussionOutcome> for Outcome {
    fn from(outcome: DiscussionOutcome) -> Self {
        Self::Discussion(outcome)
    }
}

impl From<IdentityAnswer> for Outcome {
    fn from(outcome: IdentityAnswer) -> Self {
        Self::Identity(outcome)
    }
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
    /// The room's delivery cursor passed the event's index before its set
    /// ran, another process's lane having skipped it as a hole (RFC §13.5
    /// "Delivery lanes across processes"): the set is dropped, since
    /// running it then would break the room's order, its cascade cancelled
    /// `delivery_stale`, as Python's lane drops it; it leaves its place.
    LaneSkipped,
}

/// The driver's answer to a [`StoreRequest`].
#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "an outcome is moved once, from the driver to the operation that asked: a record is carried whole"
)]
pub enum StoreOutcome {
    /// The room's context.
    Context(Box<RoomContext>),
    /// No room has the requested id.
    RoomNotFound,
    /// The event committed with the key, if any.
    Idempotent(Option<Box<RoomEvent>>),
    /// The store holds the key, and cannot read back the event it
    /// committed (RFC §13.4: a duplicate "MAY return a blocked result"),
    /// as Python's store answers `check_idempotency` true and
    /// `get_event_by_idempotency_key` `None`.
    KeySeen,
    /// The event read, if any.
    Event(Option<Box<RoomEvent>>),
    /// The events a delete removed, the target first.
    Deleted(Vec<RoomEvent>),
    /// The room's timeline, in index order.
    Events(Vec<RoomEvent>),
    /// The commit is durable: the event as the store holds it, its index
    /// the store's.
    Committed(Box<RoomEvent>),
    /// The side effects are stored, but those named, whose write failed.
    Persisted(Vec<Unpersisted>),
    /// The record is stored.
    Stored,
    /// The identity the address belongs to, if any.
    Resolved(Option<Identity>),
    /// A conditional room write: the stored room, or none when its condition missed.
    RoomUpdated(Option<Box<Room>>),
}

/// A side effect of a [`StoreRequest::PersistSideEffects`] the store did
/// not write, with the store's error.
#[derive(Clone, Debug, PartialEq)]
pub enum Unpersisted {
    /// A task.
    Task {
        /// The task's id.
        task_id: TaskId,
        /// Why the store did not write it.
        error: ErrorRecord,
    },
    /// An observation.
    Observation {
        /// The observation's id.
        observation_id: ObservationId,
        /// Why the store did not write it.
        error: ErrorRecord,
    },
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
    /// The channels of a [`ChannelRequest::StreamsIn`] that take a
    /// streamed response in its room.
    StreamsIn(Vec<ChannelId>),
    /// The call raised an error that declares how its delivery failed: its
    /// code and whether a retry may succeed, as Python's `DeliveryResult`
    /// reads an exception's `code` and `retryable` (RFC §5.13), an agent's
    /// error rebuilt across the turn backend. An answer of
    /// [`ChannelRequest::OnEvent`], and of [`ChannelRequest::ReadStream`]
    /// for a turn that raised once its stream was read.
    Raised(Box<Raised>),
    /// The channel was told of its room's attach or detach, or holds
    /// nothing to be told: an answer of [`ChannelRequest::RoomAttached`] and
    /// [`ChannelRequest::RoomDetached`].
    Told,
    /// The binding with its addresses in the channel's form: the answer of
    /// [`ChannelRequest::NamedCorrespondent`].
    Named(Box<ChannelBinding>),
    /// Who the binding delivers to, if anyone: the answer of
    /// [`ChannelRequest::RoutedRecipient`].
    Recipient(Option<String>),
}

/// An error a call raised that declares its delivery error
/// ([`ChannelOutcome::Raised`]).
#[derive(Clone, Debug, PartialEq)]
pub struct Raised {
    /// The error, as the hooks and the framework events report it.
    pub error: ErrorRecord,
    /// The failed delivery it declares: its code, its words, whether a
    /// retry may succeed.
    pub delivery: DeliveryError,
}

/// The driver's answer to a [`HookRequest`].
#[derive(Clone, Debug, PartialEq)]
pub enum HookAnswer {
    /// What the hook produced.
    Ran(Box<HookOutcome<RoomEvent>>),
    /// Every observer has finished.
    Observed,
}
