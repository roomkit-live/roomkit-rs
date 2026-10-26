//! A driver for the trace tests: rooms in memory, channels and hooks as
//! closures, and a trace of every effect it executes.
//!
//! It also checks what every driver must hold for the protocol: a commit
//! happens under its room's lock, an operation never asks for a lock it
//! holds, and every lock taken is let go when the operation ends. An event
//! takes its place in the room's delivery lane under the room's lock, a
//! channel is called for it only in its place's turn, and every place taken
//! is left when the operation ends (RFC §10.2). The lock is free while
//! awaited observers run, so that they may take it.
//!
//! An admission ticket (RFC §9.5.1) is taken before the room lock, its turn
//! awaited before the lock is taken, and it is released once the lock is
//! let go, on every path: a ticket left held when the operation ends fails
//! the test, as a lock or a place does.
//!
//! Time is simulated: an effect named in `durations` moves the clock as it
//! runs. An effect that carries a deadline (RFC §13.6) and would finish
//! past it does nothing: the clock moves to the deadline, the trace reads
//! `<effect> deadline passed`, and the driver answers `DeadlinePassed`.
//!
//! The router is scripted: each `Route` takes the next answer of `routes`,
//! a new room once they are spent.
//!
//! A hook, a channel's turn or a parse that then fails may call into a room
//! (RFC §13.5): it is scripted as a [`Reentry`], the entries it makes from
//! the event it runs on, or parses, and what it answers. The driver hands a call to the run of its context that holds
//! the room's lane (`Operation::adopt`); a call no run takes, or one its
//! run declines, runs as an operation of its own, an arrival, within the
//! caller's context (`runs`). Either way it keeps the hook's or the
//! channel's effect outstanding, executes the next effects, and completes
//! the effect once the entry is settled or the arrival is over, out of
//! order; a caller that does not wait answers at once. The trace reads
//! `<effect> calls <entry>` where the call is made, `<effect> settled` where
//! its entry is settled, and `<effect> returns` where the effect completes;
//! an arrival's effects are traced as any. Every entry must be settled
//! before the operation ends. A request made under a room's lock
//! (`Effect::locked`) is checked to be made while the room's lock is held.

mod runs;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{
    Access, ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelMediaType, ChannelOutput, ChannelType, DirectEvent, ErrorRecord, EventContent,
    EventDirection, EventId, EventSource, EventStatus, EventType, EventUpdates, HookResult,
    Identity, InboundMessage, Metadata, Observation, Participant, ParticipantId, Room, RoomContext,
    RoomEvent, RoomId, RoomStatus, RoomUpdates, Task, Timestamp,
};
use roomkit_core::ops::{
    ChannelDeclaration, ChannelOutcome, ChannelRequest, CommitRecord, Decided, DeleteEvent, Effect,
    EffectId, Entry, Environment, FrameworkEvent, HoldOutcome, HoldRequest, HookAnswer,
    HookRequest, Inbound, InboundError, InboundResult, InboundTarget, KitConfig, MutationError,
    Notice, Outcome, Request, RoomCounters, SequentialEnvironment, Settled, StoreOutcome,
    StoreRequest, TargetUpdate, UpdateEvent, default_inbound_event,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookId, HookSpec, HookTrigger};
use roomkit_core::rules::routing::RouteQuery;
use roomkit_core::rules::transcoding::text_of;
use roomkit_core::rules::{attachment, edits};

/// What a channel answers to `on_event()`.
pub(crate) type OnEvent =
    Box<dyn FnMut(&RoomEvent, &RoomContext) -> Result<ChannelOutput, ErrorRecord>>;
/// What a channel answers to `deliver()`.
pub(crate) type Deliver = Box<dyn FnMut(&RoomEvent) -> Result<ChannelOutput, ErrorRecord>>;
/// What a SYNC hook produces.
pub(crate) type Hook = Box<dyn FnMut(&RoomEvent) -> HookOutcome<RoomEvent>>;

/// What a call into the room returned: its entry's result.
pub(crate) type Called = Result<InboundResult, InboundError>;

/// The calls a hook or a channel makes into its own room while it runs (RFC
/// §13.5): the entries it makes from the event it runs on, none for an
/// event it runs on as it would without calling, and what it answers.
pub(crate) struct Reentry<T> {
    pub(crate) entries: MakeEntries,
    pub(crate) answer: Answer<T>,
}

/// How a caller makes its entries from the event it runs on.
pub(crate) type MakeEntries = Box<dyn FnMut(&RoomEvent) -> Vec<Entry>>;

/// A caller that makes at most one entry, as `call` says.
pub(crate) fn calling(mut call: impl FnMut(&RoomEvent) -> Option<Entry> + 'static) -> MakeEntries {
    Box::new(move |event| call(event).into_iter().collect())
}

/// What a caller answers once it made its call.
pub(crate) enum Answer<T> {
    /// It awaits the call's result and answers from it.
    Awaits(Box<dyn FnMut(&Called) -> T>),
    /// It does not wait for the result, and answers this at once.
    AtOnce(Box<dyn FnMut() -> T>),
    /// It makes the call from a task of its own, outside its execution
    /// context, as code that spawns a task without `keep_context` does, and
    /// answers this at once.
    Apart(Box<dyn FnMut() -> T>),
}

/// Who made a call into the room.
#[derive(Clone, Debug)]
enum Caller {
    Hook(HookId),
    Channel(ChannelId),
    /// A channel's parse, which fails once it made its call.
    Parse(ChannelId),
}

/// How a caller goes on once it made its calls.
enum Then {
    /// It waits for its call's result.
    Waits,
    /// It answers this at once, its call made within its context.
    AtOnce(Outcome),
    /// It answers this at once, its call made outside its context.
    Apart(Outcome),
}

/// A change the hooks make in the room while they run.
pub(crate) enum HookChange {
    /// They mute the channel, as a moderation hook would.
    Mute(ChannelId),
    /// They close the room, as a hook that ends the conversation would.
    CloseRoom,
    /// They close the room when they run on the event of this text.
    CloseRoomOn(&'static str),
    /// They detach the channel when they run on the event of this text.
    DetachOn(&'static str, ChannelId),
}

struct Channel {
    on_event: OnEvent,
    deliver: Deliver,
}

/// A room of the store other than `room_1`.
struct StoredRoom {
    room: Room,
    exists: bool,
    bindings: Vec<ChannelBinding>,
    participants: Vec<Participant>,
    timeline: Vec<RoomEvent>,
    author_register: Option<Metadata>,
}

impl StoredRoom {
    /// The record of a room the store does not hold.
    fn missing(room_id: RoomId) -> Self {
        Self {
            room: Room::new(room_id, Timestamp::UNIX_EPOCH),
            exists: false,
            bindings: Vec::new(),
            participants: Vec::new(),
            timeline: Vec::new(),
            author_register: None,
        }
    }
}

/// The rooms, their channels and hooks, and what the driver did.
///
/// The room the tests act on, `room_1`, is held in the fields below; the
/// store keeps any other room apart and answers for each alike.
pub(crate) struct Sim {
    pub(crate) env: SequentialEnvironment,
    pub(crate) config: KitConfig,
    pub(crate) room: Room,
    pub(crate) bindings: Vec<ChannelBinding>,
    pub(crate) participants: Vec<Participant>,
    pub(crate) timeline: Vec<RoomEvent>,
    /// The room's register of authors, its private state (RFC §14.1).
    pub(crate) author_register: Option<Metadata>,
    pub(crate) tasks: Vec<Task>,
    pub(crate) observations: Vec<Observation>,
    /// Every effect executed, in order.
    pub(crate) trace: Vec<String>,
    /// The effects executed and the notices performed, in the order the
    /// driver met them: a notice reads `emit <framework event>` or
    /// `fire <trigger>`.
    pub(crate) journal: Vec<String>,
    pub(crate) notices: Vec<Notice>,
    /// Each trigger whose hooks an operation fired, with whether its run
    /// held the room's lock as it did.
    pub(crate) fired: Vec<(HookTrigger, bool)>,
    channels: BTreeMap<ChannelId, Channel>,
    hooks: BTreeMap<HookId, Hook>,
    /// The channel each awaited observer attaches to its room as it runs.
    observer_attaches: BTreeMap<HookId, ChannelId>,
    others: BTreeMap<RoomId, StoredRoom>,
    /// The operation the test runs, then the arrivals its calls make.
    runs: Vec<runs::RunState>,
    /// The run whose effects the driver executes now.
    current: runs::RunId,
    /// The arrivals started since the driver last polled a run.
    spawned: Vec<Inbound>,
    /// The locks, places and tickets the runs hold, and the requests that
    /// wait for them.
    holders: runs::Holders,
    /// The place another call holds in `room_1`'s lane, by its event, ahead
    /// of every place the test's operation takes, until that operation
    /// publishes its decision (`hold_a_place_ahead`).
    place_ahead: Option<EventId>,
    /// The events whose place in the lane could not be taken, or whose turn
    /// could not be awaited, delivered out of it.
    placeless: BTreeSet<EventId>,
    /// How long each effect takes on the simulated clock, named by the start
    /// of its trace line (`hook pii_scan`, `lock room_1`, `await ticket
    /// room_1`, `handle_inbound sms`); the first match counts.
    pub(crate) durations: Vec<(&'static str, Duration)>,
    /// How long the nth effect named by a start of trace line takes, the
    /// others taking no time: `(start, n, duration)`, `n` counted from 1.
    pub(crate) nth_durations: Vec<(&'static str, usize, Duration)>,
    /// The trace line of every effect that carried a deadline, in order.
    pub(crate) bounded: Vec<String>,
    /// The trace line of every effect made in a room's admission order
    /// (`Effect::admitted`), with the room, in order: `hook scanner in
    /// room_1`.
    pub(crate) in_admission_order: Vec<String>,
    /// The label of every effect made under a room's lock
    /// (`Effect::locked`), with the room, in order: `commit under room_1`.
    pub(crate) under_lock: Vec<String>,
    /// The start of the label of the effect before which the call's caller
    /// stops waiting, until it does: the call is expired then
    /// (`Operation::expire`), the trace reads `caller gone`, and the effects
    /// it gives up on are dropped, never answered.
    pub(crate) leaves_before: Option<&'static str>,
    /// The contexts the SYNC hooks ran with, by hook name, in order.
    pub(crate) hook_contexts: Vec<(String, RoomContext)>,
    /// The identities the store resolves addresses to (RFC §14.1).
    pub(crate) identities: Vec<Identity>,
    /// The router's answers, in order: a room, none, or a failure. Once
    /// they are spent, the router finds no room.
    pub(crate) routes: VecDeque<Result<Option<RoomId>, ErrorRecord>>,
    /// What the router was asked, in order.
    pub(crate) route_queries: Vec<RouteQuery>,
    /// A sender another message has recorded on a room's binding of a
    /// channel by the time the room's lock is first taken: `(room, channel,
    /// sender)`.
    pub(crate) recorded_meanwhile: Option<(RoomId, ChannelId, ParticipantId)>,
    /// Whether the store finds `room_1`.
    pub(crate) room_exists: bool,
    /// What the hooks change in the room while they run.
    pub(crate) hooks_change: Option<HookChange>,
    /// The contexts the receiving channels parsed messages with.
    pub(crate) parse_contexts: Vec<RoomContext>,
    /// The contexts the awaited observers ran with.
    pub(crate) awaited_contexts: Vec<RoomContext>,
    /// The room another call creates between a read that misses it and
    /// the creation that follows, which then fails: the id is taken.
    pub(crate) created_meanwhile: Option<Room>,
    /// The messages the receiving channels parsed.
    pub(crate) parsed_messages: Vec<InboundMessage>,
    /// The contexts `on_event()` was called with, by channel, in order.
    on_event_contexts: Vec<(ChannelId, RoomContext)>,
    /// Whether releasing the room lock fails. The lock is let go all the
    /// same, as a lease that expires would be.
    pub(crate) release_fails: bool,
    /// Whether leaving a room's lane fails. The lane is let go all the same.
    pub(crate) leave_fails: bool,
    /// How long each delivery call (`on_event()`, `deliver()`) takes on the
    /// simulated clock, as a slow turn or provider would: the clock moves
    /// while a cascade runs.
    pub(crate) delivery_duration: Duration,
    /// The hooks that call into their own room as they run (RFC §13.5).
    reentrant_hooks: BTreeMap<HookId, Reentry<HookOutcome<RoomEvent>>>,
    /// The channels whose `on_event()` calls into their own room.
    reentrant_channels: BTreeMap<ChannelId, Reentry<Result<ChannelOutput, ErrorRecord>>>,
    /// The channels whose parse calls into its room, then fails.
    failing_parses: BTreeMap<ChannelId, Reentry<ErrorRecord>>,
    /// The entries the operations settled, in order.
    pub(crate) settled: Vec<Settled>,
    /// The decisions the inbound operations published (`Step::decided`),
    /// each with the trace line written last before it.
    pub(crate) decisions: Vec<(Option<String>, Decided)>,
    /// The effect that fails, named by the start of its trace line: a store
    /// write (`update room`, `delete binding sms`, `store room`), a room
    /// lock (`lock room_1`), a room's lane (`enter lane room_1`), the
    /// router (`route sms`), an identity read (`resolve_identity`), awaited
    /// observers (`await on_room_created`), or an admission ticket taken or
    /// awaited (`take ticket room_1`, `await ticket room_1`).
    /// It is answered `Failed` and changes nothing. A ticket's release named
    /// (`release ticket room_1`) fails too, and lets the ticket go all the
    /// same, as a lease that expires would.
    pub(crate) failing_effect: Option<&'static str>,
    /// An event another call commits to `room_1` just before the next read
    /// of it made holding no lock on it, as a direct call's once its lock
    /// is released.
    pub(crate) committed_before_unlocked_read: Option<RoomEvent>,
    /// An event another call commits to `room_1` just after a read of a key
    /// made holding no lock on the room, as one of the room's work reads its
    /// key before its budget.
    pub(crate) committed_after_unlocked_key_read: Option<RoomEvent>,
}

/// The instant the simulated clock starts at, 2026-10-22T14:03:00Z.
pub(crate) const START_MILLIS: i64 = 1_792_677_780_000;

/// The instant `seconds` after the simulated clock's start.
pub(crate) fn after_start(seconds: i64) -> Timestamp {
    Timestamp::from_unix_millis(START_MILLIS + seconds * 1000).unwrap()
}

pub(crate) fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

pub(crate) fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

/// The capabilities of every channel the simulated driver registers: text.
fn text_only() -> ChannelCapabilities {
    ChannelCapabilities {
        media_types: [ChannelMediaType::Text].into(),
        ..ChannelCapabilities::default()
    }
}

impl Sim {
    /// An active, empty room `room_1`.
    pub(crate) fn new() -> Self {
        Self {
            env: SequentialEnvironment::new(Timestamp::from_unix_millis(START_MILLIS).unwrap()),
            config: KitConfig::new(),
            room: Room::new(room_id(), Timestamp::UNIX_EPOCH),
            bindings: Vec::new(),
            participants: Vec::new(),
            timeline: Vec::new(),
            author_register: None,
            tasks: Vec::new(),
            observations: Vec::new(),
            trace: Vec::new(),
            journal: Vec::new(),
            notices: Vec::new(),
            fired: Vec::new(),
            channels: BTreeMap::new(),
            hooks: BTreeMap::new(),
            observer_attaches: BTreeMap::new(),
            others: BTreeMap::new(),
            runs: Vec::new(),
            current: runs::ROOT,
            spawned: Vec::new(),
            holders: runs::Holders::default(),
            place_ahead: None,
            placeless: BTreeSet::new(),
            durations: Vec::new(),
            nth_durations: Vec::new(),
            bounded: Vec::new(),
            in_admission_order: Vec::new(),
            under_lock: Vec::new(),
            leaves_before: None,
            hook_contexts: Vec::new(),
            identities: Vec::new(),
            routes: VecDeque::new(),
            route_queries: Vec::new(),
            recorded_meanwhile: None,
            room_exists: true,
            hooks_change: None,
            parse_contexts: Vec::new(),
            awaited_contexts: Vec::new(),
            created_meanwhile: None,
            parsed_messages: Vec::new(),
            on_event_contexts: Vec::new(),
            release_fails: false,
            leave_fails: false,
            delivery_duration: Duration::ZERO,
            reentrant_hooks: BTreeMap::new(),
            reentrant_channels: BTreeMap::new(),
            failing_parses: BTreeMap::new(),
            settled: Vec::new(),
            decisions: Vec::new(),
            failing_effect: None,
            committed_before_unlocked_read: None,
            committed_after_unlocked_key_read: None,
        }
    }

    /// A store that holds no room yet.
    pub(crate) fn without_rooms() -> Self {
        Self {
            room_exists: false,
            ..Self::new()
        }
    }

    /// A text transport bound read-write, which answers nothing and delivers.
    pub(crate) fn transport(&mut self, id: &str) -> &mut ChannelBinding {
        self.bind(
            id,
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            Box::new(|_, _| Ok(ChannelOutput::default())),
        )
    }

    /// A text transport bound read-write, answering with `on_event`.
    pub(crate) fn transport_with(&mut self, id: &str, on_event: OnEvent) -> &mut ChannelBinding {
        self.bind(
            id,
            ChannelType::WEBSOCKET,
            ChannelCategory::Transport,
            on_event,
        )
    }

    /// An agent bound read-write, answering with `on_event`.
    pub(crate) fn agent(&mut self, id: &str, on_event: OnEvent) -> &mut ChannelBinding {
        self.bind(id, ChannelType::AI, ChannelCategory::Intelligence, on_event)
    }

    /// An agent that answers every event it is asked with `reply`.
    pub(crate) fn echo_agent(&mut self, id: &str, reply: &'static str) -> &mut ChannelBinding {
        let on_event = echo(id, reply);
        self.agent(id, on_event)
    }

    /// Registers a transport that no room binds, declared as `declaration`
    /// says, with text capabilities; it answers nothing and delivers.
    pub(crate) fn declare(&mut self, declaration: ChannelDeclaration) {
        self.register(
            ChannelDeclaration {
                capabilities: text_only(),
                ..declaration
            },
            Box::new(|_, _| Ok(ChannelOutput::default())),
        );
    }

    /// Registers a text transport of `channel_type` that no room binds, with
    /// its type's defaults.
    pub(crate) fn unbound_transport(&mut self, id: &str, channel_type: ChannelType) {
        self.declare(ChannelDeclaration::new(
            channel(id),
            channel_type,
            ChannelCategory::Transport,
            ChannelDirection::Bidirectional,
        ));
    }

    /// Registers an agent that no room binds, answering every event it is
    /// asked with `reply`.
    pub(crate) fn unbound_agent(&mut self, id: &str, reply: &'static str) {
        let declaration = ChannelDeclaration {
            capabilities: text_only(),
            ..ChannelDeclaration::new(
                channel(id),
                ChannelType::AI,
                ChannelCategory::Intelligence,
                ChannelDirection::Bidirectional,
            )
        };
        self.register(declaration, echo(id, reply));
    }

    /// Binds the registered channel `id` to `room_1`, at its defaults.
    pub(crate) fn attach(&mut self, id: &str) -> &mut ChannelBinding {
        let declaration = self.config.channel(&channel(id)).unwrap();
        let binding = declaration.binding(room_id(), Timestamp::UNIX_EPOCH);
        self.bindings.push(binding);
        self.bindings.last_mut().unwrap()
    }

    /// Detaches the channel `id` from `room_1` before the test runs, as the
    /// integrator does: its binding goes, and the room keeps its revocation
    /// (RFC §7.5 rule 7).
    pub(crate) fn detach(&mut self, id: &str) {
        self.bindings
            .retain(|binding| binding.channel_id.as_str() != id);
        attachment::revoke(&self.room, &channel(id), Timestamp::UNIX_EPOCH).apply(&mut self.room);
    }

    /// Replaces how a bound channel delivers.
    pub(crate) fn deliver_with(&mut self, id: &str, deliver: Deliver) {
        if let Some(channel) = self.channels.get_mut(&channel(id)) {
            channel.deliver = deliver;
        }
    }

    fn bind(
        &mut self,
        id: &str,
        channel_type: ChannelType,
        category: ChannelCategory,
        on_event: OnEvent,
    ) -> &mut ChannelBinding {
        let declaration = ChannelDeclaration {
            capabilities: text_only(),
            ..ChannelDeclaration::new(
                channel(id),
                channel_type,
                category,
                ChannelDirection::Bidirectional,
            )
        };
        self.register(declaration, on_event);
        self.attach(id)
    }

    fn register(&mut self, declaration: ChannelDeclaration, on_event: OnEvent) {
        let id = declaration.channel_id.clone();
        self.config.register_channel(declaration).unwrap();
        self.channels.insert(
            id,
            Channel {
                on_event,
                deliver: Box::new(|_| Ok(ChannelOutput::default())),
            },
        );
    }

    /// Makes another call hold a place in `room_1`'s lane ahead of every
    /// place the test's operation takes, until that operation publishes its
    /// decision: the set of a call whose turn waits for this one, as an
    /// agent's turn waits for its call into the room.
    pub(crate) fn hold_a_place_ahead(&mut self) {
        self.place_ahead = Some(EventId::new("another_call_s_event").unwrap());
    }

    /// Registers a SYNC `BEFORE_BROADCAST` hook.
    pub(crate) fn sync_hook(&mut self, spec: HookSpec, hook: Hook) -> HookId {
        let id = self.config.hooks.register(spec).unwrap();
        self.hooks.insert(id, hook);
        id
    }

    /// Registers a SYNC `BEFORE_BROADCAST` hook named `name`.
    pub(crate) fn before_broadcast(&mut self, name: &str, hook: Hook) -> HookId {
        let spec = HookSpec::new(name, HookTrigger::BeforeBroadcast, HookExecution::Sync);
        self.sync_hook(spec, hook)
    }

    /// Registers a SYNC `BEFORE_BROADCAST` hook named `name` that runs off
    /// the room lock (RFC §9.5.1).
    pub(crate) fn off_lock_check(&mut self, name: &str, hook: Hook) -> HookId {
        let spec = HookSpec {
            needs_lock: false,
            ..HookSpec::new(name, HookTrigger::BeforeBroadcast, HookExecution::Sync)
        };
        self.sync_hook(spec, hook)
    }

    /// Registers an ASYNC observer on `trigger`.
    pub(crate) fn observer(&mut self, name: &str, trigger: HookTrigger) -> HookId {
        let spec = HookSpec::new(name, trigger, HookExecution::Async);
        self.config.hooks.register(spec).unwrap()
    }

    /// Registers an ASYNC observer on `trigger` that attaches the registered
    /// channel `channel_id` to its room when the driver runs it awaited, as
    /// a hook on `ON_ROOM_CREATED` attaches an agent (RFC Appendix B.1).
    pub(crate) fn observer_attaching(
        &mut self,
        name: &str,
        trigger: HookTrigger,
        channel_id: &str,
    ) -> HookId {
        let id = self.observer(name, trigger);
        self.observer_attaches.insert(id, channel(channel_id));
        id
    }

    /// Registers a SYNC `BEFORE_BROADCAST` hook named `name`, under the
    /// lock when `needs_lock`, that calls into its own room as it runs.
    pub(crate) fn reentrant_hook(
        &mut self,
        name: &str,
        needs_lock: bool,
        reentry: Reentry<HookOutcome<RoomEvent>>,
    ) -> HookId {
        let spec = HookSpec {
            needs_lock,
            ..HookSpec::new(name, HookTrigger::BeforeBroadcast, HookExecution::Sync)
        };
        let id = self.sync_hook(
            spec,
            Box::new(|_| HookOutcome::Returned(HookResult::allow())),
        );
        self.reentrant_hooks.insert(id, reentry);
        id
    }

    /// Makes the bound channel `id` call into its own room from its
    /// `on_event()`, as an agent's turn that injects does.
    pub(crate) fn reentrant_channel(
        &mut self,
        id: &str,
        reentry: Reentry<Result<ChannelOutput, ErrorRecord>>,
    ) {
        self.reentrant_channels.insert(channel(id), reentry);
    }

    /// Makes the channel `id`'s parse call into its room from the event it
    /// parses, then fail with what `reentry` answers, as a parse that
    /// injects before it fails does.
    pub(crate) fn failing_parse(&mut self, id: &str, reentry: Reentry<ErrorRecord>) {
        self.failing_parses.insert(channel(id), reentry);
    }

    /// Runs `event`, injected directly into `room` (RFC §10.5).
    pub(crate) fn inject(
        &mut self,
        room: &str,
        event: DirectEvent,
    ) -> Result<InboundResult, InboundError> {
        self.inject_for(None, room, event)
    }

    /// Injects `event` into `room` for a caller acting for `organization`
    /// (RFC §10.5, §17.2).
    pub(crate) fn inject_for(
        &mut self,
        organization: Option<&str>,
        room: &str,
        event: DirectEvent,
    ) -> Result<InboundResult, InboundError> {
        let config = Arc::new(self.config.clone());
        let organization = organization.map(str::to_owned);
        let room = RoomId::new(room).unwrap();
        let operation = Inbound::inject(config, room, organization, event);
        self.run(operation)
    }

    /// Commits `record` to `room` outside the pipeline, for a caller acting
    /// for `organization` (RFC §10.5 `commit_event`).
    pub(crate) fn commit_record(
        &mut self,
        room: &str,
        record: RoomEvent,
        organization: Option<&str>,
    ) -> Result<RoomEvent, MutationError> {
        let operation = CommitRecord::new(
            self.shared_config(),
            RoomId::new(room).unwrap(),
            record,
            organization.map(str::to_owned),
        );
        self.run(operation)
    }

    /// A text message from `+15551234567` on `channel_id`.
    pub(crate) fn message(channel_id: &str, text: &str) -> InboundMessage {
        InboundMessage::new(
            channel(channel_id),
            "+15551234567",
            EventContent::text(text),
        )
    }

    /// Runs `message` into `room_1` through the inbound operation, for a
    /// caller that names no organization.
    pub(crate) fn send(&mut self, message: InboundMessage) -> Result<InboundResult, InboundError> {
        self.send_to("room_1", None, message)
    }

    /// Runs `message` into the room `room`, for a caller acting for
    /// `organization`.
    pub(crate) fn send_to(
        &mut self,
        room: &str,
        organization: Option<&str>,
        message: InboundMessage,
    ) -> Result<InboundResult, InboundError> {
        let target = InboundTarget::Room(RoomId::new(room).unwrap());
        self.inbound(target, organization, message)
    }

    /// Runs `message`, which names no room, through the inbound operation:
    /// the scripted router is asked, for a caller acting for `organization`
    /// (RFC §10.4).
    pub(crate) fn route(
        &mut self,
        organization: Option<&str>,
        message: InboundMessage,
    ) -> Result<InboundResult, InboundError> {
        self.inbound(InboundTarget::Routed, organization, message)
    }

    /// Runs a host's direct update of `event_id` in `room_1` (RFC §10.3).
    pub(crate) fn update_event(
        &mut self,
        event_id: &str,
        updates: EventUpdates,
    ) -> Result<Option<RoomEvent>, MutationError> {
        let operation = UpdateEvent::new(
            self.shared_config(),
            room_id(),
            EventId::new(event_id).unwrap(),
            updates,
            None,
        );
        self.run(operation)
    }

    /// Runs a host's direct delete of `event_id` from `room_1` (RFC §10.3).
    pub(crate) fn delete_event(
        &mut self,
        event_id: &str,
        cascade_replies: bool,
    ) -> Result<Vec<EventId>, MutationError> {
        let operation = DeleteEvent::new(
            self.shared_config(),
            room_id(),
            EventId::new(event_id).unwrap(),
            cascade_replies,
            None,
        );
        self.run(operation)
    }

    fn inbound(
        &mut self,
        target: InboundTarget,
        organization: Option<&str>,
        message: InboundMessage,
    ) -> Result<InboundResult, InboundError> {
        let config = Arc::new(self.config.clone());
        let organization = organization.map(str::to_owned);
        self.run(Inbound::new(config, target, organization, message))
    }

    /// Scripts the router's next answer: `room`, or none.
    pub(crate) fn routes_to(&mut self, room: Option<&str>) {
        self.routes
            .push_back(Ok(room.map(|room| RoomId::new(room).unwrap())));
    }

    /// The entries a hook or a channel makes into its room on `effect`, if
    /// it is one that calls.
    fn reentry(&mut self, effect: &Effect) -> Option<(Caller, Vec<Entry>)> {
        if let Request::Hook(HookRequest::RunHook {
            hook,
            event,
            context,
            ..
        }) = &effect.request
        {
            let reentry = self.reentrant_hooks.get_mut(hook)?;
            let entries = (reentry.entries)(event);
            if entries.is_empty() {
                return None;
            }
            let name = self.config.hooks.get(*hook).unwrap().name.clone();
            self.hook_contexts.push((name, (**context).clone()));
            return Some((Caller::Hook(*hook), entries));
        }
        if let Request::Channel(ChannelRequest::OnEvent { binding, event, .. }) = &effect.request {
            let reentry = self.reentrant_channels.get_mut(&binding.channel_id)?;
            let entries = (reentry.entries)(event);
            if entries.is_empty() {
                return None;
            }
            return Some((Caller::Channel(binding.channel_id.clone()), entries));
        }
        if let Request::Channel(ChannelRequest::HandleInbound {
            channel_id,
            message,
            context,
            id,
        }) = &effect.request
        {
            let reentry = self.failing_parses.get_mut(channel_id)?;
            let declaration = self.config.channel(channel_id).unwrap();
            let event = default_inbound_event(
                message,
                declaration,
                context.room.id.clone(),
                id.clone(),
                self.env.now(),
            );
            let entries = (reentry.entries)(&event);
            if entries.is_empty() {
                return None;
            }
            return Some((Caller::Parse(channel_id.clone()), entries));
        }
        None
    }

    /// How `caller` goes on once it made its calls.
    fn then_of(&mut self, caller: &Caller) -> Then {
        let at_once = |answer: Outcome, apart: bool| {
            if apart {
                Then::Apart(answer)
            } else {
                Then::AtOnce(answer)
            }
        };
        match caller {
            Caller::Hook(hook) => match &mut self.reentrant_hooks.get_mut(hook).unwrap().answer {
                Answer::Awaits(_) => Then::Waits,
                Answer::AtOnce(answer) => {
                    at_once(Outcome::Hook(HookAnswer::Ran(Box::new(answer()))), false)
                }
                Answer::Apart(answer) => {
                    at_once(Outcome::Hook(HookAnswer::Ran(Box::new(answer()))), true)
                }
            },
            Caller::Channel(channel_id) => {
                match &mut self.reentrant_channels.get_mut(channel_id).unwrap().answer {
                    Answer::Awaits(_) => Then::Waits,
                    Answer::AtOnce(answer) => at_once(output(answer()), false),
                    Answer::Apart(answer) => at_once(output(answer()), true),
                }
            }
            Caller::Parse(channel_id) => {
                match &mut self.failing_parses.get_mut(channel_id).unwrap().answer {
                    Answer::Awaits(_) => Then::Waits,
                    Answer::AtOnce(answer) => at_once(Outcome::Failed(answer()), false),
                    Answer::Apart(answer) => at_once(Outcome::Failed(answer()), true),
                }
            }
        }
    }

    /// What `caller`, which awaits its call, answers from `result`.
    fn answer_from(&mut self, caller: &Caller, result: &Called) -> Outcome {
        match caller {
            Caller::Hook(hook) => {
                let reentry = self.reentrant_hooks.get_mut(hook).unwrap();
                let Answer::Awaits(then) = &mut reentry.answer else {
                    unreachable!("a caller that waits answers from the result");
                };
                Outcome::Hook(HookAnswer::Ran(Box::new(then(result))))
            }
            Caller::Channel(channel_id) => {
                let reentry = self.reentrant_channels.get_mut(channel_id).unwrap();
                let Answer::Awaits(then) = &mut reentry.answer else {
                    unreachable!("a caller that waits answers from the result");
                };
                output(then(result))
            }
            Caller::Parse(channel_id) => {
                let reentry = self.failing_parses.get_mut(channel_id).unwrap();
                let Answer::Awaits(then) = &mut reentry.answer else {
                    unreachable!("a caller that waits answers from the result");
                };
                Outcome::Failed(then(result))
            }
        }
    }

    /// The kit's configuration, as an operation receives it.
    pub(crate) fn shared_config(&self) -> Arc<KitConfig> {
        Arc::new(self.config.clone())
    }

    /// Commits `event` to the timeline before the test runs, at the next
    /// index.
    pub(crate) fn seed(&mut self, mut event: RoomEvent) {
        event.index = self.room.event_count;
        self.room.event_count += 1;
        self.room.latest_index = Some(event.index);
        self.timeline.push(event);
    }

    /// Commits `event` to the timeline of the room `room`, stored apart from
    /// `room_1`, before the test runs, at its next index.
    pub(crate) fn seed_other(&mut self, room: &str, event: RoomEvent) {
        let room_id = RoomId::new(room).unwrap();
        self.on_room(&room_id, |sim| sim.seed(event));
    }

    /// Stores `room`, apart from `room_1`, before the test runs.
    pub(crate) fn seed_room(&mut self, room: Room) {
        let mut stored = StoredRoom::missing(room.id.clone());
        stored.room = room;
        stored.exists = true;
        self.others.insert(stored.room.id.clone(), stored);
    }

    /// The room `id` the store holds apart from `room_1`, if any.
    pub(crate) fn other_room(&self, id: &str) -> Option<&Room> {
        self.others
            .get(&RoomId::new(id).unwrap())
            .filter(|stored| stored.exists)
            .map(|stored| &stored.room)
    }

    /// The bindings of the room `id` the store holds apart from `room_1`.
    pub(crate) fn other_bindings(&self, id: &str) -> Vec<ChannelBinding> {
        self.others
            .get(&RoomId::new(id).unwrap())
            .map(|stored| stored.bindings.clone())
            .unwrap_or_default()
    }

    /// The timeline of the room `id` the store holds apart from `room_1`, as
    /// [`timeline_lines`](Self::timeline_lines) writes it.
    pub(crate) fn other_timeline_lines(&self, id: &str) -> Vec<String> {
        self.others
            .get(&RoomId::new(id).unwrap())
            .map(|stored| stored.timeline.iter().map(describe).collect())
            .unwrap_or_default()
    }

    /// The texts of the history `channel` was last handed by `on_event()`.
    pub(crate) fn history_seen_by(&self, channel: &str) -> Vec<String> {
        self.on_event_contexts
            .iter()
            .rev()
            .find(|(id, _)| id.as_str() == channel)
            .map(|(_, context)| {
                context
                    .recent_events
                    .iter()
                    .map(|event| text_of(&event.content))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// How many commits were made, and how many of them on behalf of the
    /// section that held their room's lock (`Effect::locked`, RFC §13.5).
    pub(crate) fn commits_under_lock(&self) -> (usize, usize) {
        let under_lock = self
            .under_lock
            .iter()
            .filter(|line| line.starts_with("commit under "))
            .count();
        (self.traced("commit").len(), under_lock)
    }

    /// The trace lines matching `prefix`.
    pub(crate) fn traced(&self, prefix: &str) -> Vec<&str> {
        self.trace
            .iter()
            .map(String::as_str)
            .filter(|line| line.starts_with(prefix))
            .collect()
    }

    /// The framework events emitted, in order.
    pub(crate) fn framework_events(&self) -> Vec<&FrameworkEvent> {
        self.notices
            .iter()
            .filter_map(|notice| match notice {
                Notice::FrameworkEvent(event) => Some(event),
                Notice::AsyncHooks { .. } => None,
            })
            .collect()
    }

    /// The framework events' RFC names, in order, read from their JSON form.
    pub(crate) fn framework_event_names(&self) -> Vec<String> {
        self.framework_events()
            .into_iter()
            .map(event_name)
            .collect()
    }

    /// The triggers whose ASYNC hooks were fired, in order.
    pub(crate) fn async_triggers(&self) -> Vec<HookTrigger> {
        self.notices
            .iter()
            .filter_map(|notice| match notice {
                Notice::AsyncHooks { trigger, .. } => Some(*trigger),
                Notice::FrameworkEvent(_) => None,
            })
            .collect()
    }

    /// The payloads the ASYNC hooks were fired with, in order.
    pub(crate) fn async_payloads(&self) -> Vec<&RoomEvent> {
        self.notices
            .iter()
            .filter_map(|notice| match notice {
                Notice::AsyncHooks { event, .. } => Some(&**event),
                Notice::FrameworkEvent(_) => None,
            })
            .collect()
    }

    /// The contexts the ASYNC hooks were fired with, in order.
    pub(crate) fn async_contexts(&self) -> Vec<&RoomContext> {
        self.notices
            .iter()
            .filter_map(|notice| match notice {
                Notice::AsyncHooks { context, .. } => Some(&**context),
                Notice::FrameworkEvent(_) => None,
            })
            .collect()
    }

    /// The timeline as `index:source:status[:blocked_by]:text` lines.
    pub(crate) fn timeline_lines(&self) -> Vec<String> {
        self.timeline.iter().map(describe).collect()
    }

    /// Runs `request`, the current run's `effect`, for as long as
    /// `durations` says, within `deadline`: a request that would finish past
    /// it does nothing, and is answered `DeadlinePassed` at the deadline (RFC
    /// §13.6). `None` for a request that waits for what another run holds.
    fn execute_timed(
        &mut self,
        effect: EffectId,
        request: Request,
        deadline: Option<Timestamp>,
    ) -> Option<Outcome> {
        let line = self.label(&request);
        let nth = |prefix: &str| {
            self.trace
                .iter()
                .filter(|traced| traced.starts_with(prefix))
                .count()
                + 1
        };
        let duration = self
            .durations
            .iter()
            .find(|(prefix, _)| line.starts_with(prefix))
            .map(|(_, duration)| *duration)
            .or_else(|| {
                self.nth_durations
                    .iter()
                    .find(|(prefix, n, _)| line.starts_with(prefix) && nth(prefix) == *n)
                    .map(|(_, _, duration)| *duration)
            })
            .unwrap_or(Duration::ZERO);
        if let Some(deadline) = deadline {
            self.bounded.push(line.clone());
            let now = self.env.now();
            if now.checked_add(duration).unwrap() > deadline {
                self.env
                    .advance(deadline.duration_since(now).unwrap_or_default());
                self.trace.push(format!("{line} deadline passed"));
                return Some(Outcome::DeadlinePassed);
            }
        }
        self.env.advance(duration);
        self.execute(effect, request, deadline)
    }

    /// How the trace names `request`, or the start of it for the requests
    /// whose line says more once they ran.
    fn label(&self, request: &Request) -> String {
        match request {
            Request::Hold(HoldRequest::AcquireLock { room_id }) => format!("lock {room_id}"),
            Request::Hold(HoldRequest::ReleaseLock { room_id }) => format!("release {room_id}"),
            Request::Hold(HoldRequest::EnterLane { room_id, event_id }) => {
                format!("enter lane {room_id} {event_id}")
            }
            Request::Hold(HoldRequest::AwaitLane { room_id, event_id }) => {
                format!("await lane {room_id} {event_id}")
            }
            Request::Hold(HoldRequest::LeaveLane { room_id, event_id }) => {
                format!("leave lane {room_id} {event_id}")
            }
            Request::Hold(HoldRequest::TakeTicket { room_id }) => format!("take ticket {room_id}"),
            Request::Hold(HoldRequest::AwaitTicket { room_id }) => {
                format!("await ticket {room_id}")
            }
            Request::Hold(HoldRequest::ReleaseTicket { room_id }) => {
                format!("release ticket {room_id}")
            }
            Request::Store(StoreRequest::LoadContext { room_id, .. }) => {
                format!("load_context {room_id}")
            }
            Request::Store(StoreRequest::GetEventByIdempotencyKey { key, .. }) => {
                format!("find_key {key}")
            }
            Request::Store(StoreRequest::GetEvent { event_id }) => format!("get_event {event_id}"),
            Request::Store(StoreRequest::ListEvents { room_id }) => {
                format!("list_events {room_id}")
            }
            Request::Store(StoreRequest::Commit { .. }) => "commit".to_owned(),
            Request::Store(StoreRequest::PersistSideEffects { .. }) => "persist".to_owned(),
            Request::Store(StoreRequest::CreateRoom { room }) => format!("store room {}", room.id),
            Request::Store(StoreRequest::UpdateRoom { room_id, .. }) => {
                format!("update room {room_id}")
            }
            Request::Store(StoreRequest::StoreBinding { binding }) => {
                format!("store binding {}", binding.channel_id)
            }
            Request::Store(StoreRequest::DeleteBinding { channel_id, .. }) => {
                format!("delete binding {channel_id}")
            }
            Request::Store(StoreRequest::UpdateEvent { event_id, .. }) => {
                format!("update event {event_id}")
            }
            Request::Store(StoreRequest::DeleteEvent { event_id, .. }) => {
                format!("delete event {event_id}")
            }
            Request::Store(StoreRequest::ResolveIdentity {
                channel_type,
                address,
                ..
            }) => format!("resolve_identity {} {address}", type_name(channel_type)),
            Request::Route { query } => format!("route {} {}", query.channel_id, query.sender_id),
            Request::Channel(ChannelRequest::HandleInbound { channel_id, .. }) => {
                format!("handle_inbound {channel_id}")
            }
            Request::Channel(ChannelRequest::OnEvent { binding, event, .. }) => {
                format!("on_event {} <- {}", binding.channel_id, event.id)
            }
            Request::Channel(ChannelRequest::Deliver { binding, event, .. }) => {
                format!("deliver {} <- {}", binding.channel_id, event.id)
            }
            Request::Hook(HookRequest::RunHook { hook, .. }) => {
                format!("hook {}", self.config.hooks.get(*hook).unwrap().name)
            }
            Request::Hook(HookRequest::RunObservers { trigger, .. }) => {
                format!("await {}", trigger_name(*trigger))
            }
        }
    }

    /// Executes `request`, the current run's `effect`; `None` for a lock, a
    /// lane or a ticket's turn another run holds, which waits.
    fn execute(
        &mut self,
        effect: EffectId,
        request: Request,
        deadline: Option<Timestamp>,
    ) -> Option<Outcome> {
        if let Some(refused) = self.refused(&request) {
            return Some(refused);
        }
        let outcome = match request {
            Request::Hold(HoldRequest::AcquireLock { room_id }) => {
                return self.acquire(effect, room_id, deadline);
            }
            Request::Hold(HoldRequest::AwaitLane { room_id, event_id }) => {
                return self.await_place(effect, room_id, event_id, deadline);
            }
            Request::Hold(HoldRequest::AwaitTicket { room_id }) => {
                return self.await_ticket(effect, room_id, deadline);
            }
            Request::Hold(HoldRequest::ReleaseLock { room_id }) => self.release(&room_id),
            Request::Hold(HoldRequest::EnterLane { room_id, event_id }) => {
                self.enter_lane(room_id, event_id)
            }
            Request::Hold(HoldRequest::LeaveLane { room_id, event_id }) => {
                self.leave_lane(&room_id, &event_id)
            }
            Request::Hold(HoldRequest::TakeTicket { room_id }) => self.take_ticket(room_id),
            Request::Hold(HoldRequest::ReleaseTicket { room_id }) => self.release_ticket(&room_id),
            Request::Channel(ChannelRequest::HandleInbound {
                channel_id,
                message,
                context,
                id,
            }) => self.handle_inbound(&channel_id, message, *context, id),
            Request::Channel(ChannelRequest::OnEvent {
                binding,
                event,
                context,
            }) => self.on_event(&binding, &event, *context),
            Request::Channel(ChannelRequest::Deliver { binding, event, .. }) => {
                self.deliver(&binding, &event)
            }
            Request::Hook(HookRequest::RunHook {
                hook,
                event,
                context,
                ..
            }) => self.execute_hook(hook, &event, *context),
            Request::Hook(HookRequest::RunObservers {
                trigger,
                hooks,
                event,
                context,
            }) => {
                self.awaited_contexts.push(*context);
                self.execute_observers(trigger, &hooks, &event)
            }
            Request::Store(StoreRequest::ResolveIdentity {
                channel_type,
                address,
                organization_id,
            }) => self.resolve_identity(&channel_type, &address, organization_id.as_deref()),
            Request::Route { query } => self.execute_route(*query),
            Request::Store(StoreRequest::GetEvent { event_id }) => self.get_event(&event_id),
            Request::Store(StoreRequest::UpdateEvent { event_id, updates }) => {
                let room = self.room_of_event(&event_id);
                self.on_room(&room, |sim| sim.update_stored_event(&event_id, &updates))
            }
            Request::Store(request) => {
                let room = store_room(&request).clone();
                self.on_room(&room, |sim| sim.execute_store(request))
            }
        };
        Some(outcome)
    }

    /// The event `event_id`, whichever room holds it (RFC §14.1 `get_event`).
    fn get_event(&mut self, event_id: &EventId) -> Outcome {
        self.trace.push(format!("get_event {event_id}"));
        let found = self
            .timeline
            .iter()
            .chain(
                self.others
                    .values()
                    .flat_map(|stored| stored.timeline.iter()),
            )
            .find(|event| &event.id == event_id)
            .cloned();
        Outcome::Store(StoreOutcome::Event(found.map(Box::new)))
    }

    /// The room whose timeline holds `event_id`, `room_1` when none does.
    fn room_of_event(&self, event_id: &EventId) -> RoomId {
        self.others
            .iter()
            .find(|(_, stored)| stored.timeline.iter().any(|event| &event.id == event_id))
            .map_or_else(room_id, |(id, _)| id.clone())
    }

    /// Writes `updates` on the stored event `event_id` of the room the
    /// fields hold (RFC §14.1 `update_event`).
    fn update_stored_event(&mut self, event_id: &EventId, updates: &EventUpdates) -> Outcome {
        self.trace.push(format!("update event {event_id}"));
        let Some(stored) = self.timeline.iter_mut().find(|event| &event.id == event_id) else {
            return Outcome::Failed(ErrorRecord::new("EventNotFound", "no such event"));
        };
        updates.apply(stored);
        Outcome::Store(StoreOutcome::Stored)
    }

    /// The identity of `organization` that uses `address` on channels of
    /// `channel_type`, if any.
    fn resolve_identity(
        &mut self,
        channel_type: &ChannelType,
        address: &str,
        organization: Option<&str>,
    ) -> Outcome {
        self.trace.push(format!(
            "resolve_identity {} {address}",
            type_name(channel_type)
        ));
        let identity = self.identities.iter().find(|identity| {
            identity.organization_id.as_deref() == organization
                && identity.uses(channel_type, address)
        });
        Outcome::Store(StoreOutcome::Resolved(
            identity.map(|identity| identity.id.clone()),
        ))
    }

    /// The scripted router's next answer; no room once they are spent. The
    /// router reads no lock and holds no lane (RFC §10.4).
    fn execute_route(&mut self, query: RouteQuery) -> Outcome {
        self.trace
            .push(format!("route {} {}", query.channel_id, query.sender_id));
        assert!(
            { self.state(self.current).locks.is_empty() },
            "the router was asked holding a lock or a lane"
        );
        self.route_queries.push(query);
        match self.routes.pop_front() {
            Some(Ok(room)) => Outcome::Routed(room),
            Some(Err(error)) => Outcome::Failed(error),
            None => Outcome::Routed(None),
        }
    }

    /// A SYNC hook, and the change the hooks make in the room as they run.
    fn execute_hook(&mut self, hook: HookId, event: &RoomEvent, context: RoomContext) -> Outcome {
        let name = self.config.hooks.get(hook).unwrap().name.clone();
        self.trace.push(format!("hook {name}"));
        self.hook_contexts.push((name, context));
        let hook = self.hooks.get_mut(&hook).unwrap();
        let outcome = hook(event);
        match &self.hooks_change {
            Some(HookChange::Mute(muted)) => {
                let binding = self
                    .bindings
                    .iter_mut()
                    .find(|bound| &bound.channel_id == muted);
                binding.unwrap().muted = true;
            }
            Some(HookChange::CloseRoom) => self.room.status = RoomStatus::Closed,
            Some(HookChange::CloseRoomOn(text)) if text_of(&event.content) == *text => {
                self.room.status = RoomStatus::Closed;
            }
            Some(HookChange::DetachOn(text, detached)) if text_of(&event.content) == *text => {
                self.bindings.retain(|bound| &bound.channel_id != detached);
            }
            Some(HookChange::CloseRoomOn(_) | HookChange::DetachOn(..)) | None => {}
        }
        Outcome::Hook(HookAnswer::Ran(Box::new(outcome)))
    }

    /// Observers run to their end, awaited: each attaches its channel to the
    /// payload's room, if it was registered to.
    fn execute_observers(
        &mut self,
        trigger: HookTrigger,
        hooks: &[HookId],
        event: &RoomEvent,
    ) -> Outcome {
        let names: Vec<String> = hooks
            .iter()
            .map(|hook| self.config.hooks.get(*hook).unwrap().name.clone())
            .collect();
        self.trace.push(format!(
            "await {} {}",
            trigger_name(trigger),
            names.join(",")
        ));
        assert!(
            !self.state(self.current).locks.contains(&event.room_id),
            "awaited observers ran under the room lock, which they may need"
        );
        for hook in hooks {
            let Some(attached) = self.observer_attaches.get(hook).cloned() else {
                continue;
            };
            let declaration = self.config.channel(&attached).unwrap();
            let binding = declaration.binding(event.room_id.clone(), Timestamp::UNIX_EPOCH);
            self.on_room(&event.room_id, |sim| {
                sim.bindings.retain(|bound| bound.channel_id != attached);
                sim.bindings.push(binding);
            });
        }
        Outcome::Hook(HookAnswer::Observed)
    }

    /// The failure of the effect `failing_effect` names, traced as failed.
    fn refused(&mut self, request: &Request) -> Option<Outcome> {
        let failing = self.failing_effect?;
        let store_down = || ErrorRecord::new("StoreUnavailable", "the store is down");
        let (target, error) = match request {
            Request::Store(StoreRequest::UpdateRoom { room_id, .. }) => {
                (format!("update room {room_id}"), store_down())
            }
            Request::Store(StoreRequest::StoreBinding { binding }) => (
                format!("store binding {}", binding.channel_id),
                store_down(),
            ),
            Request::Store(StoreRequest::DeleteBinding { channel_id, .. }) => {
                (format!("delete binding {channel_id}"), store_down())
            }
            Request::Store(StoreRequest::CreateRoom { room }) => {
                (format!("store room {}", room.id), store_down())
            }
            Request::Store(StoreRequest::Commit { .. }) => ("commit".to_owned(), store_down()),
            Request::Store(StoreRequest::GetEvent { event_id }) => {
                (format!("get_event {event_id}"), store_down())
            }
            Request::Store(StoreRequest::UpdateEvent { event_id, .. }) => {
                (format!("update event {event_id}"), store_down())
            }
            Request::Store(StoreRequest::DeleteEvent { event_id, .. }) => {
                (format!("delete event {event_id}"), store_down())
            }
            // A read of the room made holding no lock on it, as a direct
            // call's once its lock is released.
            Request::Store(StoreRequest::LoadContext { room_id, .. })
                if !self.state(self.current).locks.contains(room_id) =>
            {
                (format!("unlocked load_context {room_id}"), store_down())
            }
            Request::Hold(HoldRequest::AcquireLock { room_id }) => (
                format!("lock {room_id}"),
                ErrorRecord::new("LockUnavailable", "the lock manager is down"),
            ),
            Request::Hook(HookRequest::RunObservers { trigger, .. }) => (
                format!("await {}", trigger_name(*trigger)),
                ErrorRecord::new("HooksUnavailable", "the observers could not be run"),
            ),
            Request::Store(StoreRequest::ResolveIdentity { address, .. }) => {
                (format!("resolve_identity {address}"), store_down())
            }
            Request::Store(StoreRequest::ListEvents { room_id }) => {
                (format!("list_events {room_id}"), store_down())
            }
            Request::Route { query } => (
                format!("route {}", query.channel_id),
                ErrorRecord::new("RouterError", "the CRM is unreachable"),
            ),
            Request::Hold(HoldRequest::EnterLane { room_id, event_id }) => (
                format!("enter lane {room_id} {event_id}"),
                ErrorRecord::new("LaneUnavailable", "the lane manager is down"),
            ),
            Request::Hold(HoldRequest::AwaitLane { room_id, event_id }) => (
                format!("await lane {room_id} {event_id}"),
                ErrorRecord::new("LaneUnavailable", "the lane manager is down"),
            ),
            Request::Hold(HoldRequest::TakeTicket { room_id }) => (
                format!("take ticket {room_id}"),
                ErrorRecord::new("TicketUnavailable", "the ticket queue is down"),
            ),
            Request::Hold(HoldRequest::AwaitTicket { room_id }) => (
                format!("await ticket {room_id}"),
                ErrorRecord::new("TicketUnavailable", "the ticket queue is down"),
            ),
            Request::Hold(
                HoldRequest::ReleaseLock { .. }
                | HoldRequest::ReleaseTicket { .. }
                | HoldRequest::LeaveLane { .. },
            )
            | Request::Store(
                StoreRequest::LoadContext { .. }
                | StoreRequest::GetEventByIdempotencyKey { .. }
                | StoreRequest::PersistSideEffects { .. },
            )
            | Request::Channel(_)
            | Request::Hook(HookRequest::RunHook { .. }) => return None,
        };
        if !target.starts_with(failing) {
            return None;
        }
        if let Request::Hold(
            HoldRequest::EnterLane { event_id, .. } | HoldRequest::AwaitLane { event_id, .. },
        ) = request
        {
            self.placeless.insert(event_id.clone());
        }
        self.trace.push(format!("{target} failed"));
        Some(Outcome::Failed(error))
    }

    /// Records the sender `recorded_meanwhile` names on its room's binding,
    /// once.
    fn record_meanwhile(&mut self, room_id: &RoomId) {
        if self
            .recorded_meanwhile
            .as_ref()
            .is_none_or(|(room, _, _)| room != room_id)
        {
            return;
        }
        let Some((room, channel_id, sender)) = self.recorded_meanwhile.take() else {
            return;
        };
        self.on_room(&room, |sim| {
            let binding = sim
                .bindings
                .iter_mut()
                .find(|binding| binding.channel_id == channel_id)
                .unwrap();
            binding.participant_id = Some(sender);
        });
    }

    /// Releases the current run's lock of a room, which must be held.
    fn release(&mut self, room_id: &RoomId) -> Outcome {
        self.trace.push(format!("release {room_id}"));
        self.let_lock_go(room_id);
        if self.release_fails {
            return Outcome::Failed(ErrorRecord::new("LockLost", "the lock's lease was lost"));
        }
        Outcome::Hold(HoldOutcome::Released)
    }

    /// Takes the current run's place for `event_id` in the room's delivery
    /// lane; it never waits.
    fn enter_lane(&mut self, room_id: RoomId, event_id: EventId) -> Outcome {
        self.trace.push(format!("enter lane {room_id} {event_id}"));
        self.take_place(room_id, event_id);
        Outcome::Hold(HoldOutcome::Entered)
    }

    /// Leaves the current run's place for `event_id`, which must be held.
    fn leave_lane(&mut self, room_id: &RoomId, event_id: &EventId) -> Outcome {
        self.trace.push(format!("leave lane {room_id} {event_id}"));
        self.leave_place(room_id, event_id);
        if self.leave_fails {
            return Outcome::Failed(ErrorRecord::new("LaneLost", "the lane was lost"));
        }
        Outcome::Hold(HoldOutcome::Left)
    }

    /// Takes the room's admission ticket, before its lock (RFC §9.5.1).
    fn take_ticket(&mut self, room_id: RoomId) -> Outcome {
        self.trace.push(format!("take ticket {room_id}"));
        assert!(
            !self.state(self.current).locks.contains(&room_id),
            "an admission ticket was taken under the room lock"
        );
        self.take_a_ticket(room_id);
        Outcome::Hold(HoldOutcome::Ticketed)
    }

    /// The ticket's turn, once every earlier ticket of the room is
    /// released, or once `durations` says it waited.
    fn await_ticket(
        &mut self,
        effect: EffectId,
        room_id: RoomId,
        deadline: Option<Timestamp>,
    ) -> Option<Outcome> {
        self.turn(effect, room_id, deadline)
    }

    /// Releases the current run's ticket of the room, once the room lock is
    /// let go.
    fn release_ticket(&mut self, room_id: &RoomId) -> Outcome {
        self.trace.push(format!("release ticket {room_id}"));
        assert!(
            !self.state(self.current).locks.contains(room_id),
            "the ticket was released under the room lock"
        );
        self.let_ticket_go(room_id);
        let line = format!("release ticket {room_id}");
        if self
            .failing_effect
            .is_some_and(|failing| line.starts_with(failing))
        {
            return Outcome::Failed(ErrorRecord::new("TicketLost", "the ticket queue was lost"));
        }
        Outcome::Hold(HoldOutcome::TicketReleased)
    }

    /// Runs `execute` on the room `id`. `room_1` is the room the fields
    /// hold; another room is swapped into them for the call, so that the
    /// store answers for every room alike.
    fn on_room<T>(&mut self, id: &RoomId, execute: impl FnOnce(&mut Self) -> T) -> T {
        if *id == room_id() {
            return execute(self);
        }
        let mut other = self
            .others
            .remove(id)
            .unwrap_or_else(|| StoredRoom::missing(id.clone()));
        self.swap(&mut other);
        let result = execute(self);
        self.swap(&mut other);
        if other.exists {
            self.others.insert(id.clone(), other);
        }
        result
    }

    /// Swaps the room the fields hold with `other`.
    fn swap(&mut self, other: &mut StoredRoom) {
        mem::swap(&mut self.room, &mut other.room);
        mem::swap(&mut self.room_exists, &mut other.exists);
        mem::swap(&mut self.bindings, &mut other.bindings);
        mem::swap(&mut self.participants, &mut other.participants);
        mem::swap(&mut self.timeline, &mut other.timeline);
        mem::swap(&mut self.author_register, &mut other.author_register);
    }

    /// Writes `updates` on the room, its register of authors apart, as a
    /// store that keeps a room's private state apart does (RFC §14.1).
    fn update_room(&mut self, mut updates: RoomUpdates) {
        if let Some(register) = updates.private_state.remove("author_register") {
            self.room.metadata.remove("author_register");
            self.author_register = register;
        }
        updates.apply(&mut self.room);
    }

    /// The store, for the room the fields hold: its bindings, participants,
    /// timeline and side effects.
    fn execute_store(&mut self, request: StoreRequest) -> Outcome {
        match request {
            StoreRequest::LoadContext {
                room_id,
                recent_events,
            } => {
                self.trace.push(format!("load_context {room_id}"));
                if !self.room_exists {
                    return Outcome::Store(StoreOutcome::RoomNotFound);
                }
                if room_id == self::room_id()
                    && !self.state(self.current).locks.contains(&room_id)
                    && let Some(event) = self.committed_before_unlocked_read.take()
                {
                    self.seed(event);
                }
                Outcome::Store(StoreOutcome::Context(Box::new(self.context(recent_events))))
            }
            StoreRequest::GetEventByIdempotencyKey {
                room_id,
                channel_id,
                key,
            } => {
                self.trace.push(format!("find_key {key}"));
                let found = self
                    .timeline
                    .iter()
                    .find(|event| {
                        event.idempotency_key.as_deref() == Some(key.as_str())
                            && channel_id
                                .as_ref()
                                .is_none_or(|channel_id| &event.source.channel_id == channel_id)
                    })
                    .cloned()
                    .map(Box::new);
                if !self.state(self.current).locks.contains(&room_id)
                    && let Some(event) = self.committed_after_unlocked_key_read.take()
                {
                    self.seed(event);
                }
                Outcome::Store(StoreOutcome::Idempotent(found))
            }
            StoreRequest::Commit {
                event,
                counters,
                target,
            } => self.commit(*event, counters, target.as_ref()),
            StoreRequest::ListEvents { room_id } => self.list_events(&room_id),
            StoreRequest::DeleteEvent {
                event_id,
                cascade_replies,
                ..
            } => self.delete_stored_event(&event_id, cascade_replies),
            StoreRequest::PersistSideEffects {
                tasks,
                observations,
                ..
            } => {
                self.trace.push(format!(
                    "persist {} tasks, {} observations",
                    tasks.len(),
                    observations.len()
                ));
                self.tasks.extend(tasks);
                self.observations.extend(observations);
                Outcome::Store(StoreOutcome::Persisted)
            }
            StoreRequest::CreateRoom { room } => self.create_room(room),
            StoreRequest::UpdateRoom { room_id, updates } => {
                assert!(
                    self.state(self.current).locks.contains(&room_id) || updates.status.is_none(),
                    "a status was written outside the room lock"
                );
                self.update_room(updates);
                self.trace
                    .push(format!("update room {room_id} {:?}", self.room.status));
                Outcome::Store(StoreOutcome::Stored)
            }
            StoreRequest::DeleteBinding { channel_id, .. } => {
                self.trace.push(format!("delete binding {channel_id}"));
                self.bindings.retain(|bound| bound.channel_id != channel_id);
                Outcome::Store(StoreOutcome::Stored)
            }
            StoreRequest::StoreBinding { binding } => {
                self.trace
                    .push(format!("store binding {}", binding.channel_id));
                self.bindings
                    .retain(|bound| bound.channel_id != binding.channel_id);
                self.bindings.push(binding);
                Outcome::Store(StoreOutcome::Stored)
            }
            StoreRequest::ResolveIdentity { .. }
            | StoreRequest::GetEvent { .. }
            | StoreRequest::UpdateEvent { .. } => panic!("not a room's store request"),
        }
    }

    /// Removes `event_id` from the timeline under the room's lock, with its
    /// replies one level deep when `cascade_replies` is set and the edits and
    /// deletes of the target and its replies, answered in §10.3's order: the
    /// target, the replies by index, the changes by index (RFC §14.1
    /// `delete_event`, gap G-063). The counters stay as they are.
    fn delete_stored_event(&mut self, event_id: &EventId, cascade_replies: bool) -> Outcome {
        self.trace.push(format!("delete event {event_id}"));
        assert!(
            self.state(self.current).locks.contains(&self.room.id),
            "an event was deleted outside the room lock"
        );
        let Some(at) = self.timeline.iter().position(|event| &event.id == event_id) else {
            return Outcome::Store(StoreOutcome::Deleted(Vec::new()));
        };
        let target = self.timeline.remove(at);
        let replies = if cascade_replies {
            self.take_from_timeline(|event| event.parent_event_id.as_ref() == Some(event_id))
        } else {
            Vec::new()
        };
        let mut gone: Vec<EventId> = replies.iter().map(|reply| reply.id.clone()).collect();
        gone.push(event_id.clone());
        let changes = self.take_from_timeline(|event| {
            edits::mutation(event)
                .ok()
                .flatten()
                .is_some_and(|mutation| gone.contains(mutation.target_event_id()))
        });
        let removed = [target].into_iter().chain(replies).chain(changes).collect();
        Outcome::Store(StoreOutcome::Deleted(removed))
    }

    /// Takes the events of the timeline `belongs` holds for out of it, in
    /// index order.
    fn take_from_timeline(&mut self, belongs: impl Fn(&RoomEvent) -> bool) -> Vec<RoomEvent> {
        let (mut taken, left): (Vec<RoomEvent>, Vec<RoomEvent>) = mem::take(&mut self.timeline)
            .into_iter()
            .partition(|event| belongs(event));
        self.timeline = left;
        taken.sort_by_key(|event| event.index);
        taken
    }

    /// The room's timeline, the records stored `BLOCKED` left out (RFC
    /// §14.1 `list_events`).
    fn list_events(&mut self, room_id: &RoomId) -> Outcome {
        self.trace.push(format!("list_events {room_id}"));
        let received = self
            .timeline
            .iter()
            .filter(|event| event.status != EventStatus::Blocked)
            .cloned()
            .collect();
        Outcome::Store(StoreOutcome::Events(received))
    }

    /// Commits `event` with the room's counters, under the room's lock, with
    /// the update an edit or a delete makes to its target in the same write.
    fn commit(
        &mut self,
        event: RoomEvent,
        counters: RoomCounters,
        target: Option<&TargetUpdate>,
    ) -> Outcome {
        assert!(
            self.state(self.current).locks.contains(&event.room_id),
            "an event was committed outside the room lock"
        );
        assert_eq!(
            event.index, self.room.event_count,
            "the index is the room's count"
        );
        if let Some(update) = target {
            let Some(stored) = self
                .timeline
                .iter_mut()
                .find(|stored| stored.id == update.event_id)
            else {
                return Outcome::Failed(ErrorRecord::new("EventNotFound", "the target is gone"));
            };
            update.updates.apply(stored);
        }
        self.room.event_count = counters.event_count;
        self.room.latest_index = Some(counters.latest_index);
        self.room.timers.last_activity_at = counters.last_activity_at;
        self.update_room(counters.room);
        let updating = target.map_or_else(String::new, |update| {
            format!(" updating {}", update.event_id)
        });
        self.trace
            .push(format!("commit {}{updating}", describe(&event)));
        self.timeline.push(event);
        Outcome::Store(StoreOutcome::Committed)
    }

    /// Creates the room, unless its id is taken, by another call meanwhile
    /// among others.
    fn create_room(&mut self, room: Room) -> Outcome {
        self.trace
            .push(format!("store room {} {:?}", room.id, room.status));
        if let Some(created) = self.created_meanwhile.take() {
            self.room = created;
            self.room_exists = true;
        }
        if self.room_exists {
            return Outcome::Failed(ErrorRecord::new(
                "RoomExists",
                "a room with the id exists already",
            ));
        }
        self.room = room;
        self.room_exists = true;
        Outcome::Store(StoreOutcome::Stored)
    }

    /// The default parser, for every channel.
    fn handle_inbound(
        &mut self,
        channel_id: &ChannelId,
        message: InboundMessage,
        context: RoomContext,
        id: EventId,
    ) -> Outcome {
        self.trace.push(format!("handle_inbound {channel_id}"));
        let room_id = context.room.id.clone();
        self.parse_contexts.push(context);
        let declaration = self.config.channel(channel_id).unwrap().clone();
        let now = self.env.now();
        let event = default_inbound_event(&message, &declaration, room_id, id, now);
        self.parsed_messages.push(message);
        Outcome::Channel(ChannelOutcome::Parsed(Box::new(event)))
    }

    /// A channel's `on_event()`, as its closure answers.
    fn on_event(
        &mut self,
        binding: &ChannelBinding,
        event: &RoomEvent,
        context: RoomContext,
    ) -> Outcome {
        self.env.advance(self.delivery_duration);
        self.trace
            .push(format!("on_event {} <- {}", binding.channel_id, event.id));
        self.assert_in_turn(self.current, &binding.room_id, &event.id);
        let channel = self.channels.get_mut(&binding.channel_id).unwrap();
        let answer = (channel.on_event)(event, &context);
        self.on_event_contexts
            .push((binding.channel_id.clone(), context));
        output(answer)
    }

    /// A channel's `deliver()`, as its closure answers.
    fn deliver(&mut self, binding: &ChannelBinding, event: &RoomEvent) -> Outcome {
        self.env.advance(self.delivery_duration);
        self.trace
            .push(format!("deliver {} <- {}", binding.channel_id, event.id));
        self.assert_in_turn(self.current, &binding.room_id, &event.id);
        let channel = self.channels.get_mut(&binding.channel_id).unwrap();
        output((channel.deliver)(event))
    }

    fn context(&self, recent_events: usize) -> RoomContext {
        let skip = self.timeline.len().saturating_sub(recent_events);
        RoomContext {
            room: self.room.clone(),
            bindings: self.bindings.clone(),
            participants: self.participants.clone(),
            recent_events: self.timeline.iter().skip(skip).cloned().collect(),
            author_register: self.author_register.clone(),
        }
    }
}

/// The room a store request reads or writes.
fn store_room(request: &StoreRequest) -> &RoomId {
    match request {
        StoreRequest::LoadContext { room_id, .. }
        | StoreRequest::GetEventByIdempotencyKey { room_id, .. }
        | StoreRequest::PersistSideEffects { room_id, .. }
        | StoreRequest::UpdateRoom { room_id, .. }
        | StoreRequest::DeleteBinding { room_id, .. }
        | StoreRequest::DeleteEvent { room_id, .. }
        | StoreRequest::ListEvents { room_id } => room_id,
        StoreRequest::Commit { event, .. } => &event.room_id,
        StoreRequest::CreateRoom { room } => &room.id,
        StoreRequest::StoreBinding { binding } => &binding.room_id,
        StoreRequest::ResolveIdentity { .. }
        | StoreRequest::GetEvent { .. }
        | StoreRequest::UpdateEvent { .. } => panic!("not a room's store request"),
    }
}

/// A channel type's RFC name, read from its JSON form.
fn type_name(channel_type: &ChannelType) -> String {
    channel_type.as_str().to_owned()
}

/// A channel's answer as the driver completes its call with it.
fn output(answer: Result<ChannelOutput, ErrorRecord>) -> Outcome {
    match answer {
        Ok(output) => Outcome::Channel(ChannelOutcome::Output(Box::new(output))),
        Err(error) => Outcome::Failed(error),
    }
}

/// How the trace names an entry a call makes.
fn entry_name(entry: &Entry) -> String {
    match entry {
        Entry::Direct { room_id, event, .. } => {
            format!("send_event {room_id} {:?}", text_of(&event.content))
        }
        Entry::Message {
            room_id, message, ..
        } => format!("process_inbound {room_id} {:?}", text_of(&message.content)),
        Entry::Post { room_id, post, .. } => {
            format!("post {room_id} {:?}", text_of(&post.content))
        }
    }
}

/// A notice as the journal shows it.
fn journal_line(notice: &Notice) -> String {
    match notice {
        Notice::FrameworkEvent(event) => format!("emit {}", event_name(event)),
        Notice::AsyncHooks { trigger, .. } => format!("fire {}", trigger_name(*trigger)),
    }
}

/// A framework event's RFC name, read from its JSON form.
fn event_name(event: &FrameworkEvent) -> String {
    serde_json::to_value(event)
        .unwrap()
        .get("event")
        .and_then(serde_json::Value::as_str)
        .unwrap()
        .to_owned()
}

/// A trigger's RFC name, read from its JSON form.
fn trigger_name(trigger: HookTrigger) -> String {
    serde_json::to_value(trigger)
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned()
}

/// An agent's answer to every event it is asked: `reply`.
fn echo(id: &str, reply: &'static str) -> OnEvent {
    let id_owned = id.to_owned();
    Box::new(move |event, _| Ok(respond(&id_owned, event, reply)))
}

/// An output answering `event` with one text response from `channel_id`,
/// addressed to nobody in particular.
pub(crate) fn respond(channel_id: &str, event: &RoomEvent, text: &str) -> ChannelOutput {
    ChannelOutput::respond(vec![response(channel_id, event, text)])
}

/// A text response from `channel_id` to `event`, as a channel writes it.
pub(crate) fn response(channel_id: &str, event: &RoomEvent, text: &str) -> RoomEvent {
    RoomEvent::new(
        EventId::new(format!("{channel_id}_re_{}", event.id)).unwrap(),
        event.room_id.clone(),
        EventType::Message,
        EventSource::new(
            channel(channel_id),
            ChannelType::AI,
            EventDirection::Outbound,
        ),
        EventContent::text(text),
        event.created_at,
    )
}

/// An event a hook injects, from the framework's `system` source.
pub(crate) fn notice_event(id: &str, text: &str) -> RoomEvent {
    RoomEvent::new(
        EventId::new(id).unwrap(),
        room_id(),
        EventType::Message,
        EventSource::new(
            channel("system"),
            ChannelType::SYSTEM,
            EventDirection::Outbound,
        ),
        EventContent::text(text),
        Timestamp::UNIX_EPOCH,
    )
}

/// `index:source:status[:blocked_by]:text` for an event.
pub(crate) fn describe(event: &RoomEvent) -> String {
    let status = match event.status {
        EventStatus::Pending => "pending",
        EventStatus::Delivered => "delivered",
        EventStatus::Read => "read",
        EventStatus::Failed => "failed",
        EventStatus::Blocked => "blocked",
    };
    let text = text_of(&event.content);
    match &event.blocked_by {
        Some(blocked_by) => format!(
            "#{} {} {status}:{blocked_by} {text:?}",
            event.index, event.source.channel_id
        ),
        None => format!(
            "#{} {} {status} {text:?}",
            event.index, event.source.channel_id
        ),
    }
}

/// Makes the binding `id` read-only.
pub(crate) fn read_only(binding: &mut ChannelBinding) {
    binding.access = Access::ReadOnly;
}
