//! A driver for the trace tests: rooms in memory, channels and hooks as
//! closures, and a trace of every effect it executes.
//!
//! It also checks what every driver must hold for the protocol: a commit
//! happens under its room's lock, a lock or a lane is never taken twice,
//! and every lock and lane taken is let go when the operation ends. For an
//! inbound operation it checks that the room's lane is held whenever the
//! room's lock is taken, an event committed or a channel called, and that
//! the lock is free while awaited observers run, so that they may take it.
//! An inbound operation never holds two lanes at once, the lane of a room
//! it was routed to included.
//!
//! The router is scripted: each `Route` takes the next answer of `routes`,
//! a new room once they are spent.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::mem;
use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{
    Access, ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelMediaType, ChannelOutput, ChannelType, ErrorRecord, EventContent, EventDirection,
    EventId, EventSource, EventStatus, EventType, EventUpdates, Identity, InboundMessage,
    Observation, Participant, ParticipantId, Room, RoomContext, RoomEvent, RoomId, RoomStatus,
    Task, Timestamp,
};
use roomkit_core::ops::{
    ChannelDeclaration, DeleteEvent, Environment, FrameworkEvent, Inbound, InboundError,
    InboundResult, InboundTarget, KitConfig, MutationError, Next, Notice, Operation, Outcome,
    Request, RoomCounters, SequentialEnvironment, TargetUpdate, UpdateEvent, default_inbound_event,
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

/// A change the hooks make in the room while they run.
pub(crate) enum HookChange {
    /// They mute the channel, as a moderation hook would.
    Mute(ChannelId),
    /// They close the room, as a hook that ends the conversation would.
    CloseRoom,
}

struct Channel {
    on_event: OnEvent,
    deliver: Deliver,
}

/// What the driver runs: an inbound operation holds a lane for what it
/// does in a room, another operation holds none.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Running {
    Inbound,
    Other,
}

/// A room of the store other than `room_1`.
struct StoredRoom {
    room: Room,
    exists: bool,
    bindings: Vec<ChannelBinding>,
    participants: Vec<Participant>,
    timeline: Vec<RoomEvent>,
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
    pub(crate) tasks: Vec<Task>,
    pub(crate) observations: Vec<Observation>,
    /// Every effect executed, in order.
    pub(crate) trace: Vec<String>,
    /// The effects executed and the notices performed, in the order the
    /// driver met them: a notice reads `emit <framework event>` or
    /// `fire <trigger>`.
    pub(crate) journal: Vec<String>,
    pub(crate) notices: Vec<Notice>,
    channels: BTreeMap<ChannelId, Channel>,
    hooks: BTreeMap<HookId, Hook>,
    /// The channel each awaited observer attaches to its room as it runs.
    observer_attaches: BTreeMap<HookId, ChannelId>,
    others: BTreeMap<RoomId, StoredRoom>,
    locks_held: BTreeSet<RoomId>,
    lanes_held: BTreeSet<RoomId>,
    /// What runs: an inbound operation must hold the lane of every room it
    /// acts in.
    running: Running,
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
    /// The effect that fails, named by the start of its trace line: a store
    /// write (`update room`, `delete binding sms`, `store room`), a room
    /// lock (`lock room_1`), a room's lane (`enter lane room_1`), the
    /// router (`route sms`), an identity read (`resolve_identity`) or
    /// awaited observers (`await on_room_created`).
    /// It is answered `Failed` and changes nothing.
    pub(crate) failing_effect: Option<&'static str>,
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
            tasks: Vec::new(),
            observations: Vec::new(),
            trace: Vec::new(),
            journal: Vec::new(),
            notices: Vec::new(),
            channels: BTreeMap::new(),
            hooks: BTreeMap::new(),
            observer_attaches: BTreeMap::new(),
            others: BTreeMap::new(),
            locks_held: BTreeSet::new(),
            lanes_held: BTreeSet::new(),
            running: Running::Other,
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
            failing_effect: None,
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
            ChannelType::Websocket,
            ChannelCategory::Transport,
            Box::new(|_, _| Ok(ChannelOutput::default())),
        )
    }

    /// A text transport bound read-write, answering with `on_event`.
    pub(crate) fn transport_with(&mut self, id: &str, on_event: OnEvent) -> &mut ChannelBinding {
        self.bind(
            id,
            ChannelType::Websocket,
            ChannelCategory::Transport,
            on_event,
        )
    }

    /// An agent bound read-write, answering with `on_event`.
    pub(crate) fn agent(&mut self, id: &str, on_event: OnEvent) -> &mut ChannelBinding {
        self.bind(id, ChannelType::Ai, ChannelCategory::Intelligence, on_event)
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
                ChannelType::Ai,
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
        self.running = Running::Inbound;
        let result = self.run(Inbound::new(config, target, organization, message));
        self.running = Running::Other;
        result
    }

    /// Scripts the router's next answer: `room`, or none.
    pub(crate) fn routes_to(&mut self, room: Option<&str>) {
        self.routes
            .push_back(Ok(room.map(|room| RoomId::new(room).unwrap())));
    }

    /// Checks that an inbound operation holds the lane of `room_id` before
    /// it does `what` there: the lane comes before the lock (RFC §10.2).
    fn assert_in_lane(&self, room_id: &RoomId, what: &str) {
        if self.running == Running::Inbound {
            assert!(
                self.lanes_held.contains(room_id),
                "an inbound operation {what} outside the room's lane"
            );
        }
    }

    /// Runs `operation` to its end, executing every effect it asks for.
    pub(crate) fn run<O: Operation>(&mut self, mut operation: O) -> O::Output {
        loop {
            let step = operation.poll(&mut self.env);
            self.journal.extend(step.notices.iter().map(journal_line));
            self.notices.extend(step.notices);
            match step.next {
                Next::Ready(output) => {
                    assert!(
                        self.locks_held.is_empty(),
                        "the operation ended holding the lock of {:?}",
                        self.locks_held
                    );
                    assert!(
                        self.lanes_held.is_empty(),
                        "the operation ended holding the lane of {:?}",
                        self.lanes_held
                    );
                    return output;
                }
                Next::Waiting => panic!("a driver that completes every effect never waits"),
                Next::Effects(effects) => {
                    for effect in effects {
                        let traced = self.trace.len();
                        let outcome = self.execute(effect.request);
                        self.journal.extend(self.trace.iter().skip(traced).cloned());
                        operation.complete(effect.id, outcome).unwrap();
                    }
                }
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

    fn execute(&mut self, request: Request) -> Outcome {
        if let Some(refused) = self.refused(&request) {
            return refused;
        }
        match request {
            Request::AcquireLock { room_id } => self.acquire(room_id),
            Request::ReleaseLock { room_id } => self.release(&room_id),
            Request::EnterLane { room_id } => self.enter_lane(room_id),
            Request::LeaveLane { room_id } => self.leave_lane(&room_id),
            Request::HandleInbound {
                channel_id,
                message,
                context,
                id,
            } => self.handle_inbound(&channel_id, message, *context, id),
            Request::OnEvent {
                binding,
                event,
                context,
            } => self.on_event(&binding, &event, *context),
            Request::Deliver { binding, event, .. } => self.deliver(&binding, &event),
            Request::RunHook { hook, event, .. } => self.execute_hook(hook, &event),
            Request::RunObservers {
                trigger,
                hooks,
                event,
                context,
            } => {
                self.awaited_contexts.push(*context);
                self.execute_observers(trigger, &hooks, &event)
            }
            Request::ResolveIdentity {
                channel_type,
                address,
                organization_id,
            } => self.resolve_identity(channel_type, &address, organization_id.as_deref()),
            Request::Route { query } => self.execute_route(*query),
            Request::GetEvent { event_id } => self.get_event(&event_id),
            Request::UpdateEvent { event_id, updates } => {
                let room = self.room_of_event(&event_id);
                self.on_room(&room, |sim| sim.update_stored_event(&event_id, &updates))
            }
            Request::LoadContext { .. }
            | Request::LoadBindings { .. }
            | Request::FindByIdempotencyKey { .. }
            | Request::Commit { .. }
            | Request::PersistSideEffects { .. }
            | Request::CreateRoom { .. }
            | Request::UpdateRoom { .. }
            | Request::StoreBinding { .. }
            | Request::DeleteBinding { .. }
            | Request::DeleteEvent { .. } => {
                let room = store_room(&request).clone();
                self.on_room(&room, |sim| sim.execute_store(request))
            }
        }
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
        Outcome::Event(found.map(Box::new))
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
        Outcome::Stored
    }

    /// The identity of `organization` that uses `address` on channels of
    /// `channel_type`, if any.
    fn resolve_identity(
        &mut self,
        channel_type: ChannelType,
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
        Outcome::Resolved(identity.map(|identity| identity.id.clone()))
    }

    /// The scripted router's next answer; no room once they are spent. The
    /// router reads no lock and holds no lane (RFC §10.4).
    fn execute_route(&mut self, query: RouteQuery) -> Outcome {
        self.trace
            .push(format!("route {} {}", query.channel_id, query.sender_id));
        assert!(
            self.locks_held.is_empty() && self.lanes_held.is_empty(),
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
    fn execute_hook(&mut self, hook: HookId, event: &RoomEvent) -> Outcome {
        let name = self.config.hooks.get(hook).unwrap().name.clone();
        self.trace.push(format!("hook {name}"));
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
            None => {}
        }
        Outcome::Hook(Box::new(outcome))
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
            !self.locks_held.contains(&event.room_id),
            "awaited observers ran under the room lock, which they may need"
        );
        self.assert_in_lane(&event.room_id, "awaited observers");
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
        Outcome::Observed
    }

    /// The failure of the effect `failing_effect` names, traced as failed.
    fn refused(&mut self, request: &Request) -> Option<Outcome> {
        let failing = self.failing_effect?;
        let store_down = || ErrorRecord::new("StoreUnavailable", "the store is down");
        let (target, error) = match request {
            Request::UpdateRoom { room_id, .. } => (format!("update room {room_id}"), store_down()),
            Request::StoreBinding { binding } => (
                format!("store binding {}", binding.channel_id),
                store_down(),
            ),
            Request::DeleteBinding { channel_id, .. } => {
                (format!("delete binding {channel_id}"), store_down())
            }
            Request::CreateRoom { room } => (format!("store room {}", room.id), store_down()),
            Request::Commit { .. } => ("commit".to_owned(), store_down()),
            Request::GetEvent { event_id } => (format!("get_event {event_id}"), store_down()),
            Request::UpdateEvent { event_id, .. } => {
                (format!("update event {event_id}"), store_down())
            }
            Request::DeleteEvent { event_id, .. } => {
                (format!("delete event {event_id}"), store_down())
            }
            Request::AcquireLock { room_id } => (
                format!("lock {room_id}"),
                ErrorRecord::new("LockUnavailable", "the lock manager is down"),
            ),
            Request::RunObservers { trigger, .. } => (
                format!("await {}", trigger_name(*trigger)),
                ErrorRecord::new("HooksUnavailable", "the observers could not be run"),
            ),
            Request::ResolveIdentity { address, .. } => {
                (format!("resolve_identity {address}"), store_down())
            }
            Request::Route { query } => (
                format!("route {}", query.channel_id),
                ErrorRecord::new("RouterError", "the CRM is unreachable"),
            ),
            Request::EnterLane { room_id } => (
                format!("enter lane {room_id}"),
                ErrorRecord::new("LaneUnavailable", "the lane manager is down"),
            ),
            Request::ReleaseLock { .. }
            | Request::LeaveLane { .. }
            | Request::LoadContext { .. }
            | Request::LoadBindings { .. }
            | Request::FindByIdempotencyKey { .. }
            | Request::PersistSideEffects { .. }
            | Request::HandleInbound { .. }
            | Request::OnEvent { .. }
            | Request::Deliver { .. }
            | Request::RunHook { .. } => return None,
        };
        if !target.starts_with(failing) {
            return None;
        }
        self.trace.push(format!("{target} failed"));
        Some(Outcome::Failed(error))
    }

    /// Takes a room's lock, which is never taken twice. A sender recorded
    /// meanwhile on the room is recorded just before the lock is granted, as
    /// a concurrent message that held it first would have.
    fn acquire(&mut self, room_id: RoomId) -> Outcome {
        self.trace.push(format!("lock {room_id}"));
        self.assert_in_lane(&room_id, "took the room lock");
        self.record_meanwhile(&room_id);
        assert!(
            self.locks_held.insert(room_id),
            "the room lock was taken twice"
        );
        Outcome::Locked
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

    /// Releases a room's lock, which must be held.
    fn release(&mut self, room_id: &RoomId) -> Outcome {
        self.trace.push(format!("release {room_id}"));
        assert!(
            self.locks_held.remove(room_id),
            "the room lock was released without being held"
        );
        if self.release_fails {
            return Outcome::Failed(ErrorRecord::new("LockLost", "the lock's lease was lost"));
        }
        Outcome::Released
    }

    /// Enters a room's delivery lane, which is never entered twice; an
    /// inbound operation holds one lane at a time.
    fn enter_lane(&mut self, room_id: RoomId) -> Outcome {
        self.trace.push(format!("enter lane {room_id}"));
        assert!(
            self.running == Running::Other || self.lanes_held.is_empty(),
            "an inbound operation entered a second lane"
        );
        assert!(
            self.lanes_held.insert(room_id),
            "the room's lane was entered twice"
        );
        Outcome::Entered
    }

    /// Leaves a room's delivery lane, which must be held.
    fn leave_lane(&mut self, room_id: &RoomId) -> Outcome {
        self.trace.push(format!("leave lane {room_id}"));
        assert!(
            self.lanes_held.remove(room_id),
            "the room's lane was left without being held"
        );
        if self.leave_fails {
            return Outcome::Failed(ErrorRecord::new("LaneLost", "the lane was lost"));
        }
        Outcome::Left
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
    }

    /// The store, for the room the fields hold: its bindings, participants,
    /// timeline and side effects.
    fn execute_store(&mut self, request: Request) -> Outcome {
        match request {
            Request::LoadContext {
                room_id,
                recent_events,
            } => {
                self.trace.push(format!("load_context {room_id}"));
                if !self.room_exists {
                    return Outcome::RoomNotFound;
                }
                Outcome::Context(Box::new(self.context(recent_events)))
            }
            Request::LoadBindings { room_id } => {
                self.trace.push(format!("load_bindings {room_id}"));
                Outcome::Bindings(self.bindings.clone())
            }
            Request::FindByIdempotencyKey {
                channel_id, key, ..
            } => {
                self.trace.push(format!("find_key {key}"));
                let found = self
                    .timeline
                    .iter()
                    .find(|event| {
                        event.idempotency_key.as_deref() == Some(key.as_str())
                            && event.source.channel_id == channel_id
                    })
                    .cloned()
                    .map(Box::new);
                Outcome::Idempotent(found)
            }
            Request::Commit {
                event,
                counters,
                target,
            } => self.commit(event, &counters, target.as_ref()),
            Request::DeleteEvent {
                event_id,
                cascade_replies,
                ..
            } => self.delete_stored_event(&event_id, cascade_replies),
            Request::PersistSideEffects {
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
                Outcome::Persisted
            }
            Request::CreateRoom { room } => self.create_room(room),
            Request::UpdateRoom { room_id, updates } => {
                assert!(
                    self.locks_held.contains(&room_id) || updates.status.is_none(),
                    "a status was written outside the room lock"
                );
                updates.apply(&mut self.room);
                self.trace
                    .push(format!("update room {room_id} {:?}", self.room.status));
                Outcome::Stored
            }
            Request::DeleteBinding { channel_id, .. } => {
                self.trace.push(format!("delete binding {channel_id}"));
                self.bindings.retain(|bound| bound.channel_id != channel_id);
                Outcome::Stored
            }
            Request::StoreBinding { binding } => {
                self.trace
                    .push(format!("store binding {}", binding.channel_id));
                self.bindings
                    .retain(|bound| bound.channel_id != binding.channel_id);
                self.bindings.push(binding);
                Outcome::Stored
            }
            Request::AcquireLock { .. }
            | Request::ReleaseLock { .. }
            | Request::EnterLane { .. }
            | Request::LeaveLane { .. }
            | Request::HandleInbound { .. }
            | Request::OnEvent { .. }
            | Request::Deliver { .. }
            | Request::RunHook { .. }
            | Request::RunObservers { .. }
            | Request::ResolveIdentity { .. }
            | Request::Route { .. }
            | Request::GetEvent { .. }
            | Request::UpdateEvent { .. } => panic!("not a room's store request"),
        }
    }

    /// Removes `event_id` from the timeline under the room's lock, with the
    /// replies of its thread when `cascade_replies` is set and the edits and
    /// deletes of what it removes (RFC §14.1 `delete_event`, gap G-063). The
    /// counters stay as they are.
    fn delete_stored_event(&mut self, event_id: &EventId, cascade_replies: bool) -> Outcome {
        self.trace.push(format!("delete event {event_id}"));
        assert!(
            self.locks_held.contains(&self.room.id),
            "an event was deleted outside the room lock"
        );
        let Some(at) = self.timeline.iter().position(|event| &event.id == event_id) else {
            return Outcome::Deleted(Vec::new());
        };
        let mut removed = vec![self.timeline.remove(at)];
        let mut gone = vec![event_id.clone()];
        while let Some(next) = self.timeline.iter().position(|event| {
            let reply = cascade_replies
                && event
                    .parent_event_id
                    .as_ref()
                    .is_some_and(|parent| gone.contains(parent));
            let change = edits::mutation(event)
                .ok()
                .flatten()
                .is_some_and(|mutation| gone.contains(mutation.target_event_id()));
            reply || change
        }) {
            let next = self.timeline.remove(next);
            gone.push(next.id.clone());
            removed.push(next);
        }
        Outcome::Deleted(removed)
    }

    /// Commits `event` with the room's counters, under the room's lock, with
    /// the update an edit or a delete makes to its target in the same write.
    fn commit(
        &mut self,
        event: RoomEvent,
        counters: &RoomCounters,
        target: Option<&TargetUpdate>,
    ) -> Outcome {
        assert!(
            self.locks_held.contains(&event.room_id),
            "an event was committed outside the room lock"
        );
        self.assert_in_lane(&event.room_id, "committed an event");
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
        counters.room.apply(&mut self.room);
        let updating = target.map_or_else(String::new, |update| {
            format!(" updating {}", update.event_id)
        });
        self.trace
            .push(format!("commit {}{updating}", describe(&event)));
        self.timeline.push(event);
        Outcome::Committed
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
        Outcome::Stored
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
        Outcome::Parsed(Box::new(event))
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
        self.assert_in_lane(&binding.room_id, "called on_event()");
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
        self.assert_in_lane(&binding.room_id, "called deliver()");
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
        }
    }
}

/// The room a store request reads or writes.
fn store_room(request: &Request) -> &RoomId {
    match request {
        Request::LoadContext { room_id, .. }
        | Request::LoadBindings { room_id }
        | Request::FindByIdempotencyKey { room_id, .. }
        | Request::PersistSideEffects { room_id, .. }
        | Request::UpdateRoom { room_id, .. }
        | Request::DeleteBinding { room_id, .. }
        | Request::DeleteEvent { room_id, .. } => room_id,
        Request::Commit { event, .. } => &event.room_id,
        Request::CreateRoom { room } => &room.id,
        Request::StoreBinding { binding } => &binding.room_id,
        Request::AcquireLock { .. }
        | Request::ReleaseLock { .. }
        | Request::EnterLane { .. }
        | Request::LeaveLane { .. }
        | Request::HandleInbound { .. }
        | Request::OnEvent { .. }
        | Request::Deliver { .. }
        | Request::RunHook { .. }
        | Request::RunObservers { .. }
        | Request::ResolveIdentity { .. }
        | Request::Route { .. }
        | Request::GetEvent { .. }
        | Request::UpdateEvent { .. } => panic!("not a room's store request"),
    }
}

/// A channel type's RFC name, read from its JSON form.
fn type_name(channel_type: ChannelType) -> String {
    serde_json::to_value(channel_type)
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned()
}

/// A channel's answer as the driver completes its call with it.
fn output(answer: Result<ChannelOutput, ErrorRecord>) -> Outcome {
    match answer {
        Ok(output) => Outcome::Output(Box::new(output)),
        Err(error) => Outcome::Failed(error),
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
            ChannelType::Ai,
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
            ChannelType::System,
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
