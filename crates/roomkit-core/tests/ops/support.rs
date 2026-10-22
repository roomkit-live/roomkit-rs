//! A driver for the trace tests: one room in memory, channels and hooks as
//! closures, and a trace of every effect it executes.
//!
//! It also checks what every driver must hold for the protocol: a commit
//! happens under the room's lock, the lock is never taken twice, and it is
//! released when the operation ends.

use std::collections::BTreeMap;
use std::sync::Arc;

use roomkit_core::model::{
    Access, ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelMediaType, ChannelOutput, ChannelType, ErrorRecord, EventContent, EventDirection,
    EventId, EventSource, EventStatus, EventType, InboundMessage, Observation, Room, RoomContext,
    RoomEvent, RoomId, Task, Timestamp,
};
use roomkit_core::ops::{
    ChannelDeclaration, Environment, FrameworkEvent, Inbound, InboundError, InboundResult,
    KitConfig, Next, Notice, Operation, Outcome, Request, SequentialEnvironment,
    default_inbound_event,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookId, HookSpec, HookTrigger};
use roomkit_core::rules::transcoding::text_of;

/// What a channel answers to `on_event()`.
pub(crate) type OnEvent =
    Box<dyn FnMut(&RoomEvent, &RoomContext) -> Result<ChannelOutput, ErrorRecord>>;
/// What a channel answers to `deliver()`.
pub(crate) type Deliver = Box<dyn FnMut(&RoomEvent) -> Result<ChannelOutput, ErrorRecord>>;
/// What a SYNC hook produces.
pub(crate) type Hook = Box<dyn FnMut(&RoomEvent) -> HookOutcome<RoomEvent>>;

struct Channel {
    on_event: OnEvent,
    deliver: Deliver,
}

/// The room, its channels and hooks, and what the driver did.
pub(crate) struct Sim {
    pub(crate) env: SequentialEnvironment,
    pub(crate) config: KitConfig,
    pub(crate) room: Room,
    pub(crate) bindings: Vec<ChannelBinding>,
    pub(crate) timeline: Vec<RoomEvent>,
    pub(crate) tasks: Vec<Task>,
    pub(crate) observations: Vec<Observation>,
    pub(crate) trace: Vec<String>,
    pub(crate) notices: Vec<Notice>,
    channels: BTreeMap<ChannelId, Channel>,
    hooks: BTreeMap<HookId, Hook>,
    lock_held: bool,
    /// Whether the store finds the room.
    pub(crate) room_exists: bool,
    /// A channel the hooks mute while they run, as a moderation hook would.
    pub(crate) hooks_mute: Option<ChannelId>,
    /// The contexts the receiving channels parsed messages with.
    pub(crate) parse_contexts: Vec<RoomContext>,
}

pub(crate) fn channel(id: &str) -> ChannelId {
    ChannelId::new(id).unwrap()
}

pub(crate) fn room_id() -> RoomId {
    RoomId::new("room_1").unwrap()
}

impl Sim {
    /// An active, empty room `room_1`.
    pub(crate) fn new() -> Self {
        Self {
            env: SequentialEnvironment::new(
                Timestamp::from_unix_millis(1_792_677_780_000).unwrap(),
            ),
            config: KitConfig::new(),
            room: Room::new(room_id(), Timestamp::UNIX_EPOCH),
            bindings: Vec::new(),
            timeline: Vec::new(),
            tasks: Vec::new(),
            observations: Vec::new(),
            trace: Vec::new(),
            notices: Vec::new(),
            channels: BTreeMap::new(),
            hooks: BTreeMap::new(),
            lock_held: false,
            room_exists: true,
            hooks_mute: None,
            parse_contexts: Vec::new(),
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
        let id_owned = id.to_owned();
        self.agent(
            id,
            Box::new(move |event, _| Ok(respond(&id_owned, event, reply))),
        )
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
        let capabilities = ChannelCapabilities {
            media_types: [ChannelMediaType::Text].into(),
            ..ChannelCapabilities::default()
        };
        self.config
            .register_channel(ChannelDeclaration {
                channel_id: channel(id),
                channel_type,
                category,
                direction: ChannelDirection::Bidirectional,
                capabilities: capabilities.clone(),
                provider_name: None,
            })
            .unwrap();
        self.channels.insert(
            channel(id),
            Channel {
                on_event,
                deliver: Box::new(|_| Ok(ChannelOutput::default())),
            },
        );
        let mut binding = ChannelBinding::new(
            channel(id),
            room_id(),
            channel_type,
            category,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        );
        binding.capabilities = capabilities;
        self.bindings.push(binding);
        self.bindings.last_mut().unwrap()
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

    /// A text message from `+15551234567` on `channel_id`.
    pub(crate) fn message(channel_id: &str, text: &str) -> InboundMessage {
        InboundMessage::new(
            channel(channel_id),
            "+15551234567",
            EventContent::text(text),
        )
    }

    /// Runs `message` into the room through the inbound operation.
    pub(crate) fn send(&mut self, message: InboundMessage) -> Result<InboundResult, InboundError> {
        let config = Arc::new(self.config.clone());
        self.run(Inbound::new(config, room_id(), message))
    }

    /// Runs `operation` to its end, executing every effect it asks for.
    pub(crate) fn run<O: Operation>(&mut self, mut operation: O) -> O::Output {
        loop {
            let step = operation.poll(&mut self.env);
            self.notices.extend(step.notices);
            match step.next {
                Next::Ready(output) => {
                    assert!(!self.lock_held, "the operation ended holding the room lock");
                    return output;
                }
                Next::Waiting => panic!("a driver that completes every effect never waits"),
                Next::Effects(effects) => {
                    for effect in effects {
                        let outcome = self.execute(effect.request);
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
            .map(|event| {
                serde_json::to_value(event)
                    .unwrap()
                    .get("event")
                    .and_then(serde_json::Value::as_str)
                    .unwrap()
                    .to_owned()
            })
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

    /// The timeline as `index:source:status[:blocked_by]:text` lines.
    pub(crate) fn timeline_lines(&self) -> Vec<String> {
        self.timeline.iter().map(describe).collect()
    }

    fn execute(&mut self, request: Request) -> Outcome {
        match request {
            Request::AcquireLock { .. } | Request::ReleaseLock { .. } => self.execute_lock(request),
            Request::HandleInbound { .. } | Request::OnEvent { .. } | Request::Deliver { .. } => {
                self.execute_channel(request)
            }
            Request::RunHook { hook, event, .. } => {
                let name = self.config.hooks.get(hook).unwrap().name.clone();
                self.trace.push(format!("hook {name}"));
                let hook = self.hooks.get_mut(&hook).unwrap();
                let outcome = hook(&event);
                if let Some(muted) = &self.hooks_mute {
                    let binding = self
                        .bindings
                        .iter_mut()
                        .find(|bound| &bound.channel_id == muted);
                    binding.unwrap().muted = true;
                }
                Outcome::Hook(Box::new(outcome))
            }
            Request::LoadContext { .. }
            | Request::LoadBindings { .. }
            | Request::FindByIdempotencyKey { .. }
            | Request::Commit { .. }
            | Request::PersistSideEffects { .. }
            | Request::CreateRoom { .. }
            | Request::UpdateRoom { .. }
            | Request::StoreBinding { .. } => self.execute_store(request),
        }
    }

    /// The room lock: never taken twice, released only when held.
    fn execute_lock(&mut self, request: Request) -> Outcome {
        if let Request::AcquireLock { room_id } = request {
            assert!(!self.lock_held, "the room lock was taken twice");
            self.lock_held = true;
            self.trace.push(format!("lock {room_id}"));
            return Outcome::Locked;
        }
        assert!(
            self.lock_held,
            "the room lock was released without being held"
        );
        self.lock_held = false;
        self.trace.push(format!("release {}", room_id()));
        Outcome::Released
    }

    /// The store: one room, its bindings, its timeline and side effects.
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
            Request::Commit { event, counters } => {
                assert!(
                    self.lock_held,
                    "an event was committed outside the room lock"
                );
                assert_eq!(
                    event.index, self.room.event_count,
                    "the index is the room's count"
                );
                self.room.event_count = counters.event_count;
                self.room.latest_index = Some(counters.latest_index);
                self.room.timers.last_activity_at = counters.last_activity_at;
                self.trace.push(format!("commit {}", describe(&event)));
                self.timeline.push(event);
                Outcome::Committed
            }
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
            Request::CreateRoom { room } | Request::UpdateRoom { room } => {
                self.trace
                    .push(format!("store room {} {:?}", room.id, room.status));
                self.room = room;
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
            | Request::HandleInbound { .. }
            | Request::OnEvent { .. }
            | Request::Deliver { .. }
            | Request::RunHook { .. } => panic!("not a store request"),
        }
    }

    /// The channels: the default parser, and each channel's closures.
    fn execute_channel(&mut self, request: Request) -> Outcome {
        let answer = match request {
            Request::HandleInbound {
                channel_id,
                message,
                context,
            } => {
                self.trace.push(format!("handle_inbound {channel_id}"));
                self.parse_contexts.push(*context);
                let declaration = self.config.channel(&channel_id).unwrap().clone();
                let id = self.env.new_event_id();
                let now = self.env.now();
                let event = default_inbound_event(&message, &declaration, room_id(), id, now);
                return Outcome::Parsed(Box::new(event));
            }
            Request::OnEvent {
                binding,
                event,
                context,
            } => {
                self.trace
                    .push(format!("on_event {} <- {}", binding.channel_id, event.id));
                let channel = self.channels.get_mut(&binding.channel_id).unwrap();
                (channel.on_event)(&event, &context)
            }
            Request::Deliver { binding, event, .. } => {
                self.trace
                    .push(format!("deliver {} <- {}", binding.channel_id, event.id));
                let channel = self.channels.get_mut(&binding.channel_id).unwrap();
                (channel.deliver)(&event)
            }
            Request::AcquireLock { .. }
            | Request::ReleaseLock { .. }
            | Request::LoadContext { .. }
            | Request::LoadBindings { .. }
            | Request::FindByIdempotencyKey { .. }
            | Request::Commit { .. }
            | Request::PersistSideEffects { .. }
            | Request::CreateRoom { .. }
            | Request::UpdateRoom { .. }
            | Request::StoreBinding { .. }
            | Request::RunHook { .. } => panic!("not a channel request"),
        };
        match answer {
            Ok(output) => Outcome::Output(Box::new(output)),
            Err(error) => Outcome::Failed(error),
        }
    }

    fn context(&self, recent_events: usize) -> RoomContext {
        let skip = self.timeline.len().saturating_sub(recent_events);
        RoomContext {
            room: self.room.clone(),
            bindings: self.bindings.clone(),
            participants: Vec::new(),
            recent_events: self.timeline.iter().skip(skip).cloned().collect(),
        }
    }
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
