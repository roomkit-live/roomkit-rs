//! The kit: the channels, hooks and store of a deployment, and the calls
//! that run the core's operations against them.
//!
//! - this module: the kit itself and the inbound pipeline;
//! - `registry`: registering and unregistering channels and hooks;
//! - `rooms`: the room lifecycle and channel bindings;
//! - `events`: the host's direct changes to stored events;
//! - `inject`: direct injection and records committed outside the
//!   pipeline;
//! - `timers`: applying room timers, on demand or in the background;
//! - `background`: waiting for the kit's background work, and ending it;
//! - `weak`: the kit held without keeping it alive;
//! - `organization`: the kit acting for one organization;
//! - `scope`: the organization a call acts for, which every call is
//!   written once for.

mod background;
mod events;
mod inject;
mod organization;
mod post;
mod registry;
mod rooms;
mod scope;
#[cfg(test)]
mod tests;
mod timers;
mod weak;

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use roomkit_core::model::{ChannelId, EventSource, InboundMessage, RoomEvent, RoomId};
use roomkit_core::ops::{
    Entry, Environment, FrameworkEvent, Inbound, InboundResult, InboundTarget, KitConfig,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookId, HookSpec, HookTrigger};
use tokio::sync::broadcast;

pub use self::organization::OrganizationKit;
use self::scope::Scope;
pub use self::timers::TimerSweeper;
pub use self::weak::WeakRoomKit;
use crate::channel::DynChannel;
use crate::context;
use crate::cursors::RoomCursors;
use crate::driver;
use crate::environment::SystemEnvironment;
use crate::error::KitError;
use crate::hooks::HookHandler;
use crate::locks::RoomMutexes;
use crate::queues::{RoomLanes, RoomTickets};
use crate::router::{DefaultRouter, DynRouter, InboundRoomRouter};
use crate::store::{ConversationStore, InMemoryStore};
use crate::tasks::Tasks;
use crate::tools::ToolHandler;

/// How many framework events a slow subscriber may fall behind before it
/// misses some.
const FRAMEWORK_EVENT_BACKLOG: usize = 1024;

/// The kit's call that runs a routed message, as an error names it.
const PROCESS_INBOUND: &str = "process_inbound";

/// The kit's call that runs a message into a named room, as an error names
/// it.
const PROCESS_INBOUND_IN: &str = "process_inbound_in";

/// A RoomKit deployment: its channels, its hooks and its store.
///
/// Cloning a kit is cheap; every clone is the same kit. A hook or a channel
/// the kit holds, which would make a cycle with a clone, holds the kit
/// weakly ([`downgrade`](Self::downgrade)).
///
/// ```no_run
/// # async fn example(sms: impl roomkit::Channel) -> Result<(), Box<dyn std::error::Error>> {
/// use roomkit::RoomKit;
/// use roomkit::model::{ChannelId, EventContent, InboundMessage, RoomId};
///
/// let kit = RoomKit::new();
/// kit.register_channel(sms)?;
/// let room = RoomId::new("room_1")?;
/// let channel = ChannelId::new("sms")?;
/// kit.create_room(&room).await?;
/// kit.attach(&room, &channel).await?;
/// let message = InboundMessage::new(channel, "+15551234567", EventContent::text("Bonjour"));
/// let result = kit.process_inbound_in(&room, message).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct RoomKit<S = InMemoryStore> {
    pub(crate) inner: Arc<Inner<S>>,
}

impl<S> Clone for RoomKit<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// The kit's shared state; the driver reads it while it runs an operation.
pub(crate) struct Inner<S> {
    config: RwLock<Arc<KitConfig>>,
    channels: RwLock<HashMap<ChannelId, Arc<dyn DynChannel>>>,
    hooks: RwLock<HashMap<HookId, Arc<dyn HookHandler>>>,
    /// The handlers of the tool hooks (RFC §9.3), whose payload is no event.
    tool_hooks: RwLock<HashMap<HookId, ToolHandler>>,
    router: RwLock<Arc<dyn DynRouter<S>>>,
    pub(crate) store: S,
    pub(crate) locks: RoomMutexes,
    pub(crate) lanes: RoomLanes,
    pub(crate) tickets: RoomTickets,
    /// The indexes whose turn in their room's lane is over, waiting for the
    /// room's delivery cursor (gap G-091).
    pub(crate) cursors: RoomCursors,
    /// The tasks the kit runs in the background: observers, inbound runs.
    pub(crate) tasks: Arc<Tasks>,
    /// The environment the runs started from now on read: each run keeps
    /// the one it started with.
    environment: RwLock<Arc<dyn Environment + Send + Sync>>,
    events: broadcast::Sender<FrameworkEvent>,
}

impl<S> std::fmt::Debug for Inner<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Inner").finish_non_exhaustive()
    }
}

impl RoomKit<InMemoryStore> {
    /// A kit over an in-memory store, with the system clock and random ids.
    pub fn new() -> Self {
        Self::with_store(InMemoryStore::new())
    }
}

impl Default for RoomKit<InMemoryStore> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// A kit over `store`, with the system clock and random ids.
    pub fn with_store(store: S) -> Self {
        let (events, _) = broadcast::channel(FRAMEWORK_EVENT_BACKLOG);
        Self {
            inner: Arc::new(Inner {
                config: RwLock::new(Arc::new(KitConfig::new())),
                channels: RwLock::new(HashMap::new()),
                hooks: RwLock::new(HashMap::new()),
                tool_hooks: RwLock::new(HashMap::new()),
                router: RwLock::new(Arc::new(DefaultRouter)),
                store,
                locks: RoomMutexes::default(),
                lanes: RoomLanes::default(),
                tickets: RoomTickets::default(),
                cursors: RoomCursors::default(),
                tasks: Arc::default(),
                environment: RwLock::new(Arc::new(SystemEnvironment)),
                events,
            }),
        }
    }

    /// Replaces the clock and the id source, for example with a
    /// deterministic one in tests and examples. A call already running
    /// keeps the environment it started with; the calls made from now on
    /// read this one.
    pub fn set_environment(&self, environment: impl Environment + Send + Sync + 'static) {
        *write(&self.inner.environment) = Arc::new(environment);
    }

    /// Sets the chain-depth limit (RFC §8.3), 5 by default.
    pub fn set_max_chain_depth(&self, max_chain_depth: u32) {
        self.inner
            .change_config(|config| config.max_chain_depth = max_chain_depth);
    }

    /// Bounds how long an inbound call may take from its start to its
    /// decision (RFC §13.6); `None`, the default, bounds nothing. Past it,
    /// the call writes nothing of its event, lets go of what it holds and
    /// returns `blocked` with the reason `process_timeout`, after the
    /// `process_timeout` framework event. The commit and what follows it,
    /// the event's delivery and its responses, are never bounded.
    ///
    /// The bound counts from the call's start; the waits for the room's
    /// admission ticket and its lock count against it, the wait for the
    /// event's turn in the room's delivery lane, after the commit, does not
    /// (gap G-068).
    pub fn set_process_timeout(&self, process_timeout: Option<Duration>) {
        self.inner
            .change_config(|config| config.process_timeout = process_timeout);
    }

    /// Replaces the routing strategy that finds the room of a message that
    /// names none (RFC §10.4); [`DefaultRouter`] by default. An operation
    /// already running keeps the router it started with until it asks it.
    pub fn set_router(&self, router: impl InboundRoomRouter<S>) {
        *write(&self.inner.router) = Arc::new(router);
    }

    /// The kit's store, to read rooms and timelines.
    pub fn store(&self) -> &S {
        &self.inner.store
    }

    /// Framework events from now on (RFC §8.2).
    pub fn subscribe(&self) -> broadcast::Receiver<FrameworkEvent> {
        self.inner.events.subscribe()
    }

    /// Runs `message` through the inbound pipeline (RFC §10.1) into the
    /// room the kit's router finds for it (RFC §10.4), and the whole
    /// cascade it starts. The call reads rooms unscoped, whatever their
    /// organization, and a room it creates belongs to none (RFC §17.2); an
    /// organization's [`process_inbound`](OrganizationKit::process_inbound)
    /// routes among its own rooms only. A message whose room the caller
    /// knows goes into it with [`process_inbound_in`](Self::process_inbound_in).
    ///
    /// The sender is written in the channel's form first: a phone number in
    /// E.164, an email address lower-case (RFC §5.7). The message goes to
    /// the sender's conversation on the channel, a paused one included,
    /// which the message resumes, or to the one room bound to the channel
    /// when it admits them, active, or paused with a `group` binding; the
    /// sender is then recorded on the room's binding, and a message no room
    /// was found for gets a new one, bound to its sender. On a chat channel
    /// the conversation is the chat the message was posted in, read under
    /// the channel's chat key (RFC §5.7).
    ///
    /// Inbound messages of one room overlap up to the room lock: they
    /// commit in lock order, each event taking its place in the room's
    /// delivery lane at its commit, so that each event's delivery set runs
    /// ahead of the next one's (RFC §10.2). Called from a turn or a
    /// delivery, the message is a new arrival, with its own admission
    /// ticket and `process_timeout`, drawing on the reentry budget of the
    /// cascade that delivers, so that a channel that calls into a room on
    /// every delivery stops at the cap (gap G-070), and returns at its
    /// decision when it comes into the room that turn or delivery works in.
    /// Called from under a room's lock or in its admission order, by a hook,
    /// an off-lock hook's body or a parse, a message routed back into that
    /// room fails with [`KitError::Reentrant`]: its room is known only once
    /// routed, too late for the call to be adopted (RFC §13.5). A call into
    /// another room waits for its delivery as any call does: two rooms whose
    /// deliveries call into each other at the same moment wait for each
    /// other. Made within an agent's turn, the message is at least as deep
    /// as the turn's response, whatever depth it was given (RFC §10.6), so
    /// an agent that calls into its room on every turn stops at the
    /// chain-depth limit, as its responses do (RFC §8.3).
    ///
    /// A `BEFORE_BROADCAST` hook registered with `needs_lock = false` runs
    /// before the room lock, ordered by the room's admission ticket, so the
    /// room's lifecycle changes go on while it runs; the hooks that need the
    /// lock run under it after the room's status is checked (RFC §9.5.1).
    /// The kit's `process_timeout` bounds the call up to its decision (see
    /// [`set_process_timeout`](Self::set_process_timeout)).
    ///
    /// Dropped before it returns, by a `tokio::time::timeout` for instance,
    /// the call goes on, detached. Decided, its event is delivered and its
    /// cascade runs. Before its decision it expires as one past its
    /// `process_timeout`, nothing more of its event written, and every event
    /// it committed is delivered all the same, its own when its commit was
    /// in flight, a hook's; a room it created is attached first (gap
    /// G-076).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Inbound`] when the pipeline refused or failed the
    /// message before committing it, the router's failure among others, and
    /// [`KitError::Reentrant`] for a call that would wait for itself.
    pub async fn process_inbound(
        &self,
        message: InboundMessage,
    ) -> Result<InboundResult, KitError> {
        self.unscoped().process_inbound(message).await
    }

    /// Runs `message` through the inbound pipeline (RFC §10.1) into the
    /// room `room_id`, and the whole cascade it starts, as
    /// [`process_inbound`](Self::process_inbound) runs a routed message:
    /// its sender written in the channel's form, the room's messages
    /// overlapping up to its lock, its off-lock hooks, its
    /// `process_timeout`, a turn's or a delivery's call and a dropped call
    /// alike. The call reads the room unscoped, whatever its organization,
    /// and a room it creates belongs to none (RFC §17.2); an organization's
    /// [`process_inbound_in`](OrganizationKit::process_inbound_in) reaches
    /// its own rooms only.
    ///
    /// A room that does not exist is created with its id: `ON_ROOM_CREATED`'s
    /// hooks run, and finish, before the channel is attached with a binding
    /// naming the sender (RFC §10.1 step 2). A room that exists has the
    /// channel attached when it does not bind it, unless it was detached from
    /// the room (RFC §7.5 rule 7).
    ///
    /// Called from inside a call's work in the room it names, under the
    /// room's lock or in its admission order (a hook, a channel's parse),
    /// the message is adopted by that call instead of waiting for it: it
    /// returns at its commit, with no delivery result, and its delivery set
    /// follows in the room's lane (RFC §13.5, §9.5.1 Reentrance, §10.1 step
    /// 18).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Inbound`] when the pipeline refused or failed the
    /// message before committing it, and [`KitError::Reentrant`] for a call
    /// that would wait for itself.
    pub async fn process_inbound_in(
        &self,
        room_id: &RoomId,
        message: InboundMessage,
    ) -> Result<InboundResult, KitError> {
        self.unscoped().process_inbound_in(room_id, message).await
    }

    fn config(&self) -> Arc<KitConfig> {
        Arc::clone(&read(&self.inner.config))
    }
}

impl<S: ConversationStore> Scope<'_, S> {
    /// Runs `message` into the room the router finds for it among the
    /// scope's rooms, at least as deep as the turn it is made within, if
    /// any (RFC §10.6).
    async fn process_inbound(
        &self,
        mut message: InboundMessage,
    ) -> Result<InboundResult, KitError> {
        context::as_deep_as_its_turn(&mut message.chain_depth);
        let inbound = Inbound::new(
            self.kit.config(),
            InboundTarget::Routed,
            self.organization_id(),
            message,
        );
        Ok(driver::run_inbound(&self.kit.inner, PROCESS_INBOUND, None, inbound).await??)
    }

    /// Runs `message` into the room `room_id` of the scope, adopted by the
    /// call whose work in the room made it, when one did (RFC §13.5).
    async fn process_inbound_in(
        &self,
        room_id: &RoomId,
        message: InboundMessage,
    ) -> Result<InboundResult, KitError> {
        let entry = Entry::Message {
            room_id: room_id.clone(),
            organization_id: self.organization_id(),
            message: Box::new(message),
        };
        self.kit.enter(entry, PROCESS_INBOUND_IN).await
    }
}

impl<S> Inner<S> {
    /// Refuses a call once the kit is shut down.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::ShutDown`] once [`RoomKit::shutdown`] was called.
    pub(crate) fn open(&self) -> Result<(), KitError> {
        if self.tasks.is_shut_down() {
            return Err(KitError::ShutDown);
        }
        Ok(())
    }

    /// The kit's environment, which a run starting now keeps.
    pub(crate) fn environment(&self) -> Arc<dyn Environment + Send + Sync> {
        Arc::clone(&read(&self.environment))
    }

    pub(crate) fn channel(&self, channel_id: &ChannelId) -> Option<Arc<dyn DynChannel>> {
        read(&self.channels).get(channel_id).cloned()
    }

    /// The kit's router, held only while the driver reads it.
    pub(crate) fn router(&self) -> Arc<dyn DynRouter<S>> {
        Arc::clone(&read(&self.router))
    }

    pub(crate) fn hook(&self, hook: HookId) -> Option<Arc<dyn HookHandler>> {
        read(&self.hooks).get(&hook).cloned()
    }

    /// The tool hooks of `trigger` that fire for the calls of an agent of
    /// `source`: its SYNC chain in chain order, then its observers (RFC
    /// §9.2, §9.3).
    pub(crate) fn tool_hooks(
        &self,
        trigger: HookTrigger,
        source: &EventSource,
    ) -> Vec<(HookSpec, ToolHandler)> {
        let config = Arc::clone(&read(&self.config));
        let handlers = read(&self.tool_hooks);
        let registry = &config.hooks;
        registry
            .chain(trigger, source)
            .into_iter()
            .chain(registry.observers(trigger, source))
            .filter_map(|id| Some((registry.get(id)?.clone(), handlers.get(&id)?.clone())))
            .collect()
    }

    pub(crate) fn hook_with_spec(&self, hook: HookId) -> Option<(Arc<dyn HookHandler>, HookSpec)> {
        let spec = read(&self.config).hooks.get(hook).cloned()?;
        Some((self.hook(hook)?, spec))
    }

    /// Publishes a framework event to the subscribers there are.
    pub(crate) fn publish(&self, event: FrameworkEvent) {
        tracing::debug!(target: "roomkit::events", ?event, "framework event");
        if self.events.send(event).is_err() {
            tracing::trace!(target: "roomkit::events", "no subscriber for the framework event");
        }
    }

    /// Logs what an observer of `room_id` raised, and reports its timeout
    /// (RFC §8.2 `hook_timeout`).
    pub(crate) fn report_observer(
        &self,
        spec: &HookSpec,
        trigger: HookTrigger,
        room_id: RoomId,
        outcome: &HookOutcome<RoomEvent>,
    ) {
        match outcome {
            HookOutcome::Returned(_) => {}
            HookOutcome::Raised(error) => tracing::warn!(
                target: "roomkit::hooks",
                hook = %spec.name,
                error = %error.message,
                "an observer hook raised"
            ),
            HookOutcome::Invalid { what, .. } => tracing::warn!(
                target: "roomkit::hooks",
                hook = %spec.name,
                %what,
                "an observer hook returned an unusable result"
            ),
            HookOutcome::TimedOut => {
                tracing::warn!(target: "roomkit::hooks", hook = %spec.name, "an observer hook timed out");
                self.publish(FrameworkEvent::hook_timeout(
                    room_id,
                    spec.name.clone(),
                    trigger,
                    spec.timeout.as_secs_f64(),
                ));
            }
        }
    }

    /// Changes the configuration operations start from, unless `change`
    /// fails; an operation already running keeps the one it started with.
    fn try_change_config<T, E>(
        &self,
        change: impl FnOnce(&mut KitConfig) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut current = write(&self.config);
        let mut next = KitConfig::clone(&current);
        let changed = change(&mut next)?;
        *current = Arc::new(next);
        Ok(changed)
    }

    /// Changes the configuration operations start from.
    fn change_config(&self, change: impl FnOnce(&mut KitConfig)) {
        let mut current = write(&self.config);
        let mut next = KitConfig::clone(&current);
        change(&mut next);
        *current = Arc::new(next);
    }
}

fn read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}
