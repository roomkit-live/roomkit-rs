//! The kit: the channels, hooks and store of a deployment, and the calls
//! that run the core's operations against them.
//!
//! - this module: the kit itself and the inbound pipeline;
//! - `options`: what a kit is built with, and what each call takes beside
//!   its required arguments, the organization it acts for among them (RFC
//!   §17.2);
//! - `registry`: registering and unregistering channels and hooks;
//! - `rooms`: the room lifecycle;
//! - `bindings`: a room's channel bindings and members;
//! - `events`: the host's direct changes to stored events;
//! - `inject`: direct injection and records committed outside the
//!   pipeline;
//! - `post`: the host's own voice;
//! - `timeline`: a room's timeline;
//! - `timers`: applying room timers, on demand or in the background;
//! - `background`: waiting for the kit's background work, and ending it;
//! - `shutdown`: the kit's one shutdown, which every close joins;
//! - `weak`: the kit held without keeping it alive.

mod background;
mod bindings;
mod events;
mod inject;
mod options;
mod post;
mod registry;
mod rooms;
mod shutdown;
#[cfg(test)]
mod tests;
mod timeline;
mod timers;
mod weak;

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, PoisonError, RwLock, Weak};

use roomkit_core::model::{ChannelId, ChannelType, InboundMessage, RoomEvent, RoomId};
use roomkit_core::ops::{
    Entry, Environment, FrameworkEvent as CoreEvent, Inbound, InboundTarget, KitConfig,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookId, HookSpec, HookSubject, HookTrigger};

use self::inject::Entered;
pub use self::options::{
    ArchiveRoomOptions, AttachChannelOptions, CheckAllTimersOptions, CheckRoomTimersOptions,
    CloseRoomOptions, CommitEventOptions, CreateRoomOptions, DeleteEventOptions,
    DetachChannelOptions, GetBindingOptions, GetRoomOptions, GetTimelineOptions,
    ListBindingsOptions, ListMembersOptions, MuteOptions, PauseRoomOptions, PostOptions,
    ProcessInboundOptions, ReopenRoomOptions, ResumeRoomOptions, RoomKitOptions, SendEventOptions,
    SetAccessOptions, SetVisibilityOptions, UnmuteOptions, UpdateBindingMetadataOptions,
    UpdateEventOptions,
};
pub use self::weak::WeakRoomKit;
use crate::channel::DynChannel;
use crate::context;
use crate::cursors::RoomCursors;
use crate::driver;
use crate::environment::SystemEnvironment;
use crate::error::RoomKitError;
use crate::events::{FrameworkEvent, Handled, Handler, Handlers};
use crate::hooks::{HookHandler, Kept};
use crate::inbound::InboundResult;
use crate::locks::RoomMutexes;
use crate::queues::{RoomLanes, RoomTickets};
use crate::registered::{Registered, RegisteredChannel};
use crate::router::{
    ChannelOf, DefaultInboundRoomRouter, DefaultInboundRoomRouterOptions, InboundRoomRouter,
};
use crate::store::{ConversationStore, InMemoryStore};
use crate::tasks::Tasks;
use crate::tool_calls::ToolCallEvent;
use crate::turns::Turns;
use crate::unwind;

/// The kit's call that runs a message, as an error names it.
const PROCESS_INBOUND: &str = "process_inbound";

/// A RoomKit deployment: its channels, its hooks and its store.
///
/// Cloning a kit is cheap; every clone is the same kit. A hook or a channel
/// the kit holds, which would make a cycle with a clone, holds the kit
/// weakly ([`downgrade`](Self::downgrade)).
///
/// ```no_run
/// # async fn example(sms: impl roomkit::Channel) -> Result<(), Box<dyn std::error::Error>> {
/// use roomkit::model::{ChannelId, InboundMessage, RoomId, TextContent};
/// use roomkit::{CreateRoomOptions, ProcessInboundOptions, RoomKit};
///
/// let kit = RoomKit::new(Default::default());
/// kit.register_channel(sms)?;
/// let room = RoomId::new("room_1")?;
/// let channel = ChannelId::new("sms")?;
/// kit.create_room(CreateRoomOptions { room_id: Some(room.clone()), ..Default::default() })
///     .await?;
/// kit.attach_channel(&room, &channel, Default::default()).await?;
/// let message = InboundMessage::new(channel, "+15551234567", TextContent::new("Bonjour"));
/// let options = ProcessInboundOptions { room_id: Some(room), ..Default::default() };
/// let result = kit.process_inbound(message, options).await?;
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
    /// The handlers of the hooks, by the payload their trigger hands them.
    pub(crate) hooks: RwLock<HashMap<HookId, Kept>>,
    /// The routing strategy (RFC §10.4), fixed at the kit's construction.
    router: Arc<dyn InboundRoomRouter>,
    pub(crate) store: Arc<S>,
    pub(crate) locks: RoomMutexes,
    pub(crate) lanes: RoomLanes,
    pub(crate) tickets: RoomTickets,
    /// The indexes whose turn in their room's lane is over, waiting for the
    /// room's delivery cursor (gap G-091).
    pub(crate) cursors: RoomCursors,
    /// The tasks the kit runs in the background: observers, inbound runs.
    pub(crate) tasks: Arc<Tasks>,
    /// The clock and the ids every run reads: the system's, or a test's
    /// (`testing::kit_with_environment`).
    environment: Arc<dyn Environment + Send + Sync>,
    /// The handlers `on` registered for framework events (RFC §8.2).
    pub(crate) events: Handlers,
    /// The turn backend every intelligence turn runs through (RFC §10.6).
    pub(crate) turns: Arc<Turns>,
    /// The breakers and the rate limits of the transports' sends (RFC
    /// §13.1, §13.2).
    pub(crate) sends: crate::sends::Sends,
    /// Set once the channels were closed at the kit's close, with those
    /// whose close failed: they are closed once.
    channels_closed: tokio::sync::OnceCell<shutdown::Ended>,
    /// The kit's one shutdown, which every close joins (RFC §12.10.4).
    shutdown: shutdown::Shutdown,
    /// Set once the kit's close released its store: a read the store then
    /// fails answers that the kit is closed.
    store_released: AtomicBool,
}

impl<S> std::fmt::Debug for Inner<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Inner").finish_non_exhaustive()
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// A kit built with `options`, Python's `RoomKit(...)` keywords (step 5,
    /// decision 2), on the system clock with random ids.
    pub fn new(options: RoomKitOptions<S>) -> Self {
        Self::with_environment(options, Arc::new(SystemEnvironment))
    }

    /// A kit built with `options` whose runs read `environment`.
    pub(crate) fn with_environment(
        options: RoomKitOptions<S>,
        environment: Arc<dyn Environment + Send + Sync>,
    ) -> Self {
        let RoomKitOptions {
            store,
            inbound_router,
            max_chain_depth,
            agent_response_policy,
            process_timeout,
        } = options;
        let mut config = KitConfig::new();
        config.max_chain_depth = max_chain_depth;
        config.agent_response_policy = agent_response_policy;
        config.process_timeout = Some(process_timeout);
        let inner = Arc::new_cyclic(|kit: &Weak<Inner<S>>| Inner {
            config: RwLock::new(Arc::new(config)),
            channels: RwLock::new(HashMap::new()),
            hooks: RwLock::new(HashMap::new()),
            router: inbound_router.unwrap_or_else(|| default_router(&store, kit)),
            store,
            locks: RoomMutexes::default(),
            lanes: RoomLanes::default(),
            tickets: RoomTickets::default(),
            cursors: RoomCursors::default(),
            tasks: Arc::default(),
            environment,
            events: Handlers::default(),
            turns: Arc::default(),
            sends: crate::sends::Sends::default(),
            channels_closed: tokio::sync::OnceCell::new(),
            shutdown: shutdown::Shutdown::default(),
            store_released: AtomicBool::new(false),
        });
        Self { inner }
    }

    /// The registered channel `channel_id`, as the kit holds it: its id,
    /// type, category and direction, and how it writes its addresses
    /// (Python's `get_channel`); `None` when no channel has the id.
    pub fn get_channel(&self, channel_id: &ChannelId) -> Option<Arc<dyn RegisteredChannel>> {
        self.inner.registered(channel_id)
    }

    /// The kit's store, to read rooms and timelines.
    pub fn store(&self) -> &S {
        &self.inner.store
    }

    /// Runs `handler` on every framework event of `event_type` from now on
    /// (RFC §8.2), after the handlers registered before it: Python's
    /// `RoomKit.on(event_type)`. The call that emitted an event waits for
    /// its handlers before it goes on, as Python's awaits them, sometimes
    /// holding the event's room: a handler that calls into that room waits
    /// for the call, which waits for the handler. Keep a handler short, and
    /// spawn what it starts. One that panics is logged, the call
    /// unaffected. No type stands for all.
    ///
    /// ```
    /// use roomkit::RoomKit;
    ///
    /// let kit = RoomKit::new(Default::default());
    /// kit.on("room_created", |event| async move {
    ///     println!("room {:?} created", event.room_id);
    /// });
    /// ```
    pub fn on<F, Fut>(&self, event_type: impl Into<String>, handler: F)
    where
        F: Fn(FrameworkEvent) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let handler: Handler = Arc::new(move |event| Box::pin(handler(event)));
        self.inner.events.add(event_type.into(), handler);
    }

    /// Runs `message` through the inbound pipeline (RFC §10.1), and the
    /// whole cascade it starts: into the room `options` name, or into the
    /// room the kit's router finds for it (RFC §10.4), for the organization
    /// they name. A room of another organization, or of none, is not found,
    /// nothing written; a room the call creates belongs to the organization
    /// (RFC §17.2); `None` reads rooms unscoped.
    ///
    /// The sender is written in the channel's form first: a phone number in
    /// E.164, an email address lower-case (RFC §5.7).
    ///
    /// A message into a named room: a room that does not exist is created
    /// with its id, `ON_ROOM_CREATED`'s hooks running, and finishing, before
    /// the channel is attached with a binding naming the sender (RFC §10.1
    /// step 2). A room that exists has the channel attached when it does not
    /// bind it, unless it was detached from the room (RFC §7.5 rule 7).
    /// Called from inside a call's work in the room it names, under the
    /// room's lock or in its admission order (a hook, a channel's parse),
    /// the message is adopted by that call instead of waiting for it: it
    /// returns at its commit, with no delivery result, and its delivery set
    /// follows in the room's lane (RFC §13.5, §9.5.1 Reentrance, §10.1 step
    /// 18).
    ///
    /// A routed message goes to the sender's conversation on the channel, a
    /// paused one included, which the message resumes, or to the one room
    /// bound to the channel when it admits them, active, or paused with a
    /// `group` binding; the sender is then recorded on the room's binding,
    /// and a message no room was found for gets a new one, bound to its
    /// sender. On a chat channel the conversation is the chat the message
    /// was posted in, read under the channel's chat key (RFC §5.7). Called
    /// from under a room's lock or in its admission order, by a hook, an
    /// off-lock hook's body or a parse, a message routed back into that room
    /// fails with [`RoomKitError::ReentrantCall`]: its room is known only once
    /// routed, too late for the call to be adopted (RFC §13.5).
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
    /// A call into another room waits for its delivery as any call does: two
    /// rooms whose deliveries call into each other at the same moment wait
    /// for each other. Made within an agent's turn, the message is at least
    /// as deep as the turn's response, whatever depth it was given (RFC
    /// §10.6), so an agent that calls into its room on every turn stops at
    /// the chain-depth limit, as its responses do (RFC §8.3).
    ///
    /// A `BEFORE_BROADCAST` hook registered with `needs_lock = false` runs
    /// before the room lock, ordered by the room's admission ticket, so the
    /// room's lifecycle changes go on while it runs; the hooks that need the
    /// lock run under it after the room's status is checked (RFC §9.5.1).
    /// The kit's `process_timeout` bounds the call up to its decision (see
    /// [`RoomKitOptions::process_timeout`]).
    ///
    /// With `defer_delivery`, the call returns at its decision, its blocking
    /// decisions made, and its result's [`delivery`](InboundResult::delivery)
    /// waits for the rest: the event's delivery set and the passes it
    /// spawns, in the room's lane (RFC §10.1 step 18, detached completion).
    ///
    /// The call reads its caller's execution context. A task the kit's work
    /// spawns, a delivery's, a turn's or a hook's, that makes this call
    /// into its own room and waits must be spawned with
    /// [`keep_context`](crate::keep_context), as Python's `create_task`
    /// copies its context: spawned plainly, its call waits behind the
    /// delivery that awaits the task, blocked, `process_timeout` bounding
    /// none of it.
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
    /// Returns the error that failed the message before its commit:
    /// [`RoomKitError::RoomNotFound`] for a room of another organization,
    /// [`RoomKitError::ChannelNotRegistered`], [`RoomKitError::Router`] for
    /// the router's failure, [`RoomKitError::Channel`] for its parse's, among
    /// others; and [`RoomKitError::ReentrantCall`] for a call that would wait
    /// for itself. A refusal is no error: the result says it.
    pub async fn process_inbound(
        &self,
        mut message: InboundMessage,
        options: ProcessInboundOptions,
    ) -> Result<InboundResult, RoomKitError> {
        // A content a struct literal built past its model's check is refused
        // before anything is read, as Python's model refuses it when built,
        // and one kept is written as its constructor writes it.
        message.content.check()?;
        let ProcessInboundOptions {
            room_id,
            defer_delivery,
            organization_id,
        } = options;
        let entered = if let Some(room_id) = room_id {
            let entry = Entry::Message {
                room_id,
                organization_id,
                message: Box::new(message),
            };
            self.enter(entry, PROCESS_INBOUND, defer_delivery).await?
        } else {
            context::as_deep_as_its_turn(&mut message.chain_depth);
            let routed = Inbound::new(
                self.config(),
                InboundTarget::Routed,
                organization_id,
                message,
            );
            let ran =
                driver::run_inbound(&self.inner, PROCESS_INBOUND, None, routed, defer_delivery)
                    .await;
            Entered::ran(ran, RoomKitError::from)?
        };
        Ok(entered.into_result(defer_delivery, &self.inner.tasks))
    }

    fn config(&self) -> Arc<KitConfig> {
        Arc::clone(&read(&self.inner.config))
    }
}

/// The RFC's default strategy over the kit's store, reading the kit's
/// registered channels, as Python's `DefaultInboundRoomRouter(store,
/// channel_of=kit.get_channel)`. It holds the kit weakly: the kit holds it.
fn default_router<S: ConversationStore>(
    store: &Arc<S>,
    kit: &Weak<Inner<S>>,
) -> Arc<dyn InboundRoomRouter> {
    let kit = Weak::clone(kit);
    let channel_of: ChannelOf = Arc::new(move |channel_id| kit.upgrade()?.registered(channel_id));
    let options = DefaultInboundRoomRouterOptions {
        channel_of: Some(channel_of),
    };
    Arc::new(DefaultInboundRoomRouter::new(Arc::clone(store), options))
}

impl<S> Inner<S> {
    /// The kit's configuration as it stands.
    pub(crate) fn config(&self) -> Arc<KitConfig> {
        Arc::clone(&read(&self.config))
    }

    /// Refuses a call once the kit's close started, whoever makes it, as
    /// the Python implementation's kit refuses every call but a read (RFC
    /// §12.10.4).
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once [`RoomKit::close`] started.
    pub(crate) fn open(&self) -> Result<(), RoomKitError> {
        if self.tasks.is_closing() {
            return Err(RoomKitError::KitClosed);
        }
        Ok(())
    }

    /// The kit's environment, which a run starting now keeps.
    pub(crate) fn environment(&self) -> Arc<dyn Environment + Send + Sync> {
        Arc::clone(&self.environment)
    }

    /// The registered channel `channel_id`, as a router and a caller read
    /// it.
    pub(crate) fn registered(&self, channel_id: &ChannelId) -> Option<Arc<dyn RegisteredChannel>> {
        let declaration = read(&self.config).channel(channel_id)?.clone();
        let channel = self.channel(channel_id)?;
        Some(Arc::new(Registered {
            declaration,
            channel,
        }))
    }

    pub(crate) fn channel(&self, channel_id: &ChannelId) -> Option<Arc<dyn DynChannel>> {
        read(&self.channels).get(channel_id).cloned()
    }

    /// Closes each channel registered now, in the order of their ids, one
    /// after the other (RFC §6.1 `close`), once: a second call waits for the
    /// first one's end. A channel whose close panics is logged and noted,
    /// and the channels after it close all the same (RFC §12.10.4); the
    /// channels whose close failed, each with what its panic said.
    pub(crate) async fn close_channels(&self) -> shutdown::Ended {
        let closed = self.channels_closed.get_or_init(|| async {
            let mut channels: Vec<(ChannelId, Arc<dyn DynChannel>)> = read(&self.channels)
                .iter()
                .map(|(id, channel)| (id.clone(), Arc::clone(channel)))
                .collect();
            channels.sort_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
            let mut failures = Vec::new();
            for (channel_id, channel) in channels {
                // The call itself within the catch: a close that panics
                // before it returns its future is caught too.
                let closing = async { channel.close().await };
                if let Err(panic) = unwind::unwinding(closing).await {
                    let message = unwind::message(&*panic);
                    tracing::error!(
                        target: "roomkit::kit",
                        channel_id = %channel_id,
                        %message,
                        "a channel failed to close; the remaining channels still close"
                    );
                    failures.push((channel_id, message));
                }
            }
            failures.into()
        });
        Arc::clone(closed.await)
    }

    /// The kit's router.
    pub(crate) fn router(&self) -> &dyn InboundRoomRouter {
        &*self.router
    }

    /// The tool hooks of `trigger` that fire for the calls of the agent
    /// `channel_id` of `channel_type`: its SYNC chain in chain order, then
    /// its observers (RFC §9.2, §9.3).
    pub(crate) fn tool_hooks(
        &self,
        trigger: HookTrigger,
        channel_id: &ChannelId,
        channel_type: &ChannelType,
    ) -> Vec<(HookSpec, Arc<dyn HookHandler<ToolCallEvent>>)> {
        let config = Arc::clone(&read(&self.config));
        let handlers = read(&self.hooks);
        let registry = &config.hooks;
        let subject = HookSubject::of_channel(channel_id, channel_type);
        registry
            .chain(trigger, subject)
            .into_iter()
            .chain(registry.observers(trigger, subject))
            .filter_map(|id| {
                let Kept::ToolCall(handler) = handlers.get(&id)? else {
                    return None;
                };
                Some((registry.get(id)?.clone(), Arc::clone(handler)))
            })
            .collect()
    }

    /// The handler of `hook`, whatever its payload, and its registration.
    pub(crate) fn hook_with_spec(&self, hook: HookId) -> Option<(Kept, HookSpec)> {
        let spec = read(&self.config).hooks.get(hook).cloned()?;
        Some((self.kept_hook(hook)?, spec))
    }

    /// The handler of `hook`, whatever its payload.
    pub(crate) fn kept_hook(&self, hook: HookId) -> Option<Kept> {
        read(&self.hooks).get(&hook).cloned()
    }

    /// Emits the framework event `notice` reports, now: the end of its
    /// handlers, which the call that emitted it waits for.
    pub(crate) fn emit(&self, notice: CoreEvent) -> Option<Handled> {
        let event = FrameworkEvent::of(notice, self.environment.now());
        self.events.emit(&self.tasks, event)
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
            // What an observer returns is discarded, as Python discards an
            // async hook's: one that returned nothing is no fault.
            HookOutcome::Returned(_) | HookOutcome::Invalid { .. } => {}
            HookOutcome::Raised(error) => tracing::warn!(
                target: "roomkit::hooks",
                hook = %spec.name,
                error = %error.message,
                "an observer hook raised"
            ),
            HookOutcome::TimedOut => {
                tracing::warn!(target: "roomkit::hooks", hook = %spec.name, "an observer hook timed out");
                drop(self.emit(CoreEvent::hook_timeout(
                    room_id,
                    spec.name.clone(),
                    trigger,
                    spec.timeout.as_secs_f64(),
                )));
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
}

fn read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}
