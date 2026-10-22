//! The kit: the channels, hooks and store of a deployment, and the calls
//! that run the core's operations against them.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use roomkit_core::model::{
    ChannelBinding, ChannelId, InboundMessage, Room, RoomContext, RoomEvent, RoomId,
};
use roomkit_core::ops::{
    AttachChannel, CloseRoom, CreateRoom, Environment, FrameworkEvent, Inbound, InboundResult,
    KitConfig,
};
use roomkit_core::rules::hook_chain::HookOutcome;
use roomkit_core::rules::hook_registry::{HookExecution, HookId, HookSpec, HookTrigger};
use tokio::sync::broadcast;

use crate::channel::{Channel, DynChannel};
use crate::driver;
use crate::environment::SystemEnvironment;
use crate::error::KitError;
use crate::hooks::{HookHandler, HookReturn, Observer};
use crate::locks::RoomMutexes;
use crate::store::{ConversationStore, InMemoryStore};

/// How many framework events a slow subscriber may fall behind before it
/// misses some.
const FRAMEWORK_EVENT_BACKLOG: usize = 1024;

/// A RoomKit deployment: its channels, its hooks and its store.
///
/// Cloning a kit is cheap; every clone is the same kit.
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
/// let result = kit.process_inbound(&room, message).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct RoomKit<S = InMemoryStore> {
    inner: Arc<Inner<S>>,
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
    pub(crate) store: S,
    pub(crate) locks: RoomMutexes,
    lanes: RoomMutexes,
    environment: Mutex<Box<dyn Environment + Send>>,
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
                store,
                locks: RoomMutexes::default(),
                lanes: RoomMutexes::default(),
                environment: Mutex::new(Box::new(SystemEnvironment)),
                events,
            }),
        }
    }

    /// Replaces the clock and the id source, for example with a
    /// deterministic one in tests and examples.
    pub fn set_environment(&self, environment: impl Environment + Send + 'static) {
        *self.inner.environment() = Box::new(environment);
    }

    /// Sets the chain-depth limit (RFC §8.3), 5 by default.
    pub fn set_max_chain_depth(&self, max_chain_depth: u32) {
        self.inner
            .change_config(|config| config.max_chain_depth = max_chain_depth);
    }

    /// The kit's store, to read rooms and timelines.
    pub fn store(&self) -> &S {
        &self.inner.store
    }

    /// Framework events from now on (RFC §8.2).
    pub fn subscribe(&self) -> broadcast::Receiver<FrameworkEvent> {
        self.inner.events.subscribe()
    }

    /// Registers a channel.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Config`] when a channel with its id is registered
    /// already.
    pub fn register_channel(&self, channel: impl Channel) -> Result<(), KitError> {
        let declaration = Channel::declaration(&channel);
        let channel_id = declaration.channel_id.clone();
        let channel: Arc<dyn DynChannel> = Arc::new(channel);
        self.inner.try_change_config(|config| {
            config.register_channel(declaration)?;
            // The channel is in place before an operation can see its
            // declaration.
            write(&self.inner.channels).insert(channel_id, channel);
            Ok::<(), KitError>(())
        })
    }

    /// Registers a hook with its handler (RFC §9.1).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Registration`] when the registration breaks a rule
    /// of RFC §9.1.
    pub fn hook(&self, spec: HookSpec, handler: impl HookHandler) -> Result<HookId, KitError> {
        let handler: Arc<dyn HookHandler> = Arc::new(handler);
        let id = self.inner.try_change_config(|config| {
            let id = config.hooks.register(spec)?;
            // The handler is in place before an operation can see the hook.
            write(&self.inner.hooks).insert(id, handler);
            Ok::<HookId, KitError>(id)
        })?;
        Ok(id)
    }

    /// Registers a SYNC `BEFORE_BROADCAST` hook named `name`, with the RFC's
    /// defaults (RFC §9.5).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Registration`] when the name is taken.
    pub fn before_broadcast<F, Fut>(&self, name: &str, handler: F) -> Result<HookId, KitError>
    where
        F: Fn(RoomEvent, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HookReturn> + Send + 'static,
    {
        let spec = HookSpec::new(name, HookTrigger::BeforeBroadcast, HookExecution::Sync);
        self.hook(spec, handler)
    }

    /// Registers an ASYNC observer on `trigger` (RFC §9.3): it runs after the
    /// fact, and what it raises is logged.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Registration`] when the name is taken.
    pub fn observe<F, Fut>(
        &self,
        name: &str,
        trigger: HookTrigger,
        handler: F,
    ) -> Result<HookId, KitError>
    where
        F: Fn(RoomEvent, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let spec = HookSpec::new(name, trigger, HookExecution::Async);
        self.hook(spec, Observer(handler))
    }

    /// Creates the room `room_id`.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room exists already.
    pub async fn create_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let room = Room::new(room_id.clone(), self.inner.environment().now());
        let created = driver::run(&self.inner, CreateRoom::new(self.config(), room)).await?;
        Ok(created?)
    }

    /// Attaches a registered channel to a room with the RFC's default
    /// binding: read-write, unmuted, visible to all (RFC §5.7).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::UnknownChannel`] for a channel not registered, and
    /// [`KitError::Lifecycle`] when the room does not exist.
    pub async fn attach(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
    ) -> Result<ChannelBinding, KitError> {
        self.attach_with(room_id, channel_id, |_| {}).await
    }

    /// Attaches a registered channel to a room, with the binding `configure`
    /// adjusts from the default: its access, mute, visibility, category,
    /// capabilities and metadata. The channel, the room, and the channel's
    /// type and direction stay what they are.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::UnknownChannel`] for a channel not registered, and
    /// [`KitError::Lifecycle`] when the room does not exist.
    pub async fn attach_with(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        configure: impl FnOnce(&mut ChannelBinding),
    ) -> Result<ChannelBinding, KitError> {
        let config = self.config();
        let declaration = config
            .channel(channel_id)
            .ok_or_else(|| KitError::UnknownChannel(channel_id.clone()))?;
        let mut binding = ChannelBinding::new(
            channel_id.clone(),
            room_id.clone(),
            declaration.channel_type,
            declaration.category,
            declaration.direction,
            self.inner.environment().now(),
        );
        binding.capabilities = declaration.capabilities.clone();
        configure(&mut binding);
        // `configure` adjusts the binding's permissions and data, never which
        // channel it binds to which room, nor what the channel is.
        binding.channel_id = channel_id.clone();
        binding.room_id = room_id.clone();
        binding.channel_type = declaration.channel_type;
        binding.direction = declaration.direction;
        let attached = driver::run(&self.inner, AttachChannel::new(config, binding)).await?;
        Ok(attached?)
    }

    /// Closes a room: it refuses every new event from now on (RFC §5.1).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist.
    pub async fn close_room(&self, room_id: &RoomId) -> Result<Room, KitError> {
        let closed =
            driver::run(&self.inner, CloseRoom::new(self.config(), room_id.clone())).await?;
        Ok(closed?)
    }

    /// Runs `message` into the room `room_id` through the inbound pipeline
    /// (RFC §10.1), and the whole cascade it starts.
    ///
    /// Inbound messages of one room run one at a time, which keeps each
    /// event's delivery set ahead of the next one's (RFC §10.2). A hook or a
    /// channel must not send into the same room while it runs: it would wait
    /// for itself (RFC §13.5 reentrancy comes with direct injection, step 2).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Inbound`] when the pipeline refused or failed the
    /// message before committing it.
    pub async fn process_inbound(
        &self,
        room_id: &RoomId,
        message: InboundMessage,
    ) -> Result<InboundResult, KitError> {
        let _lane = self.inner.lanes.lock(room_id).await;
        let inbound = Inbound::new(self.config(), room_id.clone(), message);
        let processed = driver::run(&self.inner, inbound).await?;
        Ok(processed?)
    }

    fn config(&self) -> Arc<KitConfig> {
        Arc::clone(&read(&self.inner.config))
    }
}

impl<S> Inner<S> {
    /// The kit's environment, held only while the driver reads it.
    pub(crate) fn environment(&self) -> MutexGuard<'_, Box<dyn Environment + Send>> {
        self.environment
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn channel(&self, channel_id: &ChannelId) -> Option<Arc<dyn DynChannel>> {
        read(&self.channels).get(channel_id).cloned()
    }

    pub(crate) fn hook(&self, hook: HookId) -> Option<Arc<dyn HookHandler>> {
        read(&self.hooks).get(&hook).cloned()
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
            HookOutcome::Invalid(what) => tracing::warn!(
                target: "roomkit::hooks",
                hook = %spec.name,
                %what,
                "an observer hook returned an unusable result"
            ),
            HookOutcome::TimedOut => {
                tracing::warn!(target: "roomkit::hooks", hook = %spec.name, "an observer hook timed out");
                self.publish(FrameworkEvent::HookTimeout {
                    room_id,
                    hook_name: spec.name.clone(),
                    trigger,
                    timeout: spec.timeout.as_secs_f64(),
                });
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
