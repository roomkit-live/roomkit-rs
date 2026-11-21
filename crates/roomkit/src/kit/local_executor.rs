//! Public access to the kit's own turn executor (RFC §10.6.6).
mod channels;
mod control;
pub(crate) use channels::WorkerChannel;
mod pool;
mod standalone;
pub use standalone::TurnExecutorOptions;

use std::collections::BTreeSet;
use std::future::Future;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, RoomContext};

use super::{HookEngine, RoomKit};
use crate::hooks::{HookPayload, IntoHookReturn, closure_kept};
use crate::turns::{HookOptions, LocalHosts, RoomHookOptions, TakesOptions};
use crate::{
    ConversationStore, HookRegistration, HookTrigger, InMemoryStore, RoomKitError, TurnTimeouts,
};

/// The kit's local executor: its turn hooks and the channels it takes.
/// Obtain the kit's executor through [`RoomKit::turn_executor`], or construct a worker with [`TurnExecutor::new`].
/// Like a hook engine, this handle keeps the kit alive; closures should capture
/// a [`WeakRoomKit`](crate::WeakRoomKit).
pub struct TurnExecutor<S = InMemoryStore> {
    kit: RoomKit<S>,
    standalone: Option<Arc<standalone::Standalone>>,
}

impl<S> Clone for TurnExecutor<S> {
    fn clone(&self) -> Self {
        Self {
            kit: self.kit.clone(),
            standalone: self.standalone.clone(),
        }
    }
}

impl<S> std::fmt::Debug for TurnExecutor<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnExecutor")
            .finish_non_exhaustive()
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// The kit's own turn executor (RFC §10.6.6), Python `turn_executor`.
    pub fn turn_executor(&self) -> TurnExecutor<S> {
        TurnExecutor {
            kit: self.clone(),
            standalone: None,
        }
    }
}

impl<S: ConversationStore> LocalHosts for super::Inner<S> {
    fn hosts_name(&self, id: &str) -> bool {
        super::read(&self.channels)
            .get(id)
            .is_some_and(|channel| !channel.is_declaration_only())
    }
}

impl<S: ConversationStore> TurnExecutor<S> {
    pub(crate) fn local_hosts(&self) -> Arc<dyn LocalHosts> {
        match &self.standalone {
            Some(worker) => Arc::<standalone::Standalone>::clone(worker),
            None => Arc::<super::Inner<S>>::clone(&self.kit.inner),
        }
    }

    #[cfg(feature = "redis")]
    pub(crate) fn look_up(&self) -> crate::turns::redis::WorkerLookup {
        let kit = self.kit.downgrade();
        let standalone = self.standalone.as_ref().map(Arc::downgrade);
        Box::new(move || {
            let kit = kit.upgrade()?;
            let standalone = match &standalone {
                Some(worker) => Some(worker.upgrade()?),
                None => None,
            };
            Some(Arc::new(Self { kit, standalone }))
        })
    }

    pub(crate) fn lifecycle(&self) -> std::sync::Weak<crate::turns::executor::Lifecycle> {
        Arc::downgrade(&self.kit.inner.turns.executor_lifecycle)
    }

    #[cfg(feature = "redis")]
    pub(crate) fn serving_backends(&self) -> &crate::turns::redis::ServingBackends {
        &self.kit.inner.turns.serving_backends
    }

    /// The id this executor answers with, Python's `executor_id`.
    pub fn executor_id(&self) -> &str {
        self.standalone.as_ref().map_or_else(
            || self.kit.inner.turns.executor_id(),
            |worker| worker.executor_id(),
        )
    }

    /// The readers and outlets selected for this executor's next channel.
    pub fn wiring(&self) -> crate::turns::TurnWiring {
        self.kit.inner.wiring()
    }

    /// Replace the environment used to wire subsequent channels.
    pub fn use_wiring(&self, env: Arc<dyn crate::turns::WiringEnv>) {
        let mut wiring = super::write(&self.kit.inner.turn_wiring);
        wiring.env = env;
        wiring.kit_defaults = false;
    }

    /// Wire a channel even before it is registered or built for a turn.
    ///
    /// # Errors
    /// Propagates a closed human-input handler's registration error.
    pub fn wire(&self, channel: &impl crate::Channel) -> Result<(), RoomKitError> {
        self.wiring().wire(channel)
    }

    /// The engine on which this executor's hooks are registered.
    pub fn hooks(&self) -> HookEngine<S> {
        self.kit.hooks_for(HookTrigger::BeforeToolUse)
    }

    /// Register a hook on a trigger this executor fires (RFC §10.6.6).
    ///
    /// # Errors
    /// Refuses an owner trigger, an invalid registration or a mismatched payload.
    pub fn hook<P, F, Fut, R>(
        &self,
        trigger: HookTrigger,
        r#fn: F,
        options: HookOptions,
    ) -> Result<HookRegistration, RoomKitError>
    where
        P: HookPayload,
        F: Fn(P, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoHookReturn<P>,
    {
        self.hooks()
            .register(options.options().registration(trigger, closure_kept(r#fn)))
    }

    /// Define a room hook by name, activated through [`RoomKit::enable_room_hook`].
    ///
    /// # Errors
    /// Refuses an empty or reserved name, an owner trigger or an invalid registration.
    pub fn room_hook<P, F, Fut, R>(
        &self,
        name: &str,
        trigger: HookTrigger,
        r#fn: F,
        options: RoomHookOptions,
    ) -> Result<(), RoomKitError>
    where
        P: HookPayload,
        F: Fn(P, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoHookReturn<P>,
    {
        let hook = HookRegistration {
            name: name.to_owned(),
            priority: options.priority,
            timeout: options.timeout,
            fail_closed: options.fail_closed,
            ..HookRegistration::new(trigger, options.execution, r#fn)
        };
        self.hooks().define_room_hook(hook)
    }

    /// Remove every room-hook definition under this name.
    pub fn remove_room_hook(&self, name: &str) -> bool {
        self.hooks().undefine_room_hook(name)
    }

    /// Take no more turns and cut running turns, except those this call runs
    /// under (RFC §10.6.8). The kit and its registered channels remain open.
    pub async fn close(&self) {
        self.kit.inner.turns.executor_lifecycle.close().await;
        if let Some(worker) = &self.standalone {
            worker.finish_builds().await;
            if crate::turns::current_turn_id().is_none() {
                worker.generations.settled().await;
            }
            if let Err(error) = self.kit.close().await {
                tracing::warn!(%error, "the executor could not close its channels");
            }
        }
        self.kit
            .inner
            .turns
            .executor_lifecycle
            .memory_backends
            .closed()
            .await;
        #[cfg(feature = "redis")]
        self.tell_redis_closed().await;
    }

    /// Whether this executor has closed.
    pub fn closed(&self) -> bool {
        self.kit.inner.turns.is_closed() || self.kit.inner.turns.executor_lifecycle.is_closed()
    }

    /// Whether it hosts this channel, even after closing.
    pub fn hosts_channel(&self, channel_id: &ChannelId) -> bool {
        self.standalone.as_ref().map_or_else(
            || self.kit.inner.hosts_name(channel_id.as_str()),
            |worker| worker.hosts(channel_id),
        )
    }

    /// Whether it declares this channel and has not closed.
    pub fn declares(&self, channel_id: &ChannelId) -> bool {
        !self.closed() && self.hosts_channel(channel_id)
    }

    /// An open-ended registry predicate has no finite advertised host list.
    pub fn hosted(&self) -> Option<BTreeSet<String>> {
        self.standalone
            .as_ref()
            .and_then(|worker| worker.hosts.clone())
    }

    /// Whether it serves this organization; the kit's executor serves all.
    pub fn serves(&self, organization_id: Option<&str>) -> bool {
        self.standalone
            .as_ref()
            .is_none_or(|worker| worker.serves(organization_id))
    }

    /// Whether it takes this channel with every requested hook name.
    pub fn takes(&self, channel_id: &ChannelId, options: TakesOptions) -> bool {
        if !self.declares(channel_id) || !self.serves(options.organization_id.as_deref()) {
            return false;
        }
        let defined = self.hooks().defined_room_hooks();
        options
            .room_hooks
            .into_iter()
            .all(|name| defined.contains(&name))
    }

    /// The bounds of the kit's executor (RFC §10.6.8).
    pub fn timeouts(&self) -> TurnTimeouts {
        self.kit.inner.turns.timeouts()
    }
}

impl<S: ConversationStore> TurnExecutor<S> {
    /// Give future turns a route to their room owner after their link ends (RFC §10.6.3).
    /// Work retained from an earlier turn continues to use the port it received at admission.
    pub fn reach_owner(&self, owner: Arc<dyn crate::turns::RoomOwner>) {
        self.kit.inner.turns.set_worker_owner(owner);
    }

    /// Run the request delivered by an application backend (RFC §10.6.2). The link
    /// releases its resources after the executor finishes sending the turn's frames.
    /// An application implementing [`ExecutorLink`](crate::turns::ExecutorLink)
    /// can admit its link on either a standalone worker or a kit's executor.
    ///
    /// ```
    /// use std::sync::Arc;
    /// use roomkit::turns::ExecutorLink;
    /// use roomkit::{ConversationStore, RoomKitError, TurnExecutor};
    ///
    /// fn accept<S: ConversationStore>(
    ///     worker: &TurnExecutor<S>, link: Arc<dyn ExecutorLink>,
    /// ) -> Result<(), RoomKitError> {
    ///     worker.take(link)
    /// }
    /// ```
    ///
    /// # Errors
    /// A closed executor or a second admission of the same turn is refused.
    pub fn take(&self, link: Arc<dyn crate::turns::ExecutorLink>) -> Result<(), RoomKitError> {
        let (written, accepted) = tokio::sync::oneshot::channel();
        let origin = crate::turns::executor::WorkerOrigin {
            context: link.turn_context(),
            waits_for_writes: true,
            accepted: Some(accepted),
            owner: Some(self.kit.inner.turns.worker_owner()),
        };
        let turn = self.start_request(link.request().clone(), origin)?;
        let kit = Arc::downgrade(&self.kit.inner);
        let running = crate::turn::TurnKit::spawn(&kit, Box::pin(turn.serve_link(link, written)));
        if !running {
            return Err(RoomKitError::KitClosed);
        }
        Ok(())
    }
}
