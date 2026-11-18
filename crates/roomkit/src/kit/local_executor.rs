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
use crate::turns::{HookOptions, RoomHookOptions, TakesOptions};
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

impl<S: ConversationStore> TurnExecutor<S> {
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
            || self.kit.inner.channel(channel_id).is_some(),
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

    /// The kit's executor serves every organization (RFC §10.6.7).
    pub fn organizations(&self) -> BTreeSet<String> {
        self.advertised_organizations().unwrap_or_default()
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
    /// Run the request delivered by an application backend (RFC §10.6.2). The link
    /// releases its resources after the executor finishes sending the turn's frames.
    ///
    /// # Errors
    /// A closed executor or a second admission of the same turn is refused.
    pub fn take(&self, link: Arc<dyn crate::turns::ExecutorLink>) -> Result<(), RoomKitError> {
        let origin = crate::turns::executor::WorkerOrigin {
            context: link.turn_context(),
            waits_for_writes: true,
            #[cfg(feature = "redis")]
            owner: None,
        };
        let turn = self.start_request(link.request().clone(), origin)?;
        let kit = Arc::downgrade(&self.kit.inner);
        let running = crate::turn::TurnKit::spawn(&kit, Box::pin(turn.serve_link(link)));
        if !running {
            return Err(RoomKitError::KitClosed);
        }
        Ok(())
    }
}
