//! Public access to the kit's own turn executor (RFC §10.6.6).
use std::collections::BTreeSet;
use std::future::Future;

use roomkit_core::model::{ChannelId, RoomContext};

use super::{HookEngine, RoomKit};
use crate::hooks::{HookPayload, IntoHookReturn, closure_kept};
use crate::turns::{HookOptions, RoomHookOptions, TakesOptions};
use crate::{
    ConversationStore, HookRegistration, HookTrigger, InMemoryStore, RoomKitError, TurnTimeouts,
};

/// The kit's local executor: its turn hooks and the channels it takes.
/// Obtain it through [`RoomKit::turn_executor`]; backend construction is private.
/// Like a hook engine, this handle keeps the kit alive; closures should capture
/// a [`WeakRoomKit`](crate::WeakRoomKit).
pub struct TurnExecutor<S = InMemoryStore> {
    kit: RoomKit<S>,
}

impl<S> Clone for TurnExecutor<S> {
    fn clone(&self) -> Self {
        Self {
            kit: self.kit.clone(),
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
        TurnExecutor { kit: self.clone() }
    }
}

impl<S: ConversationStore> TurnExecutor<S> {
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

    /// Whether this executor has closed.
    pub fn closed(&self) -> bool {
        self.kit.inner.turns.is_closed()
    }

    /// Whether it hosts this channel, even after closing.
    pub fn hosts_channel(&self, channel_id: &ChannelId) -> bool {
        self.kit.inner.channel(channel_id).is_some()
    }

    /// Whether it declares this channel and has not closed.
    pub fn declares(&self, channel_id: &ChannelId) -> bool {
        !self.closed() && self.hosts_channel(channel_id)
    }

    /// An open-ended registry predicate has no finite advertised host list.
    pub fn hosted(&self) -> Option<BTreeSet<String>> {
        None
    }

    /// The kit's executor serves every organization (RFC §10.6.7).
    pub fn organizations(&self) -> BTreeSet<String> {
        BTreeSet::from(["*".to_owned()])
    }

    /// Whether it serves this organization; the kit's executor serves all.
    pub fn serves(&self, organization_id: Option<&str>) -> bool {
        let _ = organization_id;
        true
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
