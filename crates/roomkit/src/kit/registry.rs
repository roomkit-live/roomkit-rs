//! Registering and unregistering a kit's channels and hooks.

use std::future::Future;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, RoomContext, RoomEvent};
use roomkit_core::ops::FrameworkEvent;
use roomkit_core::rules::hook_registry::{HookExecution, HookId, HookSpec, HookTrigger};

use super::{RoomKit, write};
use crate::channel::{Channel, DynChannel};
use crate::error::KitError;
use crate::hooks::{HookHandler, HookReturn, Observer};
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Registers a channel, then emits `channel_registered`.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Config`] when a channel with its id is registered
    /// already.
    pub fn register_channel(&self, channel: impl Channel) -> Result<(), KitError> {
        let declaration = Channel::declaration(&channel);
        let registered = FrameworkEvent::ChannelRegistered {
            channel_id: declaration.channel_id.clone(),
            channel_type: declaration.channel_type,
        };
        let channel_id = declaration.channel_id.clone();
        let channel: Arc<dyn DynChannel> = Arc::new(channel);
        self.inner.try_change_config(|config| {
            config.register_channel(declaration)?;
            // The channel is in place before an operation can see its
            // declaration.
            write(&self.inner.channels).insert(channel_id, channel);
            Ok::<(), KitError>(())
        })?;
        self.inner.publish(registered);
        Ok(())
    }

    /// Unregisters a channel, then emits `channel_unregistered` (RFC §8.2).
    ///
    /// A message on the channel is refused from now on. Its bindings stay in
    /// their rooms, as the store holds them: a delivery to one fails, and
    /// registering a channel under the same id again serves them (gap
    /// G-048). An operation already running keeps the channel it started
    /// with until it calls it.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::UnknownChannel`] when no channel has the id.
    pub fn unregister_channel(&self, channel_id: &ChannelId) -> Result<(), KitError> {
        let declaration = self.inner.try_change_config(|config| {
            // Unregistering fails for an unknown channel only.
            let declaration = config
                .unregister_channel(channel_id)
                .map_err(|_unknown| KitError::UnknownChannel(channel_id.clone()))?;
            // No operation can see the declaration once the channel goes.
            write(&self.inner.channels).remove(channel_id);
            Ok::<_, KitError>(declaration)
        })?;
        self.inner.publish(FrameworkEvent::ChannelUnregistered {
            channel_id: declaration.channel_id,
            channel_type: declaration.channel_type,
        });
        Ok(())
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
}
