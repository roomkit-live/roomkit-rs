//! Registering and unregistering a kit's channels and hooks.

use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, RoomContext, RoomEvent};
use roomkit_core::ops::FrameworkEvent;
use roomkit_core::rules::hook_registry::{HookExecution, HookId, HookSpec, HookTrigger};

use super::{RoomKit, write};
use crate::channel::{Channel, DynChannel};
use crate::error::KitError;
use crate::hooks::{HookError, HookHandler, HookReturn, Observer};
use crate::registered::{Registered, RegisteredChannel};
use crate::store::ConversationStore;
use crate::tools::{ToolCallEvent, ToolHandler, ToolHookSpec, ToolUse, ToolVerdict};

impl<S: ConversationStore> RoomKit<S> {
    /// Registers a channel, then emits `channel_registered`.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Config`] when a channel with its id is registered
    /// already.
    pub fn register_channel(&self, channel: impl Channel) -> Result<(), KitError> {
        let declaration = Channel::declaration(&channel);
        let registered = FrameworkEvent::channel_registered(
            declaration.channel_id.clone(),
            declaration.channel_type.clone(),
        );
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

    /// Unregisters a channel, emits `channel_unregistered` (RFC §8.2), and
    /// hands the channel back, not closed, as the Python implementation's
    /// `unregister_channel` returns it: closing is the caller's
    /// ([`RegisteredChannel::close`]). `None`, nothing emitted, for a
    /// channel no one registered.
    ///
    /// A message on the channel is refused from now on. Its bindings stay in
    /// their rooms, as the store holds them: a delivery to one fails, and
    /// registering a channel under the same id again serves them (gap
    /// G-048). An operation already running keeps the channel it started
    /// with until it calls it.
    pub fn unregister_channel(&self, channel_id: &ChannelId) -> Option<Arc<dyn RegisteredChannel>> {
        let Ok(unregistered) = self.inner.try_change_config(|config| {
            let Ok(declaration) = config.unregister_channel(channel_id) else {
                return Ok::<_, Infallible>(None);
            };
            // No operation can see the declaration once the channel goes.
            let channel = write(&self.inner.channels).remove(channel_id);
            Ok(channel.map(|channel| Registered {
                declaration,
                channel,
            }))
        });
        let unregistered = unregistered?;
        self.inner.publish(FrameworkEvent::channel_unregistered(
            unregistered.declaration.channel_id.clone(),
            unregistered.declaration.channel_type.clone(),
        ));
        Some(Arc::new(unregistered))
    }

    /// Registers a hook whose handler is a closure, given the payload and
    /// the room (RFC §9.1). The closure's argument types are the ones it is
    /// called with, so they need no annotation:
    ///
    /// ```
    /// # fn register(kit: &roomkit::RoomKit) -> Result<(), roomkit::KitError> {
    /// use roomkit::model::HookResult;
    /// use roomkit::model::hook::{AllowOptions, BlockOptions};
    /// use roomkit::{HookExecution, HookSpec, HookTrigger, text_of};
    ///
    /// let spec = HookSpec::new("spam_filter", HookTrigger::BeforeBroadcast, HookExecution::Sync);
    /// kit.hook(spec, |event, _context| {
    ///     let spam = text_of(&event.content).contains("spam");
    ///     async move {
    ///         Ok(if spam {
    ///             HookResult::block("spam", BlockOptions::default())
    ///         } else {
    ///             HookResult::allow(AllowOptions::default())
    ///         })
    ///     }
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A handler of a type of its own registers with
    /// [`hook_handler`](Self::hook_handler). A tool trigger's payload is a
    /// tool call, no event: its hooks register with
    /// [`before_tool_use`](Self::before_tool_use),
    /// [`on_tool_call`](Self::on_tool_call) and
    /// [`observe_tool_calls`](Self::observe_tool_calls).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Registration`] when the registration breaks a rule
    /// of RFC §9.1, and for a tool trigger (`PayloadMismatch`).
    pub fn hook<F, Fut>(&self, spec: HookSpec, handler: F) -> Result<HookId, KitError>
    where
        F: Fn(RoomEvent, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = HookReturn> + Send + 'static,
    {
        self.hook_handler(spec, handler)
    }

    /// Registers a hook whose handler is a type of its own implementing
    /// [`HookHandler`] (RFC §9.1).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Registration`] when the registration breaks a rule
    /// of RFC §9.1.
    pub fn hook_handler(
        &self,
        spec: HookSpec,
        handler: impl HookHandler,
    ) -> Result<HookId, KitError> {
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
    /// fact, and what it raises is logged. An observer of a lifecycle
    /// trigger reads the change its payload reports, the room, the channel
    /// and the timer's reason, with
    /// [`LifecyclePayload::of`](crate::LifecyclePayload::of).
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
        self.hook_handler(spec, Observer(handler))
    }

    /// Registers a `BEFORE_TOOL_USE` hook (RFC §9.3): the gate of every call
    /// an agent's tool loop runs, once the call passed the agent's own
    /// checks (RFC §21.1), in chain order. It reads the call with the
    /// arguments the hooks before it left, and allows it, rewrites its
    /// arguments, or blocks it with words the model reads. The trigger
    /// fails closed: a hook that raises or runs past its timeout refuses the
    /// call, the model reading the fixed refusal and the hook's observers
    /// its error.
    ///
    /// An approval that holds the call until a person answers:
    ///
    /// ```
    /// # fn register(kit: &roomkit::RoomKit) -> Result<(), roomkit::KitError> {
    /// use std::time::Duration;
    /// use roomkit::{ToolHookSpec, ToolUse};
    ///
    /// let spec = ToolHookSpec::new("approve").with_timeout(Duration::from_secs(300));
    /// kit.before_tool_use(spec, |event, _context| {
    ///     let wipes = event.call.name.starts_with("delete_");
    ///     async move {
    ///         Ok(if wipes { ToolUse::block("A manager must approve this.") } else { ToolUse::Allow })
    ///     }
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Registration`] when the name is taken.
    pub fn before_tool_use<F, Fut>(
        &self,
        spec: impl Into<ToolHookSpec>,
        handler: F,
    ) -> Result<HookId, KitError>
    where
        F: Fn(ToolCallEvent, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolUse, HookError>> + Send + 'static,
    {
        let spec = spec
            .into()
            .on(HookTrigger::BeforeToolUse, HookExecution::Sync);
        self.tool_hook(spec, ToolHandler::gate(handler))
    }

    /// Registers a SYNC `ON_TOOL_CALL` hook (RFC §9.3): it reads each call
    /// an agent's tool loop served, with its result as the hooks before it
    /// left it, and keeps the result, replaces it, or withholds it, which
    /// fails the call `blocked`. A hook that raises or runs past its timeout
    /// is skipped. A call that was refused or failed never reaches it: it
    /// reaches the observers only ([`observe_tool_calls`](Self::observe_tool_calls)).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Registration`] when the name is taken.
    pub fn on_tool_call<F, Fut>(
        &self,
        spec: impl Into<ToolHookSpec>,
        handler: F,
    ) -> Result<HookId, KitError>
    where
        F: Fn(ToolCallEvent, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolVerdict, HookError>> + Send + 'static,
    {
        let spec = spec.into().on(HookTrigger::OnToolCall, HookExecution::Sync);
        self.tool_hook(spec, ToolHandler::verdict(handler))
    }

    /// Registers an ASYNC `ON_TOOL_CALL` observer (RFC §9.3): it reads every
    /// call an agent's tool loop makes once, when it ended, served, refused
    /// or failed, with its outcome, the result as the model read it and, for
    /// the observers alone, what failed (`error_detail`). It runs in the
    /// background, what it raises logged. It runs as a task of the turn
    /// that made the call: a post it makes while the turn's delivery runs
    /// draws one of the cascade's passes, and what it sends is at least as
    /// deep as the turn was, once the turn is over too (RFC §8.3).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Registration`] when the name is taken.
    pub fn observe_tool_calls<F, Fut>(
        &self,
        spec: impl Into<ToolHookSpec>,
        handler: F,
    ) -> Result<HookId, KitError>
    where
        F: Fn(ToolCallEvent, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let spec = spec
            .into()
            .on(HookTrigger::OnToolCall, HookExecution::Async);
        self.tool_hook(spec, ToolHandler::observer(handler))
    }

    /// Registers the tool hook `spec` with `handler`.
    fn tool_hook(&self, spec: HookSpec, handler: ToolHandler) -> Result<HookId, KitError> {
        self.inner.try_change_config(|config| {
            let id = config.hooks.register_tool_hook(spec)?;
            // The handler is in place before a turn can see the hook.
            write(&self.inner.tool_hooks).insert(id, handler);
            Ok::<HookId, KitError>(id)
        })
    }
}
