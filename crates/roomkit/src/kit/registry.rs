//! Registering and unregistering a kit's channels and hooks.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;

use roomkit_core::model::{ChannelId, RoomContext};
use roomkit_core::ops::FrameworkEvent as CoreEvent;
use roomkit_core::rules::hook_registry::HookTrigger;

use super::{RoomKit, greeting, read, write};
use crate::channel::{Channel, DynChannel, config_of};
use crate::error::{RoomKitError, repr};
use crate::hooks::{HookHandler, HookOptions, HookPayload, IntoHookReturn, closure_kept};
use crate::registered::{Registered, RegisteredChannel};
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Registers a channel, then emits `channel_registered`.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4); [`RoomKitError::Value`] for capabilities their model refuses,
    /// a limit of 0, as the Python implementation's model refuses them when
    /// the channel builds them, and for a channel that steers its room turns
    /// but does not serialize them (RFC §6.4); then
    /// [`RoomKitError::ChannelAlreadyRegistered`] when a channel with its id
    /// is registered already, and [`RoomKitError::Value`] for a channel
    /// claiming the type `system` (RFC §5.3); [`RoomKitError::Runtime`] for
    /// an agent whose human-input handler is closed whole, the kit then
    /// holding the channel unannounced, as Python's does.
    pub fn register_channel(&self, channel: impl Channel) -> Result<(), RoomKitError> {
        let declaration = config_of(&channel);
        let human_input = channel.human_input_handler();
        self.register_dyn(Arc::new(channel), declaration, human_input)
    }

    /// [`register_channel`](Self::register_channel) for a channel already
    /// behind a trait object, with its declaration and its human-input
    /// handler read from it: a strategy's agent given live (RFC §19.7).
    pub(crate) fn register_dyn(
        &self,
        channel: Arc<dyn DynChannel>,
        declaration: roomkit_core::ops::ChannelConfig,
        human_input: Option<crate::human_input::HumanInputHandler>,
    ) -> Result<(), RoomKitError> {
        self.inner.open()?;
        declaration.capabilities.check()?;
        refuse_unserialized_steering(channel.as_ref(), &declaration.channel_id)?;
        let registered = CoreEvent::channel_registered(
            declaration.channel_id.clone(),
            declaration.channel_type.clone(),
        );
        let channel_id = declaration.channel_id.clone();
        let auto_greeting = greeting::auto_greeting_agent(&declaration, channel.as_ref());
        let human_input =
            human_input.map(|handler| (channel_id.clone(), handler, Arc::clone(&channel)));
        self.inner.try_change_config(|config| {
            config.register_channel(declaration)?;
            // The channel is in place before an operation can see its
            // declaration.
            write(&self.inner.channel_order).push(channel_id.clone());
            write(&self.inner.channels).insert(channel_id, channel);
            Ok::<(), RoomKitError>(())
        })?;
        if let Some(agent_id) = auto_greeting {
            self.register_auto_greet_hook(agent_id)?;
        }
        if let Some((channel_id, handler, channel)) = human_input {
            // Registered on its handler once the kit holds it, the channel
            // a kit registered last owning its id's requests (RMK-1201).
            // A handler closed whole refuses it, and the call fails with its
            // RuntimeError, the kit holding the channel unannounced, as
            // Python's register_channel raises once it holds it (RMK-1255).
            let registration = handler.register(channel_id.as_str())?;
            channel.human_input_registered(registration);
            self.inner.wire_human_input(&channel_id, &handler);
        }
        drop(self.inner.emit(registered));
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
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started, as
    /// the Python implementation's `unregister_channel` raises
    /// `KitClosedError` (RFC §12.10.4).
    pub fn unregister_channel(
        &self,
        channel_id: &ChannelId,
    ) -> Result<Option<Arc<dyn RegisteredChannel>>, RoomKitError> {
        self.inner.open()?;
        let Ok(unregistered) = self.inner.try_change_config(|config| {
            let Ok(declaration) = config.unregister_channel(channel_id) else {
                return Ok::<_, Infallible>(None);
            };
            // No operation can see the declaration once the channel goes.
            write(&self.inner.channel_order).retain(|id| id != channel_id);
            let channel = write(&self.inner.channels).remove(channel_id);
            Ok(channel.map(|channel| Registered {
                declaration,
                channel,
            }))
        });
        let Some(unregistered) = unregistered else {
            return Ok(None);
        };
        self.inner.forget_owner_tools(channel_id);
        self.forget_auto_greet(channel_id);
        drop(self.inner.emit(CoreEvent::channel_unregistered(
            unregistered.declaration.channel_id.clone(),
            unregistered.declaration.channel_type.clone(),
        )));
        Ok(Some(Arc::new(unregistered)))
    }

    /// The channels registered, in their registration order: Python's
    /// `RoomKit.list_channels()`. It answers during the kit's close and
    /// after it.
    ///
    /// ```
    /// let kit = roomkit::RoomKit::new(roomkit::RoomKitOptions::default());
    /// assert!(kit.list_channels().is_empty());
    /// ```
    pub fn list_channels(&self) -> Vec<Arc<dyn RegisteredChannel>> {
        let order = read(&self.inner.channel_order).clone();
        order
            .iter()
            .filter_map(|channel_id| self.inner.registered(channel_id))
            .collect()
    }

    /// The channels registered, by their id: Python's `RoomKit.channels`.
    /// It answers during the kit's close and after it.
    pub fn channels(&self) -> BTreeMap<ChannelId, Arc<dyn RegisteredChannel>> {
        self.list_channels()
            .into_iter()
            .map(|channel| (channel.channel_id().clone(), channel))
            .collect()
    }

    /// Registers a hook on `trigger` (RFC §9.1): Python's
    /// `RoomKit.hook(trigger, execution, priority, name, timeout,
    /// channel_types, channel_ids, directions, event_types, fail_closed,
    /// needs_lock)`, its keywords in `options`. The closure takes the
    /// trigger's payload, its type written on its argument: a
    /// [`RoomEvent`](crate::model::RoomEvent) for an event's or a lifecycle
    /// change's trigger, a [`ToolCallEvent`](crate::ToolCallEvent) for
    /// `BEFORE_TOOL_USE` and `ON_TOOL_CALL`, a
    /// [`SessionStartedEvent`](crate::SessionStartedEvent) for
    /// `ON_SESSION_STARTED`, a
    /// [`StatusEntry`](crate::orchestration::StatusEntry) for
    /// `ON_STATUS_POSTED`, an [`Observation`](crate::model::Observation)
    /// for `ON_FEEDBACK`. It returns a `HookResult`, one or what it
    /// raised, or `()`, Python's `None`; what an ASYNC hook returns is
    /// discarded.
    ///
    /// ```
    /// # fn register(kit: &roomkit::RoomKit) -> Result<(), roomkit::RoomKitError> {
    /// use roomkit::model::hook::{AllowOptions, BlockOptions};
    /// use roomkit::model::{EventContent, HookResult, RoomContext, RoomEvent};
    /// use roomkit::{HookOptions, HookTrigger};
    ///
    /// let options = HookOptions { name: "spam_filter".to_owned(), ..Default::default() };
    /// kit.hook(
    ///     HookTrigger::BeforeBroadcast,
    ///     |event: RoomEvent, _context: RoomContext| {
    ///         let spam = matches!(&event.content, EventContent::Text(text) if text.body.contains("spam"));
    ///         async move {
    ///             if spam {
    ///                 HookResult::block("spam", BlockOptions::default())
    ///             } else {
    ///                 HookResult::allow(AllowOptions::default())
    ///             }
    ///         }
    ///     },
    ///     options,
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4), and [`RoomKitError::Value`], Python's `ValueError`, when the
    /// registration breaks a rule of RFC §9.1 or its handler takes another
    /// payload than its trigger's.
    pub fn hook<P, F, Fut, R>(
        &self,
        trigger: HookTrigger,
        handler: F,
        options: HookOptions,
    ) -> Result<(), RoomKitError>
    where
        P: HookPayload,
        F: Fn(P, RoomContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoHookReturn<P>,
    {
        self.inner.open()?;
        let hook = options.registration(trigger, closure_kept(handler));
        self.hook_engine().register(hook).map(drop)
    }

    /// Registers a hook whose handler is a type of its own implementing
    /// [`HookHandler`] (RFC §9.1), as [`hook`](Self::hook) does.
    ///
    /// # Errors
    ///
    /// As [`hook`](Self::hook).
    pub fn hook_handler<P: HookPayload>(
        &self,
        trigger: HookTrigger,
        handler: impl HookHandler<P>,
        options: HookOptions,
    ) -> Result<(), RoomKitError> {
        self.inner.open()?;
        let handler: Arc<dyn HookHandler<P>> = Arc::new(handler);
        let hook = options.registration(trigger, P::kept(handler));
        self.hook_engine().register(hook).map(drop)
    }
}

/// A channel that steers its room turns serializes them (RFC §6.4): one that
/// says it steers them but not that it serializes them would be neither, its
/// steering silently dead, as Python's `register_channel` refuses it.
fn refuse_unserialized_steering(
    channel: &dyn DynChannel,
    channel_id: &ChannelId,
) -> Result<(), RoomKitError> {
    if channel.steers_room_turns() && !channel.serializes_room_turns() {
        return Err(RoomKitError::Value(format!(
            "Channel {} steers its room turns but does not serialize them",
            repr(channel_id.as_str())
        )));
    }
    Ok(())
}
