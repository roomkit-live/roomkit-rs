//! A channel explicitly receiving its own registering kit.

use std::sync::Arc;

use super::{RoomKit, WeakRoomKit};
use crate::channel::{Channel, config_of};
use crate::error::RoomKitError;
use crate::store::{ConversationStore, InMemoryStore};

/// An application channel that asks for the kit it is registered with:
/// Python's `FrameworkAwareChannel`. Register it with [`framework_aware`].
pub trait FrameworkAwareChannel<S: ConversationStore = InMemoryStore>: Channel {
    /// Called synchronously once the channel can be looked up in the kit.
    /// The weak handle upgrades to the same kit, with its actual store.
    ///
    /// # Errors
    ///
    /// An error reaches the caller of [`RoomKit::register_channel`]; the
    /// channel remains registered and its registration event is not emitted.
    fn set_framework(&self, framework: WeakRoomKit<S>) -> Result<(), RoomKitError>;
}

/// A channel explicitly opted in to framework injection at registration.
#[derive(Debug)]
pub struct FrameworkAware<C>(C);

/// Mark a channel for framework injection by [`RoomKit::register_channel`].
/// Registering an unmarked channel calls no such method.
pub fn framework_aware<C: Channel>(channel: C) -> FrameworkAware<C> {
    FrameworkAware(channel)
}

mod sealed {
    pub trait Sealed {}
    impl<C: super::Channel> Sealed for C {}
    impl<C> Sealed for super::FrameworkAware<C> {}
    impl Sealed for crate::ChannelDeclaration {}
}

/// An ordinary [`Channel`], a [`ChannelDeclaration`](crate::ChannelDeclaration)
/// or the value returned by [`framework_aware`].
pub trait ChannelRegistration<S: ConversationStore>: sealed::Sealed {
    #[doc(hidden)]
    fn register_on(self, kit: &RoomKit<S>) -> Result<(), RoomKitError>;
}

pub(super) type InjectFramework<S> = Box<dyn FnOnce(WeakRoomKit<S>) -> Result<(), RoomKitError>>;

fn register<S: ConversationStore, C: Channel>(
    kit: &RoomKit<S>,
    channel: Arc<C>,
    inject: Option<InjectFramework<S>>,
) -> Result<(), RoomKitError> {
    let declaration = config_of(channel.as_ref());
    let human_input = channel.human_input_handler();
    kit.register_dyn_with(channel, declaration, human_input, inject)
}

impl<S: ConversationStore, C: Channel> ChannelRegistration<S> for C {
    fn register_on(self, kit: &RoomKit<S>) -> Result<(), RoomKitError> {
        register(kit, Arc::new(self), None)
    }
}

impl<S: ConversationStore> ChannelRegistration<S> for crate::ChannelDeclaration {
    fn register_on(self, kit: &RoomKit<S>) -> Result<(), RoomKitError> {
        crate::DeclaredChannel::new(self)?.register_on(kit)
    }
}

impl<S: ConversationStore, C: FrameworkAwareChannel<S>> ChannelRegistration<S>
    for FrameworkAware<C>
{
    fn register_on(self, kit: &RoomKit<S>) -> Result<(), RoomKitError> {
        let channel = Arc::new(self.0);
        let recipient = Arc::clone(&channel);
        register(
            kit,
            channel,
            Some(Box::new(move |kit| recipient.set_framework(kit))),
        )
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// Install the ordinary bridges before an opted-in channel receives
    /// the kit. A rejected injection leaves its greeting uninstalled.
    pub(super) fn wire_registration(
        &self,
        greeting: Option<roomkit_core::model::ChannelId>,
        inject: Option<InjectFramework<S>>,
        wire: impl FnOnce() -> Result<(), RoomKitError>,
    ) -> Result<(), RoomKitError> {
        let greet = || greeting.map_or(Ok(()), |id| self.register_auto_greet_hook(id));
        if let Some(inject) = inject {
            wire()?;
            inject(self.downgrade())?;
            greet()
        } else {
            greet()?;
            wire()
        }
    }
}
