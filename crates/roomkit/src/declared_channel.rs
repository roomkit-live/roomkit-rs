//! The channel that a room owner holds by declaration alone (RFC §10.6.2).

use std::future::{Future, ready};

use crate::channel::{Channel, ChannelDeclaration, ChannelError};
use crate::error::{RoomKitError, repr};
use crate::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelId, ChannelOutput, ChannelType,
    InboundMessage, RoomContext, RoomEvent,
};

/// An intelligence channel bound and planned by the owner whose object is
/// built by a turn executor. Registering its declaration directly constructs
/// the same channel.
#[derive(Clone, Debug)]
pub struct DeclaredChannel {
    declaration: ChannelDeclaration,
}

impl DeclaredChannel {
    /// Holds an intelligence channel's declaration on its room owner.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::Value`] for a transport declaration: its
    /// deliveries require a live channel in the owner's process.
    pub fn new(declaration: ChannelDeclaration) -> Result<Self, RoomKitError> {
        if declaration.category != ChannelCategory::Intelligence {
            return Err(RoomKitError::Value(format!(
                "Channel {} is declared as a transport channel: only an intelligence \
                 channel can be registered by its declaration alone",
                repr(declaration.channel_id.as_str())
            )));
        }
        Ok(Self { declaration })
    }

    /// Whether the declared agent carries a thinker (§6.4).
    pub fn thinks_while_listening(&self) -> bool {
        self.declaration.thinks_while_listening
    }

    /// Its executor builds a running agent with a provider (§19.1).
    pub fn is_config_only(&self) -> bool {
        false
    }

    /// Display name from the declared agent identity.
    pub fn name(&self) -> Option<&str> {
        self.declaration.identity.as_ref()?.name.as_deref()
    }

    /// Role from the declared agent identity.
    pub fn role(&self) -> Option<&str> {
        self.declaration.identity.as_ref()?.role.as_deref()
    }

    /// Short description from the declared agent identity.
    pub fn description(&self) -> Option<&str> {
        self.declaration.identity.as_ref()?.description.as_deref()
    }

    /// The work the declared agent handles.
    pub fn scope(&self) -> Option<&str> {
        self.declaration.identity.as_ref()?.scope.as_deref()
    }

    /// The declared agent’s own instructions in concise form.
    pub fn purpose(&self) -> Option<&str> {
        self.declaration.identity.as_ref()?.purpose.as_deref()
    }

    /// The voice named by the declaration.
    pub fn voice(&self) -> Option<&str> {
        self.declaration.identity.as_ref()?.voice.as_deref()
    }

    /// The greeting declared by the agent.
    pub fn greeting(&self) -> Option<&str> {
        self.declaration.identity.as_ref()?.greeting.as_deref()
    }

    /// The language named by the declaration.
    pub fn language(&self) -> Option<&str> {
        self.declaration.identity.as_ref()?.language.as_deref()
    }

    /// Whether the agent greets automatically when its session is ready.
    pub fn auto_greet(&self) -> bool {
        self.declaration
            .identity
            .as_ref()
            .is_some_and(|identity| identity.auto_greet)
    }

    fn absent(&self, operation: &str) -> ChannelError {
        RoomKitError::Type(format!(
            "Channel {} is registered by its declaration alone: it cannot {operation} in this \
             process; an executor whose host builds it runs it",
            repr(self.declaration.channel_id.as_str())
        ))
        .into()
    }
}

impl Channel for DeclaredChannel {
    fn channel_id(&self) -> ChannelId {
        self.declaration.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        self.declaration.channel_type.clone()
    }

    fn category(&self) -> ChannelCategory {
        self.declaration.category
    }

    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    fn capabilities(&self) -> ChannelCapabilities {
        self.declaration.capabilities.clone()
    }

    fn recent_events_window(&self) -> usize {
        self.declaration.recent_events_window
    }

    fn serializes_room_turns(&self) -> bool {
        self.declaration.serializes_room_turns || self.declaration.steers_room_turns
    }

    fn steers_room_turns(&self) -> bool {
        self.declaration.steers_room_turns
    }

    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Err(self.absent("take a message")))
    }

    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Err(self.absent("run a turn")))
    }

    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Err(self.absent("be delivered to")))
    }
}
