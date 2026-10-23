//! Channels: everything that interacts with a room (RFC §4.2, §6.1).

use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use roomkit_core::model::{
    ChannelBinding, ChannelOutput, EventId, InboundMessage, RoomContext, RoomEvent, RoomId,
    Timestamp,
};
use roomkit_core::ops::{ChannelDeclaration, default_inbound_event};

/// What a channel's call failed with. The pipeline records it as the error's
/// message; it never reaches a model or another channel.
pub type ChannelError = Box<dyn Error + Send + Sync>;

/// What the pipeline gives a channel to build the event of an inbound
/// message: its id, its room, and the current instant.
#[derive(Clone, Debug, PartialEq)]
pub struct EventDraft {
    /// The id the event is committed with.
    pub id: EventId,
    /// The room it is committed to.
    pub room_id: RoomId,
    /// The current instant.
    pub now: Timestamp,
}

/// A channel (RFC §6.1): a transport carrying messages to and from people or
/// systems, or an intelligence channel answering events.
///
/// Implement the methods with `async fn`. A transport implements
/// [`deliver`](Channel::deliver); an intelligence channel implements
/// [`on_event`](Channel::on_event), each call of which is a turn. Every
/// method has a default: parse the message's content as it came, ignore the
/// event, deliver nothing.
///
/// ```
/// use roomkit::{Channel, ChannelDeclaration, ChannelError};
/// use roomkit::model::{
///     ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
///     ChannelOutput, ChannelType, RoomContext, RoomEvent,
/// };
///
/// struct Console {
///     declaration: ChannelDeclaration,
/// }
///
/// impl Channel for Console {
///     fn declaration(&self) -> ChannelDeclaration {
///         self.declaration.clone()
///     }
///
///     async fn deliver(
///         &self,
///         event: RoomEvent,
///         _binding: ChannelBinding,
///         _context: RoomContext,
///     ) -> Result<ChannelOutput, ChannelError> {
///         let _shown = roomkit::text_of(&event.content);
///         Ok(ChannelOutput::default())
///     }
/// }
/// ```
pub trait Channel: Send + Sync + 'static {
    /// What the kit knows of the channel: its id, type, category, direction
    /// and capabilities.
    fn declaration(&self) -> ChannelDeclaration;

    /// Converts a message the channel received into a room event (RFC §6.1).
    /// The pipeline then writes the fields it owns: the id of `draft`
    /// (RFC §8.1), and the type, depth, visibility, address and source the
    /// message decides.
    fn handle_inbound(
        &self,
        message: InboundMessage,
        _context: RoomContext,
        draft: EventDraft,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let event = default_inbound_event(
            &message,
            &self.declaration(),
            draft.room_id,
            draft.id,
            draft.now,
        );
        async move { Ok(event) }
    }

    /// Reacts to a room event the channel may read (RFC §6.1). For an
    /// intelligence channel, this is its turn: the events it returns in a
    /// responding output re-enter the room.
    fn on_event(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        async { Ok(ChannelOutput::default()) }
    }

    /// Pushes an event to the channel's external recipient (RFC §6.1).
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        async { Ok(ChannelOutput::default()) }
    }
}

/// A boxed, sendable future, as a dynamic channel or hook returns it.
pub(crate) type BoxFuture<'call, T> = Pin<Box<dyn Future<Output = T> + Send + 'call>>;

/// A [`Channel`] behind a trait object, so the kit holds channels of any
/// type.
pub(crate) trait DynChannel: Send + Sync {
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
        draft: EventDraft,
    ) -> BoxFuture<'_, Result<RoomEvent, ChannelError>>;

    fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>>;

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>>;
}

impl<C: Channel> DynChannel for C {
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
        draft: EventDraft,
    ) -> BoxFuture<'_, Result<RoomEvent, ChannelError>> {
        Box::pin(Channel::handle_inbound(self, message, context, draft))
    }

    fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>> {
        Box::pin(Channel::on_event(self, event, binding, context))
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>> {
        Box::pin(Channel::deliver(self, event, binding, context))
    }
}
