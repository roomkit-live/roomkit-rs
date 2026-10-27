//! Channels: everything that interacts with a room (RFC §4.2, §6.1).

use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use roomkit_core::model::{
    ChannelBinding, ChannelOutput, EventId, InboundMessage, RoomContext, RoomEvent, RoomId,
    Timestamp,
};
use roomkit_core::ops::{ChannelDeclaration, default_inbound_event};

use crate::stream::{DeliveryStream, StreamItem};

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
///
/// A transport declares how it writes its addresses: an SMS channel whose
/// numbers written without a country code are French ones.
///
/// ```
/// use roomkit::model::{ChannelCategory, ChannelDirection, ChannelId, ChannelType};
/// use roomkit::{Addressing, ChannelDeclaration, CountryCode};
///
/// let mut declaration = ChannelDeclaration::new(
///     ChannelId::new("sms")?,
///     ChannelType::SMS,
///     ChannelCategory::Transport,
///     ChannelDirection::Bidirectional,
/// );
/// declaration.country_code = Some(CountryCode::new("+33")?);
/// let form = declaration.form();
/// assert_eq!(form.addressing, Addressing::PhoneNumber);
/// assert_eq!(form.normalize("06 12 34 56 78"), "+33612345678");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub trait Channel: Send + Sync + 'static {
    /// What the kit knows of the channel: its id, type, category, direction
    /// and capabilities.
    fn declaration(&self) -> ChannelDeclaration;

    /// Converts a message the channel received into a room event (RFC §6.1).
    /// The pipeline then writes the fields it owns: the id of `draft`
    /// (RFC §8.1), and the type, depth, visibility, address and source the
    /// message decides.
    ///
    /// The default takes any message, as a transport does. A channel that
    /// receives none refuses it here with an error, before anything enters
    /// the room: an AI agent does (`roomkit_ai::AiAgent`), since an event
    /// under its id is read as its own words.
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

    /// Delivers a response as it is written (RFC §6.1 `deliver_stream`), for
    /// a channel whose declaration says it
    /// [`supports_streaming_delivery`](ChannelDeclaration::supports_streaming_delivery):
    /// `stream` yields the response's text as it comes and each row once the
    /// store holds it, with its id and its index; the channel is never
    /// delivered those rows again. It sets the pace: returning before the
    /// stream's end, or dropping it, stops the response, the text it was
    /// handed stored marked `cancelled` (RFC §12.2 step 13s). The text is
    /// shown before `BEFORE_BROADCAST` judges the row it ends in.
    ///
    /// The default reads the stream to its end, then delivers each row
    /// through [`deliver`](Channel::deliver); a response that fails is
    /// delivered as far as it came, then the failure is returned.
    fn deliver_stream(
        &self,
        stream: DeliveryStream,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        async move {
            let (rows, failure) = buffered(stream).await;
            let mut output = ChannelOutput::default();
            for row in rows {
                let delivered = self.deliver(row, binding.clone(), context.clone()).await?;
                output.tasks.extend(delivered.tasks);
                output.observations.extend(delivered.observations);
                output.metadata_updates.extend(delivered.metadata_updates);
            }
            match failure {
                Some(failure) => Err(failure),
                None => Ok(output),
            }
        }
    }

    /// Releases what the channel holds (RFC §6.1 `close`): the kit's
    /// [`shutdown`](crate::RoomKit::shutdown) calls it once for each
    /// channel registered then, in turn, after the turns running are cut. It
    /// must not wait without bound on anything the channel does not own.
    ///
    /// The default releases nothing.
    fn close(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// The rows of `stream`, read to its end, and the failure it ended on.
async fn buffered(mut stream: DeliveryStream) -> (Vec<RoomEvent>, Option<ChannelError>) {
    let mut rows = Vec::new();
    while let Some(item) = stream.next().await {
        match item {
            Ok(StreamItem::Row(row)) => rows.push(*row),
            Ok(StreamItem::Text(_)) => {}
            Err(failure) => return (rows, Some(failure)),
        }
    }
    (rows, None)
}

/// A boxed, sendable future, as a channel or a router behind a trait object
/// returns it.
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

    fn deliver_stream(
        &self,
        stream: DeliveryStream,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>>;

    fn close(&self) -> BoxFuture<'_, ()>;
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

    fn deliver_stream(
        &self,
        stream: DeliveryStream,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<ChannelOutput, ChannelError>> {
        Box::pin(Channel::deliver_stream(self, stream, binding, context))
    }

    fn close(&self) -> BoxFuture<'_, ()> {
        Box::pin(Channel::close(self))
    }
}
