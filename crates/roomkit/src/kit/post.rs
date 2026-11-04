//! The host's own voice (RFC §10.5 `post()`): what the application that
//! embeds the kit says in a room in its own name.

use roomkit_core::model::{ChannelId, EventContent, EventType, RoomEvent, RoomId};
use roomkit_core::ops::Entry;

use super::RoomKit;
use super::options::PostOptions;
use crate::error::RoomKitError;
use crate::host::HostChannel;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Writes `content` to the people of the room `room_id` in the host's
    /// own name (RFC §10.5): a notice, a workflow's question, a reminder. It
    /// comes from a host channel, a transport of type `HOST`: the channel
    /// `options` name, or the kit's own, registered on first use under the
    /// id [`HOST_CHANNEL_ID`](crate::model::HOST_CHANNEL_ID). The room reads it as
    /// a party's message, `INBOUND`, broadcasts it to its transports, and
    /// asks the agents it addresses only, none by default (RFC §19.3).
    ///
    /// The post is injected as [`send_event`](Self::send_event) injects
    /// an event, with its idempotency key, its replay and its reentrancy.
    /// A room that never bound the host channel gets it attached first, an
    /// ordinary attachment decided in one section under the room lock: a
    /// binding the integrator made, before or concurrently, is kept as it
    /// is. Its speaker, `participant_id`, must be a participant of the room
    /// the host channel reaches (`connected_via`), by their id or their
    /// identity's (RFC §5.5). One `process_timeout` bounds the whole call,
    /// the wait for the lock included; once the lock is held, the attach
    /// runs to its end. A room of another organization than the one
    /// `options` name, or of none, is not found, whoever its speaker names,
    /// nothing written (RFC §17.2).
    ///
    /// Returns the committed event, as `send_event` returns it.
    ///
    /// # Errors
    ///
    /// As Python's `post` raises, nothing written:
    /// [`RoomKitError::ChannelNotRegistered`] for a channel no one
    /// registered, [`RoomKitError::Value`] for one of another type, for
    /// the id `host` held by one, or for a key in the framework's
    /// publication namespace (RFC §22.1), [`RoomKitError::RoomNotFound`],
    /// [`RoomKitError::ParticipantNotFound`] for a speaker the host channel
    /// does not reach, and `send_event`'s refusals:
    /// [`RoomKitError::RoomClosed`], [`RoomKitError::ChannelNotFound`] for a
    /// room the host channel was detached from,
    /// [`RoomKitError::ProcessTimeout`]; or what `send_event` raises.
    pub async fn post(
        &self,
        room_id: &RoomId,
        content: impl Into<EventContent>,
        options: PostOptions,
    ) -> Result<RoomEvent, RoomKitError> {
        crate::delivery::refuse_reserved(options.idempotency_key.as_deref())?;
        let mut content = content.into();
        // A content a struct literal built past its model's check is refused
        // before anything is written, as Python's model refuses it when built,
        // and one kept is written as its constructor writes it.
        content.check()?;
        if options.channel_id.is_none() {
            self.register_host_channel()?;
        }
        let organization_id = options.organization_id.clone();
        let channel_id = options.channel_id.clone().unwrap_or_else(ChannelId::host);
        let span = self
            .inner
            .send_event_span(room_id, &channel_id, EventType::Message);
        let entry = Entry::Post {
            room_id: room_id.clone(),
            organization_id,
            post: Box::new(options.post(content)),
        };
        let posted = span.within(Box::pin(self.enter(entry, POST, false))).await;
        super::inject::injected_under(&span, posted)
    }

    /// Registers the kit's own host channel, when no channel holds its id:
    /// a concurrent first post's registration stands. A channel of another
    /// type that holds the id is the post's to refuse.
    fn register_host_channel(&self) -> Result<(), RoomKitError> {
        let host = ChannelId::host();
        if self.config().channel(&host).is_some() {
            return Ok(());
        }
        match self.register_channel(HostChannel::default()) {
            Err(RoomKitError::ChannelAlreadyRegistered(_)) | Ok(()) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// The kit's call that posts, as an error names it.
const POST: &str = "post";
