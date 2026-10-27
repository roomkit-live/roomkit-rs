//! The host's own voice (RFC §10.5 `post()`): what the application that
//! embeds the kit says in a room in its own name.

use roomkit_core::model::{ChannelId, EventContent, RoomEvent, RoomId};
use roomkit_core::ops::{ConfigError, Entry};

use super::RoomKit;
use super::options::PostOptions;
use crate::error::KitError;
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
    /// Returns [`KitError::Inbound`], nothing written, with
    /// [`InboundError::UnknownChannel`](crate::InboundError::UnknownChannel)
    /// for a channel no one registered,
    /// [`InboundError::NotAHostChannel`](crate::InboundError::NotAHostChannel)
    /// for one of another type or for the id `host` held by one,
    /// [`InboundError::RoomNotFound`](crate::InboundError::RoomNotFound),
    /// [`InboundError::ParticipantNotFound`](crate::InboundError::ParticipantNotFound)
    /// for a speaker the host channel does not reach, and
    /// [`InboundError::Refused`](crate::InboundError::Refused) as
    /// `send_event`'s: `room_closed`, `source_unbound` for a room the host
    /// channel was detached from, `process_timeout`; or with what
    /// `send_event` raises.
    pub async fn post(
        &self,
        room_id: &RoomId,
        content: EventContent,
        options: PostOptions,
    ) -> Result<RoomEvent, KitError> {
        if options.channel_id.is_none() {
            self.register_host_channel()?;
        }
        let organization_id = options.organization_id.clone();
        let entry = Entry::Post {
            room_id: room_id.clone(),
            organization_id,
            post: Box::new(options.post(content)),
        };
        let result = self.enter(entry, POST).await?;
        result.event.ok_or(KitError::NoEvent)
    }

    /// Registers the kit's own host channel, when no channel holds its id:
    /// a concurrent first post's registration stands. A channel of another
    /// type that holds the id is the post's to refuse.
    fn register_host_channel(&self) -> Result<(), KitError> {
        let host = ChannelId::host();
        if self.config().channel(&host).is_some() {
            return Ok(());
        }
        match self.register_channel(HostChannel::new()) {
            Err(KitError::Config(ConfigError::DuplicateChannel(_))) | Ok(()) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// The kit's call that posts, as an error names it.
const POST: &str = "post";
