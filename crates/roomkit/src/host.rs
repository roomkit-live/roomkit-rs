//! The host's own voice (RFC §10.5, Appendix A.14.1): the channel the
//! application that embeds the kit writes to a room's people from.

use std::future::Future;

use roomkit_core::model::{ChannelId, InboundMessage, RoomContext, RoomEvent};
use roomkit_core::ops::ChannelDeclaration;

use crate::channel::{Channel, ChannelError, EventDraft};

/// The id of the kit's own host channel, which a post that names no channel
/// speaks from, registered on first use (RFC §10.5).
pub const HOST_CHANNEL_ID: &str = "host";

/// A host channel (RFC §10.5, Appendix A.14.1): a transport of type `HOST`
/// whose direction is `INBOUND`. It brings the host's words into rooms
/// through [`RoomKit::post`](crate::RoomKit::post) and
/// [`send_event`](crate::RoomKit::send_event), receives no external
/// payload, and is never delivered to. It declares every media type, since
/// a post may carry any content; each transport the post reaches gets it
/// transcoded for what it carries (RFC §6.6).
#[derive(Clone, Debug)]
pub struct HostChannel {
    declaration: ChannelDeclaration,
}

impl HostChannel {
    /// The kit's own host channel, `host`.
    pub fn new() -> Self {
        Self::with_id(ChannelId::host())
    }

    /// A host channel named `channel_id`, which a post names to speak from
    /// it.
    pub fn with_id(channel_id: ChannelId) -> Self {
        Self {
            declaration: ChannelDeclaration::host(channel_id),
        }
    }
}

impl Default for HostChannel {
    fn default() -> Self {
        Self::new()
    }
}

impl Channel for HostChannel {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    /// A host channel receives no external payload: the pipeline refuses a
    /// message on one before anything is read (RFC §10.1 step 1), so this
    /// is never called.
    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
        _draft: EventDraft,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        let channel_id = self.declaration.channel_id.clone();
        async move {
            Err(format!(
                "channel {channel_id} is a host channel, which receives no external payload"
            )
            .into())
        }
    }
}
