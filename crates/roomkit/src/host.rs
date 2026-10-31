//! The host's own voice (RFC §10.5, Appendix A.14.1): the channel the
//! application that embeds the kit writes to a room's people from.

use roomkit_core::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelMediaType, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
};

use crate::channel::{Channel, ChannelError};

/// A host channel (RFC §10.5, Appendix A.14.1): Python's
/// `HostChannel(channel_id='host')`, a transport of type `HOST` whose
/// direction is `INBOUND`. It brings the host's words into rooms through
/// [`RoomKit::post`](crate::RoomKit::post) and
/// [`send_event`](crate::RoomKit::send_event), receives no external
/// payload, and is never delivered to. It declares every media type, since
/// a post may carry any content; each transport the post reaches gets it
/// transcoded for what it carries (RFC §6.6).
#[derive(Clone, Debug)]
pub struct HostChannel {
    channel_id: ChannelId,
}

/// What [`HostChannel::new`] takes: Python's keywords.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostChannelOptions {
    /// The channel's id, which a post names to speak from it: `host` by
    /// default.
    pub channel_id: ChannelId,
}

impl Default for HostChannelOptions {
    fn default() -> Self {
        Self {
            channel_id: ChannelId::host(),
        }
    }
}

impl HostChannel {
    /// A host channel named as `options` say.
    pub fn new(options: HostChannelOptions) -> Self {
        Self {
            channel_id: options.channel_id,
        }
    }
}

impl Default for HostChannel {
    fn default() -> Self {
        Self::new(HostChannelOptions::default())
    }
}

impl Channel for HostChannel {
    fn channel_id(&self) -> ChannelId {
        self.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::HOST
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Transport
    }

    fn direction(&self) -> ChannelDirection {
        ChannelDirection::Inbound
    }

    fn capabilities(&self) -> ChannelCapabilities {
        ChannelCapabilities {
            media_types: ChannelMediaType::all(),
            ..ChannelCapabilities::default()
        }
    }

    /// A host channel receives no external payload: the pipeline refuses a
    /// message on one before anything is read (RFC §10.1 step 1), so this
    /// is never called.
    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        std::future::ready({
            Err(format!(
                "Channel {} is a host channel and receives no external payload: the host speaks \
             through RoomKit::post() or send_event()",
                self.channel_id
            )
            .into())
        })
    }

    /// Never called: an `INBOUND` transport is not delivered to (RFC §4.2).
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        std::future::ready(Ok(ChannelOutput::empty()))
    }
}
