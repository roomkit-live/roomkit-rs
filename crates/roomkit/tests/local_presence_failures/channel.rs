//! A registered channel's public notification bridge outside a turn.

use roomkit::channel_publication::ChannelPublication;
use roomkit::model::{
    ChannelBinding, ChannelId, ChannelOutput, ChannelType, InboundMessage, RoomContext, RoomEvent,
};
use roomkit::{Channel, ChannelError};
use std::future::{Future, ready};

pub(super) struct Publishing(pub(super) ChannelPublication);
impl Channel for Publishing {
    fn channel_id(&self) -> ChannelId {
        "publisher".parse().unwrap()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::WEBHOOK
    }
    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        ready(Ok(roomkit::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
    fn channel_publication(&self) -> Option<ChannelPublication> {
        Some(self.0.clone())
    }
}
