//! An intelligence channel whose detach callback may refuse.

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent, RoomId,
};
use roomkit::{Channel, ChannelError};
use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) struct Agent {
    pub(super) refuses: bool,
    pub(super) called: Arc<AtomicUsize>,
}

impl Channel for Agent {
    fn channel_id(&self) -> ChannelId {
        "helper".parse().unwrap()
    }
    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }
    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
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
    fn on_room_detached(
        &self,
        _room: RoomId,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send {
        self.called.fetch_add(1, Ordering::SeqCst);
        if self.refuses {
            return ready(Err(std::io::Error::other("detach callback failed").into()));
        }
        ready(Ok(()))
    }
}
