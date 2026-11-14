//! The identity and lifetime of a live channel across public releases.

use std::future::{Future, ready};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelId, ChannelOutput, ChannelType, InboundMessage,
    RoomContext, RoomEvent,
};

use crate::{Channel, ChannelError, RoomKit, RoomKitError, RoomKitOptions};

struct LiveChannel(Arc<AtomicUsize>);

impl Channel for LiveChannel {
    fn channel_id(&self) -> ChannelId {
        ChannelId::new("agent").unwrap()
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
        ready(Ok(crate::default_inbound_event(self, &message, &context)))
    }
    fn deliver(
        &self,
        _: RoomEvent,
        _: ChannelBinding,
        _: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        ready(Ok(ChannelOutput::empty()))
    }
    async fn close(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

// Python: tests/test_turns_channel_release.py::test_a_channel_the_kit_holds_live_is_its_registrys
#[tokio::test]
async fn test_a_channel_the_kit_holds_live_is_its_registrys() {
    for close_executor in [false, true] {
        let kit = RoomKit::new(RoomKitOptions::default());
        let closes = Arc::new(AtomicUsize::new(0));
        let channel = LiveChannel(Arc::clone(&closes));
        let id = channel.channel_id();
        kit.register_channel(channel).unwrap();
        let original = kit.inner.channel(&id).unwrap();
        if close_executor {
            kit.turn_executor().close().await;
        }
        kit.release_channel(&id).await.unwrap();
        kit.release_channel(&id).await.unwrap();
        assert!(kit.get_channel(&id).is_some());
        // get_channel returns a fresh erased view; compare the actual held channel.
        assert!(Arc::ptr_eq(&original, &kit.inner.channel(&id).unwrap()));
        assert_eq!(closes.load(Ordering::SeqCst), 0);
        kit.close().await.unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }
}

// Python: tests/test_turns_channel_release.py::test_releasing_an_unknown_channel_is_refused
#[tokio::test]
async fn test_releasing_an_unknown_channel_is_refused() {
    let kit = RoomKit::new(RoomKitOptions::default());
    let id = ChannelId::new("nobody").unwrap();
    assert!(matches!(
        kit.release_channel(&id).await,
        Err(RoomKitError::ChannelNotRegistered { channel_id, .. }) if channel_id == id
    ));
    kit.close().await.unwrap();
}
