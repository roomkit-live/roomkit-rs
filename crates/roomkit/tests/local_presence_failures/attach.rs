//! Initial publication barriers, through the public realtime backend.

use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    RealtimeFuture,
};
use roomkit::{ChannelError, WeakRoomKit};
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct HeldAttach {
    pub(super) attempts: AtomicUsize,
    pub(super) kind: Mutex<Option<EphemeralEventType>>,
    pub(super) started: Notify,
    pub(super) release: Notify,
    pub(super) cancelled: AtomicBool,
    pub(super) completed: AtomicBool,
    pub(super) closing: Mutex<Option<WeakRoomKit>>,
    inner: InMemoryRealtime,
}
struct Ended<'a>(&'a AtomicBool);
impl Drop for Ended<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
impl RealtimeBackend for HeldAttach {
    fn publish<'a>(
        &'a self,
        channel: &'a str,
        event: EphemeralEvent,
    ) -> RealtimeFuture<'a, Result<(), ChannelError>> {
        Box::pin(async move {
            let kind = self
                .kind
                .lock()
                .unwrap()
                .unwrap_or(EphemeralEventType::PresenceOnline);
            if event.r#type == kind && self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                let _ended = Ended(&self.cancelled);
                self.started.notify_one();
                let closing = self.closing.lock().unwrap().take();
                if let Some(kit) = closing.and_then(|weak| weak.upgrade()) {
                    kit.close().await?;
                } else {
                    self.release.notified().await;
                }
                self.completed.store(true, Ordering::SeqCst);
            }
            self.inner.publish(channel, event).await
        })
    }
    fn subscribe<'a>(
        &'a self,
        channel: &'a str,
        callback: EphemeralCallback,
    ) -> RealtimeFuture<'a, String> {
        self.inner.subscribe(channel, callback)
    }
    fn unsubscribe<'a>(&'a self, subscription_id: &'a str) -> RealtimeFuture<'a, bool> {
        self.inner.unsubscribe(subscription_id)
    }
    fn close(&self) -> RealtimeFuture<'_, ()> {
        self.inner.close()
    }
}
