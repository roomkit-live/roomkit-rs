//! A retry held inside the backend until its future is dropped.

use roomkit::ChannelError;
use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    RealtimeFuture,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct HeldRetry {
    pub(super) attempts: AtomicUsize,
    pub(super) started: Notify,
    pub(super) cancelled: AtomicBool,
    inner: InMemoryRealtime,
}
struct Ended<'a>(&'a AtomicBool);
impl Drop for Ended<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
impl RealtimeBackend for HeldRetry {
    fn publish<'a>(
        &'a self,
        channel: &'a str,
        event: EphemeralEvent,
    ) -> RealtimeFuture<'a, Result<(), ChannelError>> {
        Box::pin(async move {
            if event.r#type == EphemeralEventType::PresenceOnline {
                if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return Err(std::io::Error::other("realtime down").into());
                }
                let _ended = Ended(&self.cancelled);
                self.started.notify_one();
                std::future::pending::<()>().await;
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
