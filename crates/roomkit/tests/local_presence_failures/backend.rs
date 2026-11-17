//! Recoverable failures supplied through the public realtime trait.

use std::error::Error;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use roomkit::realtime::{
    EphemeralCallback, EphemeralEvent, EphemeralEventType, InMemoryRealtime, RealtimeBackend,
    RealtimeFuture,
};
use roomkit::{ChannelError, ClassedError};
use tokio::sync::Notify;

#[derive(Debug)]
pub(super) struct FailedPublication {
    pub(super) identity: Arc<()>,
    cause: std::io::Error,
}
impl fmt::Display for FailedPublication {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("realtime down")
    }
}
impl Error for FailedPublication {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.cause)
    }
}

pub(super) struct Backend {
    pub(super) failures: Mutex<usize>,
    pub(super) attempts: AtomicUsize,
    pub(super) identity: Arc<()>,
    pub(super) delivered: Notify,
    pub(super) published: Mutex<Vec<EphemeralEvent>>,
    pub(super) failing: EphemeralEventType,
    inner: InMemoryRealtime,
}
impl Backend {
    pub(super) fn new(failures: usize, failing: EphemeralEventType) -> Self {
        Self {
            failures: Mutex::new(failures),
            attempts: AtomicUsize::new(0),
            identity: Arc::new(()),
            delivered: Notify::new(),
            published: Mutex::new(Vec::new()),
            failing,
            inner: InMemoryRealtime::default(),
        }
    }
}
impl RealtimeBackend for Backend {
    fn publish<'a>(
        &'a self,
        channel: &'a str,
        event: EphemeralEvent,
    ) -> RealtimeFuture<'a, Result<(), ChannelError>> {
        Box::pin(async move {
            if event.r#type == self.failing {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                let fail = {
                    let mut left = self.failures.lock().unwrap();
                    let fail = *left > 0;
                    *left = left.saturating_sub(1);
                    fail
                };
                if fail {
                    return Err(ClassedError::new(
                        "ConnectionError",
                        FailedPublication {
                            identity: Arc::clone(&self.identity),
                            cause: std::io::Error::other("transport unavailable"),
                        },
                    )
                    .into());
                }
            }
            self.published.lock().unwrap().push(event.clone());
            self.inner.publish(channel, event).await?;
            self.delivered.notify_one();
            Ok(())
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
