//! Completion of a human-input notification, including cancellation.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::sync::watch;
use tokio::task::AbortHandle;

use crate::channel::BoxFuture;

#[derive(Clone)]
pub(super) struct Notification {
    pub(super) channel: String,
    pub(super) abort: AbortHandle,
    pub(super) finished: watch::Receiver<()>,
}

/// The receiver closes only after the work and its cancellation guards
/// are dropped and the handler no longer tracks it. Every concurrent close
/// gets a receiver; cancelling one close does not lose the running task.
struct Tracked {
    work: Option<BoxFuture<'static, ()>>,
    cleanup: Option<Box<dyn FnOnce() + Send>>,
    _finished: watch::Sender<()>,
}

impl Future for Tracked {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        match self.work.as_mut() {
            Some(work) => work.as_mut().poll(context),
            None => Poll::Ready(()),
        }
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        drop(self.work.take());
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

pub(super) fn tracked(
    work: BoxFuture<'static, ()>,
    cleanup: impl FnOnce() + Send + 'static,
) -> (BoxFuture<'static, ()>, watch::Receiver<()>) {
    let (finished, receiver) = watch::channel(());
    (
        Box::pin(Tracked {
            work: Some(work),
            cleanup: Some(Box::new(cleanup)),
            _finished: finished,
        }),
        receiver,
    )
}
