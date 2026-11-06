//! A client's callback that panics, taken as one that raised: the Python
//! SDK logs a handler's error and goes on.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A boxed future, as an `AcpClient` returns it.
type Boxed<'call, T> = Pin<Box<dyn Future<Output = T> + Send + 'call>>;

/// `future`'s output, or none if it panicked.
pub(super) fn caught<T>(future: Boxed<'_, T>) -> Caught<'_, T> {
    Caught { future }
}

/// The future [`caught`] returns.
pub(super) struct Caught<'call, T> {
    future: Boxed<'call, T>,
}

impl<T> Future for Caught<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let future = &mut self.get_mut().future;
        match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Some(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_panicked) => Poll::Ready(None),
        }
    }
}
