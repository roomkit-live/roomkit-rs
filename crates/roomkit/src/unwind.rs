//! A future run to its end whatever it does, a panic in it caught as a
//! task's join reports one: a write's task answers a store that panicked as
//! the run answers any call that did, and the kit's close goes on past a
//! channel whose close panicked (RFC §12.10.4).

use std::any::Any;
use std::future::{Future, poll_fn};
use std::panic::{self, AssertUnwindSafe};
use std::pin::pin;
use std::task::Poll;
use std::thread;

/// Runs `future` to its end, `Err` for a panic in it.
pub(crate) async fn unwinding<F: Future>(future: F) -> thread::Result<F::Output> {
    let mut future = pin!(future);
    poll_fn(
        |cx| match panic::catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(panic) => Poll::Ready(Err(panic)),
        },
    )
    .await
}

/// What a panic said, its message when it gave one as text.
pub(crate) fn message(panic: &(dyn Any + Send)) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic with no message".to_owned())
}
