//! A connection lent to one operation of the crate, and what its caller's cut does
//! with it: a statement the operation left running is cancelled on the server, as
//! asyncpg cancels the statement of a task cancelled (HITL 3ebe4596).

use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use deadpool_postgres::Object;
use tokio_postgres::{CancelToken, NoTls};

use crate::connect::Tls;

tokio::task_local! {
    /// Whether the operation running on this task is being polled: a connection
    /// dropped while it is not is one its caller cut.
    static POLLED: Arc<AtomicBool>;
}

/// `work`, an operation of the crate. Dropped unfinished (a task cancelled, a
/// timeout, a future its caller let go), each connection it holds has its
/// statement cancelled on the server and is closed, never handed back to its
/// pool: the pool's next user would wait behind that statement, or have its own
/// cut by a late cancel. The operation's future is boxed, so that this one adds
/// no copy of it to its caller's.
pub(crate) fn cancellable<F: Future>(work: F) -> impl Future<Output = F::Output> {
    let polled = Arc::new(AtomicBool::new(false));
    let mut work = Box::pin(POLLED.scope(Arc::clone(&polled), work));
    std::future::poll_fn(move |cx| {
        polled.store(true, Ordering::Relaxed);
        let outcome = work.as_mut().poll(cx);
        polled.store(false, Ordering::Relaxed);
        outcome
    })
}

/// A connection of a pool, lent to the operation that took it: handed back when
/// the operation lets it go, cancelled and closed when its caller cut the
/// operation, or when the operation panics with it.
pub(crate) struct Lent {
    /// The connection, until the drop.
    client: Option<Object>,
    /// The operation's mark, none outside [`cancellable`].
    polled: Option<Arc<AtomicBool>>,
    /// What a cancel request connects with, when the crate knows it.
    tls: Option<Tls>,
}

impl Lent {
    /// `client`, lent to the operation running on this task.
    pub(crate) fn new(client: Object, tls: Option<Tls>) -> Self {
        let polled = POLLED.try_with(Arc::clone).ok();
        debug_assert!(
            polled.is_some(),
            "a connection lent outside cancellable() would never be cancelled"
        );
        Self {
            client: Some(client),
            polled,
            tls,
        }
    }

    /// Whether the operation was cut: dropped while not polled, or panicking.
    fn cut(&self) -> bool {
        std::thread::panicking()
            || self
                .polled
                .as_ref()
                .is_some_and(|polled| !polled.load(Ordering::Relaxed))
    }
}

impl Deref for Lent {
    type Target = Object;

    fn deref(&self) -> &Object {
        self.client.as_ref().unwrap_or_else(|| unreachable!())
    }
}

impl DerefMut for Lent {
    fn deref_mut(&mut self) -> &mut Object {
        self.client.as_mut().unwrap_or_else(|| unreachable!())
    }
}

impl Drop for Lent {
    fn drop(&mut self) {
        let cut = self.cut();
        let Some(client) = self.client.take() else {
            return;
        };
        if cut {
            cancel_then_close(client, self.tls.take());
        }
    }
}

/// Cancels on the server what `client` runs, then closes it: a statement its
/// caller left goes no further, and the connection serves no one else. Its place
/// in the pool is kept until the cancel request is sent and the connection
/// closes; asyncpg keeps it until the statement answers the cancel, a moment
/// later. Without a runtime to send the cancel from, it only closes: the
/// statement runs on to its end.
pub(crate) fn cancel_then_close(client: Object, tls: Option<Tls>) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        drop(Object::take(client));
        return;
    };
    let token = client.cancel_token();
    let closing = Closing(Some(client));
    runtime.spawn(async move {
        cancel(&token, tls).await;
        drop(closing);
    });
}

/// A connection on its way out of its pool, closed when dropped: never handed
/// back, even when the task closing it is dropped with its runtime.
struct Closing(Option<Object>);

impl Drop for Closing {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            drop(Object::take(client));
        }
    }
}

/// Sends the cancel request of `token`'s connection; one that fails leaves the
/// connection closing, its statement running on to its end. `tls` is the pool's,
/// when the crate knows it; otherwise (a pool handed in, whose connector the
/// crate cannot read) the request asks for TLS that checks no certificate when
/// the connection's mode asks for TLS, then goes in clear if that fails (an
/// address with no host name, a host rustls cannot name), which a mode of
/// `require` refuses before the request is written. asyncpg's request checks as
/// its connection does; this one does not, and exposes only the session's
/// number and key, which cancel nothing but the statement cancelled here, on a
/// connection that runs nothing else and closes right after (HITL f281e902).
async fn cancel(token: &CancelToken, tls: Option<Tls>) {
    if let Some(tls) = tls {
        drop(token.cancel_query(tls).await);
        return;
    }
    if let Some(unchecked) = Tls::unchecked()
        && token.cancel_query(unchecked).await.is_ok()
    {
        return;
    }
    drop(token.cancel_query(NoTls).await);
}
