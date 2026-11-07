//! A connection lent to one operation of the crate, and what its caller's cut does
//! with it: a statement the operation left running is cancelled on the server, as
//! asyncpg cancels the statement of a task cancelled (HITL 3ebe4596).

use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use deadpool_postgres::Object;
use tokio_postgres::NoTls;

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
/// cut by a late cancel. Boxed, so that an operation's future is held once.
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
    /// What a cancel request connects with, when the pool's owner knows it.
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
/// caller left goes no further, and the connection serves no one else. It leaves
/// its pool at once, which opens another for its next user. Without a runtime to
/// send the cancel from, it only closes: the statement runs on to its end. `tls`,
/// when the pool's owner does not know it (a pool handed in), is TLS that checks
/// no certificate, as the request carries the connection's key alone, and the
/// connection closes after it.
pub(crate) fn cancel_then_close(client: Object, tls: Option<Tls>) {
    let client = Object::take(client);
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        drop(client);
        return;
    };
    let cancel = client.cancel_token();
    runtime.spawn(async move {
        // A cancel that fails leaves the connection closing below.
        let cancelled = match tls.or_else(Tls::unchecked) {
            Some(tls) => cancel.cancel_query(tls).await,
            None => cancel.cancel_query(NoTls).await,
        };
        drop(cancelled);
        drop(client);
    });
}
