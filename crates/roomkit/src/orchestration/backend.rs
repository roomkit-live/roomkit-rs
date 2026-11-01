//! What keeps and carries a status bus's entries (RFC §19.8): Python's
//! `StatusBackend`, the in-memory one by default, a Redis or NATS one for
//! several processes.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::status::{StatusEntry, StatusLevel};

/// What a status backend's methods return: a boxed future, since a bus
/// holds its backend as an object.
pub(crate) type StatusFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What a subscriber runs for each entry posted: Python's `StatusCallback`,
/// `(entry) -> Awaitable[None] | None`. A callback that panics is logged,
/// and the others run all the same. A subscriber is told apart by its
/// `Arc`: [`unsubscribe`](StatusBackend::unsubscribe) takes the one
/// [`subscribe`](StatusBackend::subscribe) was given, or a clone of it.
pub(crate) type StatusCallback =
    Arc<dyn Fn(StatusEntry) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// What [`StatusBackend::recent`] takes beside how many entries: Python's
/// `recent(n, *, agent_id=None, status=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RecentOptions {
    /// Python `agent_id=None`: only this agent's entries.
    pub agent_id: Option<String>,
    /// Python `status=None`: only the entries of this level.
    pub status: Option<StatusLevel>,
}

/// A status bus's backend: Python's `StatusBackend`. It keeps the entries
/// posted and hands each to its subscribers.
pub trait StatusBackend: Send + Sync + 'static {
    /// Keeps `entry` and hands it to every subscriber, waiting for them.
    fn publish(&self, entry: StatusEntry) -> StatusFuture<'_, ()>;

    /// The newest `n` entries, oldest first, kept to an agent and a level
    /// when `options` names them.
    fn recent(&self, n: usize, options: RecentOptions) -> StatusFuture<'_, Vec<StatusEntry>>;

    /// Hands every entry published from now on to `callback`.
    fn subscribe(&self, callback: StatusCallback) -> StatusFuture<'_, ()>;

    /// Hands no more entry to `callback`, the `Arc` that subscribed or a
    /// clone of it; one that never subscribed is ignored.
    fn unsubscribe(&self, callback: StatusCallback) -> StatusFuture<'_, ()>;

    /// Releases what the backend holds; nothing by default.
    fn close(&self) -> StatusFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// Whether `left` and `right` are one subscriber: the same `Arc`.
pub(crate) fn same_callback(left: &StatusCallback, right: &StatusCallback) -> bool {
    std::ptr::addr_eq(Arc::as_ptr(left), Arc::as_ptr(right))
}
