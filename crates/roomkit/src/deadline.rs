//! The pre-commit window starts before a call's preparatory reads.

use std::time::Duration;

use tokio::time::Instant;

/// A runtime deadline, carried unchanged through preparation and adoption.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CallDeadline(pub(crate) Option<Instant>);

impl CallDeadline {
    pub(crate) fn new(timeout: Option<Duration>) -> Self {
        Self(timeout.and_then(|timeout| Instant::now().checked_add(timeout)))
    }

    pub(crate) async fn until<T>(self, work: impl Future<Output = T>) -> Option<T> {
        match self.0 {
            Some(at) if at <= Instant::now() => None,
            Some(at) => tokio::time::timeout_at(at, work).await.ok(),
            None => Some(work.await),
        }
    }
}
