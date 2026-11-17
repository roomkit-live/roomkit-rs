//! The pool's release budget starts when the owning scope exits or is dropped.

use roomkit::StoreError;
use tokio::sync::oneshot;
use tokio::time::{Instant, timeout_at};

use crate::store::ACQUIRE_TIMEOUT;

/// Only the owner of the checkout signals its release. An inner operation or
/// a nested scope can finish while that owner still intends to use the client.
pub(super) struct Owner(Option<oneshot::Sender<Instant>>);

impl Owner {
    pub(super) fn for_checkout() -> (Self, oneshot::Receiver<Instant>) {
        let (sender, receiver) = oneshot::channel();
        (Self(Some(sender)), receiver)
    }
}

impl Drop for Owner {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            // asyncpg's pool uses a fresh acquisition-sized budget on release.
            let _ = sender.send(Instant::now() + ACQUIRE_TIMEOUT);
        }
    }
}

/// Continue an in-flight cleanup under the deadline that the owner sets on
/// exit. The same deadline covers both cancellation and the protocol drain.
pub(super) async fn drain(
    cleanup: impl Future<Output = Result<(), StoreError>>,
    releasing: &mut oneshot::Receiver<Instant>,
) -> Result<(), StoreError> {
    tokio::pin!(cleanup);
    let deadline = tokio::select! {
        result = &mut cleanup => return result,
        deadline = releasing => deadline.map_err(StoreError::unavailable)?,
    };
    timeout_at(deadline, cleanup)
        .await
        .map_err(StoreError::unavailable)?
}
