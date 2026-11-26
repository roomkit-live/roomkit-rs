//! Local changes are visible when serve or close returns. Other processes poll the registry.
use std::sync::Arc;

use super::DistributedTurnBackend;
use crate::turns::{ExecutorsListener, TurnFuture};

impl DistributedTurnBackend {
    /// The one listener this backend gives the executors it serves: an
    /// executor that closed is reported to the registry, and the owner looks at
    /// the executors again. It keeps the backend's node weakly.
    pub(super) fn closed_listener(&self) -> ExecutorsListener {
        Arc::clone(self.0.closed_listener.get_or_init(|| {
            let node = Arc::downgrade(&self.0);
            Arc::new(move || {
                let backend = DistributedTurnBackend(node.upgrade()?);
                let told: TurnFuture<'static, ()> = Box::pin(async move {
                    if let Err(error) = backend.worker_closed().await {
                        tracing::warn!(%error, "reporting a closed executor to the registry failed");
                    }
                });
                Some(told)
            })
        }))
    }
}
