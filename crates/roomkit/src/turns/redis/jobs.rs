//! Incoming calls and frame bridges live only for this backend's lifetime.
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::task::{AbortHandle, Id};

use super::{Node, RedisTurnBackend};
use crate::turns::lock;

#[derive(Default)]
pub(super) struct Jobs {
    next: u64,
    active: HashMap<u64, AbortHandle>,
}

struct Registration {
    node: Weak<Node>,
    token: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        if let Some(node) = self.node.upgrade() {
            lock(&node.routes).jobs.active.remove(&self.token);
            node.jobs_changed.notify_waiters();
        }
    }
}

impl RedisTurnBackend {
    /// A close either owns this task through its final write or refuses it before it runs.
    pub(super) fn dispatch(&self, work: impl Future<Output = ()> + Send + 'static) -> bool {
        let mut routes = lock(&self.0.routes);
        if routes.closing {
            return false;
        }
        let jobs = &mut routes.jobs;
        let token = jobs.next;
        jobs.next += 1;
        let guard = Registration {
            node: Arc::downgrade(&self.0),
            token,
        };
        let task = tokio::spawn(async move {
            let _registration = guard;
            work.await;
        });
        jobs.active.insert(token, task.abort_handle());
        true
    }

    async fn idle(&self, caller: Option<Id>) {
        loop {
            let changed = self.0.jobs_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if lock(&self.0.routes)
                .jobs
                .active
                .values()
                .all(|job| Some(job.id()) == caller)
            {
                return;
            }
            changed.await;
        }
    }

    pub(super) async fn drain_jobs(&self) {
        let bound = self.timeouts().bounded().cancel;
        let caller = tokio::task::try_id();
        if tokio::time::timeout(bound, self.idle(caller)).await.is_ok() {
            return;
        }
        for job in lock(&self.0.routes).jobs.active.values() {
            if Some(job.id()) != caller {
                job.abort();
            }
        }
        // Abortion takes effect on the next poll. A synchronous application callback
        // can occupy a runtime thread beyond this deadline.
        if tokio::time::timeout(bound, self.idle(caller))
            .await
            .is_err()
        {
            tracing::warn!("A Redis turn handler outlived the backend's cancellation bound");
        }
    }
}
