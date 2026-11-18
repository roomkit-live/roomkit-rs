//! Local changes are visible when serve or close returns. Other processes poll Redis.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use crate::channel::BoxFuture;
use crate::turns::lock;
use crate::{ConversationStore, TurnExecutor};

use super::{Node, RedisTurnBackend};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CoverageNotice {
    OwnerAttended,
    ExecutorsChanged,
}

pub(super) type Watcher =
    Arc<dyn Fn(CoverageNotice) -> Option<BoxFuture<'static, ()>> + Send + Sync>;

#[derive(Debug, Default)]
pub(crate) struct ServingBackends(Mutex<BTreeMap<String, Weak<Node>>>);

impl<S: ConversationStore> TurnExecutor<S> {
    pub(crate) fn served_by(&self, backend: &RedisTurnBackend) {
        lock(&self.serving_backends().0)
            .insert(backend.node_id().to_owned(), Arc::downgrade(&backend.0));
    }

    pub(crate) async fn tell_redis_closed(&self) {
        let nodes = std::mem::take(&mut *lock(&self.serving_backends().0));
        for node in nodes.into_values().filter_map(|node| node.upgrade()) {
            let backend = RedisTurnBackend(node);
            if let Err(error) = backend.worker_closed().await {
                tracing::warn!(%error, "reporting a closed executor to Redis failed");
            }
        }
    }
}

impl RedisTurnBackend {
    pub(crate) fn watch_coverage(&self, watcher: Watcher) {
        lock(&self.0.watchers).push(watcher);
    }

    pub(super) async fn notify_coverage(&self, notice: CoverageNotice) {
        let mut pending = Vec::new();
        lock(&self.0.watchers).retain(|watcher| {
            if let Some(check) = watcher(notice) {
                pending.push(check);
                true
            } else {
                false
            }
        });
        for check in pending {
            check.await;
        }
    }
}
