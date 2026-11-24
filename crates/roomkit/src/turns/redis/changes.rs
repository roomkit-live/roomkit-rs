//! Local changes are visible when serve or close returns. Other processes poll Redis.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use crate::turns::lock;
use crate::{ConversationStore, TurnExecutor};

use super::{Node, RedisTurnBackend};

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
