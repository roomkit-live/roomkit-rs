//! Process-local notifications awaited by serve and worker close (§10.6.7).
use std::sync::{Arc, Mutex, Weak};

use crate::channel::BoxFuture;
use crate::turns::lock;

type Watcher = Arc<dyn Fn() -> Option<BoxFuture<'static, ()>> + Send + Sync>;

#[derive(Default)]
pub(crate) struct Coverage(Mutex<Vec<Watcher>>);

impl Coverage {
    pub(crate) fn subscribe(&self, watcher: Watcher) {
        lock(&self.0).push(watcher);
    }

    pub(super) async fn changed(&self) {
        let mut pending = Vec::new();
        lock(&self.0).retain(|watcher| {
            if let Some(work) = watcher() {
                pending.push(work);
                return true;
            }
            false
        });
        for work in pending {
            work.await;
        }
    }
}

/// Closing an executor notifies each backend that serves it once. Neither
/// direction keeps an owner or a backend alive.
#[derive(Default, Debug)]
pub(crate) struct Serving(Mutex<Vec<Weak<Coverage>>>);

impl Serving {
    pub(crate) fn listen(&self, backend: &Arc<Coverage>) {
        let mut listeners = lock(&self.0);
        let key = Arc::downgrade(backend);
        listeners.retain(|entry| entry.strong_count() != 0);
        if !listeners.iter().any(|entry| entry.ptr_eq(&key)) {
            listeners.push(key);
        }
    }

    pub(crate) async fn closed(&self) {
        let backends = std::mem::take(&mut *lock(&self.0));
        for backend in backends.into_iter().filter_map(|entry| entry.upgrade()) {
            backend.changed().await;
        }
    }
}
