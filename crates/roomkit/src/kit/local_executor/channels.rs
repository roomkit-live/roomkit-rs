//! A release lets the next request build anew. The old instance is closed
//! after the turns and controls that obtained that instance have finished.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use roomkit_core::model::ChannelId;
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tokio::task::JoinHandle;

use crate::channel::DynChannel;
use crate::{ConversationStore, RoomKitError, TurnExecutor};

#[derive(Default)]
pub(super) struct Generations {
    ids: Mutex<HashMap<ChannelId, Weak<Generation>>>,
    retiring: Mutex<Vec<JoinHandle<()>>>,
}

#[derive(Default)]
pub(super) struct Generation {
    // A release waits for the current build to finish before removing it.
    pub(super) building: RwLock<()>,
    users: Mutex<Arc<RwLock<()>>>,
}

pub(crate) struct WorkerChannel {
    pub(crate) channel: Arc<dyn DynChannel>,
    _lease: Option<(Arc<Generation>, OwnedRwLockReadGuard<()>)>,
}

impl WorkerChannel {
    pub(crate) fn registered(channel: Arc<dyn DynChannel>) -> Self {
        Self {
            channel,
            _lease: None,
        }
    }
}

impl Generation {
    pub(super) fn using(self: &Arc<Self>, channel: Arc<dyn DynChannel>) -> WorkerChannel {
        // Called under the build lock. A release replaces the lock before
        // taking it for writing, so the current generation is always readable.
        let readers = self
            .users
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let guard = readers
            .try_read_owned()
            .expect("current channel generation");
        WorkerChannel {
            channel,
            _lease: Some((Arc::clone(self), guard)),
        }
    }

    fn released(&self) -> Arc<RwLock<()>> {
        std::mem::take(&mut *self.users.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl Generations {
    pub(super) fn of(&self, id: &ChannelId) -> Arc<Generation> {
        let mut generations = self.ids.lock().unwrap_or_else(PoisonError::into_inner);
        generations.retain(|_, generation| generation.strong_count() > 0);
        let generation = generations.entry(id.clone()).or_default();
        if let Some(current) = generation.upgrade() {
            return current;
        }
        let current = Arc::default();
        *generation = Arc::downgrade(&current);
        current
    }

    fn remember(&self, task: JoinHandle<()>) {
        let mut retiring = self.retiring.lock().unwrap_or_else(PoisonError::into_inner);
        retiring.retain(|task| !task.is_finished());
        retiring.push(task);
    }

    pub(super) async fn settled(&self) {
        let retiring =
            std::mem::take(&mut *self.retiring.lock().unwrap_or_else(PoisonError::into_inner));
        for task in retiring {
            drop(task.await);
        }
    }
}

impl<S: ConversationStore> TurnExecutor<S> {
    pub(super) async fn built_channel(&self, id: &ChannelId) -> Option<WorkerChannel> {
        let Some(worker) = &self.standalone else {
            return self.kit.inner.channel(id).map(WorkerChannel::registered);
        };
        let generation = worker.generations.of(id);
        let _pending = generation.building.write().await;
        worker.wait_for_build(id).await;
        self.kit
            .inner
            .channel(id)
            .map(|channel| generation.using(channel))
    }

    pub(super) async fn release_channel(&self, id: &ChannelId) -> Result<(), RoomKitError> {
        let Some(worker) = &self.standalone else {
            return Ok(());
        };
        let generation = worker.generations.of(id);
        let (retired, readers) = {
            let _pending = generation.building.write().await;
            worker.wait_for_build(id).await;
            let Some(channel) = self.kit.unregister_channel(id)? else {
                return Ok(());
            };
            (channel, generation.released())
        };
        if let Ok(_unused) = Arc::clone(&readers).try_write_owned() {
            retired.close().await;
        } else {
            worker.generations.remember(tokio::spawn(async move {
                let _unused = readers.write_owned().await;
                retired.close().await;
            }));
        }
        Ok(())
    }
}
