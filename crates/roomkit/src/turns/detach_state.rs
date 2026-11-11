//! A detach's stored-state finalizer may outlive an abandoned Rust future.
//! A later attach or call waits for it before using the room again.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use roomkit_core::model::{ChannelId, RoomId};
use tokio::sync::watch;

use super::lock;

type Key = (ChannelId, RoomId);

#[derive(Debug, Default)]
pub(crate) struct PendingDetaches {
    pending: Mutex<HashMap<Key, watch::Receiver<bool>>>,
}

impl PendingDetaches {
    pub(crate) async fn wait(&self, channel: &ChannelId, room: &RoomId) -> bool {
        let pending = lock(&self.pending)
            .get(&(channel.clone(), room.clone()))
            .cloned();
        if let Some(mut pending) = pending {
            drop(pending.wait_for(|done| *done).await);
            true
        } else {
            false
        }
    }

    /// Link a finalizer after any older one still finishing for this binding.
    pub(crate) fn begin(self: &Arc<Self>, channel: ChannelId, room: RoomId) -> DetachTicket {
        let (finished, pending) = watch::channel(false);
        let key = (channel, room);
        let previous = lock(&self.pending).insert(key.clone(), pending);
        DetachTicket {
            held: Arc::clone(self),
            key,
            finished,
            previous,
        }
    }
}

pub(crate) struct DetachTicket {
    held: Arc<PendingDetaches>,
    key: Key,
    finished: watch::Sender<bool>,
    previous: Option<watch::Receiver<bool>>,
}

impl DetachTicket {
    pub(crate) async fn wait_previous(&mut self) {
        if let Some(pending) = &mut self.previous {
            drop(pending.wait_for(|done| *done).await);
        }
    }
}

impl Drop for DetachTicket {
    fn drop(&mut self) {
        let waiting = self.finished.subscribe();
        let mut pending = lock(&self.held.pending);
        self.finished.send_replace(true);
        if pending
            .get(&self.key)
            .is_some_and(|p| p.same_channel(&waiting))
        {
            pending.remove(&self.key);
        }
    }
}
