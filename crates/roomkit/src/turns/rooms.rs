//! State a local executor vouches for, per channel and room (RFC §10.6.2).
//! Python's `ExecutorRooms`: restore before work, discard stale state, and
//! drop the least recently written room once its record falls out.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, Weak};

use roomkit_core::model::{ChannelId, Metadata, RoomId};
use tokio::sync::watch;

use super::lock;
use super::records::ROOMS_KEPT;
use super::wire::PreviousTurn;
use crate::channel::{ChannelError, DynChannel};

type Key = (ChannelId, RoomId);

struct Entry {
    id: String,
    written: u64,
    channel: Weak<dyn DynChannel>,
}

struct State {
    kept: usize,
    next: u64,
    left: HashMap<Key, Entry>,
    order: BTreeMap<u64, Key>,
    dropping: HashMap<Key, watch::Receiver<Option<bool>>>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            kept: ROOMS_KEPT,
            next: 0,
            left: HashMap::new(),
            order: BTreeMap::new(),
            dropping: HashMap::new(),
        }
    }
}

#[derive(Default)]
pub(super) struct ExecutorRooms {
    state: Mutex<State>,
}

impl std::fmt::Debug for ExecutorRooms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorRooms").finish_non_exhaustive()
    }
}

impl ExecutorRooms {
    /// Record the entry before its work can await or be cancelled.
    pub(super) fn enter(
        self: &Arc<Self>,
        channel: &Arc<dyn DynChannel>,
        key: Key,
        id: &str,
        previous: Option<&PreviousTurn>,
    ) -> bool {
        let mut state = lock(&self.state);
        let written = state.next;
        state.next = state.next.wrapping_add(1);
        let old = state.left.insert(
            key.clone(),
            Entry {
                id: id.to_owned(),
                written,
                channel: Arc::downgrade(channel),
            },
        );
        if let Some(old) = &old {
            state.order.remove(&old.written);
        }
        state.order.insert(written, key);
        while state.left.len() > state.kept {
            let Some((_, key)) = state.order.pop_first() else {
                break;
            };
            if let Some(entry) = state.left.remove(&key) {
                self.drop_later(&mut state, key, entry.channel);
            }
        }
        old.as_ref().map(|e| e.id.as_str()) != previous.map(|p| p.turn_id.as_str())
    }

    /// Await an eviction already dropping this room, discard stale state,
    /// then restore the owner's stored state.
    pub(super) async fn ready(
        &self,
        channel: &dyn DynChannel,
        key: &Key,
        stored: Metadata,
        mut stale: bool,
    ) -> Result<(), ChannelError> {
        let pending = lock(&self.state).dropping.get(key).cloned();
        if let Some(mut pending) = pending {
            let finished = pending.wait_for(Option::is_some).await;
            stale |= finished.map_or(true, |done| *done != Some(true));
        }
        if stale {
            let mut dropping = Unvouched {
                rooms: self,
                key,
                armed: true,
            };
            channel.on_room_detached(key.1.clone()).await?;
            dropping.armed = false;
        }
        channel.restore_room_state(&key.1, stored)
    }

    pub(super) fn forget(&self, channel_id: &ChannelId, room_id: &RoomId) {
        let mut state = lock(&self.state);
        if let Some(entry) = state.left.remove(&(channel_id.clone(), room_id.clone())) {
            state.order.remove(&entry.written);
        }
    }

    fn drop_later(self: &Arc<Self>, state: &mut State, key: Key, channel: Weak<dyn DynChannel>) {
        let (done, waiting) = watch::channel(None);
        state.dropping.insert(key.clone(), waiting.clone());
        let rooms = Arc::downgrade(self);
        tokio::spawn(async move {
            let success = match channel.upgrade() {
                Some(channel) => match channel.on_room_detached(key.1.clone()).await {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::error!(target: "roomkit::turns", channel_id = %key.0,
                            room_id = %key.1, %error, "Dropping an evicted room state failed");
                        false
                    }
                },
                None => true,
            };
            done.send_replace(Some(success));
            if let Some(rooms) = rooms.upgrade() {
                let mut state = lock(&rooms.state);
                if state
                    .dropping
                    .get(&key)
                    .is_some_and(|r| r.same_channel(&waiting))
                {
                    state.dropping.remove(&key);
                }
            }
        });
    }
}

/// Python's `BaseException` path also covers cancellation while dropping.
struct Unvouched<'a> {
    rooms: &'a ExecutorRooms,
    key: &'a Key,
    armed: bool,
}

impl Drop for Unvouched<'_> {
    fn drop(&mut self) {
        if self.armed
            && let Some(entry) = lock(&self.rooms.state).left.get_mut(self.key)
        {
            "\0unvouched".clone_into(&mut entry.id);
        }
    }
}

pub(crate) fn state_key(channel_id: &ChannelId) -> String {
    format!("roomkit:channel_state:{channel_id}")
}

pub(crate) fn stored_state(metadata: &Metadata, channel_id: &ChannelId) -> Metadata {
    metadata
        .get(&state_key(channel_id))
        .and_then(serde_json::Value::as_object)
        .map(|state| state.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests;
