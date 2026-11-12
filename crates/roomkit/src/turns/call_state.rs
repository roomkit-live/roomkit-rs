//! State included in overlapping channel-call answers (RFC §10.6.3).
//! No lock is held while a verb runs; only its state comparisons are shared.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use roomkit_core::model::{Metadata, RoomId};

use crate::channel::{ChannelError, DynChannel};

/// Object identity keeps a replaced channel's in-flight calls apart too.
type Key = (usize, RoomId);
type Pending = HashMap<Key, HashMap<String, Metadata>>;

#[derive(Debug, Default)]
pub(super) struct CallStates {
    pending: Mutex<Pending>,
}

impl CallStates {
    pub(super) fn track<'a>(
        &'a self,
        channel: &dyn DynChannel,
        room: &RoomId,
        call_id: &str,
    ) -> Result<CallState<'a>, ChannelError> {
        let key = (std::ptr::from_ref(channel).addr(), room.clone());
        let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
        // Reading and registration are atomic with the other calls' reports.
        let before = channel.room_state(room)?;
        pending
            .entry(key.clone())
            .or_default()
            .insert(call_id.to_owned(), before);
        Ok(CallState {
            owner: self,
            key,
            call_id: call_id.to_owned(),
        })
    }
}

/// Retires the comparison on return, failure or cancellation.
pub(super) struct CallState<'a> {
    owner: &'a CallStates,
    key: Key,
    call_id: String,
}

impl CallState<'_> {
    pub(super) fn changed(
        &self,
        channel: &dyn DynChannel,
    ) -> Result<Option<Metadata>, ChannelError> {
        let mut pending = self
            .owner
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        // Read under the same lock as peer reports: a stale read must not
        // compare against a newer snapshot and appear to revert a peer's write.
        let after = channel.room_state(&self.key.1)?;
        let Some(peers) = pending.get_mut(&self.key) else {
            return Err("channel-call state comparison was retired".into());
        };
        if peers.get(&self.call_id) == Some(&after) {
            return Ok(None);
        }
        // A peer that restores its initial state must undo this answer,
        // even though its original before and final after are equal.
        for before in peers.values_mut() {
            before.clone_from(&after);
        }
        Ok(Some(after))
    }
}

impl Drop for CallState<'_> {
    fn drop(&mut self) {
        let mut pending = self
            .owner
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(peers) = pending.get_mut(&self.key) {
            peers.remove(&self.call_id);
            if peers.is_empty() {
                pending.remove(&self.key);
            }
        }
    }
}
