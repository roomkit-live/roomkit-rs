//! A subscription to a room's ephemerals ends with the room (RFC §8.4,
//! §17.2).
//!
//! The kit's subscriptions are kept by room id on the realtime backend,
//! which knows nothing of rooms. A room the store deletes leaves them
//! behind, and a room created again under its id, by any organization,
//! would publish to them. The kit ends them when it creates a room under
//! the id, and keeps those made while no room held the id: they wait for
//! that room and become its own, as a room's hooks do. Python's
//! `roomkit.core._room_subscriptions`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit_core::model::RoomId;

/// The kit's subscriptions to rooms' ephemerals, by room id: Python's
/// `RoomSubscriptions`.
#[derive(Debug, Default)]
pub(crate) struct RoomSubscriptions {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    /// Room id → the subscriptions made for the room that holds the id.
    held: BTreeMap<RoomId, BTreeSet<String>>,
    /// Room id → the subscriptions made while no room held it.
    waiting: BTreeMap<RoomId, BTreeSet<String>>,
    /// Subscription id → its room id.
    room_of: BTreeMap<String, RoomId>,
}

impl RoomSubscriptions {
    /// Notes the subscription `subscription_id` to `room_id`'s ephemerals:
    /// the room's when a room held the id (`held`), waiting for the room
    /// created under it otherwise.
    pub(crate) fn made(&self, room_id: &RoomId, subscription_id: &str, held: bool) {
        let mut state = lock(&self.state);
        let rooms = if held {
            &mut state.held
        } else {
            &mut state.waiting
        };
        rooms
            .entry(room_id.clone())
            .or_default()
            .insert(subscription_id.to_owned());
        state
            .room_of
            .insert(subscription_id.to_owned(), room_id.clone());
    }

    /// `subscription_id` was unsubscribed.
    pub(crate) fn ended(&self, subscription_id: &str) {
        let mut guard = lock(&self.state);
        let state = &mut *guard;
        let Some(room_id) = state.room_of.remove(subscription_id) else {
            return;
        };
        for rooms in [&mut state.held, &mut state.waiting] {
            if let Some(subscriptions) = rooms.get_mut(&room_id) {
                subscriptions.remove(subscription_id);
                if subscriptions.is_empty() {
                    rooms.remove(&room_id);
                }
            }
        }
    }

    /// Whether the id holds a subscription of the kit, held or waiting.
    pub(crate) fn holds(&self, room_id: &RoomId) -> bool {
        let state = lock(&self.state);
        state.held.contains_key(room_id) || state.waiting.contains_key(room_id)
    }

    /// The store has just created a room under `room_id`: the subscriptions
    /// an earlier room of the id left, to end, forgotten here; those
    /// waiting for this room are its own from now on.
    pub(crate) fn earlier_room(&self, room_id: &RoomId) -> Vec<String> {
        let mut state = lock(&self.state);
        let ending = state.held.remove(room_id).unwrap_or_default();
        for subscription_id in &ending {
            state.room_of.remove(subscription_id);
        }
        if let Some(waiting) = state.waiting.remove(room_id) {
            state.held.insert(room_id.clone(), waiting);
        }
        ending.into_iter().collect()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
