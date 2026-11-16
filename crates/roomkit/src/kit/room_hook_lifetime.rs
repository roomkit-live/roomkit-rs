//! A room's hooks end with the room (RFC §9.1, §17.2).
//!
//! Room hooks, and the names a room enables on the turn executors, are kept
//! in this process by room id. A room the store deletes leaves them behind,
//! and a room created again under its id, by any organization, would run
//! them. The kit forgets them when it creates a room under the id, as it
//! forgets the id's strategy (RFC §19.7), and keeps those registered while
//! no room held the id: they wait for that room and become its own. Python's
//! `roomkit.core._room_hook_lifetime`.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit_core::model::RoomId;

use super::RoomKit;
use crate::error::RoomKitError;
use crate::store::ConversationStore;

/// Which of a room id's hooks wait for the room created under the id:
/// Python's `RoomHookLifetime`.
#[derive(Debug, Default)]
pub(crate) struct RoomHookLifetime {
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    /// Room id → the names of its hooks registered while no room held it.
    waiting: BTreeMap<RoomId, BTreeSet<String>>,
    /// Room id → the registrations reading whether a room holds it, by
    /// number: whether the kit created a room under the id since.
    registering: BTreeMap<RoomId, BTreeMap<u64, bool>>,
    /// The number the next registration takes.
    next: u64,
}

/// A kit's registration of a room hook or an enabled name, under way from
/// before its read of the room to its [`note`](Self::note): Python's
/// `Registration`.
pub(crate) struct Registering<'lifetime> {
    lifetime: &'lifetime RoomHookLifetime,
    room_id: RoomId,
    number: u64,
}

impl RoomHookLifetime {
    /// A registration for `room_id`: a room the kit creates before its
    /// [`note`](Registering::note) is one it counts as holding the id.
    pub(crate) fn registering(&self, room_id: &RoomId) -> Registering<'_> {
        let mut state = lock(&self.state);
        let number = state.next;
        state.next += 1;
        state
            .registering
            .entry(room_id.clone())
            .or_default()
            .insert(number, false);
        Registering {
            lifetime: self,
            room_id: room_id.clone(),
            number,
        }
    }

    /// `room_id`'s hook or enabled name `name` is removed or disabled.
    pub(crate) fn released(&self, room_id: &RoomId, name: &str) {
        release(&mut lock(&self.state), room_id, name);
    }

    /// The names waiting for the room created under `room_id`.
    fn waiting(&self, room_id: &RoomId) -> BTreeSet<String> {
        lock(&self.state)
            .waiting
            .get(room_id)
            .cloned()
            .unwrap_or_default()
    }

    /// The store has just created a room under `room_id`: the registrations
    /// under way count it as holding the id, and the names waiting for it
    /// are its own from now on. `forget` gets those names, to forget what
    /// the room holds besides them; it runs under the lifetime's lock, so no
    /// registration lands between the take and the forget.
    pub(super) fn room_created(&self, room_id: &RoomId, forget: impl FnOnce(&BTreeSet<String>)) {
        let mut state = lock(&self.state);
        if let Some(under_way) = state.registering.get_mut(room_id) {
            for created in under_way.values_mut() {
                *created = true;
            }
        }
        let keep = state.waiting.remove(room_id).unwrap_or_default();
        forget(&keep);
    }
}

impl Registering<'_> {
    /// Runs `register`, the registration's change of the kit's hooks, and
    /// notes the name `name` gives of what it registered, under the
    /// lifetime's lock, so that no creation of the room comes between them:
    /// the name waits for the room created under the id unless a room held
    /// it when it was read (`held`) or was created since.
    pub(crate) fn note<T, E>(
        &self,
        held: bool,
        register: impl FnOnce() -> Result<T, E>,
        name: impl FnOnce(&T) -> String,
    ) -> Result<T, E> {
        let mut state = lock(&self.lifetime.state);
        let registered = register()?;
        let created = state
            .registering
            .get(&self.room_id)
            .and_then(|under_way| under_way.get(&self.number))
            .copied()
            .unwrap_or(false);
        let name = name(&registered);
        if held || created {
            release(&mut state, &self.room_id, &name);
        } else {
            state
                .waiting
                .entry(self.room_id.clone())
                .or_default()
                .insert(name);
        }
        Ok(registered)
    }
}

impl Drop for Registering<'_> {
    fn drop(&mut self) {
        let mut state = lock(&self.lifetime.state);
        let Some(under_way) = state.registering.get_mut(&self.room_id) else {
            return;
        };
        under_way.remove(&self.number);
        if under_way.is_empty() {
            state.registering.remove(&self.room_id);
        }
    }
}

fn release(state: &mut State, room_id: &RoomId, name: &str) {
    let Some(names) = state.waiting.get_mut(room_id) else {
        return;
    };
    names.remove(name);
    if names.is_empty() {
        state.waiting.remove(room_id);
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// Whether a room holds `room_id`, for a room hook's registration: read
    /// with the caller's organization (RFC §17.2), another organization's
    /// room refused as not found; with none, whether a room of any
    /// organization holds it. Python's `_room_held`.
    pub(super) async fn room_held(
        &self,
        room_id: &RoomId,
        organization_id: Option<&str>,
    ) -> Result<bool, RoomKitError> {
        if organization_id.is_some() {
            self.reached(room_id, organization_id).await?;
            return Ok(true);
        }
        self.store()
            .room_exists(room_id)
            .await
            .map_err(|error| self.read_failed(error))
    }

    /// Whether `room_id` holds hooks or enabled names an earlier room of
    /// the id left: those [`forget_room_hooks`](Self::forget_room_hooks)
    /// forgets once the store created a room under the id.
    pub(super) fn holds_earlier_room_hooks(&self, room_id: &RoomId) -> bool {
        let keep = self.inner.room_hook_lifetime.waiting(room_id);
        self.config().hooks.holds_room_hooks_beyond(room_id, &keep)
    }

    /// The store has just created a room under `room_id`: forgets the room
    /// hooks and enabled names an earlier room of the id left, keeping those
    /// registered while no room held it (RFC §9.1, §17.2). Python's
    /// `RoomHookLifetime.forget_earlier_room`.
    pub(super) fn forget_room_hooks(&self, room_id: &RoomId) {
        self.inner.room_hook_lifetime.room_created(room_id, |keep| {
            let Ok(removed) = self.inner.try_change_config(|config| {
                Ok::<_, std::convert::Infallible>(config.hooks.forget_room(room_id, keep))
            });
            self.inner.retire(removed);
        });
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kept_by_the_next_creation(overtaken: bool) -> BTreeSet<String> {
        let lifetime = RoomHookLifetime::default();
        let room = RoomId::new("r1").unwrap();
        let registering = lifetime.registering(&room);
        if overtaken {
            lifetime.room_created(&room, |_| {});
        }
        registering
            .note(false, || Ok::<_, ()>(()), |()| "h".to_owned())
            .unwrap();
        drop(registering);
        let mut kept = BTreeSet::new();
        lifetime.room_created(&room, |keep| kept.clone_from(keep));
        kept
    }

    #[test]
    fn a_registration_that_read_no_room_waits_for_it() {
        assert_eq!(
            kept_by_the_next_creation(false),
            BTreeSet::from(["h".to_owned()])
        );
    }

    #[test]
    fn a_registration_the_rooms_creation_overtakes_is_that_rooms() {
        assert_eq!(kept_by_the_next_creation(true), BTreeSet::new());
    }
}
