//! A room's hooks end with the room (RFC §9.1, §17.2).
//!
//! Room hooks, and the names a room enables on the turn executors, are kept
//! in this process by room id. A room the store deletes leaves them behind,
//! and a room created again under its id, by any organization, would run
//! them. The kit forgets them when it creates a room under the id, as it
//! forgets the id's strategy (RFC §19.7), and keeps those registered while
//! no room held the id: they wait for that room and become its own. Python's
//! `roomkit.core._room_hook_lifetime`.
//!
//! Another process may create the room again. The kit keeps the creation
//! mark (`created_at`) of the room an id's hooks and names belong to, and
//! compares it with the room in hand each time it reads the room for them
//! ([`RoomKit::settle_room`]): a later mark means the room they were
//! registered for is gone, and they are forgotten then; an operation that
//! read them before runs none of them. The hooks waiting for the room, and
//! those registered below the kit, become the first room's they are read
//! with.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{Room, RoomId, Timestamp};

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
    /// Room id → the creation mark of the room its hooks and enabled names
    /// are registered for; only while it holds some.
    marks: BTreeMap<RoomId, Timestamp>,
}

/// A kit's registration of a room hook or an enabled name, under way from
/// before its read of the room to its [`note`](Self::note): Python's
/// `Registration`.
pub(crate) struct Registering<'lifetime> {
    lifetime: &'lifetime RoomHookLifetime,
    room_id: RoomId,
    number: u64,
}

/// How the hooks and names of a room's id stand with the room in hand.
enum Settled {
    /// They are the room's.
    Own,
    /// They are a later room's: the room in hand is gone already.
    Gone,
    /// They are an earlier room's, deleted since: they are forgotten.
    Earlier,
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

    /// `room_id`'s hook or enabled name `name` is removed or disabled;
    /// `holds`, whether the id holds a hook or an enabled name still.
    pub(crate) fn released(&self, room_id: &RoomId, name: &str, holds: bool) {
        let mut state = lock(&self.state);
        release(&mut state, room_id, name);
        if !holds && !state.waiting.contains_key(room_id) {
            state.marks.remove(room_id);
        }
    }

    /// How the hooks and names of `room`'s id stand with `room`. With no
    /// mark, those waiting for a room and those `holds` finds, registered
    /// below the kit, are `room`'s from now on. Python's
    /// `RoomHookLifetime.settle`.
    fn settle(&self, room: &Room, holds: impl FnOnce() -> bool) -> Settled {
        let mut state = lock(&self.state);
        match state.marks.get(&room.id).copied() {
            None => {
                if state.waiting.remove(&room.id).is_some() || holds() {
                    state.marks.insert(room.id.clone(), room.created_at);
                }
                Settled::Own
            }
            Some(mark) if mark == room.created_at => Settled::Own,
            Some(mark) if room.created_at < mark => Settled::Gone,
            Some(_) => Settled::Earlier,
        }
    }

    /// The names waiting for the room created under `room_id`, `None`
    /// when a registration for the id is under way: the room's creation
    /// has those to settle whatever the room holds.
    fn waiting(&self, room_id: &RoomId) -> Option<BTreeSet<String>> {
        let state = lock(&self.state);
        if state.registering.contains_key(room_id) {
            return None;
        }
        Some(state.waiting.get(room_id).cloned().unwrap_or_default())
    }

    /// The store has just created a room under `room_id`: the registrations
    /// under way count it as holding the id, and the names waiting for it
    /// are its own from now on. `forget` gets those names, to forget what
    /// the room holds besides them; it runs under the lifetime's lock, so no
    /// registration lands between the take and the forget.
    pub(super) fn room_created(
        &self,
        room_id: &RoomId,
        created_at: Timestamp,
        forget: impl FnOnce(&BTreeSet<String>),
    ) {
        let mut state = lock(&self.state);
        let under_way = state.registering.get_mut(room_id);
        let registering = under_way.is_some();
        for created in under_way.into_iter().flat_map(BTreeMap::values_mut) {
            *created = true;
        }
        let keep = state.waiting.remove(room_id).unwrap_or_default();
        forget(&keep);
        if keep.is_empty() && !registering {
            state.marks.remove(room_id);
        } else {
            state.marks.insert(room_id.clone(), created_at);
        }
    }
}

impl Registering<'_> {
    /// Whether the kit created a room under the id since the registration
    /// began.
    pub(crate) fn created(&self) -> bool {
        lock(&self.lifetime.state)
            .registering
            .get(&self.room_id)
            .and_then(|under_way| under_way.get(&self.number))
            .copied()
            .unwrap_or(false)
    }

    /// Runs `register`, the registration's change of the kit's hooks, and
    /// notes the name `name` gives of what it registered, under the
    /// lifetime's lock, so that no creation of the room comes between them:
    /// the name waits for the room created under the id unless a room held
    /// it when it was read (`held`) or was created since.
    pub(crate) fn note<T, E>(
        &self,
        mark: Option<Timestamp>,
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
        if mark.is_some() || created {
            release(&mut state, &self.room_id, &name);
            // A mark is never lowered: the room read may be gone already.
            if let Some(mark) = mark
                && state
                    .marks
                    .get(&self.room_id)
                    .is_none_or(|held| *held < mark)
            {
                state.marks.insert(self.room_id.clone(), mark);
            }
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
    /// The creation mark of the room holding `room_id`, `None` for none,
    /// for a room hook's registration or a subscription: read with the
    /// caller's organization (RFC §17.2), another organization's room
    /// refused as not found; with none, the room of any organization. The
    /// id's hooks of a room another mark names, deleted since, are
    /// forgotten first. Python's `_room_mark`.
    pub(super) async fn room_mark(
        &self,
        room_id: &RoomId,
        organization_id: Option<&str>,
    ) -> Result<Option<Timestamp>, RoomKitError> {
        let room = if let Some(organization_id) = organization_id {
            let options = super::options::GetRoomOptions {
                organization_id: Some(organization_id.to_owned()),
            };
            Some(self.get_room(room_id, options).await?)
        } else {
            self.store()
                .get_room(room_id)
                .await
                .map_err(|error| self.read_failed(error))?
        };
        Ok(room.map(|room| {
            self.settle_room(&room);
            room.created_at
        }))
    }

    /// `room` is in hand: its id's hooks and enabled names of an earlier
    /// room, deleted since and created again, by this process or another,
    /// are forgotten (RFC §9.1, §17.2). Whether the id's hooks are `room`'s:
    /// a room earlier than theirs is gone already. Python's `_settle_room`.
    pub(crate) fn settle_room(&self, room: &Room) -> bool {
        let holds = || {
            let config = self.inner.config();
            !config.hooks.room_hook_names(&room.id).is_empty()
                || !config.hooks.enabled_room_hooks(&room.id).is_empty()
        };
        match self.inner.room_hook_lifetime.settle(room, holds) {
            Settled::Own => true,
            Settled::Gone => false,
            Settled::Earlier => {
                self.forget_room_hooks(&room.id, room.created_at);
                true
            }
        }
    }

    /// Whether a room created under `room_id` has room hooks to settle
    /// before it is announced, in
    /// [`forget_room_hooks`](Self::forget_room_hooks): hooks or enabled
    /// names an earlier room of the id left, names waiting for it, or a
    /// registration under way. Python settles them at every creation.
    pub(super) fn has_room_hooks_to_settle(&self, room_id: &RoomId) -> bool {
        let Some(keep) = self.inner.room_hook_lifetime.waiting(room_id) else {
            return true;
        };
        !keep.is_empty() || self.config().hooks.holds_room_hooks_beyond(room_id, &keep)
    }

    /// The store has just created a room under `room_id`: forgets the room
    /// hooks and enabled names an earlier room of the id left, keeping those
    /// registered while no room held it (RFC §9.1, §17.2). Python's
    /// `RoomHookLifetime.forget_earlier_room`.
    pub(super) fn forget_room_hooks(&self, room_id: &RoomId, created_at: Timestamp) {
        self.inner
            .room_hook_lifetime
            .room_created(room_id, created_at, |keep| {
                let Ok(removed) = self.inner.try_change_config(|config| {
                    Ok::<_, std::convert::Infallible>(config.hooks.forget_room(room_id, keep))
                });
                // An operation that read them before runs none of them.
                self.inner.retire_forgotten(removed);
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
            lifetime.room_created(&room, Timestamp::UNIX_EPOCH, |_| {});
        }
        registering
            .note(None, || Ok::<_, ()>(()), |()| "h".to_owned())
            .unwrap();
        drop(registering);
        let mut kept = BTreeSet::new();
        lifetime.room_created(&room, Timestamp::UNIX_EPOCH, |keep| kept.clone_from(keep));
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

    // Python: tests/test_room_org_scoping.py::TestScopedRoomHooks::test_a_registration_that_read_a_room_gone_keeps_the_later_rooms_hooks.
    // A registration that read a room gone ends after a later room's mark
    // was taken: the mark is not lowered, so the later room's hooks are not
    // forgotten as an earlier room's (RFC §9.1, §17.2).
    #[test]
    fn a_registration_that_read_a_room_gone_lowers_no_mark() {
        let lifetime = RoomHookLifetime::default();
        let room = RoomId::new("r1").unwrap();
        let (gone, later) = (
            Timestamp::UNIX_EPOCH,
            Timestamp::from_unix_millis(1000).unwrap(),
        );
        let registering = lifetime.registering(&room);
        lock(&lifetime.state).marks.insert(room.clone(), later);
        registering
            .note(Some(gone), || Ok::<_, ()>(()), |()| "h".to_owned())
            .unwrap();
        drop(registering);
        assert_eq!(lock(&lifetime.state).marks.get(&room), Some(&later));
    }

    // Python: tests/conformance/scenarios/tenancy/a-deleted-room-s-turn-runs-none-of-the-hooks-of-the-room-created-again.json.
    // The work of a room gone, earlier than the room the id's hooks are
    // registered for, reads its own room: that read forgets none of them,
    // and they are not its own (RFC §9.1, §17.2).
    #[tokio::test]
    async fn an_earlier_room_s_read_forgets_none_of_the_id_s_hooks() {
        use crate::{AddRoomHookOptions, CreateRoomOptions, RoomKitOptions};
        use roomkit_core::model::{HookResult, RoomEvent, hook::AllowOptions};
        use roomkit_core::rules::hook_registry::{HookExecution, HookTrigger};
        let kit = RoomKit::new(RoomKitOptions::default());
        let id = RoomId::new("shared-id").unwrap();
        let options = CreateRoomOptions {
            room_id: Some(id.clone()),
            ..CreateRoomOptions::default()
        };
        let room = kit.create_room(options).await.unwrap();
        let hook = |_event: RoomEvent, _context: roomkit_core::model::RoomContext| async {
            HookResult::allow(AllowOptions::default())
        };
        let options = AddRoomHookOptions {
            name: "audit".to_owned(),
            ..AddRoomHookOptions::default()
        };
        kit.add_room_hook(
            &id,
            HookTrigger::AfterBroadcast,
            HookExecution::Async,
            hook,
            options,
        )
        .await
        .unwrap();
        let mut gone = room.clone();
        gone.created_at =
            Timestamp::from_unix_millis(room.created_at.as_unix_millis() - 1000).unwrap();

        let own = kit.settle_room(&gone);
        let kept = kit.inner.config().hooks.room_hook_names(&id);
        kit.close().await.unwrap();
        assert_eq!((own, kept), (false, vec!["audit".to_owned()]));
    }
}
