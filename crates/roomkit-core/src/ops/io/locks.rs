//! The room locks one operation asks the driver for, one tenure of a room's
//! lock at a time.
//!
//! An operation may run several sections at once: the entries it adopted
//! run beside its own passes (gap G-070). Two of its sections that want one
//! room's lock take it one after the other: the second's request is held
//! back here, not sent, until the driver answers the first's release, so a
//! driver never sees an operation ask for a lock it holds, nor wait for its
//! own lock. A section nested under a section that holds the lock asks
//! nothing of the driver at all: `Io` answers it.

use std::collections::{BTreeMap, VecDeque};

use crate::model::RoomId;
use crate::ops::protocol::{Effect, EffectId, Outcome, Request};

/// Which lock request an effect is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Acquire,
    Release,
}

/// The rooms whose lock the operation holds or waits for, with the
/// requests held back behind each.
#[derive(Debug, Default)]
pub(super) struct RoomLocks {
    /// Each room the operation holds, or waits for, through the driver: the
    /// acquires held back until the lock is free again.
    rooms: BTreeMap<RoomId, VecDeque<Effect>>,
    /// The room and the step of each lock request not answered yet.
    requests: BTreeMap<EffectId, (RoomId, Step)>,
}

impl RoomLocks {
    /// Admits `effect` towards the driver: an acquire of a room the
    /// operation holds or waits for already is held back and `None` comes
    /// back; anything else goes out at once.
    pub(super) fn admit(&mut self, effect: Effect) -> Option<Effect> {
        let Some((room_id, step)) = lock_request(&effect.request) else {
            return Some(effect);
        };
        self.requests.insert(effect.id, (room_id.clone(), step));
        if step == Step::Release {
            return Some(effect);
        }
        if let Some(held_back) = self.rooms.get_mut(&room_id) {
            held_back.push_back(effect);
            return None;
        }
        self.rooms.insert(room_id, VecDeque::new());
        Some(effect)
    }

    /// The driver answered `effect` with `outcome`. When it let the room's
    /// lock go (a release, answered whatever way, or an acquire that did
    /// not take it), the acquire held back first goes out now.
    pub(super) fn answered(&mut self, effect: EffectId, outcome: &Outcome) -> Option<Effect> {
        let (room_id, step) = self.requests.remove(&effect)?;
        let let_go = step == Step::Release || *outcome != Outcome::Locked;
        if !let_go {
            return None;
        }
        let held_back = self.rooms.get_mut(&room_id)?;
        let next = held_back.pop_front();
        if next.is_none() {
            self.rooms.remove(&room_id);
        }
        next
    }
}

/// The room and the step of a lock request; `None` for any other request.
fn lock_request(request: &Request) -> Option<(RoomId, Step)> {
    if let Request::AcquireLock { room_id } = request {
        return Some((room_id.clone(), Step::Acquire));
    }
    if let Request::ReleaseLock { room_id } = request {
        return Some((room_id.clone(), Step::Release));
    }
    None
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    fn effect(id: u64, request: Request) -> Effect {
        let mut effect_id = EffectId::first();
        for _ in 0..id {
            effect_id = effect_id.next();
        }
        Effect {
            id: effect_id,
            request,
            deadline: None,
            locked: None,
            admitted: None,
            budget: None,
        }
    }

    fn acquire(id: u64) -> Effect {
        effect(id, Request::AcquireLock { room_id: room_id() })
    }

    fn release(id: u64) -> Effect {
        effect(id, Request::ReleaseLock { room_id: room_id() })
    }

    #[test]
    fn a_first_acquire_goes_out() {
        let mut locks = RoomLocks::default();
        assert_eq!(locks.admit(acquire(0)), Some(acquire(0)));
    }

    #[test]
    fn a_second_acquire_waits_for_the_first_section_s_release() {
        let mut locks = RoomLocks::default();
        locks.admit(acquire(0));
        let held_back = locks.admit(acquire(1));
        locks.answered(acquire(0).id, &Outcome::Locked);
        locks.admit(release(2));
        let after_release = locks.answered(release(2).id, &Outcome::Released);
        assert_eq!((held_back, after_release), (None, Some(acquire(1))));
    }

    #[test]
    fn an_acquire_that_did_not_take_the_lock_lets_the_next_one_go() {
        let mut locks = RoomLocks::default();
        locks.admit(acquire(0));
        locks.admit(acquire(1));
        assert_eq!(
            locks.answered(acquire(0).id, &Outcome::DeadlinePassed),
            Some(acquire(1))
        );
    }

    #[test]
    fn a_room_let_go_with_nothing_held_back_is_free_again() {
        let mut locks = RoomLocks::default();
        locks.admit(acquire(0));
        locks.answered(acquire(0).id, &Outcome::Locked);
        locks.admit(release(1));
        locks.answered(release(1).id, &Outcome::Released);
        assert_eq!(locks.admit(acquire(2)), Some(acquire(2)));
    }
}
