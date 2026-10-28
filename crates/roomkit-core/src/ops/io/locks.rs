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
//!
//! The room's admission ticket is released after the room's lock (RFC
//! §9.5.1 "Release"): a release of the ticket asked for while one of the
//! operation's sections holds the room's lock, or waits for it, is held
//! back until the last of them lets the lock go. A call whose trigger stops
//! while an entry it adopted works under the lock so keeps its place in
//! the room's admission order until the entry's section is over.

use std::collections::{BTreeMap, VecDeque};

use crate::model::RoomId;
use crate::ops::protocol::{Effect, EffectId, HoldOutcome, HoldRequest, Outcome, Request};

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
    /// The releases of a room's admission ticket held back until the
    /// operation lets the room's lock go for good.
    tickets: BTreeMap<RoomId, Vec<Effect>>,
    /// The room and the step of each lock request not answered yet.
    requests: BTreeMap<EffectId, (RoomId, Step)>,
}

impl RoomLocks {
    /// Admits `effect` towards the driver: an acquire of a room the
    /// operation holds or waits for already, and a release of that room's
    /// ticket, are held back and `None` comes back; anything else goes out
    /// at once.
    pub(super) fn admit(&mut self, effect: Effect) -> Option<Effect> {
        if let Request::Hold(HoldRequest::ReleaseTicket { room_id }) = &effect.request
            && self.rooms.contains_key(room_id)
        {
            self.tickets
                .entry(room_id.clone())
                .or_default()
                .push(effect);
            return None;
        }
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

    /// Withdraws `effect`, an acquire held back here, from the queue it
    /// waits in: it never goes out. Whether it was held back.
    pub(super) fn withdraw(&mut self, effect: EffectId) -> bool {
        let Some((room_id, Step::Acquire)) = self.requests.get(&effect).cloned() else {
            return false;
        };
        let Some(held_back) = self.rooms.get_mut(&room_id) else {
            return false;
        };
        let Some(at) = held_back.iter().position(|held| held.id == effect) else {
            return false;
        };
        held_back.remove(at);
        self.requests.remove(&effect);
        true
    }

    /// The room `effect` releases the lock of, when it is a release.
    pub(super) fn release_of(&self, effect: EffectId) -> Option<RoomId> {
        match self.requests.get(&effect) {
            Some((room_id, Step::Release)) => Some(room_id.clone()),
            Some((_, Step::Acquire)) | None => None,
        }
    }

    /// The driver answered `effect` with `outcome`; what goes out now. When
    /// it let the room's lock go (a release, answered whatever way, or an
    /// acquire that did not take it), the acquire held back first goes out;
    /// with none left, the releases of the room's ticket held back do.
    pub(super) fn answered(&mut self, effect: EffectId, outcome: &Outcome) -> Vec<Effect> {
        let Some((room_id, step)) = self.requests.remove(&effect) else {
            return Vec::new();
        };
        let let_go = step == Step::Release || *outcome != Outcome::Hold(HoldOutcome::Locked);
        if !let_go {
            return Vec::new();
        }
        let Some(held_back) = self.rooms.get_mut(&room_id) else {
            return Vec::new();
        };
        if let Some(next) = held_back.pop_front() {
            return vec![next];
        }
        self.rooms.remove(&room_id);
        self.tickets.remove(&room_id).unwrap_or_default()
    }
}

/// The room and the step of a lock request; `None` for any other request.
fn lock_request(request: &Request) -> Option<(RoomId, Step)> {
    match request {
        Request::Hold(HoldRequest::AcquireLock { room_id }) => {
            Some((room_id.clone(), Step::Acquire))
        }
        Request::Hold(HoldRequest::ReleaseLock { room_id }) => {
            Some((room_id.clone(), Step::Release))
        }
        Request::Hold(_)
        | Request::Store(_)
        | Request::Channel(_)
        | Request::Hook(_)
        | Request::Route { .. } => None,
    }
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
            decides: None,
        }
    }

    fn acquire(id: u64) -> Effect {
        effect(id, HoldRequest::AcquireLock { room_id: room_id() }.into())
    }

    fn release(id: u64) -> Effect {
        effect(id, HoldRequest::ReleaseLock { room_id: room_id() }.into())
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
        locks.answered(acquire(0).id, &HoldOutcome::Locked.into());
        locks.admit(release(2));
        let after_release = locks.answered(release(2).id, &HoldOutcome::Released.into());
        assert_eq!((held_back, after_release), (None, vec![acquire(1)]));
    }

    #[test]
    fn an_acquire_that_did_not_take_the_lock_lets_the_next_one_go() {
        let mut locks = RoomLocks::default();
        locks.admit(acquire(0));
        locks.admit(acquire(1));
        assert_eq!(
            locks.answered(acquire(0).id, &Outcome::DeadlinePassed),
            vec![acquire(1)]
        );
    }

    /// A call that expires gives up on the acquire it holds back: it never
    /// goes out, and the one behind it does once the lock is let go.
    #[test]
    fn a_withdrawn_acquire_never_goes_out() {
        let mut locks = RoomLocks::default();
        locks.admit(acquire(0));
        locks.admit(acquire(1));
        locks.admit(acquire(2));
        let withdrawn = locks.withdraw(acquire(1).id);
        let next = locks.answered(acquire(0).id, &Outcome::DeadlinePassed);
        assert_eq!((withdrawn, next), (true, vec![acquire(2)]));
    }

    #[test]
    fn a_room_let_go_with_nothing_held_back_is_free_again() {
        let mut locks = RoomLocks::default();
        locks.admit(acquire(0));
        locks.answered(acquire(0).id, &HoldOutcome::Locked.into());
        locks.admit(release(1));
        locks.answered(release(1).id, &HoldOutcome::Released.into());
        assert_eq!(locks.admit(acquire(2)), Some(acquire(2)));
    }

    /// RFC §9.5.1: the ticket is released after the room's lock, so a
    /// release asked for while a section holds the lock waits for its
    /// release.
    #[test]
    fn a_ticket_s_release_waits_for_the_lock_s() {
        let ticket = effect(2, HoldRequest::ReleaseTicket { room_id: room_id() }.into());
        let mut locks = RoomLocks::default();
        locks.admit(acquire(0));
        locks.answered(acquire(0).id, &HoldOutcome::Locked.into());
        let under_the_lock = locks.admit(ticket.clone());
        locks.admit(release(1));
        let after_release = locks.answered(release(1).id, &HoldOutcome::Released.into());
        assert_eq!((under_the_lock, after_release), (None, vec![ticket]));
    }
}
