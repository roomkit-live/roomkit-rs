//! The rooms' ordered queues, in one process: the admission tickets (RFC
//! §9.5.1) and the places of the delivery lanes (RFC §10.2).
//!
//! A room's queue holds what was taken in the order it was taken, and the
//! turn of each comes once everything taken before it is released. Taking
//! never waits. What is taken is released when its holder drops it, so a
//! call that ends on any path, cancelled among them, never leaves it held.
//!
//! - An event whose off-lock checks run before the room lock takes a ticket
//!   of its room, then waits for its turn before it asks for the lock:
//!   events that took a ticket commit in ticket order, whichever check
//!   finishes first.
//! - An event committed takes its place in its room's lane under the room
//!   lock, and its delivery set waits for that place's turn: the sets run in
//!   the order of the commits, one event's set completing before the next
//!   one's begins.
//!
//! Each place records the unit of the kit's work that took it, so that the
//! kit's close drops the places queued behind one a spared unit holds (RFC
//! §12.10.4).

use std::collections::{HashMap, HashSet, VecDeque};
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{EventId, RoomId};
use tokio::sync::Notify;

use crate::tasks::{Marks, Unit};

/// One queue per room, created on first use and dropped once the room holds
/// nothing in it.
#[derive(Debug)]
pub(crate) struct RoomQueues<K> {
    rooms: Mutex<HashMap<RoomId, Queue<K>>>,
}

/// One room's queue.
#[derive(Debug)]
struct Queue<K> {
    /// What is held, in the order it was taken, with the unit of the work
    /// that took it.
    held: VecDeque<(K, Option<Unit>)>,
    /// Woken whenever something of the room is released.
    released: Arc<Notify>,
}

/// Something taken in a room's queue; dropping it releases it, wherever it
/// stands.
#[derive(Debug)]
pub(crate) struct Slot<'queues, K: PartialEq> {
    room_id: RoomId,
    key: K,
    /// The room's wake-up, the same for everything the room holds.
    released: Arc<Notify>,
    queues: &'queues RoomQueues<K>,
}

/// The rooms' admission tickets (RFC §9.5.1), numbered in the order they
/// are taken.
#[derive(Debug, Default)]
pub(crate) struct RoomTickets {
    queues: RoomQueues<u64>,
    next: AtomicU64,
}

/// A ticket of a room's admission order.
pub(crate) type Ticket<'tickets> = Slot<'tickets, u64>;

/// The rooms' delivery lanes (RFC §10.2): one place per event committed,
/// by the event's id.
pub(crate) type RoomLanes = RoomQueues<EventId>;

/// An event's place in its room's delivery lane.
pub(crate) type Place<'lanes> = Slot<'lanes, EventId>;

impl<K> Default for RoomQueues<K> {
    fn default() -> Self {
        Self {
            rooms: Mutex::new(HashMap::new()),
        }
    }
}

impl<K: Clone + PartialEq> RoomQueues<K> {
    /// Takes `key` at the back of the room's queue, for the unit of the
    /// work the current code runs in: it never waits.
    pub(crate) fn take(&self, room_id: &RoomId, key: K) -> Slot<'_, K> {
        let unit = Marks::current().innermost();
        let mut rooms = self.rooms();
        let queue = rooms.entry(room_id.clone()).or_insert_with(|| Queue {
            held: VecDeque::new(),
            released: Arc::new(Notify::new()),
        });
        queue.held.push_back((key.clone(), unit));
        Slot {
            room_id: room_id.clone(),
            key,
            released: Arc::clone(&queue.released),
            queues: self,
        }
    }

    /// Waits until everything of the room taken before `slot` is released.
    /// Cancelled, it leaves the slot where it stands.
    pub(crate) async fn turn(&self, slot: &Slot<'_, K>) {
        loop {
            // Listening before looking: a release between the look and the
            // wait still wakes this one.
            let mut released = pin!(slot.released.notified());
            released.as_mut().enable();
            if self.is_first(slot) {
                return;
            }
            released.await;
        }
    }

    /// Whether nothing of the room taken before `slot` is held.
    fn is_first(&self, slot: &Slot<'_, K>) -> bool {
        self.rooms()
            .get(&slot.room_id)
            .and_then(|queue| queue.held.front())
            .is_none_or(|(first, _)| *first == slot.key)
    }

    /// The units of the work queued behind a place of a unit in `spared`,
    /// at the front of its room's queue, but those `spared` holds: their
    /// turn comes only once that unit lets its place go.
    pub(crate) fn queued_behind(&self, spared: &HashSet<Unit>) -> HashSet<Unit> {
        let is_spared = |unit: &Option<Unit>| unit.is_some_and(|unit| spared.contains(&unit));
        self.rooms()
            .values()
            .filter(|queue| queue.held.front().is_some_and(|(_, unit)| is_spared(unit)))
            .flat_map(|queue| queue.held.iter().skip(1))
            .filter_map(|(_, unit)| unit.filter(|unit| !spared.contains(unit)))
            .collect()
    }

    fn rooms(&self) -> MutexGuard<'_, HashMap<RoomId, Queue<K>>> {
        self.rooms.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How much the room holds, for tests.
    #[cfg(test)]
    pub(crate) fn held(&self, room_id: &RoomId) -> usize {
        self.rooms()
            .get(room_id)
            .map_or(0, |queue| queue.held.len())
    }
}

impl<K: PartialEq> Drop for Slot<'_, K> {
    fn drop(&mut self) {
        let mut rooms = self
            .queues
            .rooms
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(queue) = rooms.get_mut(&self.room_id) {
            if let Some(at) = queue.held.iter().position(|(key, _)| *key == self.key) {
                queue.held.remove(at);
            }
            if queue.held.is_empty() {
                rooms.remove(&self.room_id);
            }
        }
        self.released.notify_waiters();
    }
}

impl RoomTickets {
    /// Takes the room's next ticket: it never waits.
    pub(crate) fn take(&self, room_id: &RoomId) -> Ticket<'_> {
        let number = self.next.fetch_add(1, Ordering::Relaxed);
        self.queues.take(room_id, number)
    }

    /// Waits until every ticket of the room taken before `ticket` is
    /// released. Cancelled, it leaves the ticket where it stands.
    pub(crate) async fn turn(&self, ticket: &Ticket<'_>) {
        self.queues.turn(ticket).await;
    }

    /// How many tickets the room holds, for tests.
    #[cfg(test)]
    pub(crate) fn held(&self, room_id: &RoomId) -> usize {
        self.queues.held(room_id)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    fn event(id: &str) -> EventId {
        EventId::new(id).unwrap()
    }

    /// How long a wait that should end may take before the test fails.
    const BOUND: Duration = Duration::from_secs(5);

    #[tokio::test]
    async fn the_first_ticket_s_turn_comes_at_once() {
        let tickets = RoomTickets::default();
        let ticket = tickets.take(&room_id());
        assert!(
            tokio::time::timeout(BOUND, tickets.turn(&ticket))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_later_ticket_waits_for_the_earlier_one() {
        let tickets = RoomTickets::default();
        let _first = tickets.take(&room_id());
        let second = tickets.take(&room_id());
        let waited = tokio::time::timeout(Duration::from_millis(20), tickets.turn(&second)).await;
        assert!(waited.is_err(), "the second ticket should wait");
    }

    #[tokio::test]
    async fn releasing_the_earlier_ticket_gives_the_later_one_its_turn() {
        let tickets = Arc::new(RoomTickets::default());
        let first = tickets.take(&room_id());
        let waiter = {
            let tickets = Arc::clone(&tickets);
            tokio::spawn(async move {
                let second = tickets.take(&room_id());
                tickets.turn(&second).await;
            })
        };
        // Let the waiter take its ticket and start waiting.
        tokio::task::yield_now().await;
        drop(first);
        assert!(tokio::time::timeout(BOUND, waiter).await.is_ok());
    }

    /// RFC §9.5.1: a ticket released before its turn, by a call that timed
    /// out or was cancelled, stalls no later ticket.
    #[tokio::test]
    async fn a_ticket_dropped_while_waiting_leaves_the_queue() {
        let tickets = RoomTickets::default();
        let first = tickets.take(&room_id());
        let second = tickets.take(&room_id());
        let third = tickets.take(&room_id());
        let gave_up = tokio::time::timeout(Duration::from_millis(20), tickets.turn(&second)).await;
        drop(second);
        drop(first);
        assert!(
            gave_up.is_err()
                && tokio::time::timeout(BOUND, tickets.turn(&third))
                    .await
                    .is_ok()
        );
    }

    /// RFC §9.5.1, Order: turns come in the order the tickets were taken.
    #[tokio::test]
    async fn turns_come_in_ticket_order() {
        let tickets = RoomTickets::default();
        let first = tickets.take(&room_id());
        let second = tickets.take(&room_id());
        let third = tickets.take(&room_id());
        drop(first);
        let third_early =
            tokio::time::timeout(Duration::from_millis(20), tickets.turn(&third)).await;
        let second_s = tokio::time::timeout(BOUND, tickets.turn(&second)).await;
        drop(second);
        let third_s = tokio::time::timeout(BOUND, tickets.turn(&third)).await;
        assert_eq!(
            (third_early.is_err(), second_s.is_ok(), third_s.is_ok()),
            (true, true, true)
        );
    }

    #[tokio::test]
    async fn rooms_keep_their_own_order() {
        let tickets = RoomTickets::default();
        let _held = tickets.take(&room_id());
        let other = tickets.take(&RoomId::new("room_2").unwrap());
        assert!(
            tokio::time::timeout(BOUND, tickets.turn(&other))
                .await
                .is_ok()
        );
    }

    #[test]
    fn a_released_room_leaves_nothing_behind() {
        let tickets = RoomTickets::default();
        drop(tickets.take(&room_id()));
        assert!(tickets.queues.rooms().is_empty());
    }

    /// RFC §10.2: a set's turn comes once the set of every event placed
    /// before its own is over.
    #[tokio::test]
    async fn a_place_s_turn_comes_once_every_earlier_place_is_left() {
        let lanes = RoomLanes::default();
        let earlier = lanes.take(&room_id(), event("evt_1"));
        let later = lanes.take(&room_id(), event("evt_2"));
        let early = tokio::time::timeout(Duration::from_millis(20), lanes.turn(&later)).await;
        drop(earlier);
        let turn = tokio::time::timeout(BOUND, lanes.turn(&later)).await;
        assert_eq!((early.is_err(), turn.is_ok()), (true, true));
    }
}
