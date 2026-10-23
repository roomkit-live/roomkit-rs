//! The rooms' admission tickets, in one process (RFC §9.5.1).
//!
//! An event whose off-lock checks run before the room lock takes a ticket
//! of its room, then waits for every ticket taken before its own to be
//! released before it asks for the lock: events that took a ticket commit
//! in ticket order, whichever check finishes first. A ticket is released
//! when its holder drops it, so a call that ends on any path, cancelled
//! among them, never leaves one held.

use std::collections::{HashMap, VecDeque};
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::RoomId;
use tokio::sync::Notify;

/// The tickets of every room, created on first use and dropped once a room
/// holds none.
#[derive(Debug, Default)]
pub(crate) struct RoomTickets {
    rooms: Mutex<HashMap<RoomId, Queue>>,
}

/// One room's tickets.
#[derive(Debug, Default)]
struct Queue {
    /// The number the next ticket takes.
    next: u64,
    /// The tickets not released yet, in the order they were taken.
    held: VecDeque<u64>,
    /// Woken whenever a ticket of the room is released.
    released: Arc<Notify>,
}

/// A ticket of a room; dropping it releases it, wherever it stands.
#[derive(Debug)]
pub(crate) struct Ticket<'tickets> {
    room_id: RoomId,
    number: u64,
    /// The room's wake-up, the same for every ticket of the room while any
    /// is held.
    released: Arc<Notify>,
    tickets: &'tickets RoomTickets,
}

impl RoomTickets {
    /// Takes the room's next ticket: it never waits.
    pub(crate) fn take(&self, room_id: &RoomId) -> Ticket<'_> {
        let mut rooms = self.rooms();
        let queue = rooms.entry(room_id.clone()).or_default();
        let number = queue.next;
        queue.next = queue.next.wrapping_add(1);
        queue.held.push_back(number);
        Ticket {
            room_id: room_id.clone(),
            number,
            released: Arc::clone(&queue.released),
            tickets: self,
        }
    }

    /// Waits until every ticket of the room taken before `ticket` is
    /// released. Cancelled, it leaves the ticket where it stands.
    pub(crate) async fn turn(&self, ticket: &Ticket<'_>) {
        loop {
            // Listening before looking: a release between the look and the
            // wait still wakes this one.
            let mut released = pin!(ticket.released.notified());
            released.as_mut().enable();
            if self.is_first(ticket) {
                return;
            }
            released.await;
        }
    }

    /// Whether no ticket of the room taken before `ticket` is held.
    fn is_first(&self, ticket: &Ticket<'_>) -> bool {
        self.rooms()
            .get(&ticket.room_id)
            .and_then(|queue| queue.held.front())
            .is_none_or(|first| *first == ticket.number)
    }

    fn rooms(&self) -> MutexGuard<'_, HashMap<RoomId, Queue>> {
        self.rooms.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// How many tickets the room holds, for tests.
    #[cfg(test)]
    pub(crate) fn held(&self, room_id: &RoomId) -> usize {
        self.rooms()
            .get(room_id)
            .map_or(0, |queue| queue.held.len())
    }
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        let mut rooms = self.tickets.rooms();
        if let Some(queue) = rooms.get_mut(&self.room_id) {
            queue.held.retain(|number| *number != self.number);
            if queue.held.is_empty() {
                rooms.remove(&self.room_id);
            }
        }
        self.released.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
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
        assert!(tickets.rooms().is_empty());
    }
}
