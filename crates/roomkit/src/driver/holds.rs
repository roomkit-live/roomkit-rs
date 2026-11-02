//! What a run holds of a room: its lock, its places in the delivery lane
//! and its admission tickets (RFC §13.5, §10.2, §9.5.1), taken and let go
//! as the operation asks.
//!
//! A release is carried out at once, and so is the taking of a place or a
//! ticket, which never waits. A wait (for the lock, a place's turn, a
//! ticket's turn) is a future the run polls itself, beside its calls, so
//! that the run goes on completing its other effects while it waits: its
//! own adopted entries may hold what it waits for. A wait carries the
//! effect's deadline (RFC §13.6): past it, what was waited for is not taken.
//!
//! A room's lock the caller's code handed the call (a keyed publication's
//! section, RFC §22.1) is the run's from its start, until the operation,
//! nested under it, releases it.
//!
//! A place taken in a room's lane under a lock granted on a lease (a
//! release's answer made from a SYNC hook, RFC §11.7.6) keeps its rank,
//! ahead of what that section commits after it, but its turn comes once
//! the section is over: the set never runs while the work that placed it
//! still holds the room.
//!
//! The run reads the execution context of the task that runs it:
//!
//! - a lock the context holds a live lease on is granted at once to a
//!   separate operation, a lifecycle change or a record's commit, and its
//!   release gives nothing back: the lease holder releases it. It is
//!   refused to an inbound call ([`Hold::Refused`]): one nested under the
//!   lock is adopted by the section that holds it, or runs nested on the
//!   lease from its start when that section's run adopts nothing, as a
//!   lifecycle change telling a channel of it ([`Held::take_lease`]), so
//!   one that waits for it, a message routed back into the room, would
//!   commit within that section's tenure, and would wait for its own
//!   caller's delivery;
//! - the lock of a room a frame of the context stands in the admission
//!   order of is refused to an inbound call too: a call made there into the
//!   room is adopted beside the frame's work, so one that reaches the
//!   driver, a message routed back into the room (gap G-070, decision 4),
//!   would commit ahead of the event the frame's call works for outside
//!   every order, and a parse that routes every message back would recurse
//!   without bound;
//! - a ticket's turn in the admission order of a room a frame of the
//!   context stands in is never waited for (RFC §9.5.1 Reentrance): the
//!   ticket that frame's run holds, or takes once the frame's call is over,
//!   would wait for this one.
//!
//! What the run holds is mirrored in its shared side, which the calls it
//! makes read live, and forgotten when the run is dropped, whatever ends
//! it: its places and tickets are dropped with it.

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;

use roomkit_core::model::{ErrorRecord, EventId, RoomId};
use roomkit_core::ops::{EffectId, HoldOutcome, HoldRequest, Outcome};
use tokio::time::Instant;

use crate::context::{ExecutionContext, Lease, RunShared};
use crate::locks::{RoomGuard, RoomMutexes};
use crate::queues::{Place, RoomLanes, RoomTickets, Ticket};

/// The kit's room locks, delivery lanes and admission tickets.
#[derive(Clone, Copy, Debug)]
pub(super) struct Rooms<'kit> {
    pub(super) locks: &'kit RoomMutexes,
    pub(super) lanes: &'kit RoomLanes,
    pub(super) tickets: &'kit RoomTickets,
}

/// How a run treats a lock its execution context holds a live lease on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Leases {
    /// Granted at once: a separate operation called by code under the lock
    /// (RFC §13.5).
    Granted,
    /// Refused: an inbound call, which the section that holds the lock
    /// adopts when it can.
    Refused,
}

/// A wait the run polls: it answers its effect.
pub(super) type Waiting<'kit> =
    Pin<Box<dyn Future<Output = (EffectId, Waited<'kit>)> + Send + 'kit>>;

/// What a wait came to: what it took, nothing once its deadline passed.
pub(super) enum Waited<'kit> {
    Lock(RoomId, Option<RoomGuard<'kit>>),
    /// The place, by its room and event, comes back whether its turn came
    /// or not.
    Place((RoomId, EventId), Place<'kit>, bool),
    /// The ticket comes back whether its turn came or not.
    Turn(RoomId, Ticket<'kit>, bool),
    /// A hold carried out at once, answered once the room's delivery cursor
    /// caught up with the turns it ended (gap G-091).
    Answered(Outcome),
}

/// What a hold comes to.
#[expect(
    clippy::large_enum_variant,
    reason = "a hold is moved once, to the run that asked: an answer carries its outcome whole"
)]
pub(super) enum Hold<'kit> {
    /// Answered at once.
    Now(Outcome),
    /// Answered once the wait is over.
    Wait(Waiting<'kit>),
    /// Refused at once: what it waits for is held by the call this one runs
    /// within, which waits for this one.
    Refused(RoomId),
}

/// How the run holds a room's lock.
enum LockHold<'kit> {
    /// The lock itself.
    Own(
        #[expect(dead_code, reason = "held for its drop, which releases the lock")] RoomGuard<'kit>,
    ),
    /// Granted on its context's lease.
    Granted(Lease),
}

/// The room locks, lane places and admission tickets a run holds, released
/// when the operation asks, or when the run ends, whatever ends it.
pub(super) struct Held<'kit> {
    shared: Arc<RunShared>,
    leases: Leases,
    locks: HashMap<RoomId, LockHold<'kit>>,
    places: HashMap<(RoomId, EventId), Place<'kit>>,
    /// The places taken first in their room's lane, the lane idle: their
    /// sets run from their placement, which the kit's close does not drop
    /// (RFC §12.10.4), until they leave.
    running: HashSet<(RoomId, EventId)>,
    /// The places taken under a lock granted on a lease, with that lease:
    /// their turn waits for its end.
    leased: HashMap<(RoomId, EventId), Lease>,
    tickets: HashMap<RoomId, Ticket<'kit>>,
}

impl<'kit> Held<'kit> {
    pub(super) fn new(shared: Arc<RunShared>, leases: Leases) -> Self {
        Self {
            shared,
            leases,
            locks: HashMap::new(),
            places: HashMap::new(),
            running: HashSet::new(),
            leased: HashMap::new(),
            tickets: HashMap::new(),
        }
    }

    /// Takes `guard`, `room_id`'s lock its caller's code handed the call (a
    /// keyed publication's section, RFC §22.1): the run holds it from its
    /// start, a tenure of its own, so that the calls its operation makes
    /// under it (`Inbound::under_lock`), a parse among them, hold a lease on
    /// it, until the operation releases it.
    pub(super) fn take_hand(&mut self, room_id: RoomId, guard: RoomGuard<'static>) {
        self.shared.locked(room_id.clone());
        self.locks.insert(room_id, LockHold::Own(guard));
    }

    /// Takes `lease`, a lease on its room's lock the call's code holds,
    /// held by a run that did not adopt the call: the run holds the lock on
    /// it from its start, granted as a separate operation's is, so that the
    /// calls its operation makes under it (`Inbound::under_lock`) hold a
    /// lease on it, and its release gives nothing back.
    pub(super) fn take_lease(&mut self, lease: Lease) {
        self.shared.granted(lease.clone());
        self.locks
            .insert(lease.room_id().clone(), LockHold::Granted(lease));
    }

    /// Whether the set of `event_id` runs from its placement: its place was
    /// taken first in `room_id`'s lane, the lane idle; a set placed behind
    /// another is queued. The Python implementation aligns on this rule
    /// (decided 2026-10-30; RMK-1039, RMK-1030).
    pub(super) fn runs(&self, room_id: &RoomId, event_id: &EventId) -> bool {
        self.running.contains(&(room_id.clone(), event_id.clone()))
    }

    /// Carries out `request` for `effect`: a release, a place or a ticket at
    /// once, a wait as a future, within `deadline`.
    pub(super) fn hold(
        &mut self,
        rooms: Rooms<'kit>,
        context: &ExecutionContext,
        effect: EffectId,
        request: HoldRequest,
        deadline: Option<Instant>,
    ) -> Hold<'kit> {
        match request {
            HoldRequest::AcquireLock { room_id } => {
                self.acquire(rooms, context, effect, room_id, deadline)
            }
            HoldRequest::ReleaseLock { room_id } => {
                self.locks.remove(&room_id);
                self.shared.released(&room_id);
                Hold::Now(HoldOutcome::Released.into())
            }
            HoldRequest::EnterLane { room_id, event_id } => {
                let place = rooms.lanes.take(&room_id, event_id.clone());
                let key = (room_id, event_id);
                if rooms.lanes.is_first(&place) {
                    self.running.insert(key.clone());
                }
                if let Some(LockHold::Granted(lease)) = self.locks.get(&key.0) {
                    self.leased.insert(key.clone(), lease.clone());
                }
                self.places.insert(key, place);
                Hold::Now(HoldOutcome::Entered.into())
            }
            HoldRequest::AwaitLane { room_id, event_id } => {
                self.await_place(rooms, effect, room_id, event_id, deadline)
            }
            HoldRequest::LeaveLane { room_id, event_id } => {
                let key = (room_id, event_id);
                self.running.remove(&key);
                self.leased.remove(&key);
                self.places.remove(&key);
                Hold::Now(HoldOutcome::Left.into())
            }
            HoldRequest::TakeTicket { room_id } => {
                let ticket = rooms.tickets.take(&room_id);
                self.tickets.insert(room_id, ticket);
                Hold::Now(HoldOutcome::Ticketed.into())
            }
            HoldRequest::AwaitTicket { room_id } => {
                self.await_turn(rooms, context, effect, room_id, deadline)
            }
            HoldRequest::ReleaseTicket { room_id } => {
                self.tickets.remove(&room_id);
                Hold::Now(HoldOutcome::TicketReleased.into())
            }
        }
    }

    /// The room's lock: on a live lease of the context, granted at once
    /// (RFC §13.5) or refused, as the run's `leases` say; refused to an
    /// inbound call whose context stands in the room's admission order;
    /// waited for otherwise.
    fn acquire(
        &mut self,
        rooms: Rooms<'kit>,
        context: &ExecutionContext,
        effect: EffectId,
        room_id: RoomId,
        deadline: Option<Instant>,
    ) -> Hold<'kit> {
        if self.leases == Leases::Refused && context.is_admitted_to(&room_id) {
            return Hold::Refused(room_id);
        }
        if let Some(lease) = context.lease(&room_id) {
            if self.leases == Leases::Refused {
                return Hold::Refused(room_id);
            }
            self.shared.granted(lease.clone());
            self.locks.insert(room_id, LockHold::Granted(lease));
            return Hold::Now(HoldOutcome::Locked.into());
        }
        let wait = async move {
            let guard = super::within(deadline, rooms.locks.lock(&room_id)).await;
            (effect, Waited::Lock(room_id, guard))
        };
        Hold::Wait(Box::pin(wait))
    }

    /// The turn of the run's place for `event_id`, once the section whose
    /// lease the place was taken on is over, if it was: never refused,
    /// since a place is taken at its event's commit and waits for nothing
    /// the commit needs.
    fn await_place(
        &mut self,
        rooms: Rooms<'kit>,
        effect: EffectId,
        room_id: RoomId,
        event_id: EventId,
        deadline: Option<Instant>,
    ) -> Hold<'kit> {
        let key = (room_id, event_id);
        let Some(place) = self.places.remove(&key) else {
            return Hold::Now(Outcome::Failed(ErrorRecord::new(
                "Internal",
                "the run holds no place for the event in the room's lane",
            )));
        };
        let section = self.leased.remove(&key);
        let wait = async move {
            let turn = async {
                if let Some(section) = section {
                    section.over().await;
                }
                rooms.lanes.turn(&place).await;
            };
            let came = super::within(deadline, turn).await.is_some();
            (effect, Waited::Place(key, place, came))
        };
        Hold::Wait(Box::pin(wait))
    }

    /// The turn of the run's ticket of the room, never one in the admission
    /// order of a room a frame of the context stands in: the ticket that
    /// frame's run holds, or takes after it, would wait for this one.
    fn await_turn(
        &mut self,
        rooms: Rooms<'kit>,
        context: &ExecutionContext,
        effect: EffectId,
        room_id: RoomId,
        deadline: Option<Instant>,
    ) -> Hold<'kit> {
        if context.is_admitted_to(&room_id) {
            return Hold::Refused(room_id);
        }
        let Some(ticket) = self.tickets.remove(&room_id) else {
            return Hold::Now(Outcome::Failed(ErrorRecord::new(
                "Internal",
                "no ticket of the room is held",
            )));
        };
        let wait = async move {
            let admitted = super::within(deadline, rooms.tickets.turn(&ticket))
                .await
                .is_some();
            (effect, Waited::Turn(room_id, ticket, admitted))
        };
        Hold::Wait(Box::pin(wait))
    }

    /// Takes what a wait came to, and answers its effect.
    pub(super) fn settle(&mut self, waited: Waited<'kit>) -> Outcome {
        match waited {
            Waited::Lock(room_id, Some(guard)) => {
                self.shared.locked(room_id.clone());
                self.locks.insert(room_id, LockHold::Own(guard));
                HoldOutcome::Locked.into()
            }
            Waited::Lock(_, None) => Outcome::DeadlinePassed,
            Waited::Place(key, place, came) => {
                self.places.insert(key, place);
                if came {
                    HoldOutcome::LaneTurn.into()
                } else {
                    Outcome::DeadlinePassed
                }
            }
            Waited::Turn(room_id, ticket, admitted) => {
                self.tickets.insert(room_id, ticket);
                if admitted {
                    HoldOutcome::Admitted.into()
                } else {
                    Outcome::DeadlinePassed
                }
            }
            Waited::Answered(outcome) => outcome,
        }
    }
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        // The guards, places and tickets go with the run; the calls it
        // made, if any still runs, must find none of them held.
        self.shared.clear();
    }
}

/// The failure of a request that would wait for what its own context
/// holds.
pub(super) fn reentrant(room_id: &RoomId) -> ErrorRecord {
    ErrorRecord::new(
        "Reentrant",
        format!(
            "room {room_id}'s lock or admission order is held by the call this one runs \
             within: it would wait for itself"
        ),
    )
}
