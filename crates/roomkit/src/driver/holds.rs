//! What a run holds of a room: its lock, its lane and its admission
//! tickets (RFC §13.5, §10.2, §9.5.1), taken and let go as the operation
//! asks.
//!
//! A release is carried out at once. A wait is a future the run polls
//! itself, beside its calls, so that the run goes on completing its other
//! effects while it waits: its own adopted entries may hold what it waits
//! for. A wait carries the effect's deadline (RFC §13.6): past it, what
//! was waited for is not taken.
//!
//! The run reads the execution context of the task that runs it:
//!
//! - a lock the context holds a live lease on is granted at once, and its
//!   release gives nothing back: the lease holder releases it;
//! - a lane, or a ticket, a run of the context holds is never waited for:
//!   that run waits for this one, so the request fails at once
//!   ([`Hold::Refused`]).
//!
//! What the run holds is mirrored in its shared side, which the calls it
//! makes read live, and forgotten when the run is dropped, whatever ends
//! it.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use roomkit_core::model::{ErrorRecord, RoomId};
use roomkit_core::ops::{EffectId, Outcome, Request};
use tokio::time::Instant;

use crate::context::{ExecutionContext, RunShared};
use crate::locks::{RoomGuard, RoomMutexes};
use crate::tickets::{RoomTickets, Ticket};

/// The kit's room locks, lanes and admission tickets.
#[derive(Clone, Copy, Debug)]
pub(super) struct Rooms<'kit> {
    pub(super) locks: &'kit RoomMutexes,
    pub(super) lanes: &'kit RoomMutexes,
    pub(super) tickets: &'kit RoomTickets,
}

/// A wait the run polls: it answers its effect.
pub(super) type Waiting<'kit> =
    Pin<Box<dyn Future<Output = (EffectId, Waited<'kit>)> + Send + 'kit>>;

/// What a wait came to: what it took, `None` once its deadline passed.
pub(super) enum Waited<'kit> {
    Lock(RoomId, Option<RoomGuard<'kit>>),
    Lane(RoomId, Option<RoomGuard<'kit>>),
    /// The ticket comes back whether its turn came or not.
    Turn(RoomId, Ticket<'kit>, bool),
}

/// What a request comes to.
pub(super) enum Hold<'kit> {
    /// Answered at once.
    Now(Outcome),
    /// Answered once the wait is over.
    Wait(Waiting<'kit>),
    /// Refused at once: the room's lane or ticket is held by a run of the
    /// context, which waits for this one.
    Refused(RoomId),
    /// Not a hold: a call.
    Call(Box<Request>),
}

/// How the run holds a room's lock.
enum LockHold<'kit> {
    /// The lock itself.
    Own(
        #[expect(dead_code, reason = "held for its drop, which releases the lock")] RoomGuard<'kit>,
    ),
    /// Granted on its context's lease.
    Granted,
}

/// The room locks, lanes and admission tickets a run holds, released when
/// the operation asks, or when the run ends, whatever ends it.
pub(super) struct Held<'kit> {
    shared: Arc<RunShared>,
    locks: HashMap<RoomId, LockHold<'kit>>,
    lanes: HashMap<RoomId, RoomGuard<'kit>>,
    tickets: HashMap<RoomId, Ticket<'kit>>,
}

impl<'kit> Held<'kit> {
    pub(super) fn new(shared: Arc<RunShared>) -> Self {
        Self {
            shared,
            locks: HashMap::new(),
            lanes: HashMap::new(),
            tickets: HashMap::new(),
        }
    }

    /// Carries out `request` for `effect`: a release at once, a wait as a
    /// future, within `deadline`; any other request comes back as a call.
    pub(super) fn hold(
        &mut self,
        rooms: Rooms<'kit>,
        context: &ExecutionContext,
        effect: EffectId,
        request: Request,
        deadline: Option<Instant>,
    ) -> Hold<'kit> {
        match request {
            Request::AcquireLock { room_id } => {
                self.acquire(rooms, context, effect, room_id, deadline)
            }
            Request::ReleaseLock { room_id } => {
                self.locks.remove(&room_id);
                self.shared.released(&room_id);
                Hold::Now(Outcome::Released)
            }
            Request::EnterLane { room_id } => {
                if context.holds_lane(&room_id) {
                    return Hold::Refused(room_id);
                }
                let wait = async move {
                    let guard = super::within(deadline, rooms.lanes.lock(&room_id)).await;
                    (effect, Waited::Lane(room_id, guard))
                };
                Hold::Wait(Box::pin(wait))
            }
            Request::LeaveLane { room_id } => {
                self.lanes.remove(&room_id);
                self.shared.set_lane(&room_id, false);
                Hold::Now(Outcome::Left)
            }
            Request::TakeTicket { room_id } => {
                let ticket = rooms.tickets.take(&room_id);
                self.shared.set_ticket(&room_id, true);
                self.tickets.insert(room_id, ticket);
                Hold::Now(Outcome::Ticketed)
            }
            Request::AwaitTicket { room_id } => {
                self.await_turn(rooms, context, effect, room_id, deadline)
            }
            Request::ReleaseTicket { room_id } => {
                self.tickets.remove(&room_id);
                self.shared.set_ticket(&room_id, false);
                Hold::Now(Outcome::TicketReleased)
            }
            request @ (Request::LoadContext { .. }
            | Request::FindByIdempotencyKey { .. }
            | Request::GetEvent { .. }
            | Request::UpdateEvent { .. }
            | Request::DeleteEvent { .. }
            | Request::Commit { .. }
            | Request::PersistSideEffects { .. }
            | Request::CreateRoom { .. }
            | Request::UpdateRoom { .. }
            | Request::StoreBinding { .. }
            | Request::DeleteBinding { .. }
            | Request::HandleInbound { .. }
            | Request::OnEvent { .. }
            | Request::Deliver { .. }
            | Request::RunHook { .. }
            | Request::RunObservers { .. }
            | Request::ResolveIdentity { .. }
            | Request::Route { .. }) => Hold::Call(Box::new(request)),
        }
    }

    /// The room's lock: granted at once on a live lease of the context
    /// (RFC §13.5), waited for otherwise.
    fn acquire(
        &mut self,
        rooms: Rooms<'kit>,
        context: &ExecutionContext,
        effect: EffectId,
        room_id: RoomId,
        deadline: Option<Instant>,
    ) -> Hold<'kit> {
        if let Some(lease) = context.lease(&room_id) {
            self.shared.granted(lease);
            self.locks.insert(room_id, LockHold::Granted);
            return Hold::Now(Outcome::Locked);
        }
        let wait = async move {
            let guard = super::within(deadline, rooms.locks.lock(&room_id)).await;
            (effect, Waited::Lock(room_id, guard))
        };
        Hold::Wait(Box::pin(wait))
    }

    /// The turn of the run's ticket of the room, never one a run of the
    /// context holds an earlier ticket for.
    fn await_turn(
        &mut self,
        rooms: Rooms<'kit>,
        context: &ExecutionContext,
        effect: EffectId,
        room_id: RoomId,
        deadline: Option<Instant>,
    ) -> Hold<'kit> {
        if context.holds_ticket(&room_id) {
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
                Outcome::Locked
            }
            Waited::Lane(room_id, Some(guard)) => {
                self.shared.set_lane(&room_id, true);
                self.lanes.insert(room_id, guard);
                Outcome::Entered
            }
            Waited::Lock(_, None) | Waited::Lane(_, None) => Outcome::DeadlinePassed,
            Waited::Turn(room_id, ticket, admitted) => {
                self.tickets.insert(room_id, ticket);
                if admitted {
                    Outcome::Admitted
                } else {
                    Outcome::DeadlinePassed
                }
            }
        }
    }
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        // The guards and tickets go with the run; the calls it made, if any
        // still runs, must find none of them held.
        self.shared.clear();
    }
}

/// The failure of a request that would wait for what its own context
/// holds.
pub(super) fn reentrant(room_id: &RoomId) -> ErrorRecord {
    ErrorRecord::new(
        "Reentrant",
        format!(
            "room {room_id}'s lane is held by the call this one runs within: it would wait for \
             itself"
        ),
    )
}
