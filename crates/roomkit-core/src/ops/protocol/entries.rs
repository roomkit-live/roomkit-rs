//! Calls into a room made from inside the work of the operation in that
//! room (RFC §13.5, §9.5.1 Reentrance, §10.1 step 18).
//!
//! A hook or a parse that calls `send_event` or `process_inbound` for its
//! own room, under the room's lock or in its admission order, would, run
//! apart, wait for what its own operation holds: the room lock, or the
//! admission ticket. The driver hands such a call to the running operation
//! instead ([`Operation::adopt`](super::Operation::adopt)), which runs it as
//! an entry of its own and reports its result once it is decided
//! ([`Settled`]): detached completion, at the commit (gap G-070). A call
//! from a turn or a delivery is a new arrival, an operation of its own,
//! whose caller receives its decision ([`Decided`]) when it cannot wait for
//! its delivery.

use super::{InboundError, InboundResult};
use crate::model::{DirectEvent, InboundMessage, RoomId};

/// Identifies an adopted entry within its operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntryId(u64);

impl EntryId {
    pub(crate) fn first() -> Self {
        Self(0)
    }

    pub(crate) fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

/// A call into a room, made by code that the operation in that room
/// awaits.
#[derive(Clone, Debug, PartialEq)]
pub enum Entry {
    /// `process_inbound(message, room_id)` (RFC §10.1), for a caller acting
    /// for `organization_id` (RFC §17.2).
    Message {
        /// The room the call names.
        room_id: RoomId,
        /// The organization the caller acts for.
        organization_id: Option<String>,
        /// The message.
        message: Box<InboundMessage>,
    },
    /// `send_event(room_id, ...)` (RFC §10.5), for a caller acting for
    /// `organization_id` (RFC §17.2, gap G-069).
    Direct {
        /// The room the call names.
        room_id: RoomId,
        /// The organization the caller acts for.
        organization_id: Option<String>,
        /// The event.
        event: Box<DirectEvent>,
    },
}

impl Entry {
    /// The room the call names.
    pub fn room_id(&self) -> &RoomId {
        match self {
            Self::Message { room_id, .. } | Self::Direct { room_id, .. } => room_id,
        }
    }
}

/// An adopted entry decided: what its caller receives, at its commit (RFC
/// §10.1 step 18). A message's result carries no delivery: its delivery set
/// follows in the room's lane. A direct injection's refusal before its
/// commit is an error (RFC §10.5).
#[derive(Clone, Debug, PartialEq)]
pub struct Settled {
    /// The entry.
    pub entry: EntryId,
    /// Its result.
    pub result: Result<InboundResult, InboundError>,
}

/// An inbound call decided: what a caller that cannot wait for the call's
/// delivery receives, at the commit (RFC §10.1 step 18, detached
/// completion). The result carries no delivery: the event's delivery set,
/// and the rest of the cascade, follow in the room's lane. A direct
/// injection's refusal before its commit is an error (RFC §10.5).
#[derive(Clone, Debug, PartialEq)]
pub struct Decided {
    /// The room the call came into; `None` when it reached none, its
    /// routing having failed.
    pub room_id: Option<RoomId>,
    /// Its result at the decision.
    pub result: Result<InboundResult, InboundError>,
}
