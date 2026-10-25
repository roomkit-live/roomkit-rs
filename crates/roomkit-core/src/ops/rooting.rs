//! The reads that root an event's thread parent, under the room lock (RFC
//! §10.1 step 8, §5.2).
//!
//! Every path that commits an event roots its parent at its commit: an
//! inbound message, a direct injection, a response, an event a hook
//! injects, a record stored `BLOCKED` and a record committed outside the
//! pipeline. The event the parent names is read: a root is kept, a reply
//! replaced by the root it names, which is read in turn (`rules::threads`).
//! A parent that, once rooted, names no event of the room is dropped with a
//! warning: a stale reference does not lose the event, posted at the top
//! level. The caller issues each read the way its section issues requests.

use crate::model::{ErrorRecord, EventId, RoomEvent, RoomId};
use crate::ops::protocol::{Outcome, StoreRequest};
use crate::rules::threads::{self, Parent};

/// The rooting of one event's thread parent.
#[derive(Debug)]
pub(crate) struct Rooting {
    room_id: RoomId,
    /// The event being rooted, which a warning names.
    event_id: EventId,
    /// The parent as the event named it.
    named: EventId,
    /// Whether the read outstanding is the root a reply names.
    reading_root: bool,
}

/// What a read of a rooting gives.
#[derive(Debug)]
pub(crate) enum Rooted {
    /// The next read: the root the parent, a reply, names.
    Read(Box<StoreRequest>),
    /// The parent the event keeps: its thread's root, or none.
    Parent(Option<EventId>),
}

impl Rooting {
    /// The rooting of `event`'s parent and its first read, the event the
    /// parent names; `None` for an event with no parent.
    pub(crate) fn of(event: &RoomEvent) -> Option<(Self, StoreRequest)> {
        let named = event.parent_event_id.clone()?;
        let request = StoreRequest::GetEvent {
            event_id: named.clone(),
        };
        let rooting = Self {
            room_id: event.room_id.clone(),
            event_id: event.id.clone(),
            named,
            reading_root: false,
        };
        Some((rooting, request))
    }

    /// Takes the answer of the read outstanding: the root a reply names is
    /// read next, else the parent the event keeps.
    ///
    /// # Errors
    ///
    /// Returns the store's failure of the read.
    pub(crate) fn on_read(&mut self, outcome: Outcome) -> Result<Rooted, ErrorRecord> {
        let read = outcome.into_event()?;
        if self.reading_root {
            let root = threads::root(read.as_deref(), &self.room_id);
            return Ok(Rooted::Parent(self.kept(root)));
        }
        match threads::parent(read.as_deref(), &self.room_id) {
            Parent::Root(root) => Ok(Rooted::Parent(Some(root))),
            Parent::Reply { root } => {
                self.reading_root = true;
                let request = StoreRequest::GetEvent { event_id: root };
                Ok(Rooted::Read(Box::new(request)))
            }
            Parent::Missing => Ok(Rooted::Parent(self.kept(None))),
        }
    }

    /// `root`, the parent the event keeps, warning when it keeps none.
    fn kept(&self, root: Option<EventId>) -> Option<EventId> {
        if root.is_none() {
            tracing::warn!(
                target: "roomkit::core::inbound",
                room_id = %self.room_id,
                event_id = %self.event_id,
                parent = %self.named,
                "the thread parent names no event of the room once rooted; the event is posted at the top level"
            );
        }
        root
    }
}
