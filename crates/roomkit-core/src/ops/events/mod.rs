//! Direct calls on a room's timeline, owned by the host: changes to stored
//! events (RFC §10.3 "Direct mutation (host-owned)"), [`UpdateEvent`] and
//! [`DeleteEvent`], and a record committed outside the pipeline (RFC §10.5),
//! [`CommitRecord`].
//!
//! The host owns the authorization of these calls on its own surfaces, so
//! none checks who asks. Each runs under the room's lock, so that it
//! serialises against the inbound pipeline (RFC §13.5): the lock is taken,
//! the room read and its due timer transitions applied (gap G-045), the
//! event changed or the record committed, the lock released; then the room
//! is read again and the mutation trigger fires with it, with the event as
//! the change left it (RFC §10.3). A target
//! the room does not hold, or a room that does not exist, is a no-op that
//! fires nothing (gap G-063). The room's status
//! does not refuse a change: a change to what the room stored is no new
//! event (RFC §5.1, gap G-063). It refuses a record, which grows the
//! timeline.
//!
//! Made from code that holds the room's lock, a hook under the inbound
//! pipeline's lock among others, each takes the lock again at once (RFC
//! §13.5): the driver grants it within the execution context that holds it.

mod change;
mod section;

use std::sync::Arc;

use thiserror::Error;

use self::change::{Commit, Delete, Update};
use self::section::EventSection;
use super::protocol::{EffectId, Operation, Outcome, ProtocolError, Step};
use super::{Environment, KitConfig};
use crate::model::{ErrorRecord, EventId, EventUpdates, RoomEvent, RoomId, RoomStatus};
use crate::rules::idempotency;

/// Why a direct call on a room's timeline failed: an update, a delete, or
/// a record's commit. Nothing was changed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum MutationError {
    /// For a record's commit, the room does not exist or belongs to another
    /// organization than the caller's, or to none (RFC §17.2). An update or
    /// a delete in such a room is a no-op instead (gap G-063).
    #[error("room {0} does not exist")]
    RoomNotFound(RoomId),
    /// The record names another room than the one it is committed to (RFC
    /// §10.5): refused before anything is read.
    #[error("the record names room {record}, not room {room_id}")]
    OtherRoom {
        /// The room the record was to be committed to.
        room_id: RoomId,
        /// The room the record names.
        record: RoomId,
    },
    /// The room's status refuses new events (RFC §5.1): the record was not
    /// written.
    #[error("room {room_id} refuses new events: it is {status:?}")]
    Refused {
        /// The room.
        room_id: RoomId,
        /// Its status.
        status: RoomStatus,
    },
    /// The store refused or failed the change.
    #[error("the store failed: {0:?}")]
    Store(ErrorRecord),
    /// The room's lock could not be taken. A failed release is no error:
    /// what was decided under the lock stands (gap G-025).
    #[error("the room lock failed: {0:?}")]
    Lock(ErrorRecord),
}

/// Replaces fields of a stored event whole, under the room's lock (RFC
/// §10.3 `update_event(room_id, event_id, content|source|metadata)`), then,
/// once the lock is released, reads the room and fires `ON_EVENT_UPDATED`
/// with the event as it now stands and the room read then. It adds no
/// `edited` marker. Its output is the event as it now stands, `None` when
/// the room holds no such event or does not exist, which is a no-op that
/// fires nothing (gap G-063); a room the caller's organization does not
/// reach is one that does not exist (RFC §17.2).
///
/// An update that changes nothing writes nothing and fires nothing (gap
/// G-063).
#[derive(Debug)]
pub struct UpdateEvent {
    section: EventSection<Update>,
}

impl UpdateEvent {
    /// The update of `event_id` in `room_id` with `updates`, for a caller
    /// acting for `organization_id`; `None` reads the room unscoped.
    pub fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        event_id: EventId,
        updates: EventUpdates,
        organization_id: Option<String>,
    ) -> Self {
        let change = Update::new(event_id, updates);
        Self {
            section: EventSection::new(config, room_id, organization_id, change),
        }
    }
}

impl Operation for UpdateEvent {
    type Output = Result<Option<RoomEvent>, MutationError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        self.section.poll(environment, |updated| updated)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.section.complete(id, outcome)
    }
}

/// Removes a stored event from its room for good, under the room's lock
/// (RFC §10.3 `delete_event(room_id, event_id, cascade_replies)`): a hard
/// delete, which takes the event's replies with it, one level deep, unless
/// `cascade_replies` is unset, since nothing else protects them, and always
/// the edits and deletes of what it removes, an edit holding its target's
/// content (gap G-063). Once the lock is released it reads the room, then
/// fires `ON_EVENT_DELETED` once for each event removed, in the order of
/// the ids, each as it was stored, with the room read then (gap G-063). Its
/// output is the ids removed, the target's, then its replies' by index,
/// then the changes' by index, as the store answers them; none when the
/// room holds no such event or does not exist, which fires nothing (gap
/// G-063), a room the caller's organization does not reach being one that
/// does not exist (RFC §17.2).
///
/// The room's counters are left as they are: an index is never given out
/// again, and the timeline then holds fewer events than `event_count`
/// counts (gap G-064).
#[derive(Debug)]
pub struct DeleteEvent {
    section: EventSection<Delete>,
}

impl DeleteEvent {
    /// The delete of `event_id` from `room_id`, with its replies one level
    /// deep when `cascade_replies` is set (the RFC's default), for a caller
    /// acting for `organization_id`; `None` reads the room unscoped.
    pub fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        event_id: EventId,
        cascade_replies: bool,
        organization_id: Option<String>,
    ) -> Self {
        let change = Delete::new(event_id, cascade_replies);
        Self {
            section: EventSection::new(config, room_id, organization_id, change),
        }
    }
}

impl Operation for DeleteEvent {
    type Output = Result<Vec<EventId>, MutationError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        self.section.poll(environment, |removed| {
            removed.into_iter().map(|event| event.id).collect()
        })
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.section.complete(id, outcome)
    }
}

/// Commits a record outside the pipeline (RFC §10.5 `commit_event(room_id,
/// event, organization_id)`): under the room's lock, with the room's next
/// index, and nothing else: no hook, no delivery, no framework event. Its
/// output is the record as committed.
///
/// A record naming another room is refused before anything is read; a room
/// missing or of another organization than the caller's is not found
/// (§17.2), before its due timers are applied; a room whose status refuses
/// events refuses it before anything is written (§5.1). The record is
/// stored as the caller gives it, its index aside, its thread parent rooted
/// as every committed event's is (§10.1 step 8), a blank idempotency key
/// read as none (§13.4); it moves the room's `event_count` and
/// `latest_index`, not its activity, so a paused room stays paused and its
/// inactivity timer runs on (gap G-071). The room's delivery lane never
/// waits for it. A key its channel holds in the room fails the commit, as
/// the store refuses it (§14.3): a record is never a replay.
#[derive(Debug)]
pub struct CommitRecord {
    section: EventSection<Commit>,
}

impl CommitRecord {
    /// The commit of `record` to `room_id`, for a caller acting for
    /// `organization_id`; `None` reads the room unscoped (gap G-051).
    pub fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        mut record: RoomEvent,
        organization_id: Option<String>,
    ) -> Self {
        record.idempotency_key = idempotency::key(record.idempotency_key);
        let other_room = (record.room_id != room_id).then(|| record.room_id.clone());
        let change = Commit::new(record);
        let section = match other_room {
            Some(record) => {
                let error = MutationError::OtherRoom {
                    room_id: room_id.clone(),
                    record,
                };
                EventSection::refused(config, room_id, change, error)
            }
            None => EventSection::new(config, room_id, organization_id, change),
        };
        Self { section }
    }
}

impl Operation for CommitRecord {
    type Output = Result<RoomEvent, MutationError>;

    fn poll(&mut self, environment: &dyn Environment) -> Step<Self::Output> {
        self.section.poll(environment, |committed| committed)
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.section.complete(id, outcome)
    }
}
