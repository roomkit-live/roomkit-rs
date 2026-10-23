//! Direct changes to stored events, owned by the host (RFC §10.3 "Direct
//! mutation (host-owned)"): [`UpdateEvent`] and [`DeleteEvent`].
//!
//! The host owns the authorization of these calls on its own surfaces, so
//! neither checks who asks. Each runs under the room's lock, so that it
//! serialises against the inbound pipeline (RFC §13.5): the lock is taken,
//! the room read and its due timer transitions applied (gap G-045), the
//! event changed, the lock released; then the mutation trigger fires, with
//! the event as the change left it. A target the room does not hold is a
//! no-op that fires nothing. The room's status does not refuse them: a
//! change to what the room stored is no new event (RFC §5.1, gap G-063).

mod change;
mod section;

use std::sync::Arc;

use thiserror::Error;

use self::change::Change;
use self::section::EventSection;
use super::protocol::{EffectId, Operation, Outcome, ProtocolError, Step};
use super::{Environment, KitConfig};
use crate::model::{ErrorRecord, EventId, EventUpdates, RoomEvent, RoomId};

/// Why a direct update or delete failed. Nothing was changed.
#[derive(Clone, Debug, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum MutationError {
    /// The room does not exist.
    #[error("room {0} does not exist")]
    RoomNotFound(RoomId),
    /// The store refused or failed the change.
    #[error("the store failed: {0:?}")]
    Store(ErrorRecord),
    /// The room's lock could not be taken. A failed release is no error:
    /// what was decided under the lock stands (gap G-025).
    #[error("the room lock failed: {0:?}")]
    Lock(ErrorRecord),
}

/// Replaces fields of a stored event whole, under the room's lock (RFC
/// §10.3 `update_event(room_id, event_id, content|source|metadata)`), then
/// fires `ON_EVENT_UPDATED` with the event as it now stands. It adds no
/// `edited` marker. Its output is the event as it now stands, `None` when
/// the room holds no such event, which is a no-op that fires nothing.
///
/// An update that changes nothing writes nothing and fires nothing (gap
/// G-063).
#[derive(Debug)]
pub struct UpdateEvent {
    section: EventSection,
}

impl UpdateEvent {
    /// The update of `event_id` in `room_id` with `updates`.
    pub fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        event_id: EventId,
        updates: EventUpdates,
    ) -> Self {
        Self {
            section: EventSection::new(
                config,
                room_id,
                Change::Update {
                    event_id,
                    updates: Box::new(updates),
                },
            ),
        }
    }
}

impl Operation for UpdateEvent {
    type Output = Result<Option<RoomEvent>, MutationError>;

    fn poll(&mut self, environment: &mut dyn Environment) -> Step<Self::Output> {
        self.section
            .poll(environment, |done| done.events.into_iter().next())
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.section.complete(id, outcome)
    }
}

/// Removes a stored event from its room for good, under the room's lock
/// (RFC §10.3 `delete_event(room_id, event_id, cascade_replies)`): a hard
/// delete, which takes the replies of the event's thread with it unless
/// `cascade_replies` is unset, since nothing else protects them, and always
/// the edits and deletes of what it removes, an edit holding its target's
/// content (gap G-063). Then fires
/// `ON_EVENT_DELETED` once for each event removed, the target first, each
/// as it was stored (gap G-063). Its output is the ids removed, the
/// target's first; none when the room holds no such event, which fires
/// nothing.
///
/// The room's counters are left as they are: an index is never given out
/// again, and the timeline then holds fewer events than `event_count`
/// counts (gap G-064).
#[derive(Debug)]
pub struct DeleteEvent {
    section: EventSection,
}

impl DeleteEvent {
    /// The delete of `event_id` from `room_id`, with its thread's replies
    /// when `cascade_replies` is set (the RFC's default).
    pub fn new(
        config: Arc<KitConfig>,
        room_id: RoomId,
        event_id: EventId,
        cascade_replies: bool,
    ) -> Self {
        let change = Change::Delete {
            event_id,
            cascade_replies,
        };
        Self {
            section: EventSection::new(config, room_id, change),
        }
    }
}

impl Operation for DeleteEvent {
    type Output = Result<Vec<EventId>, MutationError>;

    fn poll(&mut self, environment: &mut dyn Environment) -> Step<Self::Output> {
        self.section.poll(environment, |done| {
            done.events.into_iter().map(|event| event.id).collect()
        })
    }

    fn complete(&mut self, id: EffectId, outcome: Outcome) -> Result<(), ProtocolError> {
        self.section.complete(id, outcome)
    }
}
