//! The indexes a run commits, and when the turn of each is over (RFC §13.5
//! "Delivery lanes across processes", gap G-091): an event's once its
//! delivery set left its place after running in the room's lane, an index with no set
//! once the run let the room's lock go.
//!
//! The protocol tells them apart: an event's place is taken under the
//! lock, after its commit and before the release, so an index committed
//! that took no place by the
//! release has no delivery set: a record stored `BLOCKED`, a lifecycle
//! record, a record committed outside the pipeline.
//!
//! A place the run took for an event it did not commit (an instruction's,
//! an event the persistence policy keeps out, a rerun's) has no index: its
//! set waits for the room's latest index when the place was taken, its
//! anchor, which the driver reads under the lock (RFC §13.5, §10.1.1
//! rule 5).

use std::collections::HashMap;

use roomkit_core::model::{EventId, RoomEvent, RoomId};
use roomkit_core::ops::{EffectId, Outcome, StoreOutcome};

use crate::gaps::SetTurn;

/// An event of a room, by its id, and its index.
type Committed = (RoomId, EventId, u64);

/// The indexes of one run, from their commit to the end of their turn.
#[derive(Debug, Default)]
pub(super) struct Indexes {
    /// The commits in flight, by effect.
    committing: HashMap<EffectId, Committed>,
    /// The events committed that took no place in their room's lane yet.
    unplaced: Vec<Committed>,
    /// The turn each place the run took waits for from the room's cursor,
    /// by room and event: its index, or the anchor of a place with none.
    placed: HashMap<(RoomId, EventId), SetTurn>,
}

impl Indexes {
    /// The run asks for `event`'s commit, as `effect`.
    pub(super) fn committing(&mut self, effect: EffectId, event: &RoomEvent) {
        let committed = (event.room_id.clone(), event.id.clone(), event.index);
        self.committing.insert(effect, committed);
    }

    /// `effect` was answered `outcome`: a commit that succeeded waits for
    /// its place, or for the release, with the index the store gave it,
    /// which it returns with its room.
    pub(super) fn answered(
        &mut self,
        effect: EffectId,
        outcome: &Outcome,
    ) -> Option<(RoomId, u64)> {
        let (room_id, event_id, _planned) = self.committing.remove(&effect)?;
        let Outcome::Store(StoreOutcome::Committed(stored)) = outcome else {
            return None;
        };
        self.unplaced
            .push((room_id.clone(), event_id, stored.index));
        Some((room_id, stored.index))
    }

    /// The turn the room's cursor gives the set whose place the run took in
    /// `room_id`'s lane: `None` for a place with no index whose anchor
    /// could not be read.
    pub(super) fn turn_of(&self, room_id: &RoomId, event_id: &EventId) -> Option<SetTurn> {
        self.placed
            .get(&(room_id.clone(), event_id.clone()))
            .copied()
    }

    /// The event takes its place in the room's lane: whether the run
    /// committed it, its place then holding its index. A place with no
    /// index waits for its anchor ([`anchored`](Self::anchored)).
    pub(super) fn entered(&mut self, room_id: &RoomId, event_id: &EventId) -> bool {
        let Some(at) = self
            .unplaced
            .iter()
            .position(|(room, event, _)| room == room_id && event == event_id)
        else {
            return false;
        };
        let (room, event, index) = self.unplaced.remove(at);
        self.placed.insert((room, event), SetTurn::Index(index));
        true
    }

    /// The place of `key`, which has no index, is anchored at `anchor`, the
    /// room's latest index when it was taken.
    pub(super) fn anchored(&mut self, key: (RoomId, EventId), anchor: i64) {
        self.placed.insert(key, SetTurn::After(anchor));
    }

    /// Remove the event's place, returning its index: none for a place with
    /// no index, whose set moves no cursor. The driver decides whether it
    /// ran: a queued set dropped by close is not delivered.
    pub(super) fn left(&mut self, room_id: &RoomId, event_id: &EventId) -> Option<u64> {
        match self.placed.remove(&(room_id.clone(), event_id.clone()))? {
            SetTurn::Index(index) => Some(index),
            SetTurn::After(_) => None,
        }
    }

    /// The run let the room's lock go: the indexes it committed there that
    /// took no place have no delivery set, and their turn is over.
    pub(super) fn released(&mut self, room_id: &RoomId) -> Vec<u64> {
        let (over, kept): (Vec<Committed>, Vec<Committed>) = std::mem::take(&mut self.unplaced)
            .into_iter()
            .partition(|(room, _, _)| room == room_id);
        self.unplaced = kept;
        over.into_iter().map(|(_, _, index)| index).collect()
    }
}
