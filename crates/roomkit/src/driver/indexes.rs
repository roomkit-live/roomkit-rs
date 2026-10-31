//! The indexes a run commits, and when the turn of each is over (RFC §13.5
//! "Delivery lanes across processes", gap G-091): an event's once its
//! delivery set left its place in the room's lane, an index with no set
//! once the run let the room's lock go.
//!
//! The protocol tells them apart: an event's place is taken under the
//! lock, after its commit and before the release (docs/architecture.md,
//! "Protocol rules"), so an index committed that took no place by the
//! release has no delivery set: a record stored `BLOCKED`, a lifecycle
//! record, a record committed outside the pipeline.

use std::collections::HashMap;

use roomkit_core::model::{EventId, RoomEvent, RoomId};
use roomkit_core::ops::{EffectId, Outcome, StoreOutcome};

/// An event of a room, by its id, and its index.
type Committed = (RoomId, EventId, u64);

/// The indexes of one run, from their commit to the end of their turn.
#[derive(Debug, Default)]
pub(super) struct Indexes {
    /// The commits in flight, by effect.
    committing: HashMap<EffectId, Committed>,
    /// The events committed that took no place in their room's lane yet.
    unplaced: Vec<Committed>,
    /// The index of each place the run took, by room and event.
    placed: HashMap<(RoomId, EventId), u64>,
}

impl Indexes {
    /// The run asks for `event`'s commit, as `effect`.
    pub(super) fn committing(&mut self, effect: EffectId, event: &RoomEvent) {
        let committed = (event.room_id.clone(), event.id.clone(), event.index);
        self.committing.insert(effect, committed);
    }

    /// `effect` was answered `outcome`: a commit that succeeded waits for
    /// its place, or for the release, with the index the store gave it.
    pub(super) fn answered(&mut self, effect: EffectId, outcome: &Outcome) {
        let Some((room_id, event_id, _planned)) = self.committing.remove(&effect) else {
            return;
        };
        if let Outcome::Store(StoreOutcome::Committed(stored)) = outcome {
            self.unplaced.push((room_id, event_id, stored.index));
        }
    }

    /// The event takes its place in the room's lane.
    pub(super) fn entered(&mut self, room_id: &RoomId, event_id: &EventId) {
        let Some(at) = self
            .unplaced
            .iter()
            .position(|(room, event, _)| room == room_id && event == event_id)
        else {
            // An instruction's place: it has no index (RFC §10.1.1).
            return;
        };
        let (room, event, index) = self.unplaced.remove(at);
        self.placed.insert((room, event), index);
    }

    /// The event's set left its place: its index's turn is over.
    pub(super) fn left(&mut self, room_id: &RoomId, event_id: &EventId) -> Option<u64> {
        self.placed.remove(&(room_id.clone(), event_id.clone()))
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
