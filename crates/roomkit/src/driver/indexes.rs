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
//!
//! Each index and place carries the `created_at` of the room whose context
//! the run read under the lock: a room created again under a deleted
//! room's id is another room (RFC §13.5).

use std::collections::HashMap;

use roomkit_core::model::{EventId, RoomEvent, RoomId, Timestamp};
use roomkit_core::ops::{EffectId, Outcome, StoreOutcome};

use crate::cursors::RoomCreatedAt;
use crate::gaps::SetTurn;

/// An event of a room, by its id, its index, and the room's `created_at`.
type Committed = (RoomId, EventId, u64, RoomCreatedAt);

/// The indexes of one run, from their commit to the end of their turn.
#[derive(Debug, Default)]
pub(super) struct Indexes {
    /// The commits in flight, by effect.
    committing: HashMap<EffectId, Committed>,
    /// The events committed that took no place in their room's lane yet.
    unplaced: Vec<Committed>,
    /// The turn each place the run took waits for from the room's cursor,
    /// by room and event: its index, or the anchor of a place with none,
    /// and the room's `created_at`.
    placed: HashMap<(RoomId, EventId), (SetTurn, RoomCreatedAt)>,
    /// The `created_at` of each room the run read, as it last read it.
    created_at: HashMap<RoomId, Timestamp>,
}

impl Indexes {
    /// The run read a room's context, `outcome`: its `created_at` is that
    /// of the room the run's commits and places in it are made in.
    pub(super) fn read(&mut self, outcome: &Outcome) {
        if let Outcome::Store(StoreOutcome::Context(context)) = outcome {
            let room = &context.room;
            self.created_at.insert(room.id.clone(), room.created_at);
        }
    }

    /// The run asks for `event`'s commit, as `effect`.
    pub(super) fn committing(&mut self, effect: EffectId, event: &RoomEvent) {
        let created_at = self.created_at.get(&event.room_id).copied();
        let committed = (
            event.room_id.clone(),
            event.id.clone(),
            event.index,
            created_at,
        );
        self.committing.insert(effect, committed);
    }

    /// `effect` was answered `outcome`: a commit that succeeded waits for
    /// its place, or for the release, with the index the store gave it,
    /// which it returns with its room and the room's `created_at`.
    pub(super) fn answered(
        &mut self,
        effect: EffectId,
        outcome: &Outcome,
    ) -> Option<(RoomId, u64, RoomCreatedAt)> {
        let (room_id, event_id, _planned, created_at) = self.committing.remove(&effect)?;
        let Outcome::Store(StoreOutcome::Committed(stored)) = outcome else {
            return None;
        };
        self.unplaced
            .push((room_id.clone(), event_id, stored.index, created_at));
        Some((room_id, stored.index, created_at))
    }

    /// The turn the room's cursor gives the set whose place the run took in
    /// `room_id`'s lane, and the room's `created_at`: `None` for a place
    /// with no index whose anchor could not be read.
    pub(super) fn turn_of(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
    ) -> Option<(SetTurn, RoomCreatedAt)> {
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
            .position(|(room, event, _, _)| room == room_id && event == event_id)
        else {
            return false;
        };
        let (room, event, index, created_at) = self.unplaced.remove(at);
        self.placed
            .insert((room, event), (SetTurn::Index(index), created_at));
        true
    }

    /// The place of `key`, which has no index, is anchored at `anchor`, the
    /// room's latest index when it was taken.
    pub(super) fn anchored(&mut self, key: (RoomId, EventId), anchor: i64) {
        let created_at = self.created_at.get(&key.0).copied();
        self.placed
            .insert(key, (SetTurn::After(anchor), created_at));
    }

    /// Remove the event's place, returning its index and its room's
    /// `created_at`: none for a place with no index, whose set moves no
    /// cursor. The driver decides whether it ran: a queued set dropped by
    /// close is not delivered.
    pub(super) fn left(
        &mut self,
        room_id: &RoomId,
        event_id: &EventId,
    ) -> Option<(u64, RoomCreatedAt)> {
        match self.placed.remove(&(room_id.clone(), event_id.clone()))? {
            (SetTurn::Index(index), created_at) => Some((index, created_at)),
            (SetTurn::After(_), _) => None,
        }
    }

    /// The run let the room's lock go: the indexes it committed there that
    /// took no place have no delivery set, and their turn is over; each
    /// with its room's `created_at`.
    pub(super) fn released(&mut self, room_id: &RoomId) -> Vec<(u64, RoomCreatedAt)> {
        let (over, kept): (Vec<Committed>, Vec<Committed>) = std::mem::take(&mut self.unplaced)
            .into_iter()
            .partition(|(room, _, _, _)| room == room_id);
        self.unplaced = kept;
        over.into_iter()
            .map(|(_, _, index, created_at)| (index, created_at))
            .collect()
    }
}
