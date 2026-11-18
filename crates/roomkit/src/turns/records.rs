//! What the owner records per channel and room (RFC §10.6.2): the room's
//! previous turn of the channel, the one it submitted last, ended or not.
//!
//! A record falls out once [`ROOMS_KEPT`] newer ones are kept, as Python's
//! `RoomRecords`: a room quiet that long starts over, its next turn naming
//! no previous one, and work its last turn began is refused once the
//! record is gone.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use roomkit_core::model::{ChannelId, RoomEvent, RoomId, Timestamp, Visibility};

use super::wire::PreviousTurn;

/// How many (channel, room) pairs a side keeps a record for.
pub(crate) const ROOMS_KEPT: usize = 10_000;

/// The bounds the submitted trigger set on publication after its turn.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct AnswerScope {
    pub(crate) unscoped: bool,
    pub(crate) depth: u32,
}

impl PreviousTurn {
    /// Start an entry, carrying the last 32 scopes held by the owner.
    /// A channel call leaves room state but does not create an answer scope.
    pub(crate) fn after(id: &str, previous: Option<&Self>, event: Option<&RoomEvent>) -> Self {
        let mut scopes = previous
            .map(|record| record.scopes.clone())
            .unwrap_or_default();
        if let Some(event) = event {
            let scope = AnswerScope {
                unscoped: event
                    .response_visibility
                    .as_ref()
                    .is_none_or(|v| *v == Visibility::All),
                depth: roomkit_core::rules::chain_depth::response_depth(event),
            };
            scopes.retain(|(turn, _)| turn != id);
            scopes.push_back((id.to_owned(), scope));
            if scopes.len() > 32 {
                scopes.pop_front();
            }
        }
        Self {
            turn_id: id.to_owned(),
            executor_id: None,
            scopes,
        }
    }

    pub(crate) fn scope(&self, turn: &str) -> AnswerScope {
        self.scopes
            .iter()
            .find(|(id, _)| id == turn)
            .map(|(_, scope)| *scope)
            .unwrap_or_default()
    }
}

type Key = (ChannelId, RoomId);

/// One record per (channel, room), the least recently written falling out
/// past `kept`.
#[derive(Debug)]
pub(crate) struct RoomRecords<T = PreviousTurn> {
    kept: usize,
    next: u64,
    records: HashMap<Key, (u64, T)>,
    order: BTreeMap<u64, Key>,
    /// Room id → the channels it holds a record of.
    rooms: HashMap<RoomId, BTreeSet<ChannelId>>,
    /// Room id → the creation mark of the room its records are of (RFC
    /// §17.2), while it holds some.
    marks: HashMap<RoomId, Timestamp>,
}

impl<T> Default for RoomRecords<T> {
    fn default() -> Self {
        Self::new(ROOMS_KEPT)
    }
}

impl<T> RoomRecords<T> {
    pub(crate) fn new(kept: usize) -> Self {
        Self {
            kept,
            next: 0,
            records: HashMap::new(),
            order: BTreeMap::new(),
            rooms: HashMap::new(),
            marks: HashMap::new(),
        }
    }

    pub(crate) fn get(&self, channel_id: &ChannelId, room_id: &RoomId) -> Option<&T> {
        self.records
            .get(&(channel_id.clone(), room_id.clone()))
            .map(|(_, record)| record)
    }

    /// Record the latest value for the pair and return the one it replaces.
    pub(crate) fn put(&mut self, channel_id: &ChannelId, room_id: &RoomId, record: T) -> Option<T> {
        let key = (channel_id.clone(), room_id.clone());
        let written = self.next;
        self.next = self.next.wrapping_add(1);
        self.order.insert(written, key.clone());
        self.rooms
            .entry(room_id.clone())
            .or_default()
            .insert(channel_id.clone());
        let replaced = self.records.insert(key, (written, record));
        if let Some((order, _)) = &replaced {
            self.order.remove(order);
        }
        while self.records.len() > self.kept {
            let Some((_, key)) = self.order.pop_first() else {
                break;
            };
            self.records.remove(&key);
            self.unindex(&key);
        }
        replaced.map(|(_, record)| record)
    }

    /// Forgets the room's record of the channel.
    pub(crate) fn pop(&mut self, channel_id: &ChannelId, room_id: &RoomId) {
        let key = (channel_id.clone(), room_id.clone());
        if let Some((order, _)) = self.records.remove(&key) {
            self.order.remove(&order);
        }
        self.unindex(&key);
    }

    /// The creation mark of the room `room_id`'s records are of.
    pub(crate) fn room_mark(&self, room_id: &RoomId) -> Option<Timestamp> {
        self.marks.get(room_id).copied()
    }

    /// The room `room_id`'s records are of the room of creation mark
    /// `mark`: those of another room of the id, deleted since and created
    /// again in another process, are forgotten first (RFC §17.2).
    pub(crate) fn mark_room(&mut self, room_id: &RoomId, mark: Timestamp) {
        if self.room_mark(room_id).is_some_and(|held| held != mark) {
            self.pop_room(room_id);
        }
        if self.rooms.contains_key(room_id) {
            self.marks.insert(room_id.clone(), mark);
        }
    }

    /// Forgets the room's records of every channel.
    pub(crate) fn pop_room(&mut self, room_id: &RoomId) {
        self.marks.remove(room_id);
        for channel_id in self.rooms.remove(room_id).unwrap_or_default() {
            if let Some((order, _)) = self.records.remove(&(channel_id, room_id.clone())) {
                self.order.remove(&order);
            }
        }
    }

    fn unindex(&mut self, (channel_id, room_id): &Key) {
        if let Some(channels) = self.rooms.get_mut(room_id) {
            channels.remove(channel_id);
            if channels.is_empty() {
                self.rooms.remove(room_id);
                self.marks.remove(room_id);
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }
}

impl RoomRecords {
    /// Undo only the current entry. A later turn can already own this room.
    pub(crate) fn undo_submission(
        &mut self,
        channel: &ChannelId,
        room: &RoomId,
        turn: &str,
        previous: Option<PreviousTurn>,
    ) {
        if self
            .get(channel, room)
            .is_some_and(|entry| entry.turn_id == turn)
        {
            match previous {
                Some(record) => {
                    self.put(channel, room, record);
                }
                None => self.pop(channel, room),
            }
        }
    }

    /// Records the executor that took `turn_id`, while it is the room's
    /// last turn of the channel.
    pub(crate) fn took(
        &mut self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        turn_id: &str,
        executor_id: &str,
    ) {
        if let Some((_, record)) = self.records.get_mut(&(channel_id.clone(), room_id.clone()))
            && record.turn_id == turn_id
        {
            record.executor_id = Some(executor_id.to_owned());
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn turn(id: &str) -> PreviousTurn {
        PreviousTurn::after(id, None, None)
    }

    /// Python: `tests/test_turns_channel_control.py::test_the_first_turn_of_a_room_created_again_names_no_previous_turn`:
    /// another process deleted the room and created it again; the records
    /// of the earlier room, of every channel, are dropped (RFC §17.2).
    #[test]
    fn the_records_of_a_room_created_again_are_dropped() {
        let (agent, other, room) = (
            ChannelId::new("agent").unwrap(),
            ChannelId::new("other").unwrap(),
            RoomId::new("room").unwrap(),
        );
        let (first, again) = (
            Timestamp::UNIX_EPOCH,
            Timestamp::from_unix_millis(1).unwrap(),
        );
        let mut records = RoomRecords::default();
        records.put(&agent, &room, turn("t1"));
        records.put(&other, &room, turn("t2"));
        records.mark_room(&room, first);
        records.mark_room(&room, first);
        let kept = records
            .get(&agent, &room)
            .map(|record| record.turn_id.clone());
        records.mark_room(&room, again);
        let dropped = (
            records.get(&agent, &room).is_none(),
            records.get(&other, &room).is_none(),
        );
        assert_eq!(
            (kept.as_deref(), dropped, records.room_mark(&room)),
            (Some("t1"), (true, true), None)
        );
    }

    /// The records are bounded: the least recently written falls out.
    #[test]
    fn the_least_recently_written_record_falls_out_past_the_bound() {
        let mut records = RoomRecords::new(2);
        let agent = ChannelId::new("ai").unwrap();
        let rooms = ["r1", "r2", "r3"].map(|id| RoomId::new(id).unwrap());
        records.put(&agent, &rooms[0], turn("t1"));
        records.put(&agent, &rooms[1], turn("t2"));
        records.put(&agent, &rooms[0], turn("t3"));
        records.put(&agent, &rooms[2], turn("t4"));
        assert_eq!(
            (
                records.len(),
                records.get(&agent, &rooms[0]).map(|r| r.turn_id.as_str()),
                records.get(&agent, &rooms[1]).is_none(),
            ),
            (2, Some("t3"), true)
        );
    }
}
