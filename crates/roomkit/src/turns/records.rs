//! What the owner records per channel and room (RFC §10.6.2): the room's
//! previous turn of the channel, the one it submitted last, ended or not.
//!
//! A record falls out once [`ROOMS_KEPT`] newer ones are kept, as Python's
//! `RoomRecords`: a room quiet that long starts over, its next turn naming
//! no previous one, and work its last turn began is refused once the
//! record is gone.

use std::collections::{BTreeMap, HashMap};

use roomkit_core::model::{ChannelId, RoomId};

use super::wire::PreviousTurn;

/// How many (channel, room) pairs the owner keeps a record for.
pub(crate) const ROOMS_KEPT: usize = 10_000;

type Key = (ChannelId, RoomId);

/// One record per (channel, room), the least recently written falling out
/// past `kept`.
#[derive(Debug)]
pub(crate) struct RoomRecords {
    kept: usize,
    next: u64,
    records: HashMap<Key, (u64, PreviousTurn)>,
    order: BTreeMap<u64, Key>,
}

impl Default for RoomRecords {
    fn default() -> Self {
        Self::new(ROOMS_KEPT)
    }
}

impl RoomRecords {
    pub(crate) fn new(kept: usize) -> Self {
        Self {
            kept,
            next: 0,
            records: HashMap::new(),
            order: BTreeMap::new(),
        }
    }

    pub(crate) fn get(&self, channel_id: &ChannelId, room_id: &RoomId) -> Option<&PreviousTurn> {
        self.records
            .get(&(channel_id.clone(), room_id.clone()))
            .map(|(_, record)| record)
    }

    /// Records `record` as the room's last of the channel, and returns the
    /// one it replaces.
    pub(crate) fn put(
        &mut self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        record: PreviousTurn,
    ) -> Option<PreviousTurn> {
        let key = (channel_id.clone(), room_id.clone());
        let written = self.next;
        self.next = self.next.wrapping_add(1);
        self.order.insert(written, key.clone());
        let replaced = self.records.insert(key, (written, record));
        if let Some((order, _)) = &replaced {
            self.order.remove(order);
        }
        while self.records.len() > self.kept {
            let Some((_, key)) = self.order.pop_first() else {
                break;
            };
            self.records.remove(&key);
        }
        replaced.map(|(_, record)| record)
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

    /// Forgets the room's record of the channel.
    pub(crate) fn pop(&mut self, channel_id: &ChannelId, room_id: &RoomId) {
        if let Some((order, _)) = self.records.remove(&(channel_id.clone(), room_id.clone())) {
            self.order.remove(&order);
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.records.len()
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    fn turn(id: &str) -> PreviousTurn {
        PreviousTurn {
            turn_id: id.to_owned(),
            executor_id: None,
        }
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
