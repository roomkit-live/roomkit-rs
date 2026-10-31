//! Which rooms a lookup reads (RFC §10.4, §14.1): Python's `RoomFilter`,
//! and the read of every room a filter admits.

use roomkit_core::model::{Room, RoomStatus};

use super::{ConversationStore, FindRoomsOptions, StoreError};

/// Which rooms a lookup reads: Python's `RoomFilter(*, statuses=None,
/// organization_id=None)`; an empty filter reads every room.
///
/// It is not `#[non_exhaustive]`, on purpose: a store reads every field,
/// so a new field is a breaking change on purpose. A store that
/// destructures it fails to compile until it reads the new field, rather
/// than silently reading too wide.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoomFilter {
    /// Only the rooms in one of these statuses.
    pub statuses: Option<Vec<RoomStatus>>,
    /// Only the rooms of this organization.
    pub organization_id: Option<String>,
}

impl RoomFilter {
    /// Whether the filter admits `room`.
    pub fn admits(&self, room: &Room) -> bool {
        self.statuses
            .as_ref()
            .is_none_or(|statuses| statuses.contains(&room.status))
            && self
                .organization_id
                .as_ref()
                .is_none_or(|organization| room.organization_id.as_ref() == Some(organization))
    }
}

/// How many rooms a lookup reads at a time, as Python's `_ROOM_PAGE`.
const ROOM_PAGE: u64 = 100;

/// Every room `filter` admits, read through
/// [`find_rooms`](ConversationStore::find_rooms) a page at a time, a status
/// at a time, as Python's `_rooms_read_by` reads them.
pub(crate) async fn rooms_read_by<S: ConversationStore + ?Sized>(
    store: &S,
    filter: &RoomFilter,
) -> Result<Vec<Room>, StoreError> {
    let statuses: Vec<Option<RoomStatus>> = match &filter.statuses {
        None => vec![None],
        Some(statuses) => statuses.iter().copied().map(Some).collect(),
    };
    let mut found = Vec::new();
    for status in statuses {
        let mut offset = 0;
        loop {
            let options = FindRoomsOptions {
                organization_id: filter.organization_id.clone(),
                status,
                limit: ROOM_PAGE,
                offset,
                ..FindRoomsOptions::default()
            };
            let rooms = store.find_rooms(options).await?;
            let read = rooms.len();
            found.extend(rooms);
            if u64::try_from(read).is_ok_and(|read| read < ROOM_PAGE) {
                break;
            }
            offset = offset.saturating_add(ROOM_PAGE);
        }
    }
    Ok(found)
}
