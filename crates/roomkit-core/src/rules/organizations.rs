//! Which rooms a caller may reach, by the organization it acts for (RFC
//! §17.2, gap G-051).
//!
//! Rooms are scoped by their `organization_id`. A caller acting for an
//! organization reaches that organization's rooms only: a room of another
//! organization, or of none, is not its. A caller that names no
//! organization reads rooms unscoped, as RFC §17.2 says of a framework join.

use crate::model::Room;

/// Whether a caller acting for `organization` may reach `room`: any room
/// when it names none, the organization's own rooms otherwise.
pub fn admits(organization: Option<&str>, room: &Room) -> bool {
    organization.is_none_or(|organization| room.organization_id.as_deref() == Some(organization))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{RoomId, Timestamp};

    fn room_of(organization: Option<&str>) -> Room {
        let mut room = Room::new(RoomId::new("room_1").unwrap(), Timestamp::UNIX_EPOCH);
        room.organization_id = organization.map(str::to_owned);
        room
    }

    #[test]
    fn a_caller_reaches_its_organization_s_room() {
        assert!(admits(Some("bank"), &room_of(Some("bank"))));
    }

    #[test]
    fn a_caller_never_reaches_another_organization_s_room() {
        assert!(!admits(Some("clinic"), &room_of(Some("bank"))));
    }

    #[test]
    fn a_caller_acting_for_an_organization_never_reaches_a_room_of_none() {
        assert!(!admits(Some("bank"), &room_of(None)));
    }

    #[test]
    fn a_caller_naming_no_organization_reads_every_room() {
        assert!(admits(None, &room_of(Some("bank"))));
    }
}
