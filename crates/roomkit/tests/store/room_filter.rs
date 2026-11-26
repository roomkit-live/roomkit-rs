//! Python's `RoomFilter.admits`, a room filter read without a store.

use roomkit::RoomFilter;
use roomkit::model::{Room, RoomId, RoomStatus};

/// Python's `RoomFilter.admits`: one of the statuses, the organization.
#[test]
fn a_room_filter_admits_a_room_of_its_statuses_and_organization() {
    let mut room = Room::new(RoomId::new("room_1").unwrap());
    room.organization_id = Some("bank".to_owned());
    let filters = [
        RoomFilter::default(),
        RoomFilter {
            statuses: Some(vec![RoomStatus::Paused, RoomStatus::Active]),
            organization_id: Some("bank".to_owned()),
        },
        RoomFilter {
            statuses: Some(vec![RoomStatus::Paused]),
            ..RoomFilter::default()
        },
        RoomFilter {
            organization_id: Some("clinic".to_owned()),
            ..RoomFilter::default()
        },
    ];
    assert_eq!(
        filters.map(|filter| filter.admits(&room)),
        [true, true, false, false]
    );
}
