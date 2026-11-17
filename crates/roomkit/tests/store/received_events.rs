//! Python: `tests/test_context_visibility.py::test_received_events_preserves_order_duplicates_and_objects`.

use roomkit::model::{EventStatus, RoomEvent};
use roomkit::received_events;

use super::suite::event_stored;

#[test]
fn received_events_preserves_order_duplicates_and_objects() {
    let pending = event_stored(0, EventStatus::Pending);
    let read = event_stored(1, EventStatus::Read);
    let failed = event_stored(2, EventStatus::Failed);
    let delivered = event_stored(3, EventStatus::Delivered);
    let blocked = event_stored(4, EventStatus::Blocked);
    let rows = [
        &blocked, &pending, &read, &failed, &delivered, &blocked, &pending,
    ];
    let expected = [&pending, &read, &failed, &delivered, &pending];
    let retained = received_events(rows);
    assert_eq!(retained, expected);
    assert!(
        retained
            .into_iter()
            .zip(expected)
            .all(|(a, b)| std::ptr::eq(a, b))
    );
    let empty = Vec::<RoomEvent>::new();
    assert_eq!(received_events([blocked]), empty);
    assert_eq!(received_events(std::iter::empty::<RoomEvent>()), empty);
}
