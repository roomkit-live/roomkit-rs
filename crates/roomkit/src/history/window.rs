//! The query's lower boundary survives later visibility filtering.

use roomkit_core::model::RoomEvent;

/// A fresh read includes a sentinel beyond `limit`; complete starts at zero.
/// An unknown zero-window context stays unknown and requires no extra read.
pub(crate) fn bounded(events: Vec<RoomEvent>, limit: usize) -> (Vec<RoomEvent>, Option<u64>) {
    bounded_from(events, limit, Some(0))
}

/// A carried window keeps its original boundary until the new cap drops rows.
pub(crate) fn bounded_from(
    mut events: Vec<RoomEvent>,
    limit: usize,
    start_index: Option<u64>,
) -> (Vec<RoomEvent>, Option<u64>) {
    if limit == 0 {
        return (Vec::new(), None);
    }
    if events.len() <= limit {
        return (events, start_index);
    }
    let kept = events.split_off(events.len() - limit);
    let start = kept.first().map(|event| event.index);
    (kept, start)
}
