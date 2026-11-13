//! The query's lower boundary survives later visibility filtering.

use roomkit_core::model::RoomEvent;

/// A fresh read includes a sentinel beyond `limit`; complete starts at zero.
/// An unknown zero-window context stays unknown and requires no extra read.
pub(crate) fn bounded(mut events: Vec<RoomEvent>, limit: usize) -> (Vec<RoomEvent>, Option<u64>) {
    if limit == 0 {
        return (Vec::new(), None);
    }
    if events.len() <= limit {
        return (events, Some(0));
    }
    let kept = events.split_off(events.len() - limit);
    let start = kept.first().map(|event| event.index);
    (kept, start)
}
