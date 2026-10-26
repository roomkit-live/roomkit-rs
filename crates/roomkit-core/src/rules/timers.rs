//! A room's timers: which transition falls due, and when (RFC §5.1, gap
//! G-045).
//!
//! An ACTIVE room with `inactive_after_seconds` pauses once that long has
//! passed since its last activity, or since its creation before any. A
//! PAUSED room with `closed_after_seconds` closes once that long has passed
//! since it paused. A transition takes the instant it fell due, however late
//! it is applied: a check long after both fell due pauses the room at the
//! first instant and closes it at the second.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::lifecycle::{self, Transition};
use crate::model::{Room, RoomStatus, Timestamp};

/// A timer's transition and the instant it falls due.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DueTransition {
    /// A pause or a close.
    pub transition: Transition,
    /// When it falls due.
    pub at: Timestamp,
}

/// The room's next timer transition and when it falls due, whether or not
/// it has; `None` when no timer of its status is set.
pub(crate) fn next(room: &Room) -> Option<DueTransition> {
    let (transition, since, seconds) = match room.status {
        RoomStatus::Active => (
            Transition::Pause,
            room.timers.last_activity_at.unwrap_or(room.created_at),
            room.timers.inactive_after_seconds?,
        ),
        // A paused room stored without the instant it paused counts from its
        // last modification, which the pause was.
        RoomStatus::Paused => (
            Transition::Close,
            room.paused_at.unwrap_or(room.updated_at),
            room.timers.closed_after_seconds?,
        ),
        RoomStatus::Closed | RoomStatus::Archived => return None,
    };
    let at = since.checked_add(Duration::from_secs(seconds))?;
    Some(DueTransition { transition, at })
}

/// Whether a timer of the room can still fall due: a sweeper looks at these
/// rooms only.
pub fn armed(room: &Room) -> bool {
    next(room).is_some()
}

/// The transitions due by `now`, in the order they fell due, each at its own
/// instant: none, a pause, a close, or a pause then a close.
pub fn due(room: &Room, now: Timestamp) -> Vec<DueTransition> {
    let mut room = room.clone();
    let mut due = Vec::new();
    // Each applied transition moves the room on (ACTIVE, PAUSED, CLOSED) and
    // a closed room has no timer, so this ends after two at most.
    while let Some(next) = next(&room).filter(|next| next.at <= now) {
        if !lifecycle::apply(&mut room, next.transition, next.at) {
            break;
        }
        due.push(next);
    }
    due
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{RoomId, RoomTimers};

    const START: i64 = 1_792_677_780_000;

    fn at(offset_millis: i64) -> Timestamp {
        Timestamp::from_unix_millis(START + offset_millis).unwrap()
    }

    fn room(inactive_after_seconds: Option<u64>, closed_after_seconds: Option<u64>) -> Room {
        let mut room = Room::new(RoomId::new("room_1").unwrap(), at(0));
        room.timers = RoomTimers {
            inactive_after_seconds,
            closed_after_seconds,
            last_activity_at: Some(at(0)),
        };
        room
    }

    fn paused(closed_after_seconds: u64) -> Room {
        Room {
            status: RoomStatus::Paused,
            paused_at: Some(at(0)),
            ..room(Some(60), Some(closed_after_seconds))
        }
    }

    fn pause_at(millis: i64) -> DueTransition {
        DueTransition {
            transition: Transition::Pause,
            at: at(millis),
        }
    }

    fn close_at(millis: i64) -> DueTransition {
        DueTransition {
            transition: Transition::Close,
            at: at(millis),
        }
    }

    #[test]
    fn a_pause_is_not_due_a_millisecond_before_its_instant() {
        assert_eq!(due(&room(Some(60), None), at(59_999)), []);
    }

    #[test]
    fn a_pause_is_due_at_its_instant() {
        assert_eq!(due(&room(Some(60), None), at(60_000)), [pause_at(60_000)]);
    }

    #[test]
    fn a_close_is_not_due_a_millisecond_before_its_instant() {
        assert_eq!(due(&paused(120), at(119_999)), []);
    }

    #[test]
    fn a_close_is_due_at_its_instant() {
        assert_eq!(due(&paused(120), at(120_000)), [close_at(120_000)]);
    }

    #[test]
    fn a_late_check_applies_both_transitions_at_the_instants_they_fell_due() {
        let room = room(Some(60), Some(120));
        assert_eq!(
            due(&room, at(3_600_000)),
            [pause_at(60_000), close_at(180_000)]
        );
    }

    #[test]
    fn a_late_check_before_the_close_falls_due_applies_the_pause_only() {
        let room = room(Some(60), Some(120));
        assert_eq!(due(&room, at(179_999)), [pause_at(60_000)]);
    }

    #[test]
    fn inactivity_counts_from_the_last_activity() {
        let mut room = room(Some(60), None);
        room.timers.last_activity_at = Some(at(30_000));
        assert_eq!(next(&room), Some(pause_at(90_000)));
    }

    #[test]
    fn inactivity_counts_from_the_creation_before_any_event() {
        let mut room = room(Some(60), None);
        room.timers.last_activity_at = None;
        assert_eq!(next(&room), Some(pause_at(60_000)));
    }

    #[test]
    fn a_close_timer_alone_never_pauses_an_active_room() {
        assert_eq!(next(&room(None, Some(120))), None);
    }

    #[test]
    fn a_paused_room_without_a_close_timer_stays_paused() {
        let room = Room {
            status: RoomStatus::Paused,
            paused_at: Some(at(0)),
            ..room(Some(60), None)
        };
        assert_eq!(due(&room, at(3_600_000)), []);
    }

    #[test]
    fn a_closed_room_has_no_timer() {
        let room = Room {
            status: RoomStatus::Closed,
            ..room(Some(60), Some(120))
        };
        assert!(!armed(&room));
    }

    #[test]
    fn an_active_room_with_an_inactivity_timer_is_armed() {
        assert!(armed(&room(Some(60), None)));
    }

    #[test]
    fn a_paused_room_stored_without_its_pause_counts_from_its_last_modification() {
        let room = Room {
            status: RoomStatus::Paused,
            paused_at: None,
            updated_at: at(10_000),
            ..room(Some(60), Some(120))
        };
        assert_eq!(next(&room), Some(close_at(130_000)));
    }

    #[test]
    fn a_timer_past_the_supported_range_is_never_armed() {
        let room = room(Some(u64::MAX), None);
        assert_eq!((next(&room), armed(&room)), (None, false));
    }

    #[test]
    fn a_timer_past_the_supported_range_is_never_due() {
        let room = room(Some(u64::MAX), None);
        assert_eq!(due(&room, at(i64::from(i32::MAX))), []);
    }

    #[test]
    fn a_zero_second_inactivity_timer_is_due_at_the_last_activity() {
        assert_eq!(due(&room(Some(0), None), at(0)), [pause_at(0)]);
    }

    #[test]
    fn zero_second_timers_pause_and_close_at_the_last_activity() {
        assert_eq!(
            due(&room(Some(0), Some(0)), at(0)),
            [pause_at(0), close_at(0)]
        );
    }
}
