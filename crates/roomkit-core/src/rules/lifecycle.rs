//! A room's lifecycle transitions: which status each one leaves from, and
//! what it writes (RFC §5.1, gap G-045).
//!
//! | Transition | Leaves from | Goes to |
//! |---|---|---|
//! | pause | ACTIVE | PAUSED |
//! | resume | PAUSED | ACTIVE |
//! | reopen | CLOSED | ACTIVE |
//! | close | ACTIVE, PAUSED | CLOSED |
//! | archive | ACTIVE, PAUSED, CLOSED | ARCHIVED |
//!
//! A reopen from any other status is refused: only a CLOSED room is reopened,
//! and ARCHIVED is never left (gap G-074). Any other transition from a
//! status it does not leave from does not apply and changes nothing, as
//! closing a closed room does.

use serde::{Deserialize, Serialize};

use crate::model::{Room, RoomStatus, Timestamp};

/// A change of a room's status (RFC §5.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Transition {
    /// ACTIVE to PAUSED, by the integrator or the inactivity timer.
    Pause,
    /// PAUSED to ACTIVE, by the integrator or a delivered event (gap G-045).
    Resume,
    /// CLOSED to ACTIVE: the integrator reopens an ended conversation.
    Reopen,
    /// ACTIVE or PAUSED to CLOSED, by the integrator or the close timer.
    Close,
    /// Any status but ARCHIVED to ARCHIVED, the terminal state.
    Archive,
}

impl Transition {
    /// The status the transition goes to.
    pub fn target(self) -> RoomStatus {
        match self {
            Self::Pause => RoomStatus::Paused,
            Self::Resume | Self::Reopen => RoomStatus::Active,
            Self::Close => RoomStatus::Closed,
            Self::Archive => RoomStatus::Archived,
        }
    }

    /// Whether the transition is refused on a room in `status`, rather than
    /// leaving it as it is (RFC §5.1, gap G-074): a reopen of a room that is
    /// not CLOSED, since only a CLOSED room is reopened and ARCHIVED is
    /// never left. Any other transition that does not leave from `status`
    /// changes nothing, as closing a closed room does.
    pub fn refuses(self, status: RoomStatus) -> bool {
        self == Self::Reopen && !self.leaves_from(status)
    }

    /// Whether the transition applies to a room in `status`.
    pub fn leaves_from(self, status: RoomStatus) -> bool {
        match (self, status) {
            (Self::Pause, RoomStatus::Active)
            | (Self::Resume, RoomStatus::Paused)
            | (Self::Reopen, RoomStatus::Closed)
            | (Self::Close, RoomStatus::Active | RoomStatus::Paused)
            | (Self::Archive, RoomStatus::Active | RoomStatus::Paused | RoomStatus::Closed) => true,
            (
                Self::Pause | Self::Resume | Self::Reopen | Self::Close | Self::Archive,
                RoomStatus::Active | RoomStatus::Paused | RoomStatus::Closed | RoomStatus::Archived,
            ) => false,
        }
    }
}

/// Applies `transition` to `room` as of `at`, and says whether it applied.
///
/// A pause records `paused_at`, which the close timer counts from; a close
/// records `closed_at`. A resume and a reopen move `timers.last_activity_at`
/// to `at`, which restarts the inactivity timer, and clear the instants of
/// the state they leave (gap G-045). The room's `updated_at` is the caller's
/// to write: a transition applied late takes the instant it fell due, while
/// the room is modified when the write happens.
pub fn apply(room: &mut Room, transition: Transition, at: Timestamp) -> bool {
    if !transition.leaves_from(room.status) {
        return false;
    }
    room.status = transition.target();
    match transition {
        Transition::Pause => room.paused_at = Some(at),
        Transition::Resume => {
            room.paused_at = None;
            room.timers.last_activity_at = Some(at);
        }
        Transition::Reopen => {
            room.closed_at = None;
            room.paused_at = None;
            room.timers.last_activity_at = Some(at);
        }
        Transition::Close => room.closed_at = Some(at),
        Transition::Archive => {}
    }
    true
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::RoomId;

    const STATUSES: [RoomStatus; 4] = [
        RoomStatus::Active,
        RoomStatus::Paused,
        RoomStatus::Closed,
        RoomStatus::Archived,
    ];

    fn leaves_from(transition: Transition) -> Vec<RoomStatus> {
        STATUSES
            .into_iter()
            .filter(|status| transition.leaves_from(*status))
            .collect()
    }

    fn room(status: RoomStatus) -> Room {
        Room {
            status,
            ..Room::new(RoomId::new("room_1").unwrap(), Timestamp::UNIX_EPOCH)
        }
    }

    fn later() -> Timestamp {
        Timestamp::from_unix_millis(60_000).unwrap()
    }

    #[test]
    fn a_pause_leaves_from_active_only() {
        assert_eq!(leaves_from(Transition::Pause), [RoomStatus::Active]);
    }

    #[test]
    fn a_resume_leaves_from_paused_only() {
        assert_eq!(leaves_from(Transition::Resume), [RoomStatus::Paused]);
    }

    #[test]
    fn a_reopen_leaves_from_closed_only() {
        assert_eq!(leaves_from(Transition::Reopen), [RoomStatus::Closed]);
    }

    #[test]
    fn a_close_leaves_from_active_and_paused() {
        assert_eq!(
            leaves_from(Transition::Close),
            [RoomStatus::Active, RoomStatus::Paused]
        );
    }

    #[test]
    fn an_archive_leaves_from_every_status_but_archived() {
        assert_eq!(
            leaves_from(Transition::Archive),
            [RoomStatus::Active, RoomStatus::Paused, RoomStatus::Closed]
        );
    }

    #[test]
    fn only_a_reopen_is_refused_where_it_does_not_leave_from() {
        let refused: Vec<(Transition, RoomStatus)> = [
            Transition::Pause,
            Transition::Resume,
            Transition::Reopen,
            Transition::Close,
            Transition::Archive,
        ]
        .into_iter()
        .flat_map(|transition| STATUSES.map(|status| (transition, status)))
        .filter(|(transition, status)| transition.refuses(*status))
        .collect();
        assert_eq!(
            refused,
            [
                (Transition::Reopen, RoomStatus::Active),
                (Transition::Reopen, RoomStatus::Paused),
                (Transition::Reopen, RoomStatus::Archived),
            ]
        );
    }

    #[test]
    fn archived_is_never_left() {
        let leaving: Vec<Transition> = [
            Transition::Pause,
            Transition::Resume,
            Transition::Reopen,
            Transition::Close,
            Transition::Archive,
        ]
        .into_iter()
        .filter(|transition| transition.leaves_from(RoomStatus::Archived))
        .collect();
        assert_eq!(leaving, []);
    }

    #[test]
    fn a_transition_that_does_not_apply_changes_nothing() {
        let mut closed = room(RoomStatus::Closed);
        let applied = apply(&mut closed, Transition::Pause, later());
        assert_eq!((applied, closed), (false, room(RoomStatus::Closed)));
    }

    #[test]
    fn a_pause_records_its_instant() {
        let mut paused = room(RoomStatus::Active);
        apply(&mut paused, Transition::Pause, later());
        assert_eq!(
            (paused.status, paused.paused_at),
            (RoomStatus::Paused, Some(later()))
        );
    }

    #[test]
    fn a_close_records_its_instant() {
        let mut closed = room(RoomStatus::Paused);
        apply(&mut closed, Transition::Close, later());
        assert_eq!(
            (closed.status, closed.closed_at),
            (RoomStatus::Closed, Some(later()))
        );
    }

    #[test]
    fn a_resume_restarts_the_inactivity_timer() {
        let mut resumed = room(RoomStatus::Paused);
        resumed.paused_at = Some(Timestamp::UNIX_EPOCH);
        apply(&mut resumed, Transition::Resume, later());
        assert_eq!(
            (resumed.paused_at, resumed.timers.last_activity_at),
            (None, Some(later()))
        );
    }

    #[test]
    fn a_reopen_clears_the_close_and_restarts_the_inactivity_timer() {
        let mut reopened = room(RoomStatus::Closed);
        reopened.closed_at = Some(Timestamp::UNIX_EPOCH);
        apply(&mut reopened, Transition::Reopen, later());
        assert_eq!(
            (
                reopened.status,
                reopened.closed_at,
                reopened.timers.last_activity_at
            ),
            (RoomStatus::Active, None, Some(later()))
        );
    }

    #[test]
    fn an_archive_keeps_the_instant_of_the_close() {
        let mut archived = room(RoomStatus::Closed);
        archived.closed_at = Some(Timestamp::UNIX_EPOCH);
        apply(&mut archived, Transition::Archive, later());
        assert_eq!(
            (archived.status, archived.closed_at),
            (RoomStatus::Archived, Some(Timestamp::UNIX_EPOCH))
        );
    }
}
