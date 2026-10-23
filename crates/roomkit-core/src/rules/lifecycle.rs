//! A room's lifecycle transitions: which status each one leaves from, which
//! it has nothing to do in, which it refuses, and what it writes (RFC §5.1,
//! gaps G-045, G-074).
//!
//! | Transition | Leaves from | Goes to | Done already in | Refuses |
//! |---|---|---|---|---|
//! | pause | ACTIVE | PAUSED | PAUSED | CLOSED, ARCHIVED |
//! | resume | PAUSED | ACTIVE | ACTIVE | CLOSED, ARCHIVED |
//! | reopen | CLOSED | ACTIVE | none | ACTIVE, PAUSED, ARCHIVED |
//! | close | ACTIVE, PAUSED | CLOSED | CLOSED | ARCHIVED |
//! | archive | ACTIVE, PAUSED, CLOSED | ARCHIVED | ARCHIVED | none |
//!
//! A transition done already changes nothing; one called on a status it
//! refuses fails. A reopen is never done already: only a CLOSED room is
//! reopened. ARCHIVED is never left.

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

    /// Whether the transition is refused on a room in `status` (RFC §5.1,
    /// gap G-074): it neither leaves from the status nor is done in it
    /// already. A pause of a CLOSED room, for instance, would answer with a
    /// room that is not paused, and a reopen of an ARCHIVED one would leave
    /// it ARCHIVED, a state never left.
    pub fn refuses(self, status: RoomStatus) -> bool {
        !self.leaves_from(status) && !self.is_done(status)
    }

    /// Whether a room in `status` is where the transition takes it already,
    /// so that it has nothing to do: a pause of a PAUSED room, a resume of
    /// an ACTIVE one, a close of a CLOSED one, an archive of an ARCHIVED one
    /// (gap G-074). A reopen never is: an ACTIVE room was not reopened, it
    /// was never closed.
    pub fn is_done(self, status: RoomStatus) -> bool {
        match (self, status) {
            (Self::Pause, RoomStatus::Paused)
            | (Self::Resume, RoomStatus::Active)
            | (Self::Close, RoomStatus::Closed)
            | (Self::Archive, RoomStatus::Archived) => true,
            (
                Self::Pause | Self::Resume | Self::Reopen | Self::Close | Self::Archive,
                RoomStatus::Active | RoomStatus::Paused | RoomStatus::Closed | RoomStatus::Archived,
            ) => false,
        }
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

    const TRANSITIONS: [Transition; 5] = [
        Transition::Pause,
        Transition::Resume,
        Transition::Reopen,
        Transition::Close,
        Transition::Archive,
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

    /// The pairs a transition refuses, every one it neither leaves from nor
    /// is done in; a reopen is never done (gap G-074).
    #[test]
    fn a_transition_refuses_what_it_neither_leaves_from_nor_is_done_in() {
        let refused: Vec<(Transition, RoomStatus)> = TRANSITIONS
            .into_iter()
            .flat_map(|transition| STATUSES.map(|status| (transition, status)))
            .filter(|(transition, status)| transition.refuses(*status))
            .collect();
        assert_eq!(
            refused,
            [
                (Transition::Pause, RoomStatus::Closed),
                (Transition::Pause, RoomStatus::Archived),
                (Transition::Resume, RoomStatus::Closed),
                (Transition::Resume, RoomStatus::Archived),
                (Transition::Reopen, RoomStatus::Active),
                (Transition::Reopen, RoomStatus::Paused),
                (Transition::Reopen, RoomStatus::Archived),
                (Transition::Close, RoomStatus::Archived),
            ]
        );
    }

    #[test]
    fn archived_is_never_left() {
        let leaving: Vec<Transition> = TRANSITIONS
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
