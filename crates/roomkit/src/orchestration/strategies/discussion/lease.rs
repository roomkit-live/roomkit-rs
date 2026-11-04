//! The lease: the one process that gives a room's turns (RFC §19.7.5 rule
//! 16). The part of Python's `_shared.py` that reads no store and no clock:
//! each method takes the process's id and the time now.

use super::queue::SpeakQueueState;

/// How long the lease lasts unless its holder renews it, in seconds.
pub(crate) const LEASE_SECONDS: f64 = 15.0;

/// How long before it ends a holder renews its lease, in seconds.
const RENEW_BEFORE: f64 = LEASE_SECONDS * 2.0 / 3.0;

impl SpeakQueueState {
    /// Whether the process `me` holds the lease at `now`.
    pub(crate) fn holds(&self, me: &str, now: f64) -> bool {
        self.lease_holder.as_deref() == Some(me) && self.lease_expires > now
    }

    /// Whether another process than `me` holds the lease at `now`.
    pub(crate) fn held_elsewhere(&self, me: &str, now: f64) -> bool {
        self.lease_holder
            .as_deref()
            .is_some_and(|holder| holder != me)
            && self.lease_expires > now
    }

    /// Take or renew the lease, unless another process holds it; whether `me`
    /// holds it now. Taking it over from a process whose lease expired ends
    /// that process's turn (rule 6).
    pub(crate) fn take_lease(&mut self, me: &str, now: f64) -> bool {
        if self.held_elsewhere(me, now) {
            return false;
        }
        if self.lease_holder.as_deref() != Some(me) {
            self.speaking = None;
        }
        self.lease_holder = Some(me.to_owned());
        self.lease_expires = now + LEASE_SECONDS;
        true
    }

    /// Whether `me` holds the lease and should renew it at `now`.
    pub(crate) fn renew_due(&self, me: &str, now: f64) -> bool {
        self.lease_holder.as_deref() == Some(me) && self.lease_expires - now < RENEW_BEFORE
    }

    /// Hand the lease to `holder` (the process holding an instruction's text).
    pub(crate) fn grant_lease(&mut self, holder: &str, now: f64) {
        self.lease_holder = Some(holder.to_owned());
        self.lease_expires = now + LEASE_SECONDS;
    }

    /// Give the lease up, when `me` holds it: its turn ends with it.
    pub(crate) fn release_lease(&mut self, me: &str) {
        if self.lease_holder.as_deref() == Some(me) {
            self.lease_holder = None;
            self.lease_expires = 0.0;
            self.speaking = None;
        }
    }

    /// Whether the queue itself differs from `other`, a lease renewal alone
    /// changing nothing: what moves its `version` (rule 16).
    pub(crate) fn queue_differs(&self, other: &Self) -> bool {
        let without_lease = |state: &Self| Self {
            lease_holder: None,
            lease_expires: 0.0,
            version: 0,
            ..state.clone()
        };
        without_lease(self) != without_lease(other)
    }
}

#[cfg(test)]
#[expect(clippy::cast_possible_truncation, reason = "times of a few seconds")]
mod tests {
    use pretty_assertions::assert_eq;
    use roomkit_core::model::ChannelId;

    use super::*;

    /// The time a lease ends, to the millisecond a stored `f64` keeps.
    fn millis(seconds: f64) -> i64 {
        (seconds * 1000.0).round() as i64
    }

    fn held_by(holder: &str, expires: f64) -> SpeakQueueState {
        SpeakQueueState {
            lease_holder: Some(holder.to_owned()),
            lease_expires: expires,
            ..SpeakQueueState::default()
        }
    }

    #[test]
    fn a_lease_is_held_until_it_expires() {
        let state = held_by("a", 10.0);
        assert!(state.holds("a", 9.0));
        assert!(!state.holds("a", 10.0));
        assert!(!state.holds("b", 9.0));
        assert!(state.held_elsewhere("b", 9.0));
        assert!(!state.held_elsewhere("b", 10.0));
        assert!(!state.held_elsewhere("a", 9.0));
        assert!(!SpeakQueueState::default().held_elsewhere("a", 0.0));
    }

    #[test]
    fn another_process_holding_the_lease_keeps_it() {
        let mut state = held_by("a", 10.0);
        assert!(!state.take_lease("b", 9.0));
        assert_eq!(state.lease_holder.as_deref(), Some("a"));
        assert_eq!(millis(state.lease_expires), millis(10.0));
    }

    #[test]
    fn taking_over_an_expired_lease_ends_its_holder_s_turn() {
        let mut state = held_by("a", 10.0);
        state.speaking = Some(ChannelId::new("sre").unwrap());
        assert!(state.take_lease("b", 11.0));
        assert_eq!(state.lease_holder.as_deref(), Some("b"));
        assert_eq!(millis(state.lease_expires), millis(26.0));
        assert_eq!(state.speaking, None);
    }

    #[test]
    fn renewing_its_own_lease_keeps_the_running_turn() {
        let mut state = held_by("a", 10.0);
        let sre = ChannelId::new("sre").unwrap();
        state.speaking = Some(sre.clone());
        assert!(state.take_lease("a", 9.0));
        assert_eq!(millis(state.lease_expires), millis(24.0));
        assert_eq!(state.speaking, Some(sre));
    }

    #[test]
    fn a_renewal_falls_due_past_a_third_of_the_lease() {
        let state = held_by("a", 15.0);
        assert!(!state.renew_due("a", 0.0));
        assert!(!state.renew_due("a", 5.0));
        assert!(state.renew_due("a", 5.5));
        assert!(!state.renew_due("b", 14.0));
    }

    #[test]
    fn a_grant_hands_the_lease_and_a_release_ends_the_turn() {
        let mut state = held_by("a", 10.0);
        let sre = ChannelId::new("sre").unwrap();
        state.speaking = Some(sre.clone());
        state.grant_lease("b", 11.0);
        assert_eq!(state.lease_holder.as_deref(), Some("b"));
        assert_eq!(millis(state.lease_expires), millis(26.0));

        state.release_lease("a");
        assert_eq!(state.lease_holder.as_deref(), Some("b"));
        assert_eq!(state.speaking.as_ref(), Some(&sre));

        state.release_lease("b");
        assert_eq!(state.lease_holder, None);
        assert_eq!(millis(state.lease_expires), millis(0.0));
        assert_eq!(state.speaking, None);
    }

    #[test]
    fn a_renewal_alone_is_no_change_of_the_queue() {
        let before = held_by("a", 10.0);
        let mut after = before.clone();
        after.take_lease("a", 9.0);
        after.version += 1;
        assert!(!after.queue_differs(&before));
        // The turn that runs is the queue's, even though the lease carries it.
        after.speaking = Some(ChannelId::new("sre").unwrap());
        assert!(after.queue_differs(&before));
    }
}
