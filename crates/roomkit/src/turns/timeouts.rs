//! The bounds both sides of a turn keep (RFC §10.6.8), and the beat each
//! side keeps while a turn runs.

use std::time::Duration;

use tokio::time::{Instant, Interval, MissedTickBehavior};

/// The shortest interval or bound a side keeps: a zero would make a timer
/// spin.
const SHORTEST: Duration = Duration::from_millis(1);

/// The bounds both sides of a turn keep (RFC §10.6.8), the RFC's defaults
/// unless a test sets others (`roomkit::testing::set_turn_timeouts`).
///
/// - `accept`: how long the owner waits for an executor to take a turn, 10
///   seconds; the kit's own executor takes it at once, and a turn no
///   executor serves fails `no_executor` at once;
/// - `heartbeat_interval`: how often each side tells the other it is still
///   there while a turn runs, 2 seconds;
/// - `heartbeat_timeout`: how long a side's silence lasts before the other
///   takes it for lost, 10 seconds: the owner fails the turn
///   `executor_lost`, the executor cuts it `owner_lost`. It is counted in
///   intervals of the counting side's own beat, so a stall of that side is
///   never the other's silence, and a turn busy for longer than it, still
///   beating, is never lost;
/// - `cancel`: how long the owner waits for a cancelled turn's end, 5
///   seconds, after which the turn ends failed and its later frames are
///   dropped.
///
/// A value under a millisecond is taken as a millisecond.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TurnTimeouts {
    /// How long the owner waits for an executor to take a turn.
    pub accept: Duration,
    /// How often each side beats while a turn runs.
    pub heartbeat_interval: Duration,
    /// How long a side's silence lasts before it is taken for lost.
    pub heartbeat_timeout: Duration,
    /// How long the owner waits for a cancelled turn's end.
    pub cancel: Duration,
}

impl Default for TurnTimeouts {
    fn default() -> Self {
        Self {
            accept: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(2),
            heartbeat_timeout: Duration::from_secs(10),
            cancel: Duration::from_secs(5),
        }
    }
}

impl TurnTimeouts {
    /// The RFC's defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// The same bounds, the owner waiting `accept` for an executor.
    #[must_use]
    pub fn with_accept(mut self, accept: Duration) -> Self {
        self.accept = accept.max(SHORTEST);
        self
    }

    /// The same bounds, each side beating every `interval`.
    #[must_use]
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval.max(SHORTEST);
        self
    }

    /// The same bounds, a side silent for `timeout` taken for lost.
    #[must_use]
    pub fn with_heartbeat_timeout(mut self, timeout: Duration) -> Self {
        self.heartbeat_timeout = timeout.max(SHORTEST);
        self
    }

    /// The same bounds, the owner waiting `cancel` for a cancelled turn's
    /// end.
    #[must_use]
    pub fn with_cancel(mut self, cancel: Duration) -> Self {
        self.cancel = cancel.max(SHORTEST);
        self
    }

    /// The interval each side beats at, never zero.
    pub(crate) fn interval(&self) -> Duration {
        self.heartbeat_interval.max(SHORTEST)
    }

    /// How many intervals in a row with no message make the other side
    /// lost: counted per interval, so a stall of the counting side counts
    /// once.
    pub(crate) fn silent_intervals(&self) -> u32 {
        intervals(self.heartbeat_timeout, self.interval())
    }

    /// How many silent intervals the owner waits for `accepted`.
    pub(crate) fn accept_intervals(&self) -> u32 {
        intervals(self.accept, self.interval())
    }
}

/// One side's beat while a turn runs (RFC §10.6.8): it ticks every
/// interval, the side sending its heartbeat then, and counts the intervals
/// in a row in which the other side said nothing. Counted per tick, a stall
/// of the counting side's own loop counts once: it is not the other side's
/// silence.
#[derive(Debug)]
pub(crate) struct Beat {
    interval: Interval,
    heard: bool,
    silent: u32,
}

impl Beat {
    /// A beat at `timeouts`' interval, the first tick one interval away;
    /// `heard` when the other side counts as heard at the start.
    pub(crate) fn new(timeouts: &TurnTimeouts, heard: bool) -> Self {
        let period = timeouts.interval();
        let mut interval = tokio::time::interval_at(Instant::now() + period, period);
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        Self {
            interval,
            heard,
            silent: 0,
        }
    }

    /// The other side said something.
    pub(crate) fn heard(&mut self) {
        self.heard = true;
    }

    /// Waits for the next tick: how many intervals in a row the other side
    /// was silent.
    pub(crate) async fn tick(&mut self) -> u32 {
        self.interval.tick().await;
        self.silent = if self.heard {
            0
        } else {
            self.silent.saturating_add(1)
        };
        self.heard = false;
        self.silent
    }
}

/// How many `interval`s `bound` lasts, rounded, at least one.
fn intervals(bound: Duration, interval: Duration) -> u32 {
    let ratio = bound.as_secs_f64() / interval.as_secs_f64();
    // A ratio past u32::MAX is a bound no turn reaches.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a positive ratio, clamped to u32's range first"
    )]
    let rounded = ratio.round().clamp(1.0, f64::from(u32::MAX)) as u32;
    rounded
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_are_the_rfc_s() {
        let timeouts = TurnTimeouts::default();
        assert_eq!(
            (
                timeouts.accept,
                timeouts.heartbeat_interval,
                timeouts.heartbeat_timeout,
                timeouts.cancel,
                timeouts.silent_intervals(),
                timeouts.accept_intervals(),
            ),
            (
                Duration::from_secs(10),
                Duration::from_secs(2),
                Duration::from_secs(10),
                Duration::from_secs(5),
                5,
                5
            )
        );
    }

    #[test]
    fn a_zero_is_taken_as_a_millisecond() {
        let timeouts = TurnTimeouts::new()
            .with_heartbeat_interval(Duration::ZERO)
            .with_heartbeat_timeout(Duration::ZERO);
        assert_eq!(
            (timeouts.interval(), timeouts.silent_intervals()),
            (Duration::from_millis(1), 1)
        );
    }
}
