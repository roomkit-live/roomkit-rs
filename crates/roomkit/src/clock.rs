//! The clock a kit's timed delivery paths read, outside the public API: a
//! channel's circuit breaker measures its cooldown on it (RFC §13.1), and a
//! binding's retry policy waits out its delays on it (RFC §13.2), as
//! roomkit's private `roomkit._clock` seam has them read theirs.
//!
//! The default is tokio's clock, so nothing changes for an integrator; a
//! test sets a clock of its own on a kit with
//! [`set_clock`](crate::testing::set_clock) to drive those paths without
//! waiting real time. The kit's other durations stay on tokio's clock.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use tokio::time::Instant;

/// A wait on a clock, which ends once its duration passed on it.
pub type Sleep = Pin<Box<dyn Future<Output = ()> + Send>>;

/// What a clock gives the timed paths: a reading and a wait.
pub trait Clock: fmt::Debug + Send + Sync {
    /// The instant now, on a clock that never goes back: Python's
    /// `_clock.monotonic()`.
    fn now(&self) -> Instant;

    /// A wait that ends once `duration` passed on this clock: Python's
    /// `_clock.sleep()`. Its due time is read when it is made.
    fn sleep(&self, duration: Duration) -> Sleep;
}

/// The default: tokio's own clock, which a paused runtime's test moves.
#[derive(Debug, Default)]
pub(crate) struct TokioClock;

impl Clock for TokioClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        Box::pin(tokio::time::sleep(duration))
    }
}
