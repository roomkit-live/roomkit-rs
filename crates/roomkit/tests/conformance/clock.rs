//! The virtual clock (roomkit-specs `conformance/README.md`, Setup
//! `virtual_clock` and `clock.advance`): a scenario that sets it runs its
//! kit's circuit breakers and retry delays on a clock only `clock.advance`
//! moves, through the kit's clock seam (`roomkit::testing::set_clock`), as
//! roomkit's runner installs its own on `roomkit._clock`. The runner's own
//! bounds (a call's, a gate's, the idle wait's) stay on tokio's clock.
//!
//! roomkit-rs provides no media feature, so the clock reaches only the
//! core's two durations here, and only the run's own kit: the kits of
//! `setup.processes` stay on tokio's clock, where roomkit's installed clock
//! is every kit's of the process (no scenario combines the two yet).
//!
//! An advance ends each wait due by its target in the order they fall due,
//! each at its due time, and lets the work it resumes run before the next
//! one ends; on several workers the runner cannot see when that work waits
//! again, as roomkit's reads its event loop's ready queue, so a scenario
//! orders what follows a wait with a step that waits for it (`waiters`, a
//! gate).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use roomkit::testing::{Clock, Sleep};
use serde_json::Value;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::calls::Called;
use crate::run::{Run, lock};

/// How long `waiters` waits for the waits it names (README,
/// `clock.advance`).
const WAITERS_BOUND: Duration = Duration::from_secs(5);

/// How many times an advance yields to the work a wait it ended resumed.
const RESUMED_ROUNDS: usize = 100;

/// A monotonic clock that moves only when advanced. It starts at tokio's
/// reading; each wait on it is a timer with its due time.
#[derive(Debug)]
pub(crate) struct VirtualClock {
    start: Instant,
    state: Mutex<State>,
}

#[derive(Debug, Default)]
struct State {
    /// How far the clock moved since its start.
    moved: Duration,
    /// The order the next timer is made in, which breaks a tie of due times.
    made: u64,
    timers: Vec<Timer>,
}

#[derive(Debug)]
struct Timer {
    due: Duration,
    order: u64,
    wake: oneshot::Sender<()>,
}

impl VirtualClock {
    pub(crate) fn new() -> Self {
        Self {
            start: Instant::now(),
            state: Mutex::default(),
        }
    }

    /// How many waits on the clock have not ended: a wait whose future was
    /// dropped ended.
    fn pending(&self) -> usize {
        lock(&self.state)
            .timers
            .iter()
            .filter(|timer| !timer.wake.is_closed())
            .count()
    }

    /// Moves the clock `by` forward, ending every wait due by then, in the
    /// order they fall due.
    async fn advance(&self, by: Duration) {
        let target = lock(&self.state).moved + by;
        while let Some(timer) = self.next_due(target) {
            if timer.wake.send(()).is_ok() {
                resumed().await;
            }
        }
        lock(&self.state).moved = target;
        resumed().await;
    }

    /// The earliest timer due by `target`, taken off the clock, the clock
    /// set to its due time.
    fn next_due(&self, target: Duration) -> Option<Timer> {
        let mut state = lock(&self.state);
        let position = state
            .timers
            .iter()
            .enumerate()
            .filter(|(_, timer)| timer.due <= target)
            .min_by_key(|(_, timer)| (timer.due, timer.order))
            .map(|(position, _)| position)?;
        let timer = state.timers.swap_remove(position);
        state.moved = state.moved.max(timer.due);
        Some(timer)
    }
}

impl Clock for VirtualClock {
    fn now(&self) -> Instant {
        self.start + lock(&self.state).moved
    }

    fn sleep(&self, duration: Duration) -> Sleep {
        if duration.is_zero() {
            return Box::pin(tokio::task::yield_now());
        }
        let (wake, woken) = oneshot::channel();
        {
            let mut state = lock(&self.state);
            let due = state.moved + duration;
            let order = state.made;
            state.made += 1;
            state.timers.push(Timer { due, order, wake });
        }
        Box::pin(async move {
            // A clock dropped with the run ends no wait: its work stays
            // where it waits, as it does on roomkit's.
            if woken.await.is_err() {
                std::future::pending::<()>().await;
            }
        })
    }
}

/// Yields to the work a wait that ended resumed, so that it runs to its
/// next wait before the clock moves on.
async fn resumed() {
    for _ in 0..RESUMED_ROUNDS {
        tokio::task::yield_now().await;
    }
}

/// `clock.advance` (README, "Calls"): with `waiters`, first waits until that
/// many waits are pending on the clock; then moves it `ms` forward; null.
pub(crate) async fn advance(run: &Run, spec: &Value) -> Called {
    let Some(clock) = run.clock.as_ref() else {
        run.problem("clock.advance needs setup.virtual_clock".to_owned());
        return Ok(Value::Null);
    };
    let waiters = spec["waiters"]
        .as_u64()
        .and_then(|waiters| usize::try_from(waiters).ok())
        .unwrap_or(0);
    let pended = tokio::time::timeout(WAITERS_BOUND, async {
        while clock.pending() < waiters {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    if pended.is_err() {
        run.problem(format!(
            "{waiters} waits never pended on the clock within {WAITERS_BOUND:?}"
        ));
        return Ok(Value::Null);
    }
    let ms = spec["ms"].as_u64().unwrap_or_default();
    clock.advance(Duration::from_millis(ms)).await;
    Ok(Value::Null)
}

/// The clock of a scenario that sets `setup.virtual_clock`, set on `kit`;
/// `None` for one that does not.
pub(crate) fn of(setup: &Value, kit: &crate::Kit) -> Option<Arc<VirtualClock>> {
    if setup["virtual_clock"] != true {
        return None;
    }
    let clock = Arc::new(VirtualClock::new());
    let read: Arc<dyn Clock> = Arc::<VirtualClock>::clone(&clock);
    roomkit::testing::set_clock(kit, read);
    Some(clock)
}
