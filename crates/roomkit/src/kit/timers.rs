//! Applying room timers to the rooms nobody writes to (RFC §5.1, gap
//! G-045).
//!
//! Every locked section of a room applies its due timer transitions before
//! it decides, so a room that receives a message is always exact. A silent
//! room transitions when the kit checks its timers: when the host calls
//! [`RoomKit::check_timers`], or on the period of a background
//! [`TimerSweeper`] started by [`RoomKit::run_timers`]. A transition the
//! sweeper applies late still takes the instant it fell due.

use std::time::Duration;

use roomkit_core::model::Room;
use roomkit_core::ops::CheckTimers;
use roomkit_core::rules::timers;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

use super::RoomKit;
use crate::driver;
use crate::error::KitError;
use crate::store::{ConversationStore, RoomFilter};

/// A background task checking the rooms' timers on a period, started by
/// [`RoomKit::run_timers`]. Dropping it stops the task, and so does the
/// kit's [`shutdown`](RoomKit::shutdown).
#[derive(Debug)]
#[must_use = "dropping the sweeper stops it"]
pub struct TimerSweeper {
    task: JoinHandle<()>,
}

impl TimerSweeper {
    /// Stops the sweeper. A check under way is abandoned at its next wait:
    /// the room lock it holds is let go, and a transition it wrote but did
    /// not report yet goes unreported.
    pub fn stop(self) {
        drop(self);
    }
}

impl Drop for TimerSweeper {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// Applies the timer transitions that fell due to every room with a
    /// timer armed, and returns the rooms they changed, as they now stand.
    ///
    /// Each room is checked under its own lock. A room whose check fails is
    /// logged and skipped; the others are still checked.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Store`] when the store cannot list the rooms,
    /// and [`KitError::ShutDown`] once the kit is shut down, which stops a
    /// check under way before its next room.
    pub async fn check_timers(&self) -> Result<Vec<Room>, KitError> {
        self.inner.open()?;
        let filter = RoomFilter {
            timers_armed: true,
            ..RoomFilter::default()
        };
        let rooms = self.inner.store.list_rooms(&filter).await?;
        let now = self.inner.environment().now();
        let mut changed = Vec::new();
        // A room read off the lock with nothing due needs no lock; one with a
        // transition due is checked again under it.
        for room in rooms {
            if timers::due(&room, now).is_empty() {
                continue;
            }
            let check = CheckTimers::new(self.config(), room.id.clone());
            match driver::run(&self.inner, "check_timers", check).await {
                Ok(Ok(checked)) if !checked.transitions.is_empty() => changed.push(checked.room),
                Ok(Ok(_)) => {}
                Err(KitError::ShutDown) => return Err(KitError::ShutDown),
                Ok(Err(error)) => tracing::warn!(
                    target: "roomkit::timers",
                    room_id = %room.id,
                    %error,
                    "checking a room's timers failed"
                ),
                Err(error) => tracing::error!(
                    target: "roomkit::timers",
                    room_id = %room.id,
                    %error,
                    "checking a room's timers broke the driver"
                ),
            }
        }
        Ok(changed)
    }

    /// Starts a background task that checks the rooms' timers every
    /// `period`, the first time at once, until the sweeper is dropped or the
    /// kit shut down. A silent room's transition arrives up to one period
    /// late, with the instant it fell due.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::ZeroPeriod`] for a zero `period`, and
    /// [`KitError::NoRuntime`] outside a tokio runtime.
    pub fn run_timers(&self, period: Duration) -> Result<TimerSweeper, KitError> {
        if period.is_zero() {
            return Err(KitError::ZeroPeriod);
        }
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| KitError::NoRuntime)?;
        let kit = self.clone();
        let task = runtime.spawn(async move {
            let mut ticks = tokio::time::interval(period);
            ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = kit.inner.tasks.shutting_down() => return,
                    _ = ticks.tick() => {}
                }
                match kit.check_timers().await {
                    Ok(_) => {}
                    Err(KitError::ShutDown) => return,
                    Err(error) => {
                        tracing::warn!(target: "roomkit::timers", %error, "a timer sweep failed");
                    }
                }
            }
        });
        Ok(TimerSweeper { task })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use roomkit_core::model::RoomId;
    use roomkit_core::ops::FrameworkEvent;

    use super::*;

    /// How long a wait that should end may take before the test fails.
    const BOUND: Duration = Duration::from_secs(5);

    /// A sweeper stops when its kit shuts down, rather than at its next
    /// tick an hour later.
    #[tokio::test]
    async fn a_timer_sweeper_stops_when_its_kit_shuts_down() {
        let kit = RoomKit::new();
        let room_id = RoomId::new("room_1").unwrap();
        kit.create_room_with(&room_id, |room| {
            room.timers.inactive_after_seconds = Some(0);
        })
        .await
        .unwrap();
        let mut events = kit.subscribe();
        let mut sweeper = kit.run_timers(Duration::from_secs(3600)).unwrap();
        // The first check pauses the room; the sweeper then waits for its
        // next tick.
        let paused = tokio::time::timeout(BOUND, async {
            while !matches!(events.recv().await, Ok(FrameworkEvent::RoomPaused { .. })) {}
        })
        .await;
        kit.shutdown().await;
        let stopped = tokio::time::timeout(BOUND, &mut sweeper.task).await;
        assert_eq!(
            (paused.is_ok(), stopped.is_ok_and(|ended| ended.is_ok())),
            (true, true)
        );
    }
}
