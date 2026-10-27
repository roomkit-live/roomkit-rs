//! Applying room timers to the rooms nobody writes to (RFC §5.1, gap
//! G-045).
//!
//! Every locked section of a room applies its due timer transitions before
//! it decides, so a room that receives a message is always exact. A silent
//! room transitions when the kit checks its timers: when the host calls
//! [`RoomKit::check_all_timers`] or [`RoomKit::check_room_timers`], or on
//! the period of a background
//! [`TimerSweeper`] started by [`RoomKit::run_timers`]. A transition the
//! sweeper applies late still takes the instant it fell due.

use std::sync::Arc;
use std::time::Duration;

use roomkit_core::model::{Room, RoomId};
use roomkit_core::ops::{CheckTimers, Environment};
use roomkit_core::rules::timers;
use tokio::task::{AbortHandle, JoinHandle};
use tokio::time::MissedTickBehavior;

use super::RoomKit;
use super::options::{CheckAllTimersOptions, CheckRoomTimersOptions};
use crate::driver;
use crate::error::{KitError, with_causes};
use crate::store::{ConversationStore, RoomFilter};

/// A background task checking the rooms' timers on a period, started by
/// [`RoomKit::run_timers`]. Dropping it stops the task and ends its sweep
/// under way, and so does the kit's [`shutdown`](RoomKit::shutdown).
#[derive(Debug)]
#[must_use = "dropping the sweeper stops it"]
pub struct TimerSweeper {
    task: JoinHandle<()>,
}

impl TimerSweeper {
    /// Stops the sweeper. Its sweep under way is abandoned at its next wait,
    /// with the room it checks: the room lock it holds or waits for is let
    /// go, the rooms it did not reach yet are left to the next check, and a
    /// transition it wrote but did not report yet goes unreported.
    pub fn stop(self) {
        drop(self);
    }
}

impl Drop for TimerSweeper {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Aborts a task when dropped: a sweep whose sweeper's loop is dropped
/// while it waits for the sweep would otherwise run on, detached.
struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// Applies the timer transitions that fell due to a room nobody writes
    /// to (RFC §5.1): an ACTIVE room pauses once its inactivity bound passed
    /// since its last activity, or since its creation before any; a PAUSED
    /// room closes once its close bound passed since it paused. Each
    /// transition takes the instant it fell due, however late the check.
    /// Returns the room as it now stands, its due transitions applied.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Lifecycle`] when the room does not exist or is
    /// not of the organization `options` names, nothing written.
    pub async fn check_room_timers(
        &self,
        room_id: &RoomId,
        options: CheckRoomTimersOptions,
    ) -> Result<Room, KitError> {
        let check = CheckTimers::new(self.config(), room_id.clone(), options.organization_id);
        let checked = driver::run(&self.inner, "check_room_timers", check).await??;
        Ok(checked.room)
    }

    /// Applies the timer transitions that fell due to every room with a
    /// timer armed, of the organization `options` name or of any when they
    /// name none, and returns the rooms they changed, as they now stand.
    ///
    /// Each room is checked under its own lock. A room whose check fails is
    /// logged and skipped; the others are still checked.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Store`] when the store cannot list the rooms,
    /// and [`KitError::ShutDown`] once the kit is shut down, which stops a
    /// check under way before its next room.
    pub async fn check_all_timers(
        &self,
        options: CheckAllTimersOptions,
    ) -> Result<Vec<Room>, KitError> {
        self.inner.open()?;
        let filter = RoomFilter {
            timers_armed: true,
            organization_id: options.organization_id.clone(),
            ..RoomFilter::default()
        };
        let rooms = self.inner.store.list_rooms(&filter).await?;
        // The sweep reads the environment once: each room's check runs on
        // the clock its due transitions were read with.
        let environment = self.inner.environment();
        let now = environment.now();
        let mut changed = Vec::new();
        // A room read off the lock with nothing due needs no lock; one with a
        // transition due is checked again under it.
        for room in rooms {
            if timers::due(&room, now).is_empty() {
                continue;
            }
            let organization_id = options.organization_id.clone();
            let checked = self.check_listed(&room, &environment, organization_id);
            if let Some(room) = checked.await? {
                changed.push(room);
            }
        }
        Ok(changed)
    }

    /// Checks the timers of `room`, listed with a transition due, under its
    /// lock on `environment`'s clock: the room as it now stands when a
    /// transition was applied. A check that fails is logged, the sweep going
    /// on.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::ShutDown`] once the kit is shut down.
    async fn check_listed(
        &self,
        room: &Room,
        environment: &Arc<dyn Environment + Send + Sync>,
        organization_id: Option<String>,
    ) -> Result<Option<Room>, KitError> {
        let check = CheckTimers::new(self.config(), room.id.clone(), organization_id);
        let environment = Arc::clone(environment);
        match driver::run_on(&self.inner, environment, "check_all_timers", check).await {
            Ok(Ok(checked)) if !checked.transitions.is_empty() => return Ok(Some(checked.room)),
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
                error = %with_causes(&error),
                "checking a room's timers broke the driver"
            ),
        }
        Ok(None)
    }

    /// Starts a background task that checks the rooms' timers every
    /// `period`, the first time at once, until the sweeper is dropped or the
    /// kit shut down. A silent room's transition arrives up to one period
    /// late, with the instant it fell due.
    ///
    /// Each sweep runs as background work the kit tracks: [`idle`](Self::idle)
    /// waits for one under way, and [`shutdown`](Self::shutdown) ends it at
    /// its next wait, a room's lock it holds let go, and waits for it. The
    /// sweeper waiting for its next tick keeps the kit idle.
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
                if !kit.sweep().await {
                    return;
                }
            }
        });
        Ok(TimerSweeper { task })
    }

    /// Runs one sweep as a task the kit tracks, and waits for it; `false`
    /// once the kit is shut down, which ends the sweeper. The sweep belongs
    /// to the sweeper: a sweeper stopped while it waits ends it too.
    async fn sweep(&self) -> bool {
        let kit = self.clone();
        let Some(sweep) = self
            .inner
            .tasks
            .spawn(async move { kit.check_all_timers(CheckAllTimersOptions::default()).await })
        else {
            return false;
        };
        let _owned = AbortOnDrop(sweep.abort_handle());
        match sweep.await {
            Ok(Ok(_)) => true,
            Ok(Err(KitError::ShutDown)) => false,
            Ok(Err(error)) => {
                tracing::warn!(
                    target: "roomkit::timers",
                    error = %with_causes(&error),
                    "a timer sweep failed"
                );
                true
            }
            // The shutdown ended the sweep under way.
            Err(ended) if ended.is_cancelled() => false,
            Err(_panicked) => {
                tracing::error!(target: "roomkit::timers", "a timer sweep panicked");
                true
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use roomkit_core::model::{RoomId, RoomStatus, RoomTimers};
    use roomkit_core::ops::FrameworkEvent;

    use super::*;
    use crate::{CreateRoomOptions, RoomKitOptions};

    /// How long a wait that should end may take before the test fails.
    const BOUND: Duration = Duration::from_secs(5);

    /// How long a wait that should not end is given before the test takes
    /// it that it does not.
    const BRIEF: Duration = Duration::from_millis(20);

    fn room_id() -> RoomId {
        RoomId::new("room_1").unwrap()
    }

    /// A kit whose room's inactivity timer fell due at its creation.
    async fn timed_kit() -> RoomKit {
        let kit = RoomKit::new(RoomKitOptions::default());
        let options = CreateRoomOptions {
            room_id: Some(room_id()),
            timers: Some(RoomTimers {
                inactive_after_seconds: Some(0),
                ..RoomTimers::default()
            }),
            ..Default::default()
        };
        kit.create_room(options).await.unwrap();
        kit
    }

    /// A sweeper of `kit` whose first sweep is under way: it waits for the
    /// room's lock, which the test holds as a lifecycle change would.
    async fn sweep_under_way(kit: &RoomKit) -> TimerSweeper {
        let sweeper = kit.run_timers(Duration::from_secs(3600)).unwrap();
        // Nothing tells the sweep apart from the outside: the kit's tasks
        // are counted.
        let deadline = tokio::time::Instant::now() + BOUND;
        while kit.inner.tasks.running() == 0 {
            assert!(tokio::time::Instant::now() < deadline, "no sweep started");
            tokio::task::yield_now().await;
        }
        sweeper
    }

    /// `idle` waits for a sweep under way, which then applies the pause.
    #[tokio::test]
    async fn idle_waits_for_a_sweep_under_way() {
        let kit = timed_kit().await;
        let held = kit.inner.locks.lock(&room_id()).await;
        let _sweeper = sweep_under_way(&kit).await;
        let busy = tokio::time::timeout(BRIEF, kit.idle()).await.is_err();
        drop(held);
        tokio::time::timeout(BOUND, kit.idle()).await.unwrap();
        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!((busy, room.status), (true, RoomStatus::Paused));
    }

    /// The shutdown ends a sweep under way and waits for it: once it
    /// returned, the sweep writes nothing, the lock let go or not.
    #[tokio::test]
    async fn shutdown_ends_a_sweep_under_way() {
        let kit = timed_kit().await;
        let held = kit.inner.locks.lock(&room_id()).await;
        let mut sweeper = sweep_under_way(&kit).await;
        tokio::time::timeout(BOUND, kit.shutdown()).await.unwrap();
        let stopped = tokio::time::timeout(BOUND, &mut sweeper.task).await;
        drop(held);
        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!(
            (stopped.is_ok_and(|ended| ended.is_ok()), room.status),
            (true, RoomStatus::Active)
        );
    }

    /// Stopping the sweeper ends its sweep under way: the kit is idle at
    /// once, and the sweep writes nothing once the lock it waits for is let
    /// go.
    #[tokio::test]
    async fn stopping_the_sweeper_ends_its_sweep_under_way() {
        let kit = timed_kit().await;
        let held = kit.inner.locks.lock(&room_id()).await;
        sweep_under_way(&kit).await.stop();
        let idle = tokio::time::timeout(BOUND, kit.idle()).await;
        drop(held);
        kit.idle().await;
        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!((idle.is_ok(), room.status), (true, RoomStatus::Active));
    }

    /// A sweeper stops when its kit shuts down, rather than at its next
    /// tick an hour later.
    #[tokio::test]
    async fn a_timer_sweeper_stops_when_its_kit_shuts_down() {
        let kit = timed_kit().await;
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
