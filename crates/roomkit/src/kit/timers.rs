//! Applying room timers to the rooms nobody writes to (RFC §5.1, gap
//! G-045).
//!
//! Every locked section of a room applies its due timer transitions before
//! it decides, so a room that receives a message is always exact. A silent
//! room transitions when the host checks its timers, calling
//! [`RoomKit::check_all_timers`] or [`RoomKit::check_room_timers`] on the
//! period it chooses, as with the Python implementation. A transition
//! applied late still takes the instant it fell due.

use std::sync::Arc;

use roomkit_core::model::{Room, RoomId, RoomStatus};
use roomkit_core::ops::{CheckTimers, Environment};
use roomkit_core::rules::timers;

use super::RoomKit;
use super::options::{CheckAllTimersOptions, CheckRoomTimersOptions};
use crate::driver;
use crate::error::{RoomKitError, with_causes};
use crate::store::rooms::rooms_read_by;
use crate::store::{ConversationStore, RoomFilter};

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
    /// Returns [`RoomKitError::RoomNotFound`] when the room does not exist or
    /// is not of the organization `options` names, nothing written.
    pub async fn check_room_timers(
        &self,
        room_id: &RoomId,
        options: CheckRoomTimersOptions,
    ) -> Result<Room, RoomKitError> {
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
    /// Returns [`RoomKitError::Store`] when the store cannot list the rooms,
    /// and [`RoomKitError::KitClosed`] once the kit is shut down, which stops a
    /// check under way before its next room.
    pub async fn check_all_timers(
        &self,
        options: CheckAllTimersOptions,
    ) -> Result<Vec<Room>, RoomKitError> {
        self.inner.open()?;
        // Every ACTIVE and PAUSED room, read a page at a time before any is
        // checked, as Python's sweep reads them (RMK-1009).
        let filter = RoomFilter {
            statuses: Some(vec![RoomStatus::Active, RoomStatus::Paused]),
            organization_id: options.organization_id.clone(),
        };
        let rooms = rooms_read_by(&*self.inner.store, &filter).await?;
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
    /// Returns [`RoomKitError::KitClosed`] once the kit is shut down.
    async fn check_listed(
        &self,
        room: &Room,
        environment: &Arc<dyn Environment + Send + Sync>,
        organization_id: Option<String>,
    ) -> Result<Option<Room>, RoomKitError> {
        let check = CheckTimers::new(self.config(), room.id.clone(), organization_id);
        let environment = Arc::clone(environment);
        match driver::run_on(&self.inner, environment, "check_all_timers", check).await {
            Ok(Ok(checked)) if !checked.transitions.is_empty() => return Ok(Some(checked.room)),
            Ok(Ok(_)) => {}
            Err(RoomKitError::KitClosed) => return Err(RoomKitError::KitClosed),
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
}
