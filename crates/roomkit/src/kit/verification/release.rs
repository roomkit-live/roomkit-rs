//! The release of held messages once their request completes (RFC
//! §11.7.6): Python's `_release_held`.
//!
//! What the request's sender wrote while it was open, the held window of
//! its room and identity, gets back the visibility it had before it was
//! held, through the store (the core's `RestoreHeld`, which fires
//! `ON_EVENT_UPDATED` on the budget of the work the release is made in),
//! and the room's agents then run on the last one as on any inbound
//! message of its author (the core's `Inbound::rerun`): the earlier ones
//! are its context. Nothing is copied, and nothing becomes an instruction.

use roomkit_core::model::{RoomEvent, RoomId, VerificationRequest};
use roomkit_core::ops::{Inbound, RestoreHeld, Restored};
use roomkit_core::rules::verification::release::{held_window, window_scan};

use crate::context::ExecutionContext;
use crate::driver;
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::kit::events::room_work;
use crate::store::{ConversationStore, ListEventsOptions};

/// The kit's call a release's runs are named by in the logs.
const RELEASE: &str = "release_held";

impl<S: ConversationStore> RoomKit<S> {
    /// Releases what `request`'s sender wrote while it was open, and returns
    /// the events released, oldest first: each restored to the scope it had
    /// before it was held, then the room's agents run on the last one.
    ///
    /// # Errors
    ///
    /// The store's failure, read or write, and what the run of the room's
    /// agents fails with before its decision.
    pub(super) async fn release_held(
        &self,
        request: &VerificationRequest,
    ) -> Result<Vec<RoomEvent>, RoomKitError> {
        let held = self.held_window(request).await?;
        if held.is_empty() {
            return Ok(Vec::new());
        }
        let event_ids = held.into_iter().map(|event| event.id).collect();
        let restored = self.restore_held(&request.room_id, event_ids).await?;
        self.answer_released(&request.room_id, &restored).await?;
        Ok(restored.events)
    }

    /// The held messages `request` releases, oldest first: its sender's, in
    /// its room, received from its window's start on and within the
    /// policy's age and number, never one released before (its mark gone).
    async fn held_window(
        &self,
        request: &VerificationRequest,
    ) -> Result<Vec<RoomEvent>, RoomKitError> {
        let policy = &self.inner.verification.policy;
        let options = ListEventsOptions {
            limit: window_scan(policy.release_max_messages),
            newest_first: true,
            ..ListEventsOptions::default()
        };
        let recent = self
            .store()
            .list_events(&request.room_id, options)
            .await
            .map_err(|error| self.read_failed(error))?;
        let now = self.inner.environment().now();
        Ok(held_window(recent, request, policy, now))
    }

    /// Restores `event_ids` under the room's lock, as the work the current
    /// code runs in makes it, if any: their `ON_EVENT_UPDATED` hooks run on
    /// that work's budget, on one of their own from outside (RFC §9.5.1).
    async fn restore_held(
        &self,
        room_id: &RoomId,
        event_ids: Vec<roomkit_core::model::EventId>,
    ) -> Result<Restored, RoomKitError> {
        let operation = RestoreHeld::new(self.config(), room_id.clone(), event_ids);
        let operation = match room_work(room_id) {
            Some((draw, floor)) => operation.made_in(draw, floor),
            None => operation,
        };
        Ok(driver::run(&self.inner, RELEASE, operation).await??)
    }

    /// Runs the room's agents on the last event `restored` holds, unless its
    /// delivery was cancelled while it was held (RFC §10.1 "Explicit
    /// delivery cancellation"): no agent answers it then. The earlier ones
    /// are its context.
    async fn answer_released(
        &self,
        room_id: &RoomId,
        restored: &Restored,
    ) -> Result<(), RoomKitError> {
        let Some(last) = restored.events.last() else {
            return Ok(());
        };
        if restored.cancelled.contains(&last.id) {
            return Ok(());
        }
        let floor = ExecutionContext::current().turn_depth().unwrap_or(0);
        let rerun = Inbound::rerun(self.config(), room_id.clone(), last.id.clone(), floor);
        let ran = driver::run_inbound(&self.inner, RELEASE, Some(room_id), rerun, false).await;
        ran.output??;
        Ok(())
    }
}
