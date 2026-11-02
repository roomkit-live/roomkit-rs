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

use std::sync::Arc;

use roomkit_core::model::{RoomEvent, RoomId, VerificationRequest};
use roomkit_core::ops::{Inbound, RestoreHeld, Restored};
use roomkit_core::rules::verification::release::{held_window, window_scan};

use crate::context::ExecutionContext;
use crate::delivery_keys::NamedGuard;
use crate::driver;
use crate::error::RoomKitError;
use crate::kit::RoomKit;
use crate::kit::events::room_work;
use crate::store::{ConversationStore, ListEventsOptions};
use crate::tasks::Kind;

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

    /// Runs the room's agents on the last event `restored` holds, named by
    /// its delivery's key, unless that delivery was cancelled while it was
    /// held (RFC §10.1 "Explicit delivery cancellation"), in this process or
    /// another: no agent answers it then. The earlier ones are its context.
    async fn answer_released(
        &self,
        room_id: &RoomId,
        restored: &Restored,
    ) -> Result<(), RoomKitError> {
        let Some(last) = restored.events.last() else {
            return Ok(());
        };
        let named = self.unhold_released(&restored.events);
        let cancelled = named
            .as_ref()
            .is_some_and(|named| named.named().cancelled().is_some());
        if cancelled || restored.cancelled.contains(&last.id) {
            return Ok(());
        }
        let context = ExecutionContext::current();
        let floor = context.turn_depth().unwrap_or(0);
        let rerun = Inbound::rerun(self.config(), room_id.clone(), last.id.clone(), floor);
        if context.lease(room_id).is_some() {
            self.answer_after_the_section(room_id.clone(), rerun, named);
            return Ok(());
        }
        let ran = driver::run_rerun(&self.inner, RELEASE, room_id, rerun, named).await;
        ran.output??;
        Ok(())
    }

    /// Runs `rerun`, the answer of a release made under the room's lock (a
    /// SYNC hook's), once the section holding that lock is over: it takes
    /// the lock after it, so its set takes its place in the room's lane
    /// behind that work's and runs once that work's delivery is over, the
    /// room read as that work left it (RFC §11.7.6). Its caller does not
    /// wait for it, which would wait for itself; what it fails with is
    /// logged.
    fn answer_after_the_section(&self, room_id: RoomId, rerun: Inbound, named: Option<NamedGuard>) {
        let inner = Arc::clone(&self.inner);
        let answer = async move {
            let ran = driver::run_rerun(&inner, RELEASE, &room_id, rerun, named).await;
            let failure = match ran.output {
                Ok(Ok(_)) => return,
                Ok(Err(error)) => RoomKitError::from(error),
                Err(error) => error,
            };
            tracing::warn!(
                target: "roomkit::verification",
                room_id = %room_id,
                error = %failure,
                "the answer of a release made under the room's lock failed"
            );
        };
        // Outside the section's context: its lease on the lock is not this
        // run's.
        let detached = ExecutionContext::default().scope(answer);
        drop(self.inner.tasks.spawn_alone(Kind::Run, detached));
    }

    /// Stops holding the deliveries of `released`, Python's
    /// `_unhold_released`: the last one's, which names its answer, or `None`
    /// when this process holds none; the earlier ones are over.
    fn unhold_released(&self, released: &[RoomEvent]) -> Option<NamedGuard> {
        let keys = &self.inner.delivery_keys;
        let mut named: Vec<Option<NamedGuard>> = released
            .iter()
            .map(|event| keys.unhold(&event.id))
            .collect();
        named.pop().flatten()
    }
}
