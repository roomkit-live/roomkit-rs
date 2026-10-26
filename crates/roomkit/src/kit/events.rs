//! The host's direct changes to stored events (RFC §10.3 "Direct mutation
//! (host-owned)"): the kit's calls for the core's `UpdateEvent` and
//! `DeleteEvent`.

use roomkit_core::model::{EventId, EventUpdates, RoomEvent, RoomId};
use roomkit_core::ops::{DeleteEvent, UpdateEvent};

use roomkit_core::rules::chain_depth::Draw;

use super::RoomKit;
use super::scope::Scope;
use crate::context::ExecutionContext;
use crate::driver;
use crate::error::KitError;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Replaces the fields `updates` names on the room's stored event
    /// `event_id`, each whole, under the room lock; once the lock is
    /// released, reads the room and fires `ON_EVENT_UPDATED` with the event
    /// as it now stands and the room read then (RFC §10.3). Unlike
    /// an edit sent through the pipeline, it checks no authorship and adds no
    /// `edited` marker: the host authorizes it on its own surface.
    ///
    /// Returns the event as it now stands, or `None` when the room holds no
    /// such event or does not exist, which changes and fires nothing (gap
    /// G-063). An update that changes nothing writes and fires nothing
    /// either.
    ///
    /// Made from the body of an ASYNC hook a cascade fires once an event's
    /// delivery set has run, an update that changes something draws one of
    /// that cascade's hook calls before it writes; made from other work of
    /// the room, it draws nothing. Its trigger's hook bodies run on the
    /// budget of that work, or of its own from outside (RFC §10.3 "A direct
    /// change from the room's work", gap G-087).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Mutation`] when the store or the room lock fails,
    /// and with [`MutationError::ReentryLoopCap`](crate::MutationError::ReentryLoopCap)
    /// when a hook's body is past its cascade's hook calls, nothing written.
    pub async fn update_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        updates: EventUpdates,
    ) -> Result<Option<RoomEvent>, KitError> {
        self.unscoped()
            .update_event(room_id, event_id, updates)
            .await
    }

    /// Deletes the room's stored event `event_id` for good, under the room
    /// lock, with its replies one level deep (the events whose
    /// `parent_event_id` names it) when `cascade_replies` is set, the RFC's
    /// default, since nothing else protects them, and the edits and deletes
    /// of the target and its replies, an edit holding its target's content.
    /// Once the lock is released, it reads the room and fires
    /// `ON_EVENT_DELETED` once for each event removed, in the order of the
    /// ids it returns, with the room read then (RFC §10.3). The room's
    /// counters stay: an index is never given again, and the timeline then
    /// holds fewer events than `event_count` counts.
    ///
    /// Returns the ids removed: the target's, then its replies' by index,
    /// then the changes' by index, an event both a reply and a change among
    /// the replies; none when the room holds no such event or does not
    /// exist, which changes and fires nothing (gap G-063).
    ///
    /// Made from an ASYNC hook's body, a delete of an event the room holds
    /// draws one hook call before it removes anything, as an update does
    /// (gap G-087).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Mutation`] when the store or the room lock fails,
    /// and with [`MutationError::ReentryLoopCap`](crate::MutationError::ReentryLoopCap)
    /// when a hook's body is past its cascade's hook calls, nothing removed.
    pub async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        cascade_replies: bool,
    ) -> Result<Vec<EventId>, KitError> {
        self.unscoped()
            .delete_event(room_id, event_id, cascade_replies)
            .await
    }
}

impl<S: ConversationStore> Scope<'_, S> {
    /// Updates the stored event in a room the scope reaches; another room
    /// holds no event of the scope's.
    pub(super) async fn update_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        updates: EventUpdates,
    ) -> Result<Option<RoomEvent>, KitError> {
        let operation = UpdateEvent::new(
            self.kit.config(),
            room_id.clone(),
            event_id.clone(),
            updates,
            self.organization_id(),
        );
        let operation = match room_work(room_id) {
            Some((draw, floor)) => operation.made_in(draw, floor),
            None => operation,
        };
        Ok(driver::run(&self.kit.inner, "update_event", operation).await??)
    }

    /// Deletes the stored event from a room the scope reaches; another room
    /// holds no event of the scope's.
    pub(super) async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        cascade_replies: bool,
    ) -> Result<Vec<EventId>, KitError> {
        let operation = DeleteEvent::new(
            self.kit.config(),
            room_id.clone(),
            event_id.clone(),
            cascade_replies,
            self.organization_id(),
        );
        let operation = match room_work(room_id) {
            Some((draw, floor)) => operation.made_in(draw, floor),
            None => operation,
        };
        Ok(driver::run(&self.kit.inner, "delete_event", operation).await??)
    }
}

/// The work of `room_id` a direct change made by the current task is made
/// in, while it runs ([`ExecutionContext::room_work`]): what it draws on
/// its cascade's budget, a hook call from a hook's body only, and the least
/// depth of its triggers' calls (gap G-087); `None` outside the room's work.
/// A change of the room's lifecycle runs its triggers in it so.
pub(super) fn room_work(room_id: &RoomId) -> Option<(Draw, u32)> {
    ExecutionContext::current().room_work(room_id)
}
