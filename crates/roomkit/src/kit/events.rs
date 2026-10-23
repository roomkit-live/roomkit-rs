//! The host's direct changes to stored events (RFC §10.3 "Direct mutation
//! (host-owned)"): the kit's calls for the core's `UpdateEvent` and
//! `DeleteEvent`.

use roomkit_core::model::{EventId, EventUpdates, RoomEvent, RoomId};
use roomkit_core::ops::{DeleteEvent, UpdateEvent};

use super::RoomKit;
use super::scope::Scope;
use crate::driver;
use crate::error::KitError;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// Replaces the fields `updates` names on the room's stored event
    /// `event_id`, each whole, under the room lock, then fires
    /// `ON_EVENT_UPDATED` with the event as it now stands (RFC §10.3). Unlike
    /// an edit sent through the pipeline, it checks no authorship and adds no
    /// `edited` marker: the host authorizes it on its own surface.
    ///
    /// Returns the event as it now stands, or `None` when the room holds no
    /// such event or does not exist, which changes and fires nothing (gap
    /// G-063). An update that changes nothing writes and fires nothing
    /// either.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Mutation`] when the store or the room lock fails.
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
    /// lock, with the replies of its thread when `cascade_replies` is set,
    /// the RFC's default, since nothing else protects them, and the edits
    /// and deletes of what it removes, an edit holding its target's content;
    /// then fires
    /// `ON_EVENT_DELETED` once for each event removed (RFC §10.3). The
    /// room's counters stay: an index is never given again, and the timeline
    /// then holds fewer events than `event_count` counts.
    ///
    /// Returns the ids removed, the target's first; none when the room holds
    /// no such event or does not exist, which changes and fires nothing (gap
    /// G-063).
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Mutation`] when the store or the room lock fails.
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
        Ok(driver::run(&self.kit.inner, "delete_event", operation).await??)
    }
}
