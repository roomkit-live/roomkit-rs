//! The host's direct changes to stored events (RFC §10.3 "Direct mutation
//! (host-owned)"): the kit's calls for the core's `UpdateEvent` and
//! `DeleteEvent`.

use roomkit_core::model::{EventId, EventUpdates, RoomEvent, RoomId};
use roomkit_core::ops::{DeleteEvent, UpdateEvent};

use super::RoomKit;
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
    /// such event, which changes and fires nothing. An update that changes
    /// nothing writes and fires nothing either.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Mutation`] when the room does not exist, or when
    /// the store or the room lock fails.
    pub async fn update_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        updates: EventUpdates,
    ) -> Result<Option<RoomEvent>, KitError> {
        let operation = UpdateEvent::new(self.config(), room_id.clone(), event_id.clone(), updates);
        Ok(driver::run(&self.inner, operation).await??)
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
    /// no such event, which changes and fires nothing.
    ///
    /// # Errors
    ///
    /// Returns [`KitError::Mutation`] when the room does not exist, or when
    /// the store or the room lock fails.
    pub async fn delete_event(
        &self,
        room_id: &RoomId,
        event_id: &EventId,
        cascade_replies: bool,
    ) -> Result<Vec<EventId>, KitError> {
        let operation = DeleteEvent::new(
            self.config(),
            room_id.clone(),
            event_id.clone(),
            cascade_replies,
        );
        Ok(driver::run(&self.inner, operation).await??)
    }
}
