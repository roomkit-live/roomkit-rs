//! A room's read markers (RFC §14.1): each channel's high-water mark on the
//! room's events, which the kit marks and lists for the organization a
//! call names (RFC §17.2), as the Python implementation's `mark_read`,
//! `mark_all_read` and `list_read_markers` do.

use std::collections::BTreeMap;

use roomkit_core::model::{ChannelId, EventId, RoomId};

use super::RoomKit;
use crate::error::RoomKitError;
use crate::store::ConversationStore;

/// What [`RoomKit::mark_read`] takes beside the room, the channel and the
/// event: Python's `mark_read(room_id, channel_id, event_id, *,
/// organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MarkReadOptions {
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2). The room is read with it first, and a room of another
    /// organization is not found, nothing marked; `None` reads no room.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::mark_all_read`] takes beside the room and the channel:
/// Python's `mark_all_read(room_id, channel_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MarkAllReadOptions {
    /// Python `organization_id=None`, as [`MarkReadOptions::organization_id`].
    pub organization_id: Option<String>,
}

/// What [`RoomKit::list_read_markers`] takes beside the room: Python's
/// `list_read_markers(room_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListReadMarkersOptions {
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2). The room is read with it first, and a room of another
    /// organization is not found; `None` reads the store's markers as they
    /// are, none for a room it lacks.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// Marks the room's event `event_id` read on `channel_id` (RFC §14.1):
    /// the channel's marker moves to the event's index when that is
    /// higher, never back; an event the room does not hold marks nothing.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started (RFC
    /// §12.10.4); [`RoomKitError::RoomNotFound`] for a room the organization
    /// `options` names does not reach; the store's error when it fails.
    pub async fn mark_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        event_id: &EventId,
        options: MarkReadOptions,
    ) -> Result<(), RoomKitError> {
        self.inner.open()?;
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        Ok(self
            .store()
            .mark_read(room_id, channel_id, event_id)
            .await?)
    }

    /// Marks every event of the room read on `channel_id` (RFC §14.1): its
    /// marker moves to the room's highest index, never back; a room with no
    /// event gets no marker.
    ///
    /// # Errors
    ///
    /// As [`mark_read`](Self::mark_read).
    pub async fn mark_all_read(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        options: MarkAllReadOptions,
    ) -> Result<(), RoomKitError> {
        self.inner.open()?;
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        Ok(self.store().mark_all_read(room_id, channel_id).await?)
    }

    /// Every channel's read marker in the room, the highest index it read
    /// (RFC §14.1); a channel with none is absent. With one channel per
    /// member, the per-member read position a "seen by" receipt is made of.
    /// A read: it answers during the kit's close and after it, while the
    /// store is open.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] for a room the organization
    /// `options` names does not reach; [`RoomKitError::KitClosed`] once the
    /// kit's close released its store; the store's error when it fails.
    pub async fn list_read_markers(
        &self,
        room_id: &RoomId,
        options: ListReadMarkersOptions,
    ) -> Result<BTreeMap<ChannelId, u64>, RoomKitError> {
        self.reached(room_id, options.organization_id.as_deref())
            .await?;
        self.store()
            .list_read_markers(room_id)
            .await
            .map_err(|error| self.read_failed(error))
    }
}
