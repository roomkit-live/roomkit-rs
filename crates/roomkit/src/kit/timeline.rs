//! The kit's timeline read (RFC §14.1, "Reading a timeline"), the Python
//! implementation's `get_timeline`.

use roomkit_core::model::{RoomEvent, RoomId};

use super::RoomKit;
use super::options::{GetRoomOptions, GetTimelineOptions};
use crate::error::RoomKitError;
use crate::store::ConversationStore;

impl<S: ConversationStore> RoomKit<S> {
    /// A page of the room `room_id`'s timeline, in ascending index order
    /// (RFC §14.1, "Reading a timeline"), as `options` ask: the Python
    /// implementation's `get_timeline`. The room is read first, then the
    /// query is checked, as the Python implementation does: a room that
    /// does not exist, or that the organization `options` name does not
    /// reach, is not found whatever the query.
    ///
    /// What a channel may see of the page is [`visible_events`](crate::visible_events)'s
    /// answer, given the room's bindings.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::Store`] with [`StoreError::RoomNotFound`] when the
    /// room is not found, with [`StoreError::Value`] when the
    /// options name both cursors, and when the store fails a read.
    ///
    /// [`StoreError::RoomNotFound`]: crate::StoreError::RoomNotFound
    /// [`StoreError::Value`]: crate::StoreError::Value
    pub async fn get_timeline(
        &self,
        room_id: &RoomId,
        options: GetTimelineOptions,
    ) -> Result<Vec<RoomEvent>, RoomKitError> {
        let room = GetRoomOptions {
            organization_id: options.organization_id.clone(),
        };
        self.get_room(room_id, room).await?;
        self.store()
            .list_events(room_id, options.query())
            .await
            .map_err(|error| self.read_failed(error))
    }
}
