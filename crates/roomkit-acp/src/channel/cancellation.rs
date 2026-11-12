//! Public room cancellation without opening a connection (RFC §6.4 item 7).

use roomkit::model::RoomId;
use std::sync::PoisonError;

use super::AcpChannel;
use crate::{AcpError, AcpTransport};

impl<T: AcpTransport> AcpChannel<T> {
    /// Request cancellation of the room's active ACP turn. An existing idle
    /// session also receives the notification, as Python's `cancel(room_id)`.
    /// Returns false when there is no session or preparation to cancel.
    ///
    /// # Errors
    /// The transport failed to write the cancellation notification.
    pub async fn cancel(&self, room_id: &RoomId) -> Result<bool, AcpError> {
        let preparation = self
            .shared
            .preparations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(room_id)
            .cloned();
        if preparation.is_some_and(|preparation| preparation.cancel()) {
            return Ok(true);
        }
        self.shared.connector.cancel_room(room_id).await
    }
}
