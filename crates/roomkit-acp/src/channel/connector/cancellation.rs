//! Cancellation uses the connection already hosting the room, never opens one.

use agent_client_protocol_schema::v1::CancelNotification;
use roomkit::model::RoomId;

use super::Connector;
use crate::{AcpConnection, AcpError, AcpTransport};

impl<T: AcpTransport> Connector<T> {
    /// Cancel the captured turn session, even if its room has moved on.
    pub(in crate::channel) async fn cancel_session(&self, session: String) -> Result<(), AcpError> {
        let connection = self.lock().connection.clone();
        if let Some(connection) = connection {
            connection.cancel(CancelNotification::new(session)).await?;
        }
        Ok(())
    }

    pub(in crate::channel) async fn cancel_room(&self, room: &RoomId) -> Result<bool, AcpError> {
        let target = {
            let state = self.lock();
            state.connection.clone().zip(
                state
                    .turn_sessions
                    .get(room)
                    .or_else(|| state.sessions.get(room))
                    .cloned(),
            )
        };
        let Some((connection, session)) = target else {
            return Ok(false);
        };
        connection.cancel(CancelNotification::new(session)).await?;
        Ok(true)
    }
}
