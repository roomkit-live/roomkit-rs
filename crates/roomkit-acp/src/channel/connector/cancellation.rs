//! Cancellation uses the connection already hosting the room, never opens one.

use agent_client_protocol_schema::v1::CancelNotification;
use roomkit::model::RoomId;

use super::Connector;
use crate::{AcpConnection, AcpError, AcpTransport};

impl<T: AcpTransport> Connector<T> {
    pub(in crate::channel) async fn cancel_room(&self, room: &RoomId) -> Result<bool, AcpError> {
        let target = {
            let state = self.lock();
            state
                .connection
                .clone()
                .zip(state.sessions.get(room).cloned())
        };
        let Some((connection, session)) = target else {
            return Ok(false);
        };
        connection.cancel(CancelNotification::new(session)).await?;
        Ok(true)
    }
}
