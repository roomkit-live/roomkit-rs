//! Cancellation uses the connection already hosting the room, never opens one.

use agent_client_protocol_schema::v1::CancelNotification;
use roomkit::model::RoomId;

use super::Connector;
use crate::channel::client::Abandoned;
use crate::{AcpConnection, AcpError, AcpTransport};

/// Cancel `session`'s prompt with `session/cancel`, and answer each of its
/// permission requests still pending `cancelled`, as ACP requires of a
/// client that cancels a prompt: `abandoned`, taken before the notification
/// goes, drops their decisions, the external tool handler's included, and
/// the answers follow the notification, however its sending ends (RFC §6.4).
/// Python's `_cancel_session`.
pub(in crate::channel) async fn cancel_prompt<C: AcpConnection>(
    connection: &C,
    abandoned: Abandoned,
    session: String,
) -> Result<(), AcpError> {
    let sent = connection.cancel(CancelNotification::new(session)).await;
    drop(abandoned);
    sent
}

impl<T: AcpTransport> Connector<T> {
    /// Cancel the captured turn session, even if its room has moved on.
    pub(in crate::channel) async fn cancel_session(&self, session: String) -> Result<(), AcpError> {
        let connection = self.lock().connection.clone();
        if let Some(connection) = connection {
            let abandoned = self.pending.abandon(&session);
            cancel_prompt(&*connection, abandoned, session).await?;
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
        let abandoned = self.pending.abandon(&session);
        cancel_prompt(&*connection, abandoned, session).await?;
        Ok(true)
    }
}
