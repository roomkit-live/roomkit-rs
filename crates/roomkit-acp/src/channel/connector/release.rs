//! Release a session the channel will not keep, including interrupted setup.

use crate::connection::AcpConnection;
use crate::error::AcpError;
use agent_client_protocol_schema::v1::CloseSessionRequest;
use std::sync::Arc;

/// A session not retained by the channel is released even if setup is cancelled.
pub(in crate::channel) struct Unset<C: AcpConnection> {
    pub(in crate::channel) connection: Arc<C>,
    pub(in crate::channel) session: Option<String>,
    pub(in crate::channel) closes: bool,
}

impl<C: AcpConnection> Unset<C> {
    pub(in crate::channel) async fn release(&mut self) {
        if let Some(session) = self.session.take() {
            // Once explicit release starts, dropping its future must not
            // issue a second close for the same remote session.
            release(&*self.connection, &session, self.closes).await;
        }
    }
}

impl<C: AcpConnection> Drop for Unset<C> {
    fn drop(&mut self) {
        let Some(session) = self.session.take() else {
            return;
        };
        let connection = Arc::clone(&self.connection);
        let closes = self.closes;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                release(&*connection, &session, closes).await;
            });
        }
    }
}

async fn release<C: AcpConnection>(connection: &C, session: &str, closes: bool) {
    if !closes {
        tracing::warn!(
            session,
            "ACP session stays open: agent does not announce session/close"
        );
        return;
    }
    if let Err(error) = connection
        .close_session(CloseSessionRequest::new(session.to_owned()))
        .await
    {
        if matches!(error, AcpError::ConnectionClosed) {
            tracing::debug!(session, %error, "ACP session close: agent gone");
        } else {
            tracing::warn!(session, %error, "ACP session close refused");
        }
    }
}
