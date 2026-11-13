//! A standalone instruction owns one temporary session (RFC §10.1.1 step 7).

use std::path::Path;
use std::sync::Arc;

use roomkit::model::{Metadata, RoomId};
use serde_json::Value;

use super::{Connector, release::Unset, sessions::set_option};
use crate::channel::config;
use crate::{AcpConnection, AcpError, AcpTransport};

/// Ownership is shared with channel shutdown: removing the registered session
/// transfers its one close to that caller. An old connection owns no new session.
pub(in crate::channel) struct TurnSession<T: AcpTransport> {
    connector: Arc<Connector<T>>,
    pub(in crate::channel) connection: Arc<T::Connection>,
    pub(in crate::channel) id: String,
    room: RoomId,
    options: Vec<Value>,
}

impl<T: AcpTransport> Connector<T> {
    pub(in crate::channel) async fn open_turn(
        self: &Arc<Self>,
        room: &RoomId,
        connection: &Arc<T::Connection>,
        cwd: &Path,
    ) -> Result<TurnSession<T>, AcpError> {
        let request = self.session_request(room, cwd, "turn").await?;
        let answer = connection.new_session(request).await?;
        let id = answer.session_id.0.to_string();
        self.keep_turn(room, connection, &id)?;
        let mut session = TurnSession {
            connector: Arc::clone(self),
            connection: Arc::clone(connection),
            id,
            room: room.clone(),
            options: Vec::new(),
        };
        session.options = config::descriptors(answer.config_options.unwrap_or_default())?;
        Ok(session)
    }

    fn keep_turn(
        &self,
        room: &RoomId,
        connection: &Arc<T::Connection>,
        id: &str,
    ) -> Result<(), AcpError> {
        let mut state = self.lock();
        if state.closed
            || !state
                .connection
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, connection))
        {
            return Err(AcpError::ConnectionClosed);
        }
        if state
            .sessions
            .values()
            .chain(state.turn_sessions.values())
            .any(|session| session == id)
        {
            // Never take ownership of a relay's already live session: even
            // closing this rejected opening would destroy that conversation.
            return Err(AcpError::Runtime(format!(
                "ACP session/new for a standalone turn in room {} returned open session {}; \
                 the transport must file a 'turn' session apart from the room's",
                roomkit_core::rules::python_repr(room.as_str()),
                roomkit_core::rules::python_repr(id),
            )));
        }
        state.turn_sessions.insert(room.clone(), id.to_owned());
        self.turns.remember(id, room);
        Ok(())
    }

    pub(in crate::channel) async fn configure_turn(
        &self,
        session: &TurnSession<T>,
    ) -> Result<(), AcpError> {
        // Host choices and channel defaults are required. A reported room
        // value copied afterwards is best-effort, just as Python's _copy_config.
        let options = self
            .configure(
                &session.room,
                &session.connection,
                &session.id,
                session.options.clone(),
                Metadata::new(),
            )
            .await?;
        let fresh = config::values(&options);
        for (id, value) in config::values(&self.options(&session.room)) {
            if fresh.get(&id) == Some(&value) {
                continue;
            }
            if let Err(error) = set_option(&*session.connection, &session.id, &id, &value).await {
                tracing::warn!(config_id = id, %error, "ACP standalone turn could not take the room's configuration");
            }
        }
        Ok(())
    }

    fn discard_turn(&self, session: &TurnSession<T>) -> Option<Unset<T::Connection>> {
        let mut state = self.lock();
        if !state
            .connection
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(current, &session.connection))
            || state.turn_sessions.get(&session.room) != Some(&session.id)
        {
            return None;
        }
        state.turn_sessions.remove(&session.room);
        self.turns.forget(&session.id);
        if !state.closes_sessions {
            tracing::warn!(
                session = session.id,
                "ACP standalone turn session stays open: agent does not announce session/close"
            );
            return None;
        }
        Some(Unset {
            connection: Arc::clone(&session.connection),
            session: Some(session.id.clone()),
            closes: state.closes_sessions,
        })
    }
}

impl<T: AcpTransport> TurnSession<T> {
    pub(in crate::channel) async fn release(&mut self) {
        if let Some(mut release) = self.connector.discard_turn(self) {
            release.release().await;
        }
    }
}

impl<T: AcpTransport> Drop for TurnSession<T> {
    fn drop(&mut self) {
        // Unset schedules release when opening is abandoned, including a
        // cancellation that arrives as the session/new response is handed over.
        drop(self.connector.discard_turn(self));
    }
}
