//! Room sessions and their configuration, applied before a session is kept.

use std::path::Path;
use std::sync::Arc;

use agent_client_protocol_schema::v1::{NewSessionRequest, SetSessionConfigOptionRequest};
use roomkit::model::{Metadata, RoomId};
use serde_json::{Value, json};

use super::{Connector, release::Unset};
use crate::channel::config;
use crate::connection::AcpConnection;
use crate::error::AcpError;
use crate::transport::AcpTransport;

impl<T: AcpTransport> Connector<T> {
    pub(in crate::channel) async fn session(
        &self,
        room: &RoomId,
        connection: &Arc<T::Connection>,
        cwd: &Path,
    ) -> Result<String, AcpError> {
        self.session_with(room, connection, cwd, Metadata::new())
            .await
            .map(|(id, _)| id)
    }

    async fn session_with(
        &self,
        room: &RoomId,
        connection: &Arc<T::Connection>,
        cwd: &Path,
        overlay: Metadata,
    ) -> Result<(String, bool), AcpError> {
        if let Some(session) = self.session_of(room) {
            return Ok((session, false));
        }
        let _opening = self.opening(room).await;
        if let Some(session) = self.session_of(room) {
            return Ok((session, false));
        }
        self.open_session(room, connection, cwd, overlay).await
    }

    async fn open_session(
        &self,
        room: &RoomId,
        connection: &Arc<T::Connection>,
        cwd: &Path,
        overlay: Metadata,
    ) -> Result<(String, bool), AcpError> {
        let mut meta = Metadata::new();
        meta.insert("roomkit.live/roomId".to_owned(), json!(room.as_str()));
        meta.insert("roomkit.live/sessionScope".to_owned(), json!("room"));
        let answer = connection
            .new_session(NewSessionRequest::new(cwd).meta(meta))
            .await?;
        let id = answer.session_id.0.to_string();
        let mut unset = Unset {
            connection: Arc::clone(connection),
            session: Some(id.clone()),
            closes: self.lock().closes_sessions,
        };
        let options = config::descriptors(answer.config_options.unwrap_or_default())?;
        let configured = self
            .configure(room, connection, &id, options, overlay)
            .await;
        let options = match configured {
            Ok(options) => options,
            Err(error) => {
                unset.release().await;
                return Err(error);
            }
        };
        let mut state = self.lock();
        if state.closed {
            return Err(AcpError::Runtime("ACPChannel is closed".to_owned()));
        }
        state.sessions.insert(room.clone(), id.clone());
        state.options.insert(room.clone(), options);
        unset.session = None;
        Ok((id, true))
    }

    async fn configure(
        &self,
        room: &RoomId,
        connection: &T::Connection,
        session: &str,
        mut options: Vec<Value>,
        overlay: Metadata,
    ) -> Result<Vec<Value>, AcpError> {
        let mut wanted = self.setup.clone();
        wanted.extend(self.lock().config.get(room).cloned().unwrap_or_default());
        wanted.extend(overlay);
        for (id, value) in wanted {
            if config::values(&options).get(&id) == Some(&value) {
                continue;
            }
            let reported = set_option(connection, session, &id, &value).await.map_err(|error| {
                if matches!(error, AcpError::ConnectionClosed) { return error; }
                AcpError::SetupRefused {
                    message: format!("ACP session for room {} could not take {id}={} ({}); it is not prompted. Set another value with set_config_option, or close_session to forget the room's", roomkit_core::rules::python_repr(room.as_str()), config::display(&value), self.channel_id),
                    source: Box::new(error),
                }
            })?;

            if !reported.is_empty() {
                options = reported;
            }
        }
        Ok(options)
    }

    pub(in crate::channel) async fn set_option(
        &self,
        room: &RoomId,
        cwd: &Path,
        id: &str,
        value: Value,
    ) -> Result<Metadata, AcpError> {
        let connection = self.connection().await?;
        let overlay = Metadata::from_iter([(id.to_owned(), value.clone())]);
        let (session, opened) = self.session_with(room, &connection, cwd, overlay).await?;
        let options = if opened {
            self.options(room)
        } else {
            set_option(&*connection, &session, id, &value).await?
        };
        let values = config::values(&options);
        let mut state = self.lock();
        if state.sessions.get(room) == Some(&session) {
            state.options.insert(room.clone(), options);
            state
                .config
                .entry(room.clone())
                .or_default()
                .insert(id.to_owned(), values.get(id).cloned().unwrap_or(value));
        }
        Ok(values)
    }

    pub(in crate::channel) fn options(&self, room: &RoomId) -> Vec<Value> {
        self.lock().options.get(room).cloned().unwrap_or_default()
    }

    pub(in crate::channel) fn room_state(&self, room: &RoomId) -> Metadata {
        self.lock()
            .config
            .get(room)
            .filter(|config| !config.is_empty())
            .map_or_else(Metadata::new, |config| {
                Metadata::from_iter([("config".to_owned(), json!(config))])
            })
    }

    pub(in crate::channel) fn restore_room_state(&self, room: &RoomId, state: &Metadata) {
        let mut held = self.lock();
        if held.config.contains_key(room) {
            return;
        }
        let Some(config) = state.get("config").and_then(Value::as_object) else {
            return;
        };
        let config: Metadata = config
            .iter()
            .filter(|(_, value)| value.is_string() || value.is_boolean())
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        if !config.is_empty() {
            held.config.insert(room.clone(), config);
        }
    }

    pub(in crate::channel) async fn close_session(&self, room: &RoomId) -> bool {
        let opening = self.opening(room).await;
        let _retire = RetireOpening {
            connector: self,
            room,
            lock: tokio::sync::OwnedMutexGuard::mutex(&opening),
        };
        let (session, connection, closes) = {
            let mut state = self.lock();
            state.config.remove(room);
            state.options.remove(room);
            (
                state.sessions.remove(room),
                state.connection.clone(),
                state.closes_sessions,
            )
        };
        let Some(session) = session else {
            return false;
        };
        if let Some(connection) = connection {
            Unset {
                connection,
                session: Some(session),
                closes,
            }
            .release()
            .await;
        }
        true
    }

    async fn opening(&self, room: &RoomId) -> tokio::sync::OwnedMutexGuard<()> {
        loop {
            let lock = Arc::clone(
                self.openings
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .entry(room.clone())
                    .or_default(),
            );
            let guard = Arc::clone(&lock).lock_owned().await;
            if self
                .openings
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(room)
                .is_some_and(|current| Arc::ptr_eq(current, &lock))
            {
                return guard;
            }
        }
    }
}

struct RetireOpening<'a, T: AcpTransport> {
    connector: &'a Connector<T>,
    room: &'a RoomId,
    lock: &'a Arc<tokio::sync::Mutex<()>>,
}

impl<T: AcpTransport> Drop for RetireOpening<'_, T> {
    fn drop(&mut self) {
        let mut openings = self
            .connector
            .openings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if openings
            .get(self.room)
            .is_some_and(|current| Arc::ptr_eq(current, self.lock))
        {
            openings.remove(self.room);
        }
    }
}

/// Request construction is inside setup's refusal boundary, as in the Python SDK.
async fn set_option<C: AcpConnection>(
    connection: &C,
    session: &str,
    id: &str,
    value: &Value,
) -> Result<Vec<Value>, AcpError> {
    let request = SetSessionConfigOptionRequest::new(
        session.to_owned(),
        id.to_owned(),
        config::option_value(value)?,
    );
    config::descriptors(connection.set_config_option(request).await?.config_options)
}
