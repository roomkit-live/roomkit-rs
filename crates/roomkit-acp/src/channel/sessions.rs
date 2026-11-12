//! The host's per-room ACP configuration API (RFC §6.4, §10.6.3).

use std::sync::{Arc, PoisonError};

use roomkit::model::{Metadata, RoomId};
use roomkit::{ChannelError, RoomKitError};
use serde_json::{Value, json};

use super::{AcpChannel, Shared, config, turn};
use crate::error::AcpError;
use crate::transport::AcpTransport;

impl<T: AcpTransport> AcpChannel<T> {
    /// The current option values reported by the room's agent session.
    /// Empty before a session is opened; this read never opens one (RFC §6.4).
    #[must_use]
    pub fn session_config(&self, room_id: &RoomId) -> Metadata {
        config::values(&self.config_options(room_id))
    }

    /// Full ACP option descriptors, including choices and current values.
    /// Empty before a session is opened; this read never opens one (RFC §6.4).
    #[must_use]
    pub fn config_options(&self, room_id: &RoomId) -> Vec<Value> {
        self.shared.connector.options(room_id)
    }

    /// Set one option, opening the room's session when necessary, and return
    /// all values the agent reports. Host settings override channel defaults
    /// until `close_session`. Use `RoomKit::channel_call` for durable room state.
    ///
    /// # Errors
    /// The connection or agent's refusal; a new session whose required setup
    /// fails is released and never prompted (RFC §6.4).
    pub async fn set_config_option(
        &self,
        room_id: &RoomId,
        config_id: &str,
        value: Value,
    ) -> Result<Metadata, AcpError> {
        self.shared
            .connector
            .set_option(room_id, &self.shared.cwd, config_id, value)
            .await
    }

    /// Forget the room's session and host settings, returning whether it had
    /// a session. Wait for its turn and opening to end, then ask the agent to
    /// close when it supports that verb. A refused remote close is logged;
    /// local state is still forgotten. Channel defaults remain (RFC §6.4).
    pub async fn close_session(&self, room_id: &RoomId) -> bool {
        let room = self.shared.holding_room(room_id).await;
        let _retire = RetireRoom {
            shared: &self.shared,
            room: room_id,
            lock: tokio::sync::OwnedMutexGuard::mutex(&room),
        };
        self.shared.connector.close_session(room_id).await
    }

    pub(super) async fn call_session(
        &self,
        room_id: &RoomId,
        verb: &str,
        arguments: &Metadata,
    ) -> Result<Value, ChannelError> {
        match verb {
            "session_config" => { no_arguments(arguments)?; Ok(json!(self.session_config(room_id))) }
            "config_options" => { no_arguments(arguments)?; Ok(json!(self.config_options(room_id))) }
            "close_session" => { no_arguments(arguments)?; Ok(json!(self.close_session(room_id).await)) }
            "set_config_option" => {
                let id = arguments.get("config_id").and_then(Value::as_str)
                    .ok_or_else(|| RoomKitError::Type("set_config_option requires config_id".to_owned()))?;
                let value = arguments.get("value").cloned()
                    .ok_or_else(|| RoomKitError::Type("set_config_option requires value".to_owned()))?;
                if arguments.len() != 2 { return Err(RoomKitError::Type("unexpected set_config_option argument".to_owned()).into()); }
                self.set_config_option(room_id, id, value).await.map(|values| json!(values)).map_err(turn::raised)
            }
            _ => Err(RoomKitError::Value(format!(
                "ACP channel '{}' has no per-room verb '{verb}': it has close_session, config_options, session_config, set_config_option", self.shared.channel_id
            )).into()),
        }
    }
}

fn no_arguments(arguments: &Metadata) -> Result<(), ChannelError> {
    if arguments.is_empty() {
        return Ok(());
    }
    Err(RoomKitError::Type("unexpected ACP session verb argument".to_owned()).into())
}

impl<T: AcpTransport> Shared<T> {
    pub(super) async fn holding_room(&self, room: &RoomId) -> tokio::sync::OwnedMutexGuard<()> {
        loop {
            let lock = self.room_lock(room);
            let guard = Arc::clone(&lock).lock_owned().await;
            if self
                .rooms
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(room)
                .is_some_and(|current| Arc::ptr_eq(current, &lock))
            {
                return guard;
            }
        }
    }
}

struct RetireRoom<'a, T: AcpTransport> {
    shared: &'a Shared<T>,
    room: &'a RoomId,
    lock: &'a Arc<tokio::sync::Mutex<()>>,
}

impl<T: AcpTransport> Drop for RetireRoom<'_, T> {
    fn drop(&mut self) {
        let mut rooms = self
            .shared
            .rooms
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if rooms
            .get(self.room)
            .is_some_and(|current| Arc::ptr_eq(current, self.lock))
        {
            rooms.remove(self.room);
        }
    }
}
