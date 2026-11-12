//! The room remains held through prompt cancellation and recovery cleanup.

use std::sync::Arc;

use agent_client_protocol_schema::v1::CancelNotification;
use roomkit::model::{ResponseMetadata, RoomId};
use tokio::sync::OwnedMutexGuard;
use tokio::task::AbortHandle;

use crate::channel::{Active, Shared};
use crate::connection::AcpConnection;
use crate::transport::AcpTransport;

pub(in crate::channel::turn) struct Hold<T: AcpTransport> {
    pub(super) shared: Arc<Shared<T>>,
    pub(super) room_id: RoomId,
    pub(super) record: ResponseMetadata,
    room: Option<OwnedMutexGuard<()>>,
    _active: Active<T>,
    pub(super) prompt: Option<Prompt<T::Connection>>,
    pub(super) recovering: bool,
    pub(super) preparation: Arc<crate::channel::preparation::Preparation>,
    retire: bool,
}

pub(super) struct Prompt<C: AcpConnection> {
    pub(super) task: AbortHandle,
    pub(super) connection: Arc<C>,
    pub(super) session: String,
}

impl<T: AcpTransport> Hold<T> {
    pub(super) async fn acquire(
        shared: Arc<Shared<T>>,
        room_id: RoomId,
        record: ResponseMetadata,
    ) -> Self {
        let room = shared.holding_room(&room_id).await;
        let active = shared.active();
        let preparation = Arc::new(crate::channel::preparation::Preparation::default());
        shared
            .preparations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(room_id.clone(), Arc::clone(&preparation));
        Self {
            shared,
            room_id,
            record,
            room: Some(room),
            _active: active,
            prompt: None,
            recovering: false,
            preparation,
            retire: false,
        }
    }

    /// Only consuming the terminal item completes recovery. Dropping queued
    /// output after the remote prompt returned still abandons the turn.
    pub(in crate::channel::turn) fn completed(&mut self) {
        self.recovering = false;
    }

    pub(in crate::channel::turn) fn needs_cleanup(&self) -> bool {
        self.recovering
    }

    pub(in crate::channel::turn) async fn failed(&mut self) {
        if !self.recovering {
            return;
        }
        if let Some(mut session) = self.shared.connector.discard(&self.room_id) {
            session.release().await;
        }
        self.retire = true;
        self.recovering = false;
    }
}

impl<T: AcpTransport> Drop for Hold<T> {
    fn drop(&mut self) {
        self.shared
            .preparations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.room_id);
        let prompt = self
            .prompt
            .take()
            .filter(|prompt| !prompt.task.is_finished());
        if let Some(prompt) = &prompt {
            prompt.task.abort();
        }
        if prompt.is_some() || self.recovering {
            super::super::interrupted(&self.record);
        }
        let release = self
            .recovering
            .then(|| self.shared.connector.discard(&self.room_id))
            .flatten();
        if prompt.is_none() && release.is_none() {
            if (self.recovering || self.retire)
                && let Some(room) = &self.room
            {
                self.shared
                    .retire_room(&self.room_id, OwnedMutexGuard::mutex(room));
            }
            return;
        }
        let room = self.room.take();
        let shared = Arc::clone(&self.shared);
        let room_id = self.room_id.clone();
        let retire = self.recovering || self.retire;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Some(prompt) = prompt {
                    let _gone = prompt
                        .connection
                        .cancel(CancelNotification::new(prompt.session))
                        .await;
                }
                if let Some(mut release) = release {
                    release.release().await;
                }
                if retire && let Some(room) = &room {
                    shared.retire_room(&room_id, OwnedMutexGuard::mutex(room));
                }
                drop(room);
            });
        }
    }
}
