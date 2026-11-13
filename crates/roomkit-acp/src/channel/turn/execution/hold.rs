//! The room remains held through prompt cancellation and recovery cleanup.

use std::sync::Arc;

use agent_client_protocol_schema::v1::CancelNotification;
use roomkit::model::{ResponseMetadata, RoomId};
use tokio::sync::OwnedMutexGuard;
use tokio::task::AbortHandle;

use crate::channel::connector::standalone::TurnSession;
use crate::channel::{Active, Shared};
use crate::connection::AcpConnection;
use crate::transport::AcpTransport;

pub(in crate::channel::turn) struct Hold<T: AcpTransport> {
    pub(super) shared: Arc<Shared<T>>,
    pub(super) room_id: RoomId,
    pub(super) record: ResponseMetadata,
    room: Option<OwnedMutexGuard<()>>,
    active: Option<Active<T>>,
    pub(super) prompt: Option<Prompt<T::Connection>>,
    pub(super) recovering: bool,
    pub(super) preparation: Arc<crate::channel::preparation::Preparation>,
    retire: bool,
    pub(super) generation: u64,
    pub(super) session_generation: Option<u64>,
    pub(super) standalone: bool,
    pub(super) temporary: Option<TurnSession<T>>,
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
        turn_id: Option<String>,
    ) -> Self {
        let room = shared.holding_room(&room_id).await;
        let active = shared.active();
        let preparation = Arc::new(crate::channel::preparation::Preparation::new(turn_id));
        shared
            .preparations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(room_id.clone(), Arc::clone(&preparation));
        let generation = shared.connector.generation(&room_id);
        Self {
            shared,
            room_id,
            record,
            room: Some(room),
            active: Some(active),
            prompt: None,
            recovering: false,
            preparation,
            retire: false,
            generation,
            session_generation: None,
            standalone: false,
            temporary: None,
        }
    }

    /// Only consuming the terminal item completes recovery. Dropping queued
    /// output after the remote prompt returned still abandons the turn.
    pub(in crate::channel::turn) fn completed(&mut self) {
        self.recovering = false;
    }

    pub(in crate::channel::turn) fn retire_steering(&self) {
        self.preparation.retire();
    }

    pub(in crate::channel::turn) fn needs_cleanup(&self) -> bool {
        self.temporary.is_some() || self.needs_room_cleanup()
    }

    pub(in crate::channel::turn) fn standalone(&self) -> bool {
        self.standalone
    }

    fn needs_room_cleanup(&self) -> bool {
        // A standalone turn releases only its temporary session. Detachment's
        // queued closer releases the room session after this turn unlocks it,
        // so a slow room close cannot hold the standalone response open.
        !self.standalone
            && (self.recovering
                || self.session_generation.is_some_and(|generation| {
                    self.shared.connector.generation(&self.room_id) != generation
                }))
    }

    pub(in crate::channel::turn) fn detached(&self) -> bool {
        self.shared.connector.generation(&self.room_id) != self.generation
    }

    pub(in crate::channel::turn) async fn cleanup(&mut self) {
        if !self.needs_cleanup() {
            return;
        }
        // Python's finished turn leaves its registry before releasing the
        // replacement session. The room guard still spans that release.
        self.active.take();
        if let Some(mut session) = self.temporary.take() {
            session.release().await;
        }
        if self.needs_room_cleanup()
            && let Some(mut session) = self.shared.connector.discard(&self.room_id)
        {
            session.release().await;
        }
        self.retire = self.recovering;
        self.session_generation = None;
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
            .needs_room_cleanup()
            .then(|| self.shared.connector.discard(&self.room_id))
            .flatten();
        let temporary = self.temporary.take();
        if prompt.is_none() && release.is_none() && temporary.is_none() {
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
                if let Some(mut temporary) = temporary {
                    temporary.release().await;
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
