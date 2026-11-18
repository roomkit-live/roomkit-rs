//! Connect a configured transport to the room owner's existing turn driver.
mod bridge;
pub(super) use bridge::{ActiveLink, run_link};

use super::wire::{HostResult, OwnerMessage, OwnerMessageEnvelope};
use super::{
    RoomCall, RoomOwner, RoomPublication, ServedExecutor, TurnBackendHandle, TurnFuture, Turns,
};
use crate::kit::Inner;
use crate::{ConversationStore, RoomKit, RoomKitError};
use std::fmt;
use std::sync::{Arc, OnceLock, Weak};
use tokio::sync::OnceCell;

pub(crate) struct Application {
    pub(crate) backend: TurnBackendHandle,
    owner: OnceLock<Arc<dyn RoomOwner>>,
    attending: OnceCell<()>,
    serving: OnceCell<()>,
}

impl fmt::Debug for Application {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Application").finish_non_exhaustive()
    }
}

impl Application {
    pub(super) fn new(backend: TurnBackendHandle) -> Self {
        Self {
            backend,
            owner: OnceLock::new(),
            attending: OnceCell::new(),
            serving: OnceCell::new(),
        }
    }

    pub(super) fn bind(&self, owner: Arc<dyn RoomOwner>) {
        let _owner = self.owner.set(owner);
    }

    pub(super) async fn ready(&self) -> Result<(), RoomKitError> {
        let owner = self.owner.get().ok_or_else(owner_closed)?;
        self.attending
            .get_or_try_init(|| self.backend.attend(Arc::clone(owner)))
            .await?;
        self.serving
            .get_or_try_init(|| async {
                if let Some(executor) = owner.executor() {
                    self.backend.serve(executor).await?;
                }
                Ok::<_, RoomKitError>(())
            })
            .await?;
        Ok(())
    }
}

fn owner_closed() -> RoomKitError {
    RoomKitError::Runtime("The room owner has closed".into())
}

struct KitOwner<S> {
    kit: Weak<Inner<S>>,
    executor: Arc<dyn ServedExecutor>,
}

impl Turns {
    pub(super) async fn ready(&self) -> Result<(), RoomKitError> {
        if let Some(app) = &self.application {
            app.ready().await?;
        }
        Ok(())
    }

    pub(crate) fn bind_application_owner<S: ConversationStore>(&self, kit: &RoomKit<S>) {
        if let Some(application) = &self.application {
            application.bind(Arc::new(KitOwner {
                kit: Arc::downgrade(&kit.inner),
                executor: super::owner_executor(kit),
            }));
        }
    }
}

impl<S: ConversationStore> RoomOwner for KitOwner<S> {
    fn executor(&self) -> Option<Arc<dyn ServedExecutor>> {
        self.kit.upgrade()?;
        Some(Arc::clone(&self.executor))
    }
    fn room_call(&self, message: RoomCall) -> TurnFuture<'_, Result<HostResult, RoomKitError>> {
        Box::pin(async move {
            let inner = self.kit.upgrade().filter(|inner| !inner.turns.is_closed());
            let Some(inner) = inner else {
                return Ok(HostResult {
                    call_id: message.call_id,
                    result: serde_json::Value::Null,
                    error: Some(super::error::kit_error(&owner_closed())),
                    record_changes: None,
                });
            };
            let envelope = OwnerMessageEnvelope {
                message: OwnerMessage::RoomCall(message),
            };
            inner
                .turns
                .receive_owner_message(Arc::new(Weak::clone(&self.kit)), envelope)
                .await
                .ok_or_else(owner_closed)
        })
    }
    fn room_publication(
        &self,
        message: RoomPublication,
    ) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            if let Some(inner) = self.kit.upgrade().filter(|inner| !inner.turns.is_closed()) {
                let envelope = OwnerMessageEnvelope {
                    message: OwnerMessage::RoomPublication(message),
                };
                inner
                    .turns
                    .receive_owner_message(Arc::new(Weak::clone(&self.kit)), envelope)
                    .await;
            }
            Ok(())
        })
    }
}
