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
    /// What the owner starts as it first uses its backend (Python's
    /// `_on_first_use`).
    first_use: OnceLock<Box<dyn Fn() + Send + Sync>>,
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
            first_use: OnceLock::new(),
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
            .get_or_try_init(|| async {
                if let Some(first_use) = self.first_use.get() {
                    first_use();
                }
                self.backend.attend(Arc::clone(owner)).await
            })
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

    /// Have `start` run as the owner first uses its backend, before it
    /// attends it; nothing on a kit with no backend configured.
    pub(crate) fn on_first_use(&self, start: impl Fn() + Send + Sync + 'static) {
        if let Some(application) = &self.application {
            // The kit sets it once, as it is built.
            drop(application.first_use.set(Box::new(start)));
        }
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
