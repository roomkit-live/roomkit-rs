//! The route a turn's work retains to its submitting owner (RFC §10.6.3).
use std::fmt;
use std::sync::Arc;
use std::task::{Context, Waker};

use serde_json::Value;

use super::wire::{HostResult, RoomCall, RoomPublication};
use super::{ServedExecutor, TurnFuture};
use crate::RoomKitError;
use crate::turn::TurnKit;

/// A room owner's port outside a turn (RFC §10.6.3 and §10.6.7).
pub trait RoomOwner: Send + Sync + 'static {
    /// The owner's executor, when its live channels must run there alone.
    fn executor(&self) -> Option<Arc<dyn ServedExecutor>> {
        None
    }

    /// Run a host verb made after its turn, returning the verb's result or error record.
    fn room_call(&self, message: RoomCall) -> TurnFuture<'_, Result<HostResult, RoomKitError>>;

    /// Publish an ephemeral or framework event made after its turn.
    fn room_publication(
        &self,
        message: RoomPublication,
    ) -> TurnFuture<'_, Result<(), RoomKitError>>;
}

#[derive(Clone)]
pub(crate) struct OwnerPort(Arc<dyn RoomOwner>);

impl fmt::Debug for OwnerPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RoomOwner")
    }
}

/// The route captured at admission. A later registration only affects later turns.
#[derive(Debug)]
pub(crate) enum WorkerOwner {
    Unreachable,
    Application {
        port: OwnerPort,
        publications: Arc<tokio::sync::Mutex<()>>,
    },
    #[cfg(feature = "redis")]
    Redis(super::redis::PeerOwner),
}

impl WorkerOwner {
    fn application(port: Option<OwnerPort>) -> Self {
        match port {
            Some(port) => Self::Application {
                port,
                publications: Arc::default(),
            },
            None => Self::Unreachable,
        }
    }

    pub(crate) fn reachable(&self, executor_alive: bool) -> bool {
        !matches!(self, Self::Unreachable)
            && (executor_alive || matches!(self, Self::Application { .. }))
    }

    pub(crate) async fn call(
        &self,
        message: RoomCall,
        error_classes: &super::ErrorClasses,
    ) -> Result<Value, RoomKitError> {
        match self {
            Self::Unreachable => Err(ended(&message.verb)),
            Self::Application { port, .. } => {
                let reply = port.0.room_call(message).await?;
                match reply.error {
                    Some(error) => Err(error_classes.rebuild(&error)),
                    None => Ok(reply.result),
                }
            }
            #[cfg(feature = "redis")]
            Self::Redis(peer) => peer.call(message, error_classes).await,
        }
    }

    pub(crate) fn publish(&self, kit: &dyn TurnKit, message: RoomPublication) {
        match self {
            Self::Unreachable => {}
            Self::Application { port, publications } => {
                let owner = Arc::clone(&port.0);
                let sending = Arc::clone(publications);
                let mut send = Box::pin(async move {
                    let _order = sending.lock().await;
                    if let Err(error) = owner.room_publication(message).await {
                        tracing::debug!(%error, "a publication after a turn did not reach its owner");
                    }
                });
                // Enter the FIFO before returning from the synchronous channel outlet.
                // Suspended deliveries finish under the worker's normal publisher lifetime.
                if send
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
                {
                    kit.spawn_publisher(send);
                }
            }
            #[cfg(feature = "redis")]
            Self::Redis(peer) => peer.publish(super::wire::OwnerMessage::RoomPublication(message)),
        }
    }
}

pub(crate) fn ended(verb: &str) -> RoomKitError {
    RoomKitError::Runtime(format!(
        "The turn this host served has ended: {verb} is not sent"
    ))
}

impl super::Turns {
    pub(crate) fn set_worker_owner(&self, port: Arc<dyn RoomOwner>) {
        *super::lock(&self.worker_owner) = Some(OwnerPort(port));
    }

    pub(crate) fn worker_owner(&self) -> WorkerOwner {
        WorkerOwner::application(super::lock(&self.worker_owner).clone())
    }
}
