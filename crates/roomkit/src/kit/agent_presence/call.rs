//! Presence work after a room operation still belongs to its public call.
//! It receives the close's grace, preserves the caller's scopes, and releases
//! the publication gate when the caller drops it or the close cuts it.

use std::future::Future;
use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::kit::Inner;
use crate::tasks::{Kind, Marks, Phase, Unit};
use crate::{ConversationStore, RoomKitError};

pub(super) async fn run<S, F>(inner: &Arc<Inner<S>>, work: F) -> Result<(), RoomKitError>
where
    S: ConversationStore,
    F: Future<Output = ()> + Send + 'static,
{
    let owner = Arc::clone(inner);
    let future = async move { until_cut(&owner, work).await };
    let task = Marks::current()
        .with(Unit::new())
        .sync_scope(|| {
            inner
                .tasks
                .spawn_joined(Kind::Call, crate::keep_context(future))
        })
        .ok_or(RoomKitError::KitClosed)?;
    let mut held = Held(task);
    match (&mut held.0).await {
        Ok(result) => result,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) if inner.tasks.ended_by_close(error.id()) => Err(RoomKitError::KitClosed),
        Err(_) => Err(RoomKitError::Cancelled),
    }
}

async fn until_cut<S: ConversationStore>(
    inner: &Inner<S>,
    work: impl Future<Output = ()>,
) -> Result<(), RoomKitError> {
    let mut phases = inner.tasks.phases();
    let marks = Marks::current();
    tokio::pin!(work);
    loop {
        if *phases.borrow() >= Phase::Cut && !inner.tasks.spares(&marks) {
            return Err(RoomKitError::KitClosed);
        }
        tokio::select! {
            biased;
            changed = phases.changed() => {
                if changed.is_err() {
                    return Err(RoomKitError::KitClosed);
                }
            }
            () = &mut work => return Ok(()),
        }
    }
}

struct Held(JoinHandle<Result<(), RoomKitError>>);
impl Drop for Held {
    fn drop(&mut self) {
        self.0.abort();
    }
}
