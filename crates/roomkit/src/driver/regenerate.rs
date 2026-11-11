//! The read before regeneration chooses its path. Unlike the later queue
//! edit, this read is still a call the kit's close cuts (RFC §12.10.4).

use std::sync::Arc;

use roomkit_core::model::{Room, RoomId};

use super::{CallLifetime, Owned};
use crate::context::ExecutionContext;
use crate::error::RoomKitError;
use crate::kit::Inner;
use crate::store::ConversationStore;
use crate::tasks::{Kind, Marks, Phase};

pub(crate) async fn regeneration_room<S: ConversationStore>(
    inner: &Arc<Inner<S>>,
    room_id: &RoomId,
) -> Result<Option<Room>, RoomKitError> {
    inner.open()?;
    let (kit, room_id) = (Arc::clone(inner), room_id.clone());
    let read = async move {
        let marks = Marks::current();
        let mut phases = kit.tasks.phases();
        tokio::select! {
            result = kit.store.get_room(&room_id) => result.map_err(RoomKitError::from),
            _ = phases.wait_for(|phase| *phase >= Phase::Cut && !kit.tasks.spares(&marks)) => {
                Err(RoomKitError::KitClosed)
            }
        }
    };
    let context = ExecutionContext::current();
    let task = inner
        .tasks
        .spawn(Kind::Call, context.scope(read))
        .ok_or(RoomKitError::KitClosed)?;
    // Dropping the caller cancels the read, just as a selection's run.
    let mut task = Owned(task, Arc::new(CallLifetime::default()));
    match (&mut task.0).await {
        Ok(result) => result,
        Err(failure) if failure.is_panic() => std::panic::resume_unwind(failure.into_panic()),
        Err(aborted) if inner.tasks.ended_by_close(aborted.id()) => Err(RoomKitError::KitClosed),
        Err(_) => Err(RoomKitError::Cancelled),
    }
}
