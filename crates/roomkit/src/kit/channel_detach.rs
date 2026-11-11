//! Python's channel detach finally block: stored state is forgotten even
//! when the callback is cancelled. Abandoned Rust futures leave that write
//! to tracked work, which the kit's idle and close join.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, Metadata, RoomId};

use super::Inner;
use super::channel_state::store_state;
use crate::RoomKitError;
use crate::context::ExecutionContext;
use crate::store::ConversationStore;
use crate::tasks::Kind;
use crate::turns::{DetachTicket, PendingDetaches, state_key};

pub(crate) struct DetachCleanup {
    // Dropping this guard starts the finalizer, even if the callback was
    // abandoned. Dropping its JoinHandle leaves that tracked work running.
    start: Option<tokio::sync::oneshot::Sender<DetachTicket>>,
    task: Option<tokio::task::JoinHandle<Result<(), RoomKitError>>>,
    pending: Arc<PendingDetaches>,
    channel: ChannelId,
    room: RoomId,
}

impl DetachCleanup {
    pub(crate) async fn begin<S: ConversationStore>(
        inner: &Arc<Inner<S>>,
        channel: &ChannelId,
        room: &RoomId,
    ) -> Result<Self, RoomKitError> {
        inner.turns.pending_detaches.wait(channel, room).await;
        let (start, waiting) = tokio::sync::oneshot::channel::<DetachTicket>();
        let owner = Arc::clone(inner);
        let (channel, room) = (channel.clone(), room.clone());
        let (target, room_id) = (channel.clone(), room.clone());
        let finish = async move {
            let Ok(mut ticket) = waiting.await else {
                return Err(RoomKitError::Cancelled);
            };
            ticket.wait_previous().await;
            // Already tracked before the callback: a cut gives this
            // finalizer its unwind instead of refusing a newly spawned one.
            let result = forget(&owner, &target, &room_id).await;
            if let Err(error) = &result {
                tracing::warn!(target: "roomkit::turns", room = %room_id, channel = %target, %error,
                    "forgetting a detached channel's stored state failed");
            }
            result
        };
        let task = inner
            .tasks
            .spawn(Kind::Call, ExecutionContext::default().scope(finish))
            .ok_or(RoomKitError::KitClosed)?;
        Ok(Self {
            start: Some(start),
            task: Some(task),
            pending: Arc::clone(&inner.turns.pending_detaches),
            channel,
            room,
        })
    }

    fn start(&mut self) {
        if let Some(start) = self.start.take() {
            // Only the finalizer is a barrier. The callback may itself
            // reattach its channel through the room's inherited lock.
            let ticket = self.pending.begin(self.channel.clone(), self.room.clone());
            drop(start.send(ticket));
        }
    }

    pub(crate) async fn finish(mut self) -> Result<(), RoomKitError> {
        self.start();
        let Some(task) = self.task.take() else {
            return Err(RoomKitError::Internal(
                "detach finalizer already joined".to_owned(),
            ));
        };
        match task.await {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            Err(_) => Err(RoomKitError::Cancelled),
        }
    }
}

impl Drop for DetachCleanup {
    fn drop(&mut self) {
        self.start();
    }
}

async fn forget<S: ConversationStore>(
    inner: &Inner<S>,
    channel: &ChannelId,
    room_id: &RoomId,
) -> Result<(), RoomKitError> {
    if inner.tasks.is_sealed() {
        return Err(RoomKitError::KitClosed);
    }
    if let Some(room) = inner.store.get_room(room_id).await?
        && room.metadata.contains_key(&state_key(channel))
    {
        store_state(&*inner.store, room_id, channel, Metadata::new()).await?;
    }
    Ok(())
}
