//! Reserve, retry and forget the room state shared by every Redis owner.
use super::{Claim, Watch};
use crate::kit::{Inner, RoomKit};
use crate::tasks::Kind;
use crate::{ConversationStore, RedisTurnBackend, RoomKitError};
use roomkit_core::model::{ChannelCategory, ChannelId, RoomId};
use std::sync::{Arc, PoisonError, Weak, atomic::Ordering};

pub(super) async fn reserve(
    backend: &RedisTurnBackend,
    claim: &Claim,
    room: &RoomId,
    channel: &ChannelId,
) -> Result<bool, RoomKitError> {
    if claim.pending.load(Ordering::SeqCst) && claim.reserved.load(Ordering::SeqCst) {
        // This owner won the change, but its realtime publication failed.
        return Ok(true);
    }
    let result = backend
        .claim_presence(channel.as_str(), room.as_str(), Some(claim.online))
        .await;
    claim
        .reserved
        .store(matches!(result, Ok(true)), Ordering::SeqCst);
    claim.pending.store(result.is_err(), Ordering::SeqCst);
    result
}

pub(super) async fn forget(backend: &RedisTurnBackend, room: &RoomId, channel: &ChannelId) {
    if let Err(error) = backend
        .claim_presence(channel.as_str(), room.as_str(), None)
        .await
    {
        tracing::warn!(%room, %channel, %error, "forgetting an agent's presence failed");
    }
}

pub(in crate::kit) fn on_room_close<S: ConversationStore>(
    owner: Weak<Inner<S>>,
) -> Box<dyn Fn(&RoomId) + Send + Sync> {
    Box::new(move |room| {
        if let Some(owner) = owner.upgrade()
            && let Some(backend) = &owner.turns.remote
        {
            owner
                .agent_presence
                .forget_closed_later(&owner, backend, room);
        }
    })
}

impl Watch {
    pub(in crate::kit) fn forget_closed_later<S: ConversationStore>(
        self: &Arc<Self>,
        owner: &Inner<S>,
        backend: &RedisTurnBackend,
        room: &RoomId,
    ) {
        self.remote
            .forgetting
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(room.clone());
        let (watch, store, backend) = (Arc::clone(self), Arc::clone(&owner.store), backend.clone());
        drop(owner.tasks.spawn_alone(Kind::Observer, async move {
            let _gate = watch.gate.lock().await;
            watch.finish_forgets(&*store, &backend).await;
        }));
    }

    async fn finish_forgets<S: ConversationStore>(&self, store: &S, backend: &RedisTurnBackend) {
        let rooms = std::mem::take(
            &mut *self
                .remote
                .forgetting
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for room in rooms {
            match store.list_bindings(&room).await {
                Ok(bindings) => {
                    for binding in bindings
                        .into_iter()
                        .filter(|b| b.category == ChannelCategory::Intelligence)
                    {
                        forget(backend, &room, &binding.channel_id).await;
                    }
                }
                Err(error) => {
                    tracing::warn!(%room, %error, "reading the closed room's agents failed");
                }
            }
        }
    }
}

impl<S: ConversationStore> RoomKit<S> {
    pub(in crate::kit) async fn finish_presence_forgets(&self) {
        if let Some(backend) = &self.inner.turns.remote {
            self.inner
                .agent_presence
                .finish_forgets(self.store(), backend)
                .await;
        }
    }

    pub(in crate::kit) async fn release_pending_presence(&self, backend: &RedisTurnBackend) {
        let watch = &self.inner.agent_presence;
        // A realtime publisher may itself close the kit while it holds the publication gate.
        let keys: Vec<_> = watch
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .claims
            .iter()
            .filter(|(_, claim)| {
                claim.pending.load(Ordering::SeqCst) && claim.reserved.load(Ordering::SeqCst)
            })
            .map(|(key, _)| key.clone())
            .collect();
        for (room, channel) in keys {
            forget(backend, &room, &channel).await;
        }
    }
}
