//! Local presence claims and retries. A forgotten claim can never be
//! restored by an older publication that completes after a detach or close.

mod claims;
mod remote;
pub(in crate::kit) use claims::on_room_close;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use roomkit_core::model::{ChannelCategory, ChannelId, RoomId};
use tokio::sync::Mutex as AsyncMutex;

use crate::kit::RoomKit;
use crate::tasks::Kind;
use crate::{ConversationStore, PublishPresenceOptions, RoomKitError};

type Key = (RoomId, ChannelId);

#[derive(Debug)]
struct Claim {
    online: bool,
    pending: AtomicBool,
    reserved: AtomicBool,
}

#[derive(Debug, Default)]
struct State {
    claims: BTreeMap<Key, Arc<Claim>>,
    epochs: BTreeMap<Key, Arc<()>>,
    retrying: bool,
}

#[derive(Debug, Default)]
pub(crate) struct Watch {
    state: Mutex<State>,
    gate: AsyncMutex<()>,
    remote: remote::Monitor,
}

impl Watch {
    pub(crate) async fn detached(
        &self,
        room: &RoomId,
        channel: &ChannelId,
        backend: Option<&crate::turns::TurnBackendHandle>,
    ) {
        // The room mutation has finished and released its lock. An earlier
        // publication finishes before this detach forgets its claim.
        let _gate = self.gate.lock().await;
        self.forget(room, Some(channel));
        if let Some(backend) = backend {
            claims::forget(backend, room, channel).await;
        }
    }

    pub(crate) fn forget(&self, room: &RoomId, channel: Option<&ChannelId>) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let keep = |(held_room, held_channel): &Key| {
            held_room != room || channel.is_some_and(|channel| held_channel != channel)
        };
        state.claims.retain(|key, _| keep(key));
        state.epochs.retain(|key, _| keep(key));
    }

    fn begin(&self, key: Key, retry: bool) -> Option<Arc<()>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if retry && !state.claims.contains_key(&key) {
            return None;
        }
        Some(Arc::clone(state.epochs.entry(key).or_default()))
    }

    fn claim(
        &self,
        key: Key,
        epoch: &Arc<()>,
        online: bool,
        retry: bool,
        shared: bool,
    ) -> Option<Arc<Claim>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state
            .epochs
            .get(&key)
            .is_some_and(|held| Arc::ptr_eq(held, epoch))
        {
            return None;
        }
        if let Some(claim) = state
            .claims
            .get(&key)
            .filter(|claim| claim.online == online)
        {
            return (shared || (retry && claim.pending.load(Ordering::SeqCst)))
                .then(|| Arc::clone(claim));
        }
        let claim = Arc::new(Claim {
            online,
            pending: AtomicBool::new(false),
            reserved: AtomicBool::new(false),
        });
        state.claims.insert(key, Arc::clone(&claim));
        Some(claim)
    }

    fn pending(&self) -> Vec<Key> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .claims
            .iter()
            .filter(|(_, claim)| claim.pending.load(Ordering::SeqCst))
            .map(|(key, _)| key.clone())
            .collect()
    }
}

impl<S: ConversationStore> RoomKit<S> {
    pub(in crate::kit) fn publish_agent_presence_later(&self, room: RoomId) {
        let kit = self.downgrade();
        drop(self.inner.tasks.spawn_alone(Kind::Observer, async move {
            if let Some(kit) = kit.upgrade() {
                kit.publish_agent_presence(&room, None).await;
            }
        }));
    }

    pub(in crate::kit) async fn publish_presence_in_call(
        &self,
        room: &RoomId,
        channel: Option<&ChannelId>,
    ) -> Result<(), RoomKitError> {
        let (kit, room, channel) = (self.clone(), room.clone(), channel.cloned());
        super::call::run(&self.inner, async move {
            kit.publish_agent_presence(&room, channel.as_ref()).await;
        })
        .await
    }

    pub(crate) async fn publish_agent_presence(&self, room: &RoomId, channel: Option<&ChannelId>) {
        self.start_presence_watch();
        let _gate = self.inner.agent_presence.gate.lock().await;
        self.finish_presence_forgets().await;
        let result = self.publish_agents(room, channel).await;
        if let Err(error) = result {
            tracing::warn!(target: "roomkit::turns", %error, "publishing the room's agent presence failed");
        }
        self.retry_agent_presence();
    }

    async fn publish_agents(
        &self,
        room: &RoomId,
        channel: Option<&ChannelId>,
    ) -> Result<(), RoomKitError> {
        let executors = self.executors().await?;
        let channels = if let Some(channel) = channel {
            vec![channel.clone()]
        } else {
            self.store()
                .list_bindings(room)
                .await?
                .into_iter()
                .filter(|binding| binding.category == ChannelCategory::Intelligence)
                .map(|binding| binding.channel_id)
                .collect()
        };
        for channel in channels {
            self.inner.agent_presence.remote.recheck(&channel);
            if let Err(error) = self.publish_agent(room, &channel, false, &executors).await {
                tracing::warn!(target: "roomkit::turns", %room, %channel, %error, "publishing the agent's presence failed; it is tried again");
            }
        }
        Ok(())
    }

    async fn publish_agent(
        &self,
        room: &RoomId,
        channel: &ChannelId,
        retry: bool,
        executors: &[crate::turns::ExecutorInfo],
    ) -> Result<(), RoomKitError> {
        if self
            .inner
            .turns
            .configured_backend()
            .is_some_and(|b| !b.lists_executors())
            && self
                .inner
                .channel(channel)
                .is_some_and(|c| c.is_declaration_only())
        {
            return Ok(());
        }
        let key = (room.clone(), channel.clone());
        let Some(epoch) = self.inner.agent_presence.begin(key.clone(), retry) else {
            return Ok(());
        };
        let organization = self
            .store()
            .get_room(room)
            .await?
            .and_then(|room| room.organization_id);
        let presence = self.presence_of(channel, room, organization, executors)?;
        let backend = self.inner.turns.configured_backend();
        let shared = backend.is_some();
        let Some(claim) =
            self.inner
                .agent_presence
                .claim(key, &epoch, presence.online, retry, shared)
        else {
            return Ok(());
        };
        if let Some(backend) = backend
            && !claims::reserve(backend, &claim, room, channel).await?
        {
            return Ok(());
        }
        let result = self
            .publish_presence(
                room,
                channel.as_str(),
                if claim.online { "online" } else { "offline" },
                PublishPresenceOptions::default(),
            )
            .await;
        // Cancellation keeps the claim but does not create a retry. Only
        // a recoverable refusal queues it, as Python's except Exception.
        // Only this allocation changes: a detach or room close may already
        // have removed it, and a new attach has its own independent claim.
        claim.pending.store(result.is_err(), Ordering::SeqCst);
        result
    }

    fn retry_agent_presence(&self) {
        let watch = &self.inner.agent_presence;
        let mut state = watch.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.retrying
            || !state
                .claims
                .values()
                .any(|claim| claim.pending.load(Ordering::SeqCst))
        {
            return;
        }
        state.retrying = true;
        let weak = self.downgrade();
        let closing = self.inner.turns.cutting();
        let interval = self.turn_executor().timeouts().heartbeat_interval;
        drop(state);
        drop(
            self.inner
                .tasks
                .spawn_alone(Kind::Observer, retry_loop(weak, closing, interval)),
        );
    }

    async fn retry_pending_presence(&self) {
        let _gate = self.inner.agent_presence.gate.lock().await;
        let Ok(executors) = self.executors().await else {
            return;
        };
        for (room, channel) in self.inner.agent_presence.pending() {
            match self.store().get_binding(&room, &channel).await {
                Ok(None) => self.inner.agent_presence.forget(&room, Some(&channel)),
                Ok(Some(_)) => {
                    if let Err(error) = self.publish_agent(&room, &channel, true, &executors).await
                    {
                        tracing::debug!(target: "roomkit::turns", %room, %channel, %error, "publishing the agent's presence failed again");
                    }
                }
                Err(error) => {
                    tracing::debug!(target: "roomkit::turns", %room, %channel, %error, "reading a pending presence binding failed");
                }
            }
        }
    }
}

async fn retry_loop<S: ConversationStore>(
    weak: crate::WeakRoomKit<S>,
    mut closing: tokio::sync::watch::Receiver<bool>,
    interval: std::time::Duration,
) {
    loop {
        let Some(kit) = weak.upgrade() else {
            return;
        };
        if *closing.borrow() {
            return;
        }
        tokio::select! {
            _ = closing.changed() => return,
            () = kit.retry_pending_presence() => {}
        }
        {
            let mut state = kit
                .inner
                .agent_presence
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if !state
                .claims
                .values()
                .any(|claim| claim.pending.load(Ordering::SeqCst))
            {
                state.retrying = false;
                return;
            }
        }
        drop(kit);
        tokio::select! {
            _ = closing.changed() => return,
            () = tokio::time::sleep(interval) => {}
        }
    }
}
