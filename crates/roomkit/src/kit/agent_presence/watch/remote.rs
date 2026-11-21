//! Discover changes to declared agents' coverage and claim their publications across owners.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, PoisonError};

use roomkit_core::model::{ChannelId, RoomId, RoomStatus};

use crate::kit::{RoomKit, read};
use crate::store::FindRoomIdsByChannelOptions;
use crate::turns::ExecutorInfo;
use crate::{ConversationStore, RoomFilter, RoomKitError};

const ROOMS_READ: usize = 10_000;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Cover {
    executor_id: String,
    hooks: BTreeSet<String>,
    organizations: Option<BTreeSet<String>>,
}

#[derive(Debug, Default)]
pub(super) struct Monitor {
    started: AtomicBool,
    covering: Mutex<BTreeMap<ChannelId, BTreeSet<Cover>>>,
    pub(super) forgetting: Mutex<BTreeSet<RoomId>>,
}

impl Monitor {
    // A room can observe a worker that comes and goes between monitor polls.
    // Recheck that channel's rooms even if the registry returns to its old value.
    pub(super) fn recheck(&self, agent: &ChannelId) {
        self.covering
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(agent);
    }
}

impl<S: ConversationStore> RoomKit<S> {
    pub(in crate::kit) fn wire_presence_watch(&self) {
        let Some(backend) = self.inner.turns.configured_backend() else {
            return;
        };
        let concrete: &dyn std::any::Any = &**backend;
        if let Some(memory) = concrete.downcast_ref::<crate::InMemoryTurnBackend>() {
            let weak = self.downgrade();
            memory.coverage.subscribe(Arc::new(move || {
                let kit = weak.upgrade()?;
                Some(Box::pin(async move { kit.coverage_notified(true).await }))
            }));
        }
        #[cfg(feature = "redis")]
        if let Some(redis) = &self.inner.turns.remote {
            let weak = self.downgrade();
            redis.watch_coverage(Arc::new(move |notice| {
                let kit = weak.upgrade()?;
                Some(Box::pin(async move {
                    kit.coverage_notified(
                        notice == crate::turns::redis::CoverageNotice::ExecutorsChanged,
                    )
                    .await;
                }))
            }));
        }
    }

    async fn coverage_notified(&self, changed: bool) {
        self.start_presence_watch();
        if changed
            && self
                .inner
                .agent_presence
                .remote
                .started
                .load(Ordering::Acquire)
        {
            self.check_agent_coverage().await;
        }
    }

    pub(in crate::kit) fn start_presence_watch(&self) {
        let Some(backend) = self
            .inner
            .turns
            .configured_backend()
            .filter(|b| b.lists_executors())
        else {
            return;
        };
        let watch = &self.inner.agent_presence.remote;
        if watch.started.load(Ordering::Acquire)
            || tokio::runtime::Handle::try_current().is_err()
            || self.declared_agents().is_empty()
            || self.inner.tasks.is_closing()
            || watch.started.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let weak = self.downgrade();
        let mut closing = self.inner.turns.cutting();
        let interval = backend.timeouts().unwrap_or_default().interval();
        // A permanent registry monitor is idle between heartbeats and must not hold
        // a kit task open. Dropping or closing the owner stops it, including a pending read.
        drop(tokio::spawn(async move {
            loop {
                let Some(kit) = weak.upgrade() else { return };
                if *closing.borrow() {
                    return;
                }
                tokio::select! {
                    _ = closing.changed() => return,
                    () = kit.check_agent_coverage() => {}
                }
                drop(kit);
                tokio::select! {
                    _ = closing.changed() => return,
                    () = tokio::time::sleep(interval) => {}
                }
            }
        }));
    }

    fn declared_agents(&self) -> Vec<ChannelId> {
        read(&self.inner.channels)
            .iter()
            .filter(|(_, ch)| ch.is_declaration_only())
            .map(|(id, _)| id.clone())
            .collect()
    }

    async fn check_agent_coverage(&self) {
        if self.inner.tasks.is_closing() {
            return;
        }
        let watch = &self.inner.agent_presence;
        let _gate = watch.gate.lock().await;
        self.finish_presence_forgets().await;
        let infos = match self.executors().await {
            Ok(infos) => infos,
            Err(error) => {
                tracing::warn!(%error, "reading agent coverage failed");
                return;
            }
        };
        for agent in self.declared_agents() {
            if let Err(error) = self.refresh_coverage(&agent, &infos).await {
                tracing::warn!(%agent, %error, "reading rooms covered by the agent failed");
            }
        }
        self.retry_agent_presence();
    }

    async fn refresh_coverage(
        &self,
        agent: &ChannelId,
        infos: &[ExecutorInfo],
    ) -> Result<(), RoomKitError> {
        let mut covering = BTreeSet::new();
        for info in infos {
            let organization = info
                .organizations
                .as_ref()
                .and_then(BTreeSet::first)
                .map(String::as_str);
            if info.covers(agent.as_str(), &[], organization)? {
                covering.insert(Cover {
                    executor_id: info.executor_id.clone(),
                    hooks: info.room_hooks.clone(),
                    organizations: info.organizations.clone(),
                });
            }
        }
        let seen = &self.inner.agent_presence.remote.covering;
        if seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(agent)
            == Some(&covering)
        {
            return Ok(());
        }
        let rooms = self
            .store()
            .find_room_ids_by_channel(
                agent,
                FindRoomIdsByChannelOptions {
                    room_filter: Some(RoomFilter {
                        statuses: Some(vec![RoomStatus::Active, RoomStatus::Paused]),
                        ..RoomFilter::default()
                    }),
                    limit: ROOMS_READ,
                    group: None,
                },
            )
            .await?;
        if rooms.len() == ROOMS_READ {
            tracing::warn!(%agent, "publishing agent presence in only the oldest {ROOMS_READ} rooms");
        }
        for room in rooms {
            if let Err(error) = self.publish_agent(&room, agent, false, infos).await {
                tracing::warn!(%room, %agent, %error, "publishing the agent's presence failed");
            }
        }
        seen.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(agent.clone(), covering);
        Ok(())
    }
}
