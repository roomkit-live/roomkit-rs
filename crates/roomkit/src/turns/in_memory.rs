//! Explicit single-process turn transport (RFC §10.6.1, §10.6.7).
mod coverage;
pub(crate) use coverage::{Coverage, Serving};
mod links;
mod placement;
mod port;
#[cfg(feature = "redis")]
pub(super) use port::{local_call, local_publication};

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};

use super::records::RoomRecords;
use super::{
    ChannelControl, ChannelResult, ExecutorInfo, ExecutorInfoOptions, LocalPresenceClaims,
    MemoryTurn, RoomOwner, ServedExecutor, TurnBackend, TurnFuture, TurnLink, TurnRequest, lock,
};
use crate::RoomKitError;

type Worker = Arc<dyn ServedExecutor>;

#[derive(Default)]
struct State {
    workers: Vec<Worker>,
    owner: Option<Arc<dyn RoomOwner>>,
    holders: RoomRecords<Worker>,
    turns: BTreeMap<u64, Weak<MemoryTurn>>,
    next: u64,
    revision: u64,
    closed: bool,
}

/// A turn transport for room owners and executors in the same process.
/// Clones share the registered workers, room affinity, and active links.
#[derive(Clone, Default)]
pub struct InMemoryTurnBackend {
    state: Arc<Mutex<State>>,
    claims: Arc<LocalPresenceClaims>,
    pub(crate) coverage: Arc<Coverage>,
}

impl fmt::Debug for InMemoryTurnBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryTurnBackend")
            .finish_non_exhaustive()
    }
}

impl InMemoryTurnBackend {
    /// Create an empty process-local transport.
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the locally served workers in registration order.
    ///
    /// # Errors
    /// The in-memory catalogue can always be read.
    pub fn executors(&self) -> TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>> {
        Box::pin(async { Ok(self.catalogue()) })
    }

    /// Claim a process-local presence change shared by clones of this transport.
    pub fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> TurnFuture<'a, Result<bool, RoomKitError>> {
        Box::pin(async move { Ok(self.claims.claim(channel_id, room_id, online)) })
    }

    fn open(&self) -> Result<(), RoomKitError> {
        if lock(&self.state).closed {
            return Err(closed());
        }
        Ok(())
    }

    async fn serve_worker(&self, worker: Worker) {
        worker.reach_owner(Arc::new(port::OwnerPort(Arc::downgrade(&self.state))));
        let identity = worker.native().lifecycle();
        let owner = {
            let mut state = lock(&self.state);
            if !state
                .workers
                .iter()
                .any(|prior| prior.native().lifecycle().ptr_eq(&identity))
            {
                state.workers.push(worker);
                state.revision = state.revision.wrapping_add(1);
            }
            state.owner.clone()
        };
        if owner
            .and_then(|port| port.executor())
            .is_some_and(|own| own.native().lifecycle().ptr_eq(&identity))
        {
            return;
        }
        if let Some(lifecycle) = identity.upgrade() {
            lifecycle.memory_backends.listen(&self.coverage);
        }
        self.coverage.changed().await;
    }

    fn attend_owner(&self, owner: Arc<dyn RoomOwner>) {
        let mut state = lock(&self.state);
        if let Some(first) = &state.owner {
            if !Arc::ptr_eq(first, &owner) {
                tracing::warn!(
                    "A second room owner attends this in-memory turn backend: the work of a turn after it ended reaches the first one only"
                );
            }
            return;
        }
        state.owner = Some(owner);
        state.revision = state.revision.wrapping_add(1);
    }

    fn catalogue(&self) -> Vec<ExecutorInfo> {
        let (workers, owner) = {
            let state = lock(&self.state);
            (state.workers.clone(), state.owner.clone())
        };
        let own = owner
            .and_then(|owner| owner.executor())
            .map(|e| e.native().lifecycle());
        workers
            .into_iter()
            .filter(|e| {
                !e.closed()
                    && own
                        .as_ref()
                        .is_none_or(|id| !id.ptr_eq(&e.native().lifecycle()))
            })
            .map(|e| {
                ExecutorInfo::new(
                    e.executor_id(),
                    ExecutorInfoOptions {
                        hosts: e.hosted(),
                        organizations: e.organizations(),
                        room_hooks: e.room_hooks(),
                        ..Default::default()
                    },
                )
                .with_local_hosts(e.native().local_hosts())
            })
            .collect()
    }
}

impl TurnBackend for InMemoryTurnBackend {
    fn submit(
        &self,
        request: TurnRequest,
    ) -> TurnFuture<'_, Result<Arc<dyn TurnLink>, RoomKitError>> {
        Box::pin(async move {
            let worker = self.select(
                &request.channel_id,
                &request.room_id,
                &request.room_hooks,
                request.context.room.organization_id.as_deref(),
                true,
            )?;
            links::submit(self, &worker, request)
        })
    }

    fn serve(&self, worker: Worker) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            self.serve_worker(worker).await;
            Ok(())
        })
    }

    fn attend(&self, owner: Arc<dyn RoomOwner>) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            self.attend_owner(owner);
            Ok(())
        })
    }

    fn control(
        &self,
        message: ChannelControl,
    ) -> TurnFuture<'_, Result<Option<ChannelResult>, RoomKitError>> {
        Box::pin(self.apply(message))
    }

    fn executors(&self) -> Option<TurnFuture<'_, Result<Vec<ExecutorInfo>, RoomKitError>>> {
        Some(self.executors())
    }

    fn claim_presence<'a>(
        &'a self,
        channel_id: &'a str,
        room_id: &'a str,
        online: Option<bool>,
    ) -> Option<TurnFuture<'a, Result<bool, RoomKitError>>> {
        Some(self.claim_presence(channel_id, room_id, online))
    }

    fn close(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            let live = {
                let mut state = lock(&self.state);
                state.closed = true;
                std::mem::take(&mut state.turns)
            };
            for turn in live.into_values().filter_map(|turn| turn.upgrade()) {
                turn.shut();
            }
            Ok(())
        })
    }
}

fn closed() -> RoomKitError {
    super::TurnError::link_reason("backend_closed").into_kit_error()
}
