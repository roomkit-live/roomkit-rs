//! Construction, declarations and lazy channel registration of a worker executor.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError, Weak};

use roomkit_core::model::ChannelId;
use tokio::sync::watch;

use crate::channel::{BoxFuture, DynChannel};
use crate::turn::TurnKit;
#[cfg(test)]
use crate::turns::CarriedRequest;
use crate::turns::wire::WireError;
use crate::turns::{WorkerTurn, executor};
use crate::{ConversationStore, RoomKitError};

use super::channels::Generations;
use super::pool::Pool;
use super::{TurnExecutor, WorkerChannel};

mod constructor;
pub use constructor::TurnExecutorOptions;

type Built = Result<Option<Arc<dyn DynChannel>>, Box<WireError>>;
struct Building(watch::Receiver<Option<Built>>);

impl Building {
    async fn result(&self) -> Built {
        let mut reply = self.0.clone();
        reply
            .wait_for(Option::is_some)
            .await
            .ok()
            .and_then(|done| done.clone())
            .unwrap_or_else(|| {
                Err(executor::factory_error(&RoomKitError::Runtime(
                    "the channel factory task ended without its result".into(),
                )))
            })
    }
}
type HostPredicate = dyn Fn(&ChannelId) -> bool + Send + Sync;
type Factory = dyn Fn(ChannelId) -> BoxFuture<'static, Built> + Send + Sync;

pub(super) struct Standalone {
    id: String,
    pub(super) hosts: Option<BTreeSet<String>>,
    organizations: Option<BTreeSet<String>>,
    factory: Box<Factory>,
    building: Mutex<HashMap<ChannelId, Weak<Building>>>,
    pub(super) generations: Arc<Generations>,
    pub(super) owns_channels: bool,
    pub(super) pool: Option<Arc<Pool>>,
    predicate: Option<Box<HostPredicate>>,
}

impl crate::turns::LocalHosts for Standalone {
    fn hosts_name(&self, id: &str) -> bool {
        if let Some(predicate) = &self.predicate {
            return ChannelId::new(id).is_ok_and(|id| predicate(&id));
        }
        self.matches_host_entry(id)
    }
}

impl Standalone {
    pub(super) fn executor_id(&self) -> &str {
        &self.id
    }

    pub(super) fn hosts(&self, id: &ChannelId) -> bool {
        if let Some(predicate) = &self.predicate {
            return predicate(id);
        }
        self.matches_host_entry(id.as_str())
    }

    fn matches_host_entry(&self, id: &str) -> bool {
        self.hosts.as_ref().is_none_or(|hosts| {
            hosts.iter().any(|name| {
                name.strip_suffix('*')
                    .map_or(name == id, |prefix| id.starts_with(prefix))
            })
        })
    }

    pub(super) fn serves(&self, organization: Option<&str>) -> bool {
        self.organizations
            .as_ref()
            .map_or(organization.is_none(), |names| {
                names.contains("*") || organization.is_some_and(|id| names.contains(id))
            })
    }

    pub(super) async fn wait_for_build(&self, id: &ChannelId) {
        let pending = self
            .building
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id)
            .and_then(Weak::upgrade);
        if let Some(pending) = pending {
            drop(pending.result().await);
        }
    }

    pub(super) async fn finish_builds(&self) {
        let pending: Vec<_> = self
            .building
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        for build in pending {
            drop(build.result().await);
        }
    }

    pub(super) fn lookup(&self, id: ChannelId) -> BoxFuture<'static, Built> {
        (self.factory)(id)
    }

    async fn build(&self, id: ChannelId, recent: bool) -> Built {
        let entry = {
            let mut pending = self.building.lock().unwrap_or_else(PoisonError::into_inner);
            pending.retain(|_, state| state.strong_count() != 0);
            if let Some(entry) = pending.get(&id).and_then(Weak::upgrade) {
                entry
            } else {
                let (answer, received) = watch::channel(None);
                let entry = Arc::new(Building(received));
                pending.insert(id.clone(), Arc::downgrade(&entry));
                let keep = Arc::clone(&entry);
                let building = self.pool.as_ref().map(|pool| pool.hold(&id));
                let build = (self.factory)(id);
                tokio::spawn(async move {
                    let result = build.await;
                    if let (Some(used), Ok(Some(_))) = (&building, &result) {
                        used.record(recent);
                    }
                    answer.send_replace(Some(result));
                    drop(keep);
                });
                entry
            }
        };
        entry.result().await
    }
}

impl<S: ConversationStore> TurnExecutor<S> {
    /// The kit serves every organization (`Some({"*"})`). A standalone worker with
    /// no organization declaration serves rooms without an organization (`None`).
    pub fn organizations(&self) -> Option<BTreeSet<String>> {
        self.standalone.as_ref().map_or_else(
            || Some(BTreeSet::from(["*".to_owned()])),
            |worker| worker.organizations.clone(),
        )
    }

    pub(super) async fn worker_channel(
        &self,
        id: ChannelId,
        organization: Option<&str>,
        recent: bool,
    ) -> Result<WorkerChannel, Box<WireError>> {
        if !self.serves(organization) {
            return Err(executor::refused(&id, self.executor_id(), organization));
        }
        let channel = match &self.standalone {
            Some(worker) if !worker.owns_channels => worker
                .lookup(id.clone())
                .await?
                .map(WorkerChannel::registered),
            Some(worker) => {
                let generation = worker.generations.of(&id);
                let _build = generation.building.read().await;
                let used = worker.pool.as_ref().map(|pool| pool.hold(&id));
                let channel = match self.kit.inner.channel(&id) {
                    Some(channel) => Some(channel),
                    None => worker.build(id.clone(), recent).await?,
                };
                match channel {
                    Some(channel) => {
                        if let Some(used) = &used {
                            used.record(recent);
                        }
                        Some(generation.using(channel, used).await)
                    }
                    None => None,
                }
            }
            None => self.kit.inner.channel(&id).map(WorkerChannel::registered),
        };
        channel.ok_or_else(|| executor::missing(&id, self.executor_id()))
    }

    #[cfg(test)]
    pub(crate) fn take_carried(&self, carried: CarriedRequest) -> Result<WorkerTurn, RoomKitError> {
        self.take_from(carried, executor::WorkerOrigin::default())
    }

    #[cfg(test)]
    pub(crate) fn take_from(
        &self,
        carried: CarriedRequest,
        origin: executor::WorkerOrigin,
    ) -> Result<WorkerTurn, RoomKitError> {
        let request = executor::parse(carried).map_err(|why| RoomKitError::TurnLink {
            reason: "frame_invalid".into(),
            message: Some(why),
        })?;
        self.start_request(request, origin)
    }

    pub(super) fn start_request(
        &self,
        request: crate::turns::TurnRequest,
        origin: executor::WorkerOrigin,
    ) -> Result<WorkerTurn, RoomKitError> {
        let entered = self
            .serves(request.context.room.organization_id.as_deref())
            .then(|| self.wiring());
        let channel_id = request.channel_id.clone();
        let organization = request.context.room.organization_id.clone();
        let room = request.room_id.clone();
        let names = request.room_hooks.clone();
        let facade = self.clone();
        let build = Box::pin(async move {
            if !facade.serves(organization.as_deref()) {
                return Err(executor::refused(
                    &channel_id,
                    facade.executor_id(),
                    organization.as_deref(),
                ));
            }
            let hooks = facade.hooks();
            if !hooks.defines_room_hooks(&names) {
                return Err(executor::no_executor(format!(
                    "Executor {} defines no room hook among {names:?}",
                    facade.executor_id()
                )));
            }
            if facade.standalone.is_some() {
                hooks.use_room_hooks(&room, names);
            }
            // Turn placement checks the advertised hosts. A direct channel control
            // already names the channel whose host registry it is asking.
            if !facade.hosts_channel(&channel_id) {
                return Err(executor::no_executor(format!(
                    "No executor serves channel '{channel_id}'"
                )));
            }
            facade
                .worker_channel(channel_id, organization.as_deref(), true)
                .await
        });
        let kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(&self.kit.inner));
        WorkerTurn::start(
            &self.kit.inner.turns,
            &kit,
            self.executor_id().to_owned(),
            request,
            build,
            origin,
            entered,
        )
    }
}
