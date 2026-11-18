//! Construction, declarations and lazy channel registration of a worker executor.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use roomkit_core::model::ChannelId;
use tokio::sync::watch;

use crate::channel::{BoxFuture, DynChannel};
use crate::turn::TurnKit;
use crate::turns::wire::WireError;
use crate::turns::{CarriedRequest, WorkerTurn, executor};
use crate::{
    Channel, ConversationStore, InMemoryStore, RoomKit, RoomKitError, RoomKitOptions, TurnTimeouts,
};

use super::channels::Generations;
use super::{TurnExecutor, WorkerChannel};

/// The executor's advertised identity, tenant/host scope, and liveness bounds.
#[derive(Clone, Debug, Default)]
pub struct TurnExecutorOptions {
    /// Exact channel IDs and terminal-wildcard prefixes. Omit for a local-only open declaration.
    pub hosts: Option<BTreeSet<String>>,
    /// Exact organizations, or "*" for all. Omit to serve only rooms without an organization.
    pub organizations: Option<BTreeSet<String>>,
    /// The identity advertised to owners. Defaults to a freshly generated executor ID.
    pub executor_id: Option<String>,
    /// The heartbeat bounds for work on this executor.
    pub timeouts: TurnTimeouts,
}

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
type Factory = dyn Fn(ChannelId) -> BoxFuture<'static, Built> + Send + Sync;

pub(super) struct Standalone {
    id: String,
    pub(super) hosts: Option<BTreeSet<String>>,
    organizations: Option<BTreeSet<String>>,
    factory: Box<Factory>,
    building: Mutex<HashMap<ChannelId, Weak<Building>>>,
    pub(super) generations: Generations,
}

impl Standalone {
    pub(super) fn executor_id(&self) -> &str {
        &self.id
    }

    pub(super) fn hosts(&self, id: &ChannelId) -> bool {
        self.hosts.as_ref().is_none_or(|hosts| {
            hosts.iter().any(|name| {
                name.strip_suffix('*')
                    .map_or(name == id.as_str(), |prefix| {
                        id.as_str().starts_with(prefix)
                    })
            })
        })
    }

    pub(super) fn organizations(&self) -> BTreeSet<String> {
        self.organizations.clone().unwrap_or_default()
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

    async fn build(&self, id: ChannelId) -> Built {
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
                let build = (self.factory)(id);
                tokio::spawn(async move {
                    answer.send_replace(Some(build.await));
                    drop(keep);
                });
                entry
            }
        };
        entry.result().await
    }
}

impl TurnExecutor<InMemoryStore> {
    /// Construct an independent executor whose factory builds a channel the first
    /// time a turn needs it. A successful build is shared by turns of that channel.
    ///
    /// # Errors
    /// Refuses a misplaced host wildcard, an empty organization declaration or a
    /// wildcard in an organization ID other than "*".
    pub fn new<F, Fut, C>(
        build_channel: F,
        options: TurnExecutorOptions,
    ) -> Result<Self, RoomKitError>
    where
        F: Fn(ChannelId) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<C>, RoomKitError>> + Send + 'static,
        C: Channel,
    {
        if options
            .hosts
            .iter()
            .flatten()
            .any(|name| name.strip_suffix('*').unwrap_or(name).contains('*'))
        {
            return Err(RoomKitError::Value(
                "a host pattern may contain '*' only at its end".into(),
            ));
        }
        if options.organizations.as_ref().is_some_and(|names| {
            names.is_empty() || names.iter().any(|name| name.contains('*') && name != "*")
        }) {
            return Err(RoomKitError::Value(
                "organizations must be nonempty and contain exact IDs or '*'".into(),
            ));
        }
        let kit = RoomKit::new(RoomKitOptions::default());
        kit.inner.turns.set_timeouts(options.timeouts.bounded());
        let id = options
            .executor_id
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| kit.inner.turns.executor_id().to_owned());
        let registry = kit.clone();
        let build_channel = Arc::new(build_channel);
        let factory = move |id: ChannelId| -> BoxFuture<'static, Built> {
            let kit = registry.clone();
            let factory = Arc::clone(&build_channel);
            Box::pin(async move {
                if let Some(channel) = kit.inner.channel(&id) {
                    return Ok(Some(channel));
                }
                let Some(channel) = factory(id.clone())
                    .await
                    .map_err(|error| executor::factory_error(&error))?
                else {
                    return Ok(None);
                };
                if channel.channel_id() != id {
                    return Err(executor::factory_error(&RoomKitError::Value(format!(
                        "the factory returned {} for {id}",
                        channel.channel_id()
                    ))));
                }
                kit.register_channel(channel)
                    .map_err(|error| executor::factory_error(&error))?;
                Ok(kit.inner.channel(&id))
            })
        };
        let standalone = Standalone {
            id,
            hosts: options.hosts,
            organizations: options.organizations,
            factory: Box::new(factory),
            building: Mutex::default(),
            generations: Generations::default(),
        };
        Ok(Self {
            kit,
            standalone: Some(Arc::new(standalone)),
        })
    }
}

impl<S: ConversationStore> TurnExecutor<S> {
    pub(crate) fn advertised_organizations(&self) -> Option<BTreeSet<String>> {
        self.standalone.as_ref().map_or_else(
            || Some(self.organizations()),
            |worker| worker.organizations.clone(),
        )
    }

    pub(super) async fn worker_channel(
        &self,
        id: ChannelId,
        organization: Option<&str>,
    ) -> Result<WorkerChannel, Box<WireError>> {
        if !self.serves(organization) {
            return Err(executor::refused(&id, self.executor_id(), organization));
        }
        if !self.hosts_channel(&id) {
            return Err(executor::no_executor(format!(
                "No executor serves channel '{id}'"
            )));
        }
        let channel = match &self.standalone {
            Some(worker) => {
                let generation = worker.generations.of(&id);
                let _build = generation.building.read().await;
                if let Some(channel) = self.kit.inner.channel(&id) {
                    return Ok(generation.using(channel).await);
                }
                match worker.build(id.clone()).await? {
                    Some(channel) => Some(generation.using(channel).await),
                    None => None,
                }
            }
            None => self.kit.inner.channel(&id).map(WorkerChannel::registered),
        };
        channel.ok_or_else(|| executor::missing(&id, self.executor_id()))
    }

    pub(crate) fn take(&self, carried: CarriedRequest) -> Result<WorkerTurn, RoomKitError> {
        let request = executor::parse(carried).map_err(|why| RoomKitError::TurnLink {
            reason: "frame_invalid".into(),
            message: Some(why),
        })?;
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
            facade
                .worker_channel(channel_id, organization.as_deref())
                .await
        });
        let kit: Arc<dyn TurnKit> = Arc::new(Arc::downgrade(&self.kit.inner));
        WorkerTurn::start(
            &self.kit.inner.turns,
            &kit,
            self.executor_id().to_owned(),
            request,
            build,
        )
    }
}
