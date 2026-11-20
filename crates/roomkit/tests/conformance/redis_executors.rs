//! RFC §6.4 and §10.6. The normal runner places declared channels on workers
//! with independent Redis nodes and compares the complete scenario expectations.
#[path = "redis_executors/channel.rs"]
mod channel;
#[path = "redis_executors/silent.rs"]
mod silent;
#[path = "redis_executors/site.rs"]
mod site;
#[path = "redis_executors/topology.rs"]
mod topology;

use futures_core::future::BoxFuture;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use roomkit::{RoomKitError, TurnExecutor, TurnExecutorOptions};
use serde::Deserialize;
use serde_json::Value;

use crate::identity::RunSlot;
use channel::WorkerChannel;

pub(super) fn run(
    scenario: &Value,
    runtime: fn() -> tokio::runtime::Runtime,
) -> Option<Vec<String>> {
    let url = crate::scenarios::redis_url()?;
    let unknown = crate::shape::unknown_members(scenario);
    if !unknown.is_empty() {
        return Some(unknown);
    }
    Some(runtime().block_on(async {
        match tokio::time::timeout(crate::SCENARIO_BOUND, exercise(&url, scenario)).await {
            Ok(Ok(problems)) => problems,
            Ok(Err(error)) => vec![error.to_string()],
            Err(_) => vec![format!(
                "distributed executor did not finish within {:?}",
                crate::SCENARIO_BOUND
            )],
        }
    }))
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Worker {
    id: String,
    hosts: BTreeSet<String>,
    organizations: Option<BTreeSet<String>>,
    started: Option<bool>,
    answers: Option<bool>,
    gate: Option<String>,
    builds: Option<BTreeSet<String>>,
    max_channels: Option<usize>,
    #[serde(default)]
    error_classes: Option<Vec<crate::channel_verbs::turn_errors::FixtureClass>>,
    #[serde(default)]
    telemetry: bool,
    #[serde(default)]
    hooks: Vec<Value>,
    #[serde(default)]
    room_hooks: Vec<Value>,
    #[serde(skip)]
    counts: Arc<std::sync::Mutex<BTreeMap<String, usize>>>,
}

impl Worker {
    fn channel(
        &self,
        channel: &str,
        declarations: &[Value],
        slot: &RunSlot,
    ) -> Result<Option<WorkerChannel>, RoomKitError> {
        if self
            .builds
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(channel))
        {
            return Ok(None);
        }
        let Some(spec) = declarations.iter().find(|spec| spec["id"] == channel) else {
            return Ok(None);
        };
        let run = slot
            .run()
            .ok_or_else(|| RoomKitError::Runtime("scenario owner is unavailable".into()))?;
        WorkerChannel::build(spec, &run, self, channel)
            .map(Some)
            .map_err(RoomKitError::Value)
    }

    fn next_build(&self, channel: &str) -> usize {
        let mut counts = crate::run::lock(&self.counts);
        let generation = counts.entry(channel.to_owned()).or_default();
        *generation += 1;
        *generation
    }
}

struct Workers {
    topology: topology::Topology,
    declarations: Vec<Worker>,
    channels: Arc<Vec<Value>>,
    slot: RunSlot,
    active: Mutex<BTreeMap<String, site::Site>>,
    mortal: BTreeSet<String>,
    /// Killed, then started again under their id by a later step: the
    /// restart follows the kill at once, as a supervisor's would.
    restarted: BTreeSet<String>,
}

impl Workers {
    fn build(&self, worker: &Worker) -> Result<TurnExecutor, RoomKitError> {
        let declarations = Arc::clone(&self.channels);
        let observed = self.slot.clone();
        let serving = worker.clone();
        let telemetry: Option<Arc<dyn roomkit::telemetry::TelemetryProvider>> = if worker.telemetry
        {
            let run = self
                .slot
                .run()
                .ok_or_else(|| RoomKitError::Runtime("scenario owner is unavailable".into()))?;
            let recorder = Arc::new(roomkit::telemetry::MockTelemetryProvider::default());
            crate::run::lock(&run.telemetry).insert(
                format!("executor/{id}", id = worker.id),
                Arc::clone(&recorder),
            );
            Some(recorder)
        } else {
            None
        };
        TurnExecutor::new(
            move |channel| {
                std::future::ready(serving.channel(channel.as_str(), &declarations, &observed))
            },
            TurnExecutorOptions {
                executor_id: Some(worker.id.clone()),
                hosts: Some(worker.hosts.clone()),
                organizations: worker.organizations.clone(),
                timeouts: self.topology.owner().timeouts(),
                max_channels: worker.max_channels,
                telemetry,
                error_classes: worker
                    .error_classes
                    .as_deref()
                    .map(crate::channel_verbs::turn_errors::classes)
                    .unwrap_or_default(),
                ..TurnExecutorOptions::default()
            },
        )
    }
    async fn start(&self, id: &str) -> Result<(), RoomKitError> {
        let worker = self
            .declarations
            .iter()
            .find(|w| w.id == id)
            .ok_or_else(|| RoomKitError::Value(format!("unknown executor: {id}")))?;
        let mut active = self.active.lock().await;
        if active.contains_key(id) {
            return Err(RoomKitError::Value(format!(
                "executor {id} already started"
            )));
        }
        let executor = self.build(worker)?;
        let run = self
            .slot
            .run()
            .ok_or_else(|| RoomKitError::Runtime("scenario owner is unavailable".into()))?;
        for (kind, hooks) in [
            ("kit_executor.hook", &worker.hooks),
            ("kit_executor.room_hook", &worker.room_hooks),
        ] {
            for hook in hooks {
                crate::kit_executor::register(&run, &executor, kind, hook)
                    .map_err(RoomKitError::Value)??;
            }
        }
        let site = if worker.answers == Some(false) {
            site::Site::silent(&self.topology, executor, worker, run).await?
        } else {
            site::Site::start(&self.topology, executor, self.mortal.contains(id)).await?
        };
        active.insert(id.to_owned(), site);
        Ok(())
    }

    /// Withdraw the declaration while retaining the worker, its builds and its active turns.
    /// Resuming re-advertises that same instance on the same Redis node.
    async fn advertise(&self, id: &str, paused: bool) -> Result<(), RoomKitError> {
        let active = self.active.lock().await;
        let site = active
            .get(id)
            .ok_or_else(|| RoomKitError::Value(format!("executor {id} is not running")))?;
        if !paused {
            return site.advertise(site.executor.clone()).await;
        }
        let no_channels = TurnExecutor::new(
            |_| {
                std::future::ready(Err::<Option<WorkerChannel>, _>(RoomKitError::Runtime(
                    "a paused executor was asked to build a channel".into(),
                )))
            },
            TurnExecutorOptions {
                executor_id: Some(id.to_owned()),
                hosts: Some(BTreeSet::new()),
                ..TurnExecutorOptions::default()
            },
        )?;
        site.advertise(no_channels).await
    }

    async fn remove(&self, id: &str) -> Result<site::Site, RoomKitError> {
        self.active
            .lock()
            .await
            .remove(id)
            .ok_or_else(|| RoomKitError::Value(format!("executor {id} is not started")))
    }
}

impl crate::run::WorkerControl for Workers {
    fn publication_taken<'a>(&'a self, id: &'a str) -> BoxFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            let hosting = self
                .active
                .lock()
                .await
                .get(id)
                .map(|site| site.hosting.clone())
                .ok_or_else(|| RoomKitError::Value(format!("executor {id} is not running")))?;
            roomkit::testing::settle_redis_publications(&hosting, self.topology.owner()).await
        })
    }

    fn start_all(&self) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(async move {
            for worker in &self.declarations {
                if worker.started.unwrap_or(true) {
                    self.start(&worker.id).await?;
                }
            }
            Ok(())
        })
    }

    fn change<'a>(
        &'a self,
        operation: &'a str,
        id: &'a str,
    ) -> BoxFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            match operation {
                "executor.start" => self.start(id).await,
                "executor.pause" => self.advertise(id, true).await,
                "executor.resume" => self.advertise(id, false).await,
                "executor.close" => self.remove(id).await?.close(&self.topology).await,
                "executor.kill" => {
                    let observe = !self.restarted.contains(id);
                    self.remove(id).await?.kill(&self.topology, observe).await
                }
                other => Err(RoomKitError::Value(format!(
                    "unsupported executor operation: {other}"
                ))),
            }
        })
    }
}

async fn exercise(url: &str, scenario: &Value) -> Result<Vec<String>, RoomKitError> {
    #[derive(Deserialize)]
    struct Setup {
        executors: Vec<Worker>,
        #[serde(default)]
        channels: Vec<Value>,
    }
    let setup: Setup = serde_json::from_value(scenario["setup"].clone())
        .map_err(|error| RoomKitError::Value(error.to_string()))?;
    let workers = Arc::new(Workers {
        topology: topology::Topology::new(url, scenario)?,
        declarations: setup.executors,
        channels: Arc::new(setup.channels),
        slot: RunSlot::default(),
        active: Mutex::default(),
        mortal: scenario["steps"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|step| step["call"] == "executor.kill")
            .filter_map(|step| step["executor"].as_str().map(str::to_owned))
            .collect(),
        restarted: restarted(scenario),
    });
    let owner = workers.topology.owner().clone();
    let backend = roomkit::turns::TurnBackendHandle::new(Arc::new(owner.clone()));
    let problems = crate::harness::run_with(
        scenario,
        workers.slot.clone(),
        Some(Arc::<Workers>::clone(&workers)),
        |mut options| {
            options.turn_backend = Some(backend.clone());
            options
        },
    )
    .await;
    let running: Vec<_> = workers.active.lock().await.keys().cloned().collect();
    for id in running {
        workers.remove(&id).await?.close(&workers.topology).await?;
    }
    owner.close().await?;
    Ok(problems)
}

/// The executors a step kills and a later step starts again under the same id.
fn restarted(scenario: &Value) -> BTreeSet<String> {
    let steps: Vec<&Value> = scenario["steps"].as_array().into_iter().flatten().collect();
    steps
        .iter()
        .enumerate()
        .filter(|(_, step)| step["call"] == "executor.kill")
        .filter_map(|(at, step)| {
            let id = step["executor"].as_str()?;
            steps
                .get(at + 1..)?
                .iter()
                .any(|later| later["call"] == "executor.start" && later["executor"] == id)
                .then(|| id.to_owned())
        })
        .collect()
}

#[test]
fn an_executor_inventory_needs_no_channel_definitions() {
    if !crate::backend_available() {
        return;
    }
    let scenario = serde_json::json!({
        "id": "catalog/no-channels",
        "setup": {"executors": []},
        "steps": [{"call": "executors", "expect": {"result": []}}]
    });
    if let Some(problems) = run(&scenario, crate::scenarios::ONE_THREAD.build) {
        assert!(problems.is_empty(), "{problems:?}");
    }
}
