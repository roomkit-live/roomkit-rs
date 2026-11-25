//! RFC §6.4 and §10.6. The normal runner places declared channels on workers
//! with independent Redis nodes and compares the complete scenario expectations.
#[path = "redis_executors/channel.rs"]
mod channel;
#[path = "redis_executors/own_process.rs"]
mod own_process;
#[path = "redis_executors/owner_kill.rs"]
mod owner_kill;
#[path = "redis_executors/presence.rs"]
mod presence;
#[path = "redis_executors/reserved.rs"]
mod reserved;
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

/// `scenario` run a second time, its kit served by the runner's own
/// transport, written on the public contract alone (roomkit-specs
/// `conformance/README.md`, "On a transport written on the contract alone").
/// None for a scenario whose kit the runner serves by a transport of its own
/// already, and without a Redis server.
pub(super) fn run_on_the_contract_alone(
    scenario: &Value,
    runtime: fn() -> tokio::runtime::Runtime,
) -> Option<Vec<String>> {
    if on_a_transport_of_the_runner(scenario) {
        return None;
    }
    let mut again = scenario.clone();
    again["setup"]["kit"]["turn_backend_contract_only"] = Value::Bool(true);
    run(&again, runtime)
}

/// Whether the runner serves the scenario's kit by a transport of its own:
/// the one a setting of its kit asks for, or the one a step that kills the
/// owner or reserves an executor has the kit's turns cross
/// (`Workers::kit_connection`).
fn on_a_transport_of_the_runner(scenario: &Value) -> bool {
    let kit = &scenario["setup"]["kit"];
    kit["turn_backend_contract_only"] == true
        || kit["turn_backend_faults"] == true
        || kit["executor_catalogue"] == false
        || kills_owner(scenario)
        || calls(scenario, "executor.reserve")
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Worker {
    id: String,
    hosts: Option<BTreeSet<String>>,
    host_predicate: Option<BTreeSet<String>>,
    kit: Option<String>,
    organizations: Option<BTreeSet<String>>,
    started: Option<bool>,
    answers: Option<bool>,
    gate: Option<String>,
    builds: Option<BTreeSet<String>>,
    max_channels: Option<usize>,
    /// Runs as in a process of its own: on a runtime and a thread of its own.
    own_process: Option<bool>,
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
    presence: presence::Presence,
    declarations: Vec<Worker>,
    channels: Arc<Vec<Value>>,
    slot: RunSlot,
    active: Mutex<BTreeMap<String, site::Site>>,
    mortal: BTreeSet<String>,
    /// Killed, then started again under their id by a later step: the
    /// restart follows the kill at once, as a supervisor's would.
    restarted: BTreeSet<String>,
    /// The kit's turn backend, when a step kills the kit as its turns' owner.
    owner_kill: Option<Arc<owner_kill::OwnerKill>>,
    /// The kit's turn backend, when a step reserves an executor for its turns.
    reserved: std::sync::OnceLock<Arc<reserved::Reserved>>,
}

impl Workers {
    /// The kit's connection to `owner`, its turns crossing the wrappers the
    /// scenario's steps need: one a kill silences, and over it one that
    /// submits them to a reserved executor.
    fn kit_connection(
        &self,
        scenario: &Value,
        owner: roomkit::RedisTurnBackend,
    ) -> crate::run::OwnerConnection {
        use roomkit::turns::TurnBackendHandle;
        let mut main = self.presence.connection(owner);
        if let Some(killable) = &self.owner_kill {
            // The kit's turns cross the wrapper alone.
            main.turns = TurnBackendHandle::new(Arc::<owner_kill::OwnerKill>::clone(killable));
        }
        if calls(scenario, "executor.reserve") {
            let reserving = reserved::Reserved::new(main.turns.clone(), self.slot.clone());
            let reserving = self.reserved.get_or_init(|| Arc::new(reserving));
            main.turns = TurnBackendHandle::new(Arc::<reserved::Reserved>::clone(reserving));
        }
        main
    }

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
        let build = move |channel: roomkit::model::ChannelId| {
            std::future::ready(serving.channel(channel.as_str(), &declarations, &observed))
        };
        let options = TurnExecutorOptions {
            executor_id: Some(worker.id.clone()),
            hosts: worker.hosts.clone(),
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
        };
        if let Some(accepted) = worker.host_predicate.clone() {
            TurnExecutor::new_with_host_predicate(
                build,
                move |id| accepted.contains(id.as_str()),
                options,
            )
        } else {
            TurnExecutor::new(build, options)
        }
    }
    async fn start(&self, id: &str) -> Result<(), RoomKitError> {
        let worker = self
            .declarations
            .iter()
            .find(|w| w.id == id)
            .ok_or_else(|| RoomKitError::Value(format!("unknown executor: {id}")))?;
        if worker.own_process == Some(true) {
            own_process::check(worker, &self.channels)?;
        }
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
            let isolated = self.mortal.contains(id) || worker.own_process == Some(true);
            site::Site::start(&self.topology, executor, isolated, worker.kit.as_deref()).await?
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

    /// Refuses a step that cannot reach an executor in a process of its own
    /// (README, an executor's `own_process`): steps start or close it.
    fn refuse_apart(&self, id: &str) -> Result<(), RoomKitError> {
        if self
            .declarations
            .iter()
            .any(|worker| worker.id == id && worker.own_process == Some(true))
        {
            return Err(RoomKitError::Value(format!(
                "executor {id} runs in a process of its own: steps start or close it"
            )));
        }
        Ok(())
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
            self.refuse_apart(id)?;
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

    fn close_process_workers<'a>(
        &'a self,
        name: &'a str,
    ) -> BoxFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            for worker in &self.declarations {
                if worker.kit.as_deref() == Some(name) {
                    let site = self.active.lock().await.remove(&worker.id);
                    if let Some(site) = site {
                        site.close(&self.topology).await?;
                    }
                }
            }
            Ok(())
        })
    }
    fn connection(
        &self,
        name: &str,
        restart: bool,
    ) -> Result<crate::run::OwnerConnection, RoomKitError> {
        Ok(self.presence.connection(self.topology.node(name, restart)?))
    }
    fn fault(
        &self,
        operation: &str,
        enabled: bool,
        gate: Option<String>,
    ) -> Result<(), RoomKitError> {
        self.presence.fault(operation, enabled, gate)
    }
    fn reserve(&self, executor: Option<&str>, process: Option<&str>) -> Result<(), RoomKitError> {
        let reserved = self.reserved.get().ok_or_else(|| {
            RoomKitError::Value("executor.reserve: the kit's backend was not wrapped".into())
        })?;
        let Some(id) = executor else {
            reserved.reserve(None);
            return Ok(());
        };
        if !self.declarations.iter().any(|worker| worker.id == id) {
            return Err(RoomKitError::Value(format!(
                "no executor {id} in setup.executors"
            )));
        }
        let node = process
            .map(|name| self.topology.node(name, false))
            .transpose()?
            .map(|node| node.node_id().to_owned());
        reserved.reserve(Some(roomkit::turns::ExecutorInfo::new(
            id,
            roomkit::turns::ExecutorInfoOptions {
                node,
                ..Default::default()
            },
        )));
        Ok(())
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
            if matches!(
                operation,
                "executor.pause" | "executor.resume" | "executor.kill"
            ) {
                self.refuse_apart(id)?;
            }
            match operation {
                "executor.start" => self.start(id).await,
                "executor.pause" => self.advertise(id, true).await,
                "executor.resume" => self.advertise(id, false).await,
                "executor.close" => self.remove(id).await?.close(&self.topology).await,
                "executor.kill" => {
                    let observe = !self.restarted.contains(id);
                    self.remove(id).await?.kill(&self.topology, observe).await
                }
                "owner.kill" => {
                    let owner = self.owner_kill.as_ref().ok_or_else(|| {
                        RoomKitError::Value("owner.kill: the kit's backend was not wrapped".into())
                    })?;
                    owner.kill();
                    Ok(())
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
    let kit = &scenario["setup"]["kit"];
    if kills_owner(scenario)
        && (kit["turn_backend_faults"] == true || kit["executor_catalogue"] == false)
    {
        return Err(RoomKitError::Value(
            "owner.kill takes no presence fixture of the kit".into(),
        ));
    }
    let topology = topology::Topology::new(url, scenario)?;
    let slot = RunSlot::default();
    let owner = topology.owner().clone();
    let owner_kill = kills_owner(scenario).then(|| {
        Arc::new(owner_kill::OwnerKill::new(
            Arc::new(owner.clone()),
            slot.clone(),
        ))
    });
    let workers = Arc::new(Workers {
        presence: presence::Presence::new(&scenario["setup"]["kit"], slot.clone()),
        topology,
        declarations: setup.executors,
        channels: Arc::new(setup.channels),
        slot,
        active: Mutex::default(),
        mortal: scenario["steps"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|step| step["call"] == "executor.kill")
            .filter_map(|step| step["executor"].as_str().map(str::to_owned))
            .collect(),
        restarted: restarted(scenario),
        owner_kill,
        reserved: std::sync::OnceLock::new(),
    });
    let main = workers.kit_connection(scenario, owner.clone());
    let problems = crate::harness::run_with(
        scenario,
        workers.slot.clone(),
        Some(Arc::<Workers>::clone(&workers)),
        |mut options| {
            options.turn_backend = Some(main.turns);
            if main.realtime.is_some() {
                options.realtime = main.realtime;
            }
            options
        },
    )
    .await;
    if let Some(killed) = &workers.owner_kill {
        killed.close();
    }
    let running: Vec<_> = workers.active.lock().await.keys().cloned().collect();
    for id in running {
        workers.remove(&id).await?.close(&workers.topology).await?;
    }
    owner.close().await?;
    let mut problems = problems;
    problems.extend(workers.presence.untaken());
    Ok(problems)
}

/// Whether a step kills the run's kit as its turns' owner.
fn kills_owner(scenario: &Value) -> bool {
    calls(scenario, "owner.kill")
}

/// Whether a step of `scenario` makes the call `name`.
fn calls(scenario: &Value, name: &str) -> bool {
    scenario["steps"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|step| step["call"] == name)
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

#[test]
fn pausing_a_silent_executor_is_refused_before_another_peer_can_advertise() {
    if !crate::backend_available() {
        return;
    }
    let scenario = serde_json::json!({
        "id": "executors/silent-pause",
        "setup": { "executors": [{"id":"mute", "hosts":["agent"], "answers":false}] },
        "steps": [
            {"call":"executor.pause","executor":"mute","expect":{"error":"invalid_argument"}},
            {"call":"executor.resume","executor":"mute","expect":{"error":"invalid_argument"}},
            {"call":"executor.close","executor":"mute"}
        ]
    });
    if let Some(problems) = run(&scenario, crate::scenarios::ONE_THREAD.build) {
        assert!(problems.is_empty(), "{problems:?}");
    }
}

/// A scenario whose kit the runner already serves by a transport of its own
/// is not run a second time on the contract alone.
#[test]
fn a_kit_on_a_transport_of_the_runner_is_not_run_again() {
    use serde_json::json;
    for scenario in [
        json!({"setup": {"kit": {"turn_backend_contract_only": true}, "executors": []}}),
        json!({"setup": {"kit": {"turn_backend_faults": true}, "executors": []}}),
        json!({"setup": {"kit": {"executor_catalogue": false}, "executors": []}}),
        json!({"setup": {"executors": []}, "steps": [{"call": "owner.kill"}]}),
        json!({"setup": {"executors": []}, "steps": [{"call": "executor.reserve"}]}),
    ] {
        assert!(on_a_transport_of_the_runner(&scenario), "{scenario}");
        let again = run_on_the_contract_alone(&scenario, crate::scenarios::ONE_THREAD.build);
        assert!(again.is_none(), "{scenario}");
    }
    let plain = json!({"setup": {"executors": []}});
    assert!(!on_a_transport_of_the_runner(&plain));
}
