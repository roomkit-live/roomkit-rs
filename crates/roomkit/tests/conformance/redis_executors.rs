//! RFC §6.4 and §10.6. The normal runner places declared channels on workers
//! with independent Redis nodes and compares the complete scenario expectations.
mod channel;
mod topology;

use futures_core::future::BoxFuture;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use roomkit::{RedisTurnBackend, RoomKitError, TurnExecutor, TurnExecutorOptions};
use serde::Deserialize;
use serde_json::Value;

use crate::identity::RunSlot;
use channel::WorkerChannel;

const SUPPORTED: &[&str] = &[
    "discussion/an-offline-agent-s-turn-is-skipped-not-failed",
    "executors/a-channel-call-no-executor-takes-fails-no-executor",
    "executors/a-channel-call-runs-on-the-executor-holding-the-room",
    "executors/a-declared-agent-s-turn-runs-on-the-executor-hosting-it",
    "executors/a-delegation-to-a-declared-worker-runs-on-its-executor",
    "executors/a-detach-forgets-what-the-room-kept-for-a-channel",
    "executors/a-later-turn-on-another-executor-replays-the-instruction-it-read",
    "executors/a-live-agent-s-turn-stays-on-the-kit-s-executor",
    "executors/a-provider-the-factory-hands-every-build-outlives-a-build-let-go",
    "executors/a-publication-from-an-executor-no-longer-holding-the-room-is-dropped",
    "executors/a-released-channel-is-built-anew-for-the-next-turn",
    "executors/a-room-s-agent-presence-names-the-executors-covering-each-agent",
    "executors/a-room-s-turns-stay-on-the-executor-holding-its-state",
    "executors/a-room-whose-executor-builds-nothing-for-its-agent-stays-on-it",
    "executors/a-room-whose-executor-closed-moves-to-another",
    "executors/a-skill-activated-on-one-executor-rides-the-next-turn-on-another",
    "executors/a-turn-apart-asks-a-person-through-the-room-owner",
    "executors/a-turn-apart-calls-a-tool-the-room-owner-serves",
    "executors/a-turn-apart-calls-the-host-verbs-during-and-after-it",
    "executors/a-turn-apart-emits-its-framework-events",
    "executors/a-turn-apart-publishes-its-thinking-and-tool-calls-to-the-room",
    "executors/a-turn-cut-on-its-executor-names-it-in-its-record",
    "executors/a-turn-no-executor-declares-fails-and-the-room-goes-on",
    "executors/a-turn-running-on-a-released-channel-ends-on-it",
    "executors/a-turn-s-record-names-the-executor-that-ran-it-apart",
    "executors/a-verb-the-channel-lacks-fails",
    "executors/an-agent-its-executor-builds-nothing-for-fails-no-executor",
    "executors/an-executor-builds-an-agent-at-its-first-turn-not-when-a-room-attaches-it",
    "executors/an-executor-declaring-every-agent-takes-the-turns-of-agents-it-was-never-named",
    "executors/an-executor-drops-the-state-a-turn-elsewhere-made-stale",
    "executors/an-executor-lacking-a-room-s-hook-does-not-cover-its-agent-there",
    "executors/an-executor-lacking-a-room-s-hook-does-not-take-its-turn",
    "executors/an-executor-past-its-bound-closes-the-agent-it-used-least-recently",
    "executors/an-executor-serving-every-organization-takes-every-room-s-turns",
    "executors/an-executor-takes-the-turns-of-the-organizations-it-serves-alone",
    "executors/another-executor-replays-the-tool-calls-of-the-first",
    "executors/attach-and-detach-reach-every-executor-declaring-the-channel",
    "executors/executors-declaring-prefixes-take-only-the-agents-under-them",
    "executors/one-build-serving-two-rooms-reads-each-turn-s-room-alone",
    "executors/the-kit-lists-the-executors-served-and-what-each-declares",
    "executors/what-the-room-keeps-for-a-channel-reaches-the-next-executor",
    "executors/work-a-turn-apart-began-publishes-to-its-room-after-it",
    "tenancy/a-deleted-room-s-turn-reaches-nothing-of-the-room-created-again",
];

pub(super) fn run(
    scenario: &Value,
    runtime: fn() -> tokio::runtime::Runtime,
) -> Option<Vec<String>> {
    if !scenario["id"]
        .as_str()
        .is_some_and(|id| SUPPORTED.contains(&id) || topology::is_presence(id))
    {
        return None;
    }
    let url = std::env::var("REDIS_URL").ok()?;
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
    builds: Option<BTreeSet<String>>,
    max_channels: Option<usize>,
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
    active: Mutex<BTreeMap<String, (RedisTurnBackend, TurnExecutor)>>,
}

impl Workers {
    fn build(&self, worker: &Worker) -> Result<TurnExecutor, RoomKitError> {
        let declarations = Arc::clone(&self.channels);
        let observed = self.slot.clone();
        let serving = worker.clone();
        TurnExecutor::new(
            move |channel| {
                std::future::ready(serving.channel(channel.as_str(), &declarations, &observed))
            },
            TurnExecutorOptions {
                executor_id: Some(worker.id.clone()),
                hosts: Some(worker.hosts.clone()),
                organizations: worker.organizations.clone(),
                max_channels: worker.max_channels,
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
        let hosting = self.topology.worker()?;
        hosting.serve(executor.clone()).await?;
        active.insert(id.to_owned(), (hosting, executor));
        Ok(())
    }

    /// Withdraw the declaration while retaining the worker, its builds and its active turns.
    /// Resuming re-advertises that same instance on the same Redis node.
    async fn advertise(&self, id: &str, paused: bool) -> Result<(), RoomKitError> {
        let active = self.active.lock().await;
        let (hosting, executor) = active
            .get(id)
            .ok_or_else(|| RoomKitError::Value(format!("executor {id} is not running")))?;
        if !paused {
            return hosting.serve(executor.clone()).await;
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
        hosting.serve(no_channels).await
    }

    async fn close(&self, id: &str) -> Result<(), RoomKitError> {
        let entry = self.active.lock().await.remove(id);
        let (hosting, executor) =
            entry.ok_or_else(|| RoomKitError::Value(format!("executor {id} is not started")))?;
        self.topology.close_worker(&hosting, &executor).await
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
                .map(|(backend, _)| backend.clone())
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
                "executor.close" => self.close(id).await,
                "executor.pause" => self.advertise(id, true).await,
                "executor.resume" => self.advertise(id, false).await,
                other => Err(RoomKitError::Value(format!(
                    "unsupported executor operation: {other}"
                ))),
            }
        })
    }
}

async fn exercise(url: &str, scenario: &Value) -> Result<Vec<String>, RoomKitError> {
    let setup = &scenario["setup"];
    let parse_error = |e: serde_json::Error| RoomKitError::Value(e.to_string());
    let workers = Arc::new(Workers {
        topology: topology::Topology::new(url, scenario)?,
        declarations: serde_json::from_value(setup["executors"].clone()).map_err(parse_error)?,
        channels: Arc::new(serde_json::from_value(setup["channels"].clone()).map_err(parse_error)?),
        slot: RunSlot::default(),
        active: Mutex::default(),
    });
    let owner = workers.topology.owner().clone();
    let problems = crate::harness::run_with(
        scenario,
        workers.slot.clone(),
        Some(Arc::<Workers>::clone(&workers)),
        |mut options| {
            options.turn_backend = Some(owner.clone());
            options
        },
    )
    .await;
    let running: Vec<_> = workers.active.lock().await.keys().cloned().collect();
    for id in running {
        workers.close(&id).await?;
    }
    owner.close().await?;
    Ok(problems)
}
