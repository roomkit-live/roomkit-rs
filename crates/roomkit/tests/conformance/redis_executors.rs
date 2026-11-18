//! RFC §6.4 and §10.6. The normal runner places declared channels on workers
//! with independent Redis nodes and compares the complete scenario expectations.
mod channel;

use futures_core::future::BoxFuture;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::Mutex;

use roomkit::{
    RedisTurnBackend, RedisTurnBackendOptions, RoomKitError, TurnExecutor, TurnExecutorOptions,
};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::identity::RunSlot;
use channel::WorkerChannel;

const SUPPORTED: &[&str] = &[
    "discussion/an-offline-agent-s-turn-is-skipped-not-failed",
    "executors/a-channel-call-no-executor-takes-fails-no-executor",
    "executors/a-declared-agent-s-turn-runs-on-the-executor-hosting-it",
    "executors/a-delegation-to-a-declared-worker-runs-on-its-executor",
    "executors/a-live-agent-s-turn-stays-on-the-kit-s-executor",
    "executors/a-provider-the-factory-hands-every-build-outlives-a-build-let-go",
    "executors/a-released-channel-is-built-anew-for-the-next-turn",
    "executors/a-room-s-agent-presence-names-the-executors-covering-each-agent",
    "executors/a-room-whose-executor-builds-nothing-for-its-agent-stays-on-it",
    "executors/a-room-whose-executor-closed-moves-to-another",
    "executors/a-turn-apart-calls-a-tool-the-room-owner-serves",
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
    "executors/an-executor-past-its-bound-closes-the-agent-it-used-least-recently",
    "executors/an-executor-serving-every-organization-takes-every-room-s-turns",
    "executors/an-executor-takes-the-turns-of-the-organizations-it-serves-alone",
    "executors/attach-and-detach-reach-every-executor-declaring-the-channel",
    "executors/executors-declaring-prefixes-take-only-the-agents-under-them",
    "executors/one-build-serving-two-rooms-reads-each-turn-s-room-alone",
    "executors/the-kit-lists-the-executors-served-and-what-each-declares",
    "executors/what-the-room-keeps-for-a-channel-reaches-the-next-executor",
];

pub(super) fn run(
    scenario: &Value,
    runtime: fn() -> tokio::runtime::Runtime,
) -> Option<Vec<String>> {
    if !scenario["id"]
        .as_str()
        .is_some_and(|id| SUPPORTED.contains(&id))
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
    url: String,
    options: RedisTurnBackendOptions,
    declarations: Vec<Worker>,
    channels: Arc<Vec<Value>>,
    slot: RunSlot,
    active: Mutex<BTreeMap<String, (RedisTurnBackend, TurnExecutor)>>,
}

impl Workers {
    fn backend(&self) -> Result<RedisTurnBackend, RoomKitError> {
        RedisTurnBackend::new(Some(&self.url), self.options.clone())
    }

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
        let hosting = self.backend()?;
        hosting.serve(executor.clone()).await?;
        active.insert(id.to_owned(), (hosting, executor));
        Ok(())
    }

    async fn close(&self, id: &str) -> Result<(), RoomKitError> {
        let entry = self.active.lock().await.remove(id);
        let (hosting, executor) =
            entry.ok_or_else(|| RoomKitError::Value(format!("executor {id} is not started")))?;
        let removed = hosting.close().await;
        executor.close().await;
        removed
    }
}

impl crate::run::WorkerControl for Workers {
    fn change<'a>(
        &'a self,
        operation: &'a str,
        id: &'a str,
    ) -> BoxFuture<'a, Result<(), RoomKitError>> {
        Box::pin(async move {
            match operation {
                "executor.start" => self.start(id).await,
                "executor.close" => self.close(id).await,
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
        url: url.to_owned(),
        options: RedisTurnBackendOptions {
            key_prefix: format!("roomkit:test:scenario:{}", Uuid::new_v4()),
            ..RedisTurnBackendOptions::default()
        },
        declarations: serde_json::from_value(setup["executors"].clone()).map_err(parse_error)?,
        channels: Arc::new(serde_json::from_value(setup["channels"].clone()).map_err(parse_error)?),
        slot: RunSlot::default(),
        active: Mutex::default(),
    });
    let owner = workers.backend()?;
    for worker in &workers.declarations {
        if worker.started.unwrap_or(true) {
            workers.start(&worker.id).await?;
        }
    }
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
