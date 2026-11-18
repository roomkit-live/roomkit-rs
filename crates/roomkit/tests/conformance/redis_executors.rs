//! RFC §6.4 and §10.6. The normal runner places declared channels on workers
//! with independent Redis nodes and compares the complete scenario expectations.
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

use crate::agents::{self, Scripted};
use crate::identity::RunSlot;

const SUPPORTED: &[&str] = &[
    "executors/the-kit-lists-the-executors-served-and-what-each-declares",
    "executors/a-room-s-agent-presence-names-the-executors-covering-each-agent",
    "executors/a-turn-s-record-names-the-executor-that-ran-it-apart",
    "executors/a-turn-apart-emits-its-framework-events",
    "executors/a-turn-cut-on-its-executor-names-it-in-its-record",
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Worker {
    id: String,
    hosts: BTreeSet<String>,
    organizations: Option<BTreeSet<String>>,
    started: Option<bool>,
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
        TurnExecutor::new(
            move |channel| {
                let observed = observed.clone();
                let declarations = Arc::clone(&declarations);
                async move {
                    let spec = declarations
                        .iter()
                        .find(|spec| spec["id"] == channel.as_str() && spec["declared"] == true);
                    let Some(spec) = spec else {
                        return Ok(None);
                    };
                    let run = observed.run().ok_or_else(|| {
                        RoomKitError::Runtime("scenario owner is unavailable".into())
                    })?;
                    let (built, models) = agents::scripted_agent_of(spec, &run, false)
                        .map_err(RoomKitError::Value)?;
                    crate::run::lock(&run.models).extend(models);
                    let Scripted::Channel(channel) = built else {
                        return Err(RoomKitError::Value(
                            "this executor fixture requires a scripted AI channel".into(),
                        ));
                    };
                    Ok(Some(*channel))
                }
            },
            TurnExecutorOptions {
                executor_id: Some(worker.id.clone()),
                hosts: Some(worker.hosts.clone()),
                organizations: worker.organizations.clone(),
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
        Some(workers.clone()),
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
