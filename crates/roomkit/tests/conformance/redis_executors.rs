//! RFC §6.4 and §10.6. The normal runner places declared channels on workers
//! with independent Redis nodes and compares the complete scenario expectations.
use std::collections::BTreeSet;
use std::sync::Arc;

use roomkit::{
    RedisTurnBackend, RedisTurnBackendOptions, RoomKitError, TurnExecutor, TurnExecutorOptions,
};
use serde::Deserialize;
use serde_json::Value;
use uuid::Uuid;

use crate::agents::{self, Scripted};
use crate::identity::RunSlot;

const SUPPORTED: &[&str] = &[
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
}

async fn exercise(url: &str, scenario: &Value) -> Result<Vec<String>, RoomKitError> {
    let namespace = format!("roomkit:test:scenario:{}", Uuid::new_v4());
    let backend = || {
        RedisTurnBackend::new(
            Some(url),
            RedisTurnBackendOptions {
                key_prefix: namespace.clone(),
                ..RedisTurnBackendOptions::default()
            },
        )
    };
    let owner = backend()?;
    let slot = RunSlot::default();
    let setup = &scenario["setup"];
    let workers: Vec<Worker> = serde_json::from_value(setup["executors"].clone())
        .map_err(|error| RoomKitError::Value(error.to_string()))?;
    let channels: Arc<Vec<Value>> = Arc::new(
        serde_json::from_value(setup["channels"].clone())
            .map_err(|error| RoomKitError::Value(error.to_string()))?,
    );
    let mut active = Vec::new();
    for worker in workers {
        let declarations = Arc::clone(&channels);
        let observed = slot.clone();
        let executor = TurnExecutor::new(
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
                executor_id: Some(worker.id),
                hosts: Some(worker.hosts),
                ..TurnExecutorOptions::default()
            },
        )?;
        let hosting = backend()?;
        hosting.serve(executor.clone()).await?;
        active.push((hosting, executor));
    }
    let problems = crate::harness::run_with(scenario, slot, |mut options| {
        options.turn_backend = Some(owner.clone());
        options
    })
    .await;
    for (hosting, executor) in active {
        hosting.close().await?;
        executor.close().await;
    }
    owner.close().await?;
    Ok(problems)
}
