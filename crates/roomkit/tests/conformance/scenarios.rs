//! Every scenario of the suite, run on the kit the including crate makes
//! (`crate::new_kit`), on the runtime and against the known failures each
//! pass hands in.

use std::path::{Path, PathBuf};

use serde_json::Value;

use crate::junit::{self, Case, Outcome};
use crate::{harness, shape};

/// The repository's copy of the suite.
pub(crate) fn suite() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance")
}

/// The scenario files under `directory`, in path order.
pub(crate) fn scenarios(directory: &Path, found: &mut Vec<PathBuf>) {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    entries.sort();
    for path in entries {
        if path.is_dir() {
            scenarios(&path, found);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "json")
        {
            found.push(path);
        }
    }
}

fn scenario_id(root: &Path, path: &Path) -> String {
    let relative = path.strip_prefix(root).unwrap().with_extension("");
    relative.to_string_lossy().replace('\\', "/")
}

/// A pass over the scenarios: its name in the suite's output, the name of
/// its runtime in the result file (`crate::junit`), and how that runtime is
/// built.
pub(crate) struct Pass {
    pub(crate) name: &'static str,
    pub(crate) runtime: &'static str,
    pub(crate) build: fn() -> tokio::runtime::Runtime,
}

/// One thread, which runs every task: the pass every store's runner makes.
pub(crate) const ONE_THREAD: Pass = Pass {
    name: "one thread",
    runtime: "current_thread",
    build: current_thread,
};

/// Four workers, as a program on tokio's default multi-thread runtime runs.
pub(crate) const FOUR_WORKERS: Pass = Pass {
    name: "four workers",
    runtime: "multi_thread_4",
    build: four_workers,
};

fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn four_workers() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap()
}

/// Optional features supported by the shared Rust runner on every backend.
const PROVIDED_FEATURES: &[&str] = &[];

const NO_EXECUTOR: &str = "§10.6.7: this pass has no turn executor beside the kit";

pub(crate) type ExecutorPass = fn(&Value, fn() -> tokio::runtime::Runtime) -> Option<Vec<String>>;

/// A node of a scenario's distributed turn backend, with the broker and
/// the registry it runs on.
pub(crate) struct Node {
    pub(crate) backend: roomkit::turns::DistributedTurnBackend,
    pub(crate) broker: std::sync::Arc<dyn roomkit::turns::TurnBroker>,
    pub(crate) registry: std::sync::Arc<dyn roomkit::turns::TurnRegistry>,
}

/// What makes a node across a Redis server, from its URL, a key prefix and
/// the scenario's bounds: a runner built with the Redis assembly has one.
pub(crate) type RedisNode =
    fn(&str, &str, roomkit::TurnTimeouts) -> Result<Node, roomkit::RoomKitError>;

/// The Redis server the scenarios with executors run across, as `REDIS_URL`
/// names it.
pub(crate) fn redis_url() -> Option<String> {
    std::env::var("REDIS_URL").ok()
}

/// Whether this run's executors are served across Redis: `REDIS_URL` names a
/// server and the runner has its Redis assembly. They are served by the
/// broker and the registry of this process otherwise.
pub(crate) fn across_redis() -> bool {
    crate::REDIS_NODE.is_some() && redis_url().is_some()
}

/// The backend's name in the result file of a pass: `<backend>+redis-executors`
/// when it serves its executors across Redis, so that roomkit-specs records
/// it apart from the core's own pass; `backend` otherwise.
pub(crate) fn result_backend(backend: &str, across_redis: bool) -> String {
    if across_redis {
        format!("{backend}+redis-executors")
    } else {
        backend.to_owned()
    }
}

/// The scenario at `path`, which names `id`.
fn load(path: &Path, id: &str) -> Value {
    let scenario: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(scenario["id"], id, "{} names another id", path.display());
    scenario
}

/// A shape the runner cannot read is a failure too, never a passing skip.
fn problems(scenario: &Value, runtime: fn() -> tokio::runtime::Runtime) -> Vec<String> {
    let unknown = shape::unknown_members(scenario);
    if !unknown.is_empty() {
        return vec![format!(
            "members the runner does not know:\n  {}",
            unknown.join("\n  ")
        )];
    }
    runtime().block_on(async {
        tokio::time::timeout(crate::SCENARIO_BOUND, harness::run(scenario))
            .await
            .unwrap_or_else(|_| vec![format!("did not finish within {:?}", crate::SCENARIO_BOUND)])
    })
}

/// What a pass found: each selected scenario with its outcome, in path
/// order, the ids of those failing, and what breaks the suite.
#[derive(Default)]
pub(crate) struct Run {
    pub(crate) cases: Vec<Case>,
    pub(crate) failing: Vec<String>,
    pub(crate) report: Vec<String>,
}

/// Every scenario under `root` whose id contains `filter`, run on the
/// runtime `pass` builds, against `known`, the known failures
/// (roomkit-specs `conformance/README.md`, "Known failures").
pub(crate) fn run(
    root: &Path,
    filter: Option<&str>,
    pass: &Pass,
    known: &[(&str, &str)],
    executors: Option<ExecutorPass>,
) -> Run {
    let mut paths = Vec::new();
    scenarios(root, &mut paths);
    assert!(!paths.is_empty(), "no scenario under {}", root.display());
    let mut run = Run::default();
    for path in &paths {
        let id = scenario_id(root, path);
        if filter.is_some_and(|needle| !id.contains(needle)) {
            continue;
        }
        let scenario = load(path, &id);
        let missing = missing_features(&scenario);
        let case = if !missing.is_empty() {
            Case {
                id,
                outcome: Outcome::Skipped(format!(
                    "missing scenario features: {}",
                    missing.join(", ")
                )),
            }
        } else if scenario["setup"].get("executors").is_some() {
            match executors.and_then(|check| check(&scenario, pass.build)) {
                Some(problems) => judge(id, &problems, known, &mut run),
                None => Case {
                    id,
                    outcome: Outcome::Skipped(NO_EXECUTOR.to_owned()),
                },
            }
        } else {
            judge(id, &problems(&scenario, pass.build), known, &mut run)
        };
        run.cases.push(case);
    }
    for (listed, _) in known {
        if !paths.iter().any(|path| scenario_id(root, path) == *listed) {
            run.report
                .push(format!("known failure {listed} names no scenario"));
        }
    }
    run
}

/// The features `scenario` requires that this runner does not provide.
fn missing_features(scenario: &Value) -> Vec<&str> {
    scenario["requires_features"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|name| !PROVIDED_FEATURES.contains(name))
        .collect()
}

/// Every scenario with executors under `root` whose id contains `filter`,
/// run a second time by `again`, its kit served by a transport written on
/// the public contract alone (roomkit-specs `conformance/README.md`, "On a
/// transport written on the contract alone"), on the runtime `pass` builds
/// and against `known`, the known failures of its first run. A scenario
/// `again` declines is no case of the pass.
pub(crate) fn run_on_the_contract_alone(
    root: &Path,
    filter: Option<&str>,
    pass: &Pass,
    known: &[(&str, &str)],
    again: ExecutorPass,
) -> Run {
    let mut paths = Vec::new();
    scenarios(root, &mut paths);
    let mut run = Run::default();
    for path in &paths {
        let id = scenario_id(root, path);
        if filter.is_some_and(|needle| !id.contains(needle)) {
            continue;
        }
        let scenario = load(path, &id);
        if scenario["setup"].get("executors").is_none() || !missing_features(&scenario).is_empty() {
            continue;
        }
        if let Some(problems) = again(&scenario, pass.build) {
            let case = judge(id, &problems, known, &mut run);
            run.cases.push(case);
        }
    }
    run
}

/// The scenario `id` run on `pass`, its failure noted in `run`, and what
/// breaks the suite: an unexpected failure, or a known one that passes.
#[expect(
    clippy::print_stdout,
    reason = "the suite prints its known failures, as a test does"
)]
fn judge(id: String, problems: &[String], known: &[(&str, &str)], run: &mut Run) -> Case {
    let listed = known.iter().find(|(listed, _)| *listed == id);
    if !problems.is_empty() {
        run.failing.push(id.clone());
    }
    let outcome = match (problems.is_empty(), listed) {
        (true, Some((_, reason))) if !reason.starts_with("timing:") => {
            run.report
                .push(format!("{id}: passes, but is listed as a known failure"));
            Outcome::Passed
        }
        // A known failure that depends on the machine's speed may pass.
        (true, _) => Outcome::Passed,
        (false, None) => {
            run.report
                .push(format!("{id}:\n  {}", problems.join("\n  ")));
            Outcome::Failed(problems.join("\n"))
        }
        (false, Some((_, reason))) => {
            println!("known failure {id}:\n  {}", problems.join("\n  "));
            Outcome::Failed(format!("known failure: {reason}\n{}", problems.join("\n")))
        }
    };
    Case { id, outcome }
}

/// Every scenario of the repository's copy runs on `pass`; those this
/// implementation fails there are exactly `known`, the known failures.
/// When `ROOMKIT_CONFORMANCE_JUNIT` names a directory, the pass writes its
/// result file there (`crate::junit`), under the name [`result_backend`]
/// gives it.
#[expect(
    clippy::print_stdout,
    reason = "the suite prints its count, as a test does"
)]
pub(crate) fn every_scenario_gives_what_the_rfc_says(pass: &Pass, known: &[(&str, &str)]) {
    let filter = std::env::var("ROOMKIT_CONFORMANCE_FILTER").ok();
    let executors = crate::EXECUTOR_PASS;
    let backend = result_backend(crate::BACKEND, across_redis());
    let output = junit::claim(&backend, pass.runtime);
    let run = run(
        &suite().join("scenarios"),
        filter.as_deref(),
        pass,
        known,
        executors,
    );
    if let Some(output) = &output {
        junit::write(output, &backend, pass.runtime, &run.cases);
    }
    let skipped = run
        .cases
        .iter()
        .filter(|case| matches!(case.outcome, Outcome::Skipped(_)))
        .count();
    println!(
        "{}: {} scenarios, {skipped} skipped, {} failing: {:?}",
        pass.name,
        run.cases.len(),
        run.failing.len(),
        run.failing
    );
    assert!(!run.cases.is_empty(), "no scenario matches {filter:?}");
    assert!(run.report.is_empty(), "\n{}", run.report.join("\n"));
}
