//! Every scenario of the suite, run on the kit the including crate makes
//! (`crate::new_kit`), on the runtime and against the known failures each
//! pass hands in.

use std::path::{Path, PathBuf};

use serde_json::Value;

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

/// A runtime of one thread, which runs every task: the pass every store's
/// runner makes.
pub(crate) fn current_thread() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// The area of the scenarios that need a turn executor beside the kit.
const APART: &str = "executors/";

/// Why no runner of this repository runs that area (RFC §10.6.7): the turn
/// backend and its executor are private here.
const NO_EXECUTOR_APART: &str = "§10.6.7: an executor apart needs a public turn backend";

/// Python's per-turn model router has no Rust API or runner yet (RMK-1647).
/// Each exact case must fail; a newly supported case fails the suite until
/// its entry is removed. The turn-record scenario already passes unlisted.
const MODEL_ROUTER_FAILURES: &[(&str, &str)] = &[
    (
        "ai-router/a-generation-that-spoke-is-not-handed-over",
        "§6.4: the public model router and its runner are not ported (RMK-1647)",
    ),
    (
        "ai-router/a-generation-the-router-hands-over-keeps-the-turn-s-later-rounds",
        "§6.4: the public model router and its runner are not ported (RMK-1647)",
    ),
    (
        "ai-router/a-retry-the-router-names-no-model-for-fails-with-the-last-refusal",
        "§6.4: the public model router and its runner are not ported (RMK-1647)",
    ),
    (
        "ai-router/before-ai-generation-names-the-model-the-router-picks",
        "§6.4: the public model router and its runner are not ported (RMK-1647)",
    ),
    (
        "ai-router/the-router-picks-each-turn-s-model-and-its-rounds-stay-on-it",
        "§6.4: the public model router and its runner are not ported (RMK-1647)",
    ),
    (
        "ai-router/the-turn-s-context-is-built-for-the-model-the-router-picks",
        "§6.4: the public model router and its runner are not ported (RMK-1647)",
    ),
    (
        "reasoning/a-fallback-s-blocks-go-back",
        "§6.4: later tool rounds return to the primary instead of staying on the fallback (RMK-1647)",
    ),
    (
        "tools/a-fallback-s-generations-are-priced-at-the-primary-s-rate",
        "§6.4: later tool rounds return to the primary instead of staying on the fallback (RMK-1647)",
    ),
];

/// Whether `id` is a scenario no runner here runs, reported as a known failure.
#[expect(
    clippy::print_stdout,
    reason = "the suite prints its known failures, as a test does"
)]
fn reported_apart(id: &str, failing: &mut Vec<String>) -> bool {
    // This discussion scenario also requires a separately started executor
    // (RFC §19.7.5, RMK-1626), which the runner cannot create yet. Remove
    // this exact case when the public executor and its runner are ported.
    let apart =
        id.starts_with(APART) || id == "discussion/an-offline-agent-s-turn-is-skipped-not-failed";
    if apart {
        println!("known failure {id}:\n  {NO_EXECUTOR_APART}");
        failing.push(id.to_owned());
    }
    apart
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

/// Every scenario runs on a runtime `runtime` builds, the pass named
/// `pass`; those this implementation fails there are exactly `known`, the
/// known failures (roomkit-specs `conformance/README.md`, "Known
/// failures").
#[expect(
    clippy::print_stdout,
    reason = "the suite prints its known failures and its count, as a test does"
)]
pub(crate) fn every_scenario_gives_what_the_rfc_says(
    pass: &str,
    runtime: fn() -> tokio::runtime::Runtime,
    known: &[(&str, &str)],
) {
    let root = suite().join("scenarios");
    let mut paths = Vec::new();
    scenarios(&root, &mut paths);
    assert!(!paths.is_empty(), "no scenario under {}", root.display());
    let filter = std::env::var("ROOMKIT_CONFORMANCE_FILTER").ok();
    let mut selected = 0;
    let mut report = Vec::new();
    let mut failing = Vec::new();
    for path in &paths {
        let id = scenario_id(&root, path);
        if filter.as_ref().is_some_and(|needle| !id.contains(needle)) {
            continue;
        }
        selected += 1;
        let scenario = load(path, &id);
        if reported_apart(&id, &mut failing) {
            continue;
        }
        let problems = problems(&scenario, runtime);
        let listed = known
            .iter()
            .chain(MODEL_ROUTER_FAILURES)
            .find(|(listed, _)| *listed == id);
        if !problems.is_empty() {
            failing.push(id.clone());
        }
        match (problems.is_empty(), listed) {
            // A failure that depends on the machine's speed may pass.
            (true, Some((_, reason))) if reason.starts_with("timing:") => {}
            (true, Some(_)) => {
                report.push(format!("{id}: passes, but is listed as a known failure"));
            }
            (false, None) => report.push(format!("{id}:\n  {}", problems.join("\n  "))),
            (false, Some(_)) => println!("known failure {id}:\n  {}", problems.join("\n  ")),
            (true, None) => {}
        }
    }
    for (listed, _) in known.iter().chain(MODEL_ROUTER_FAILURES) {
        if !paths.iter().any(|path| scenario_id(&root, path) == *listed) {
            report.push(format!("known failure {listed} names no scenario"));
        }
    }
    println!(
        "{pass}: {} scenarios, {} failing: {failing:?}",
        selected,
        failing.len()
    );
    assert!(selected > 0, "no scenario matches {filter:?}");
    assert!(report.is_empty(), "\n{}", report.join("\n"));
}
