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
    let mut report = Vec::new();
    let mut failing = Vec::new();
    for path in &paths {
        let id = scenario_id(&root, path);
        let scenario: Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(
            scenario["id"],
            id.as_str(),
            "{} names another id",
            path.display()
        );
        let unknown = shape::unknown_members(&scenario);
        if !unknown.is_empty() {
            report.push(format!(
                "{id}: members the runner does not know:\n  {}",
                unknown.join("\n  ")
            ));
            continue;
        }
        let problems = runtime().block_on(async {
            tokio::time::timeout(crate::SCENARIO_BOUND, harness::run(&scenario))
                .await
                .unwrap_or_else(|_| {
                    vec![format!("did not finish within {:?}", crate::SCENARIO_BOUND)]
                })
        });
        let listed = known.iter().find(|(listed, _)| *listed == id);
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
    for (listed, _) in known {
        if !paths.iter().any(|path| scenario_id(&root, path) == *listed) {
            report.push(format!("known failure {listed} names no scenario"));
        }
    }
    println!(
        "{pass}: {} scenarios, {} failing: {failing:?}",
        paths.len(),
        failing.len()
    );
    assert!(report.is_empty(), "\n{}", report.join("\n"));
}
