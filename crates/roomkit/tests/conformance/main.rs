//! The shared conformance suite, run on the tokio runtime (RFC §1.4,
//! §26.1).
//!
//! The scenarios come from roomkit-specs `conformance/`, copied into
//! `conformance/` at the repository's root by `make conformance-sync`,
//! which records the specs commit in `conformance/SPECS_COMMIT` and the
//! copy's fingerprint in `conformance/SCENARIOS_SHA256`. A scenario
//! this implementation is known to fail is listed in [`KNOWN_FAILURES`]
//! with the rule it breaks; it is expected to fail, and the suite passes
//! only while it does, so a fix shows.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#![expect(
    clippy::indexing_slicing,
    reason = "the runner reads scenarios as serde_json values, whose index reads null for a \
              missing member and never panics; its own lists are indexed within their length"
)]

mod calls;
mod compare;
mod errors;
mod harness;
mod matching;
mod normalize;
mod run;
mod scripted;
mod shape;
mod templates;

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Scenario id → the rule this implementation breaks (roomkit-specs
/// `conformance/README.md`, "Known failures").
const KNOWN_FAILURES: &[(&str, &str)] = &[
    (
        "hooks/an-async-hook-on-a-sync-trigger-observes",
        "RFC §9.2: a hook registered ASYNC on BEFORE_BROADCAST never runs; the inbound \
         operation fires no observer of BEFORE_BROADCAST once its chain allowed the event",
    ),
    (
        "inbound/an-author-s-name-and-rank-are-recorded",
        "RFC §10.1 step 12: roomkit-rs records no author and keeps no register of them \
         (step 3 decision 2, milestone 2)",
    ),
    (
        "rooms/the-delivery-cursor-follows-the-lane",
        "RFC §5.1, §1.4: roomkit-rs's Room has no delivered_index (step 3 decision 2, \
         milestone 2)",
    ),
];

/// A scenario that has not finished by then fails instead of hanging the
/// suite.
const SCENARIO_BOUND: Duration = Duration::from_secs(60);

fn suite() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance")
}

fn scenarios(directory: &Path, found: &mut Vec<PathBuf>) {
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

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

/// The copy is the one `make conformance-sync` took: the fingerprint it
/// recorded, `sha256sum` over `sha256sum` of each file of `scenarios/` by
/// path, in byte order, is the copy's.
#[test]
fn the_copy_is_the_one_synced_from_its_specs_commit() {
    let commit = std::fs::read_to_string(suite().join("SPECS_COMMIT")).unwrap();
    assert!(
        commit.trim().len() == 40 && commit.trim().bytes().all(|byte| byte.is_ascii_hexdigit()),
        "SPECS_COMMIT names no commit: {commit:?}"
    );
    let mut paths = Vec::new();
    scenarios(&suite().join("scenarios"), &mut paths);
    let mut files: Vec<(String, String)> = paths
        .iter()
        .map(|path| {
            let relative = path
                .strip_prefix(suite())
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            (relative, hex(&Sha256::digest(std::fs::read(path).unwrap())))
        })
        .collect();
    files.sort_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
    let lines: Vec<String> = files
        .iter()
        .map(|(relative, digest)| format!("{digest}  {relative}\n"))
        .collect();
    let fingerprint = hex(&Sha256::digest(lines.concat().as_bytes()));
    let recorded = std::fs::read_to_string(suite().join("SCENARIOS_SHA256")).unwrap();
    assert_eq!(
        fingerprint,
        recorded.trim(),
        "conformance/scenarios differs from its sync: run `make conformance-sync`"
    );
}

/// Every scenario runs; those this implementation fails are exactly the
/// known failures.
#[test]
fn every_scenario_gives_what_the_rfc_says() {
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
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let problems = runtime.block_on(async {
            tokio::time::timeout(SCENARIO_BOUND, harness::run(&scenario))
                .await
                .unwrap_or_else(|_| vec![format!("did not finish within {SCENARIO_BOUND:?}")])
        });
        let known = KNOWN_FAILURES.iter().any(|(known, _)| *known == id);
        if !problems.is_empty() {
            failing.push(id.clone());
        }
        match (problems.is_empty(), known) {
            (true, true) => report.push(format!("{id}: passes, but is listed as a known failure")),
            (false, false) => report.push(format!("{id}:\n  {}", problems.join("\n  "))),
            (false, true) => println!("known failure {id}:\n  {}", problems.join("\n  ")),
            (true, false) => {}
        }
    }
    for (known, _) in KNOWN_FAILURES {
        if !paths.iter().any(|path| scenario_id(&root, path) == *known) {
            report.push(format!("known failure {known} names no scenario"));
        }
    }
    println!(
        "{} scenarios, {} failing: {failing:?}",
        paths.len(),
        failing.len()
    );
    assert!(report.is_empty(), "\n{}", report.join("\n"));
}
