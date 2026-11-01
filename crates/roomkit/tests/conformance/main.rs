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

mod agents;
mod calls;
mod compare;
mod errors;
mod harness;
mod matching;
mod normalize;
mod run;
mod scenarios;
mod scripted;
mod shape;
mod store_calls;
mod templates;

use std::time::Duration;

use roomkit::{InMemoryStore, RoomKit, RoomKitOptions};
use sha2::{Digest, Sha256};

use crate::scenarios::{scenarios, suite};

/// The kit the scenarios run on, over the in-memory store; roomkit-sqlite
/// runs them on its store too (crates/roomkit-sqlite/tests/conformance).
pub(crate) type Kit = RoomKit<InMemoryStore>;

/// A kit over an empty store, built with what `configure` makes of the
/// defaults, and what keeps the store until the run ends: nothing here.
#[expect(
    clippy::unused_async,
    reason = "every store's kit is made the same way, some awaiting their store"
)]
pub(crate) async fn new_kit(configure: impl FnOnce(RoomKitOptions) -> RoomKitOptions) -> (Kit, ()) {
    (RoomKit::new(configure(RoomKitOptions::default())), ())
}

/// Scenario id → the rule this implementation breaks (roomkit-specs
/// `conformance/README.md`, "Known failures").
pub(crate) const KNOWN_FAILURES: &[(&str, &str)] = &[];

/// A scenario that has not finished by then fails instead of hanging the
/// suite.
pub(crate) const SCENARIO_BOUND: Duration = Duration::from_secs(60);

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

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::new(), |mut text, byte| {
        let _ = write!(text, "{byte:02x}");
        text
    })
}

/// Every scenario runs; those this implementation fails are exactly the
/// known failures.
#[test]
fn every_scenario_gives_what_the_rfc_says() {
    scenarios::every_scenario_gives_what_the_rfc_says();
}
