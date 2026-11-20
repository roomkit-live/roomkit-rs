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

mod acp_agents;
mod acp_fixture_tests;
mod agent_model_tests;
mod agents;
mod ai_hooks;
mod audits;
mod below_kit;
mod calls;
mod channel_verbs;
mod chat_wire;
mod compare;
mod continuation;
mod delegate_calls;
mod discussion_calls;
mod errors;
mod extra_tools;
mod handoff_calls;
mod harness;
mod held_ends;
mod host_verbs;
mod identity;
mod input_calls;
mod junit;
mod junit_tests;
mod kit_executor;
mod locks;
mod matching;
mod mcp_host;
mod mcp_servers;
mod media;
mod member_calls;
mod memory;
mod normalize;
mod process_tests;
mod report_claims;
mod room_calls;
mod room_queries;
mod routing_calls;
mod run;
mod sandbox;
mod scenarios;

#[cfg(feature = "redis")]
mod redis_executors;
#[cfg(feature = "redis")]
const EXECUTOR_PASS: Option<scenarios::ExecutorPass> = Some(redis_executors::run);
#[cfg(not(feature = "redis"))]
const EXECUTOR_PASS: Option<scenarios::ExecutorPass> = None;

mod scripted;
mod served_call_tests;
mod shape;
mod skill_registry;
mod skills;
mod speaking;
mod store_calls;
mod store_faults;
mod strategy_calls;
mod subscription_calls;
mod task_calls;
mod templates;
mod verification_calls;

use std::time::Duration;

use roomkit::{InMemoryStore, RoomKit, RoomKitOptions};
use sha2::{Digest, Sha256};

use crate::scenarios::{scenarios, suite};
use crate::store_faults::FaultingStore;

/// The kit the scenarios run on, over the in-memory store; roomkit-sqlite
/// runs them on its store too (crates/roomkit-sqlite/tests/conformance).
pub(crate) type Kit = RoomKit<Store>;
/// The store of the kit the runner builds, which a `store_fault` step makes
/// fail or hold (`store_faults`).
pub(crate) type Store = FaultingStore<InMemoryStore>;
/// The backend's name in the result file (`junit`).
pub(crate) type StoreLocation = ();

pub(crate) const BACKEND: &str = "memory";

/// The memory runner needs no external service for its fixture checks.
pub(crate) const fn backend_available() -> bool {
    true
}

/// A kit over an empty store, built with what `configure` makes of the
/// defaults, and what keeps the store until the run ends: nothing here.
#[expect(
    clippy::unused_async,
    reason = "every store's kit is made the same way, some awaiting their store"
)]
pub(crate) async fn new_kit(
    configure: impl FnOnce(RoomKitOptions<Store>) -> RoomKitOptions<Store>,
) -> (Kit, ()) {
    (RoomKit::new(configure(empty_options())), ())
}

/// The defaults over an empty in-memory store, wrapped.
fn empty_options() -> RoomKitOptions<Store> {
    RoomKitOptions::with_store(FaultingStore::new(InMemoryStore::default()))
}

/// Memory stays readable after close: its shared store is also the observer.
#[expect(
    clippy::unused_async,
    reason = "persistent stores open a separate observer"
)]
pub(crate) async fn observer_store(
    store: &std::sync::Arc<Store>,
    _kept: &(),
) -> std::sync::Arc<Store> {
    std::sync::Arc::clone(store)
}

/// Another process serving the rooms (README, `processes`): a kit over the
/// run's `store` and `lock_manager`.
#[expect(
    clippy::unused_async,
    reason = "every store's process is made the same way, some awaiting their store"
)]
pub(crate) async fn process_kit(
    store: &std::sync::Arc<Store>,
    _kept: &(),
    lock_manager: std::sync::Arc<dyn roomkit::RoomLockManager>,
    connection: Option<run::OwnerConnection>,
) -> Kit {
    RoomKit::new(RoomKitOptions {
        turn_backend: connection.as_ref().map(|node| node.turns.clone()),
        realtime: connection.and_then(|node| node.realtime),
        store: std::sync::Arc::clone(store),
        lock_manager: Some(lock_manager),
        ..empty_options()
    })
}

/// Scenario id → the rule this implementation breaks (roomkit-specs
/// `conformance/README.md`, "Known failures").
pub(crate) const KNOWN_FAILURES: &[(&str, &str)] = scenarios::HOST_LOCK_FAILURES;

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
    scenarios::every_scenario_gives_what_the_rfc_says(&scenarios::ONE_THREAD, KNOWN_FAILURES);
}

/// Every scenario gives the same on four workers as on one thread.
#[test]
fn every_scenario_gives_the_same_on_several_workers() {
    scenarios::every_scenario_gives_what_the_rfc_says(&scenarios::FOUR_WORKERS, KNOWN_FAILURES);
}
