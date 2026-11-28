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

mod distributed_executors;
const EXECUTOR_PASS: Option<scenarios::ExecutorPass> = Some(distributed_executors::run);

mod scripted;
mod served_call_tests;
mod shape;
mod skill_registry;
mod skills;
mod speaking;
mod store_calls;
mod store_faults;
mod store_records;
mod store_rooms;
mod store_spans;
mod strategy_calls;
mod subscription_calls;
mod task_calls;
mod telemetry_calls;
mod templates;
mod verification_calls;

use std::time::Duration;

use roomkit::{InMemoryStore, RoomKit, RoomKitOptions};
use sha2::{Digest, Sha256};

use crate::scenarios::{scenarios, suite};
use crate::store_faults::FaultingStore;

/// The kit the scenarios run on, over the in-memory store: the conformance
/// is the core's, and runs on no other store.
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
pub(crate) const KNOWN_FAILURES: &[(&str, &str)] = &[
    (
        "discussion/a-kept-result-whose-message-was-deleted-for-good-is-posted-again",
        "RFC §19.7.5 rule 19 (RK-19.7.5-188): the kit posts a task's result at once, outside the \
         discussion, never as a post entry of the speak queue kept in the room's \
         private state until committed and keyed by the result's id (RMK-2193)",
    ),
    (
        "discussion/a-posted-result-is-the-agent-s-message-without-a-generation-and-asks-no-one",
        "RFC §19.7.5 rule 19 (RK-19.7.5-178): the kit posts a task's result at once, outside the \
         discussion, never as a post entry of the speak queue kept in the room's \
         private state until committed and keyed by the result's id (RMK-2193)",
    ),
    (
        "discussion/a-posted-result-queued-when-the-discussion-ends-is-posted-once-no-turn-runs",
        "RFC §19.7.5 rule 19 (RK-19.7.5-182): the kit posts a task's result at once, outside the \
         discussion, never as a post entry of the speak queue kept in the room's \
         private state until committed and keyed by the result's id (RMK-2193)",
    ),
    (
        "discussion/a-result-a-closing-process-did-not-post-is-posted-by-the-next-uninstall-after-later-messages",
        "RFC §19.7.5 rule 19 (RK-19.7.5-187): the kit posts a task's result at once, outside the \
         discussion, never as a post entry of the speak queue kept in the room's \
         private state until committed and keyed by the result's id (RMK-2193)",
    ),
    (
        "discussion/a-result-s-post-that-comes-second-is-a-replay-and-stores-nothing",
        "RFC §19.7.5 rule 19 (RK-19.7.5-186): the kit posts a task's result at once, outside the \
         discussion, never as a post entry of the speak queue kept in the room's \
         private state until committed and keyed by the result's id (RMK-2193)",
    ),
    (
        "discussion/a-result-to-post-is-kept-with-the-room-s-private-state-until-it-is-committed",
        "RFC §19.7.5 rule 19 (RK-19.7.5-144): the kit posts a task's result at once, outside the \
         discussion, never as a post entry of the speak queue kept in the room's \
         private state until committed and keyed by the result's id (RMK-2193)",
    ),
    (
        "discussion/an-uninstall-in-another-process-posts-the-result-another-holds-before-the-agents-leave",
        "RFC §19.7.5 rule 19 (RK-19.7.5-183): the kit posts a task's result at once, outside the \
         discussion, never as a post entry of the speak queue kept in the room's \
         private state until committed and keyed by the result's id (RMK-2193)",
    ),
    (
        "discussion/an-uninstall-under-the-room-lock-posts-the-waiting-result-itself",
        "RFC §19.7.5 rule 19 (RK-19.7.5-182, RK-19.7.5-185): the kit posts a task's result at once, outside the \
         discussion, never as a post entry of the speak queue kept in the room's \
         private state until committed and keyed by the result's id (RMK-2193)",
    ),
    (
        "memory/a-turn-a-memory-s-summary-joins-keeps-its-label-out-of-the-compaction-s-quote",
        "RFC §6.4 (RK-6.4-376): a compaction's summary quotes a labelled turn whole, the \
         summary a memory joined ahead of it and its label inside the quote, since the \
         transcript keeps no speaker on its messages (RMK-1229)",
    ),
    (
        "streaming/a-streamed-row-s-hooks-run-without-the-room-lock",
        "RFC §9.5.1, §13.5 (RK-9.5.1-067, RK-13.5-007): a streamed row's BEFORE_BROADCAST \
         hooks run in the room's locked pass, so no other message commits while a hook holds \
         the row (RMK-2172)",
    ),
    (
        "tasks/a-task-announced-as-the-kit-s-close-began-ends-cancelled-before-it-runs",
        "RFC §23.3 (RK-23.3-087): the kit's close holds a call's own task, not the \
         delegation it was made in: once the child room's attach returned, the close leaves its \
         grace before the task is announced, and refuses its ON_TASK_DELEGATED and \
         ON_TASK_COMPLETED observers (RMK-2223)",
    ),
    (
        "verification/a-deferred-code-reply-from-a-locked-hook-is-answered-after-the-hook-s-work",
        "RFC §11.7.6 (RK-11.7.6-008): a deferred code reply sent from a hook's locked section \
         has its released message answered before that hook's event is delivered, the \
         release's rerun placed in the room's lane ahead of it (RMK-2183)",
    ),
    (
        "status/a-task-the-close-cuts-reaches-the-status-hooks-before-the-store-is-sealed",
        "timing: RFC §19.8 (RK-19.8-018): on several workers the end a cut turn posts during \
         the kit's close is published on a task the close cuts before its room's \
         ON_STATUS_POSTED hooks run; one thread runs them in time (RMK-2184)",
    ),
];

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

/// Every scenario with executors gives the same when its kit takes its turns
/// through a transport written on the public contract alone (roomkit-specs
/// `conformance/README.md`, "On a transport written on the contract alone").
/// The pass writes no result file.
#[test]
fn every_scenario_with_executors_gives_the_same_on_the_contract_alone() {
    let filter = std::env::var("ROOMKIT_CONFORMANCE_FILTER").ok();
    let run = scenarios::run_on_the_contract_alone(
        &suite().join("scenarios"),
        filter.as_deref(),
        &scenarios::FOUR_WORKERS,
        KNOWN_FAILURES,
        distributed_executors::run_on_the_contract_alone,
    );
    println!(
        "four workers, on the contract alone: {} scenarios, {} failing: {:?}",
        run.cases.len(),
        run.failing.len(),
        run.failing
    );
    assert!(run.report.is_empty(), "\n{}", run.report.join("\n"));
}
