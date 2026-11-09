//! The shared conformance suite (RFC §1.4, §26.1) on the Postgres store: the
//! runner is roomkit's (crates/roomkit/tests/conformance), included by
//! path, and so are the scenarios it reads, the repository's copy.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#![expect(
    clippy::indexing_slicing,
    reason = "the runner reads scenarios as serde_json values, whose index reads null for a \
              missing member and never panics; its own lists are indexed within their length"
)]

#[path = "../../../roomkit/tests/conformance/acp_agents.rs"]
mod acp_agents;
#[path = "../../../roomkit/tests/conformance/agents.rs"]
mod agents;
#[path = "../../../roomkit/tests/conformance/ai_hooks.rs"]
mod ai_hooks;
#[path = "../../../roomkit/tests/conformance/audits.rs"]
mod audits;
#[path = "../../../roomkit/tests/conformance/calls.rs"]
mod calls;
#[path = "../../../roomkit/tests/conformance/chat_wire.rs"]
mod chat_wire;
#[path = "../../../roomkit/tests/conformance/compare.rs"]
mod compare;
#[path = "../../../roomkit/tests/conformance/delegate_calls.rs"]
mod delegate_calls;
#[path = "../../../roomkit/tests/conformance/discussion_calls.rs"]
mod discussion_calls;
#[path = "../../../roomkit/tests/conformance/errors.rs"]
mod errors;
#[path = "../../../roomkit/tests/conformance/handoff_calls.rs"]
mod handoff_calls;
#[path = "../../../roomkit/tests/conformance/harness.rs"]
mod harness;
#[path = "../../../roomkit/tests/conformance/identity.rs"]
mod identity;
#[path = "../../../roomkit/tests/conformance/input_calls.rs"]
mod input_calls;
#[path = "../../../roomkit/tests/conformance/locks.rs"]
mod locks;
#[path = "../../../roomkit/tests/conformance/matching.rs"]
mod matching;
#[path = "../../../roomkit/tests/conformance/media.rs"]
mod media;
#[path = "../../../roomkit/tests/conformance/member_calls.rs"]
mod member_calls;
#[path = "../../../roomkit/tests/conformance/memory.rs"]
mod memory;
#[path = "../../../roomkit/tests/conformance/normalize.rs"]
mod normalize;
#[path = "../../../roomkit/tests/conformance/room_calls.rs"]
mod room_calls;
#[path = "../../../roomkit/tests/conformance/room_queries.rs"]
mod room_queries;
#[path = "../../../roomkit/tests/conformance/routing_calls.rs"]
mod routing_calls;
#[path = "../../../roomkit/tests/conformance/run.rs"]
mod run;
#[path = "../../../roomkit/tests/conformance/scenarios.rs"]
mod scenarios;
#[path = "../../../roomkit/tests/conformance/scripted.rs"]
mod scripted;
#[path = "../../../roomkit/tests/conformance/shape.rs"]
mod shape;
#[path = "../../../roomkit/tests/conformance/skills.rs"]
mod skills;
#[path = "../../../roomkit/tests/conformance/store_calls.rs"]
mod store_calls;
#[path = "../../../roomkit/tests/conformance/strategy_calls.rs"]
mod strategy_calls;
#[path = "../support/mod.rs"]
mod support;
#[path = "../../../roomkit/tests/conformance/task_calls.rs"]
mod task_calls;
#[path = "../../../roomkit/tests/conformance/templates.rs"]
mod templates;
#[path = "../../../roomkit/tests/conformance/verification_calls.rs"]
mod verification_calls;

use std::time::Duration;

use std::sync::Arc;

use roomkit::{RoomKit, RoomKitOptions};
use roomkit_postgres::{
    PostgresAdvisoryLockManager, PostgresAdvisoryLockManagerOptions, PostgresStore,
};

use crate::support::TempSchema;

/// Scenario id → the rule this implementation breaks on this store: the
/// in-memory store's, and those a reason led by `timing:` names, which
/// depend on the machine's speed and may pass.
pub(crate) const KNOWN_FAILURES: &[(&str, &str)] = &[
    (
        "orchestration/a-team-worker-past-its-task-timeout-is-read-as-timed-out-beside-the-others",
        "timing: the worker that answers has 50 ms, which a debug build's round trips to a server \
         pass, and is cut with the one that must be (RMK-1334)",
    ),
    (
        "orchestration/a-supervised-step-past-its-task-timeout-stops-the-chain-unreviewed",
        "timing: the same 50 ms, passed under load only (RMK-1334)",
    ),
];

/// A scenario that has not finished by then fails instead of hanging the
/// suite.
pub(crate) const SCENARIO_BOUND: Duration = Duration::from_secs(60);

/// The kit the scenarios run on, over the Postgres store.
pub(crate) type Kit = RoomKit<PostgresStore>;
/// The store of the kit the runner builds.
pub(crate) type Store = PostgresStore;

/// A kit over a store on a new schema, with the advisory lock a deployment on
/// this store takes, unless the scenario scripts one; built with what
/// `configure` makes of the defaults, and the schema, dropped once the run is
/// over.
pub(crate) async fn new_kit(
    configure: impl FnOnce(RoomKitOptions<PostgresStore>) -> RoomKitOptions<PostgresStore>,
) -> (Kit, TempSchema) {
    let schema = TempSchema::new().await;
    let store = schema.initialized_store().await;
    let mut options = configure(RoomKitOptions::with_store(store));
    if options.lock_manager.is_none() {
        let locks = PostgresAdvisoryLockManager::new(PostgresAdvisoryLockManagerOptions {
            dsn: Some(schema.dsn()),
            max_size: 8,
            ..PostgresAdvisoryLockManagerOptions::default()
        });
        locks.init().await.unwrap();
        options.lock_manager = Some(Arc::new(locks));
    }
    (RoomKit::new(options), schema)
}

/// Observe the same schema through a pool no scenario call can close.
pub(crate) async fn observer_store(_store: &Arc<Store>, schema: &TempSchema) -> Arc<Store> {
    Arc::new(schema.initialized_store().await)
}

/// Another process serving the rooms (README, `processes`): a kit over a
/// store of its own on the run's schema and an advisory lock manager of its
/// own, as another process opens them.
pub(crate) async fn process_kit(
    _store: &Arc<PostgresStore>,
    schema: &TempSchema,
    _lock_manager: Arc<dyn roomkit::RoomLockManager>,
) -> Kit {
    let store = schema.initialized_store().await;
    let locks = PostgresAdvisoryLockManager::new(PostgresAdvisoryLockManagerOptions {
        dsn: Some(schema.dsn()),
        max_size: 8,
        ..PostgresAdvisoryLockManagerOptions::default()
    });
    locks.init().await.unwrap();
    RoomKit::new(RoomKitOptions {
        lock_manager: Some(Arc::new(locks)),
        ..RoomKitOptions::with_store(store)
    })
}

/// Every scenario runs; those this implementation fails on this store are
/// exactly the known failures. Without `POSTGRES_DSN` it skips itself.
#[test]
fn every_scenario_gives_what_the_rfc_says() {
    if support::dsn().is_none() {
        eprintln!("POSTGRES_DSN is not set: the conformance suite is skipped on Postgres");
        return;
    }
    scenarios::every_scenario_gives_what_the_rfc_says(
        "one thread",
        scenarios::current_thread,
        KNOWN_FAILURES,
    );
}
