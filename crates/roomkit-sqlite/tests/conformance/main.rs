//! The shared conformance suite (RFC §1.4, §26.1) on the SQLite store: the
//! runner is roomkit's (crates/roomkit/tests/conformance), included by
//! path, and so are the scenarios it reads, the repository's copy.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#![expect(
    clippy::indexing_slicing,
    reason = "the runner reads scenarios as serde_json values, whose index reads null for a \
              missing member and never panics; its own lists are indexed within their length"
)]

#[path = "../../../roomkit/tests/conformance/agents.rs"]
mod agents;
#[path = "../../../roomkit/tests/conformance/calls.rs"]
mod calls;
#[path = "../../../roomkit/tests/conformance/compare.rs"]
mod compare;
#[path = "../../../roomkit/tests/conformance/errors.rs"]
mod errors;
#[path = "../../../roomkit/tests/conformance/harness.rs"]
mod harness;
#[path = "../../../roomkit/tests/conformance/identity.rs"]
mod identity;
#[path = "../../../roomkit/tests/conformance/matching.rs"]
mod matching;
#[path = "../../../roomkit/tests/conformance/member_calls.rs"]
mod member_calls;
#[path = "../../../roomkit/tests/conformance/memory.rs"]
mod memory;
#[path = "../../../roomkit/tests/conformance/normalize.rs"]
mod normalize;
#[path = "../../../roomkit/tests/conformance/run.rs"]
mod run;
#[path = "../../../roomkit/tests/conformance/scenarios.rs"]
mod scenarios;
#[path = "../../../roomkit/tests/conformance/scripted.rs"]
mod scripted;
#[path = "../../../roomkit/tests/conformance/shape.rs"]
mod shape;
#[path = "../../../roomkit/tests/conformance/store_calls.rs"]
mod store_calls;
#[path = "../support/mod.rs"]
mod support;
#[path = "../../../roomkit/tests/conformance/task_calls.rs"]
mod task_calls;
#[path = "../../../roomkit/tests/conformance/templates.rs"]
mod templates;

use std::time::Duration;

use roomkit::{RoomKit, RoomKitOptions};
use roomkit_sqlite::{SqliteStore, SqliteStoreOptions};

use crate::support::TempFile;

/// Scenario id → the rule this implementation breaks on this store: none
/// beyond the in-memory store's (crates/roomkit/tests/conformance).
pub(crate) const KNOWN_FAILURES: &[(&str, &str)] = &[];

/// A scenario that has not finished by then fails instead of hanging the
/// suite.
pub(crate) const SCENARIO_BOUND: Duration = Duration::from_secs(60);

/// The kit the scenarios run on, over the SQLite store.
pub(crate) type Kit = RoomKit<SqliteStore>;
/// The store of the kit the runner builds.
pub(crate) type Store = SqliteStore;

/// A kit over a store on a new file, built with what `configure` makes of
/// the defaults, and the file, removed once the run is over.
#[expect(
    clippy::unused_async,
    reason = "the shared scenarios await their kit, which another store may open at once"
)]
pub(crate) async fn new_kit(
    configure: impl FnOnce(RoomKitOptions<SqliteStore>) -> RoomKitOptions<SqliteStore>,
) -> (Kit, TempFile) {
    let file = TempFile::new("conformance");
    let store = SqliteStore::new(SqliteStoreOptions {
        path: file.path().into(),
    });
    (
        RoomKit::new(configure(RoomKitOptions::with_store(store))),
        file,
    )
}

/// Every scenario runs; those this implementation fails on this store are
/// exactly the known failures.
#[test]
fn every_scenario_gives_what_the_rfc_says() {
    scenarios::every_scenario_gives_what_the_rfc_says();
}
