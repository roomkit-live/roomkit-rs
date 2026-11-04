//! The store suite (RFC §14), the in-memory store's, on the Postgres store:
//! the suite's files are roomkit's (crates/roomkit/tests/store/suite),
//! included by path.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

#[path = "../../../roomkit/tests/store/suite/mod.rs"]
mod suite;
#[path = "../support/mod.rs"]
mod support;

use std::ops::Deref;

use roomkit_postgres::PostgresStore;

use crate::support::TempSchema;

/// A Postgres store on a schema of its own, dropped with it.
pub(crate) struct Store {
    store: PostgresStore,
    _schema: TempSchema,
}

impl Deref for Store {
    type Target = PostgresStore;

    fn deref(&self) -> &PostgresStore {
        &self.store
    }
}

/// An empty store, on a new schema. Without `POSTGRES_DSN` this binary has
/// nothing to run: the first test to ask ends it as a success, the suite
/// skipped, since every test of the shared suite asks.
#[expect(clippy::print_stderr, reason = "says why the suite ran nothing")]
pub(crate) async fn store() -> Store {
    if support::dsn().is_none() {
        eprintln!("POSTGRES_DSN is not set: the store suite is skipped on Postgres");
        std::process::exit(0);
    }
    let schema = TempSchema::new().await;
    Store {
        store: schema.initialized_store().await,
        _schema: schema,
    }
}
