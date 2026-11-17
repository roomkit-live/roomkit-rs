//! Public connection scopes against an isolated PostgreSQL schema.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod abandon;
mod cancellation;
mod errors;
mod fixture;
mod nominal;
mod release_budget;
mod release_ready;
#[path = "../support/mod.rs"]
mod support;
