//! Public connection scopes against an isolated PostgreSQL schema.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod abandon;
mod cancellation;
mod errors;
mod fixture;
mod nominal;
#[path = "../support/mod.rs"]
mod support;
