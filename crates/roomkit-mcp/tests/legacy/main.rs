//! Legacy SSE public transport boundaries, beside the shared lifecycle scenarios.
#![cfg(feature = "http")]
#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod boundaries;
mod calls;
mod headers;
mod opening;
mod peer;
