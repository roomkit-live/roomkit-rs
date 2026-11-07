//! Every turn through the turn backend (RFC §10.6, step 4 milestone 7),
//! end to end through a kit and an AI agent: the messages a turn carries,
//! its cancellation, its close, and the depth of what it sends across.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]
#![expect(
    clippy::indexing_slicing,
    reason = "the tests read messages as serde_json values, whose index reads null for a \
              missing member and never panics"
)]

mod busy;
mod cuts;
mod delegation;
mod depth;
mod host;
mod result_tool;
mod steering;
mod streams;
mod stuck_target;
mod support;
mod tap;
mod told_ends;
