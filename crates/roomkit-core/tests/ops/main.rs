//! Trace tests of the operations: a simulated driver executes every effect,
//! and each test checks the effects asked for, in order, and the result.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "the simulated driver fails the test on a broken invariant"
)]

mod answering;
mod authors;
mod budget;
mod edits;
mod inbound;
mod injection;
mod instructions;
mod lifecycle;
mod named_room;
mod off_lock;
mod post;
mod rerun;
mod routing;
mod support;
mod timeout;
// The lines `tracing` emits, for a test that checks one is logged.
#[expect(
    clippy::disallowed_types,
    reason = "the test binary's log capture, shared with roomkit's tests: no room lock"
)]
#[path = "../../../roomkit/tests/support/warnings.rs"]
mod warnings;
