//! Trace tests of the operations: a simulated driver executes every effect,
//! and each test checks the effects asked for, in order, and the result.

#![expect(
    clippy::unwrap_used,
    clippy::panic,
    reason = "the simulated driver fails the test on a broken invariant"
)]

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
mod routing;
mod support;
mod timeout;
