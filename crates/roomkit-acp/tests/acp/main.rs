//! roomkit-acp's connection and stdio transport, through the public API:
//! a scripted agent over an in-memory pipe, and the `fake-acp-agent`
//! fixture over a real process's stdio.

#![expect(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "a test fails on a broken fixture"
)]

mod channel;
mod connection;
mod stdio;
mod support;
