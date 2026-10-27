//! MCP servers over stdio (RFC §21.2): the process's lifecycle, the
//! listing and its bounds, a call's result, and an agent serving a
//! server's tools. Each test starts the fake server
//! (`fixtures/fake_server.rs`) in a mode of its own. MCP is proven by each
//! implementation's tests and examples, not by the shared suite (step 4,
//! decision 14).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod agent;
mod calls;
#[cfg(target_os = "linux")]
mod lifecycle;
mod listing;
mod support;
