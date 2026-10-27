//! Skills from the host's own directories (RFC §24): discovery and
//! registration, then the agent's turns that list, activate and read them.
//! Skills are proven by each implementation's tests, not by the shared
//! suite (step 4, decision 14).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod discovery;
mod support;
mod turns;
