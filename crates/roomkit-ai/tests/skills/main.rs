//! Skills from the host's own directories (RFC §24): discovery and
//! registration, then the agent's turns that list, activate and read them.
//! Skills are proven by each implementation's tests (step 4, decision 14);
//! a turn's own registry by the shared suite too
//! (`conformance/scenarios/skills/`).

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod discovery;
mod support;
mod turns;
#[path = "../../../roomkit/tests/support/warnings.rs"]
mod warnings;
