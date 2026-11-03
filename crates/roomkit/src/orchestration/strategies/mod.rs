//! The strategies the kit builds in (RFC §19.7): Python's
//! `roomkit.orchestration.strategies`. The loop (§19.7.4), read from
//! [`roomkit::orchestration`](crate::orchestration), and what it shares
//! with the supervisor (§19.7.3).

mod loop_run;
pub(super) mod review_loop;
pub mod supervisor;
