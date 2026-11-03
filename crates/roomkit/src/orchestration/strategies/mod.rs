//! The strategies the kit builds in (RFC §19.7): Python's
//! `roomkit.orchestration.strategies`. The loop (§19.7.4) and the
//! supervisor (§19.7.3), their options in
//! [`roomkit::orchestration`](crate::orchestration).

mod loop_review;
mod loop_run;
pub(super) mod review_loop;
pub mod supervisor;

#[doc(inline)]
pub use self::review_loop::Loop;
#[doc(inline)]
pub use self::supervisor::Supervisor;
