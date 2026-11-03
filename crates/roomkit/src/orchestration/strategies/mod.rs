//! The strategies the kit builds in (RFC §19.7): Python's
//! `roomkit.orchestration.strategies`. The pipeline (§19.7.1), the loop
//! (§19.7.4), the supervisor (§19.7.3) and the swarm (§19.7), their options in
//! [`roomkit::orchestration`](crate::orchestration).

mod loop_review;
mod loop_run;
pub(super) mod pipeline;
pub(super) mod review_loop;
pub mod supervisor;
pub(super) mod swarm;

#[doc(inline)]
pub use self::pipeline::Pipeline;
#[doc(inline)]
pub use self::review_loop::Loop;
#[doc(inline)]
pub use self::supervisor::Supervisor;
#[doc(inline)]
pub use self::swarm::Swarm;
