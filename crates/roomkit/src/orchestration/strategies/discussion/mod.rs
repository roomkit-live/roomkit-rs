//! The discussion (RFC §19.7.5): Python's
//! `roomkit.orchestration.strategies.discussion`. Several agents and one or
//! more people hold one conversation in a room: any agent may address any
//! other by `@handle`, and one agent speaks at a time.

mod config;
pub(in crate::orchestration) mod dispatch;
mod dispatching;
mod driver;
mod lease;
mod models;
mod notes;
mod queue;
pub(crate) mod room;
pub(in crate::orchestration) mod strategy;

#[doc(inline)]
pub use self::dispatch::{
    DispatchCandidate, DispatchDecision, DispatchDecisionEvent, DispatchPolicy, DispatchTurn,
    MockDispatchPolicy,
};
#[doc(inline)]
pub use self::models::{SpeakQueue, SpeakQueueChange, SpeakQueueEvent};
#[doc(inline)]
pub use self::strategy::Discussion;
pub(crate) use self::strategy::DoneFn;
