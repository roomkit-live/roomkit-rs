//! The discussion (RFC §19.7.5): Python's
//! `roomkit.orchestration.strategies.discussion`. Several agents and one or
//! more people hold one conversation in a room: any agent may address any
//! other by `@handle`, and one agent speaks at a time.

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "read by the running turns, lot 8c")
)]
mod lease;
mod models;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "read by the running turns, lot 8c")
)]
mod notes;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "read by the running turns, lot 8c")
)]
mod queue;

#[doc(inline)]
pub use self::models::{SpeakQueue, SpeakQueueChange, SpeakQueueEvent};
