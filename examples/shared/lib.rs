//! What the RoomKit examples share, so that each one reads as the roomkit
//! calls it shows.
//!
//! - [`kit`] and [`Clock`]: a kit whose clock moves only when told and whose
//!   ids are numbered in order, so that every run prints the same;
//! - [`Log`], [`framework_events`] and [`next_event`]: what the example's hooks and
//!   channels note, and what the kit reports, while it runs;
//! - [`timeline`] and [`print_timeline`]: the room an example left;
//! - [`ScriptedAgent`]: an agent standing in for a model, answering from a
//!   script and keeping what it read;
//! - [`Checks`]: the checks that make an example verify what it shows, and
//!   its exit code.

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

mod agents;
mod checks;
mod clock;
mod observe;
mod print;

pub use self::agents::{Script, ScriptedAgent, Turn};
pub use self::checks::Checks;
pub use self::clock::{Clock, kit};
pub use self::observe::{Log, framework_events, next_event};
pub use self::print::{print_timeline, timeline};
