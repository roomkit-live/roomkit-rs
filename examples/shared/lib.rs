//! What the RoomKit examples share, so that each one reads as the roomkit
//! calls it shows.
//!
//! - [`kit`] and [`Clock`]: a kit whose clock moves only when told and whose
//!   ids are numbered in order, so that every run prints the same;
//! - [`Log`] and [`framework_events`]: what the example's hooks and
//!   channels note, and what the kit reports, while it runs;
//! - [`timeline`] and [`print_timeline`]: the room an example left;
//! - [`ScriptedAgent`]: an agent standing in for a model, answering from a
//!   script and keeping what it read;
//! - [`SmsNumber`]: a business's SMS number, sending to the number each
//!   room's binding names;
//! - [`recording`]: a recording transport named by a string;
//! - [`text_of`]: what a person reads of a content;
//! - [`Checks`]: the checks that make an example verify what it shows, and
//!   its exit code.

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

mod agents;
mod checks;
mod clock;
mod observe;
mod print;
mod recording;
mod sms;
mod text;

pub use self::agents::{Script, ScriptedAgent, Turn};
pub use self::checks::Checks;
pub use self::clock::{Clock, kit, kit_with};
pub use self::observe::{Log, framework_events};
pub use self::print::{every_row, is_deleted, is_edited, print_timeline, timeline};
pub use self::recording::recording;
pub use self::sms::SmsNumber;
pub use self::text::text_of;
