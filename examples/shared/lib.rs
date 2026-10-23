//! What the RoomKit examples share, so that each one reads as the roomkit
//! calls it shows.
//!
//! - [`kit`] and [`Clock`]: a kit whose clock moves only when told and whose
//!   ids are numbered in order, so that every run prints the same;
//! - [`timeline`] and [`print_timeline`]: the room an example left;
//! - [`Checks`]: the checks that make an example verify what it shows, and
//!   its exit code.

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

mod checks;
mod clock;
mod print;

pub use self::checks::Checks;
pub use self::clock::{Clock, kit};
pub use self::print::{print_timeline, timeline};
