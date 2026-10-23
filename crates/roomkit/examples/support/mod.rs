//! What every example shares: printing a room's timeline, and checking what
//! it shows.

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::process::ExitCode;

use roomkit::model::{EventStatus, RoomEvent, RoomId, Visibility};
use roomkit::{ConversationStore, RoomKit, StoreError, text_of};

/// A room's whole timeline, refused events included, as the store holds it.
pub(crate) async fn timeline(
    kit: &RoomKit,
    room_id: &RoomId,
) -> Result<Vec<RoomEvent>, StoreError> {
    kit.store().list_events(room_id, true).await
}

/// Prints the timeline, one event per line, with the channels an event is
/// visible to when it is not visible to all.
pub(crate) fn print_timeline(events: &[RoomEvent]) {
    println!("Timeline:");
    for event in events {
        let status = match (&event.status, &event.blocked_by) {
            (EventStatus::Blocked, Some(blocked_by)) => format!("blocked by {blocked_by}"),
            (status, _) => format!("{status:?}").to_lowercase(),
        };
        let visibility = if event.visibility == Visibility::All {
            String::new()
        } else {
            format!(" (visible to {})", event.visibility)
        };
        println!(
            "  #{} {:<10} depth {} {:<32} {:?}{visibility}",
            event.index,
            event.source.channel_id,
            event.chain_depth,
            status,
            text_of(&event.content)
        );
    }
}

/// The checks an example makes on what it ran, printed as they are made.
#[derive(Debug, Default)]
pub(crate) struct Checks {
    failed: usize,
}

impl Checks {
    /// Prints `PASS` or `FAIL` for `what`.
    pub(crate) fn expect(&mut self, what: &str, holds: bool) {
        if holds {
            println!("PASS {what}");
        } else {
            println!("FAIL {what}");
            self.failed += 1;
        }
    }

    /// The process's exit code: success when every check passed.
    pub(crate) fn finish(self) -> ExitCode {
        if self.failed == 0 {
            println!("All checks passed.");
            ExitCode::SUCCESS
        } else {
            println!("{} check(s) failed.", self.failed);
            ExitCode::FAILURE
        }
    }
}
