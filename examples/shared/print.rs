//! The room an example left, as it prints it.

use roomkit::model::{EventStatus, RoomEvent, RoomId, Visibility};
use roomkit::{ConversationStore, RoomKit, StoreError, text_of};

/// A room's whole timeline, refused events included, as the store holds it.
///
/// # Errors
///
/// Returns the store's error when the read fails.
pub async fn timeline(kit: &RoomKit, room_id: &RoomId) -> Result<Vec<RoomEvent>, StoreError> {
    kit.store().list_events(room_id, true).await
}

/// Prints the timeline, one event per line, with the channels an event is
/// visible to when it is not visible to all.
pub fn print_timeline(events: &[RoomEvent]) {
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
