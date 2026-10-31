//! The room an example left, as it prints it.

use crate::text_of;
use roomkit::model::{EventStatus, RoomEvent, RoomId, Visibility};
use roomkit::{ConversationStore, RoomKit, StoreError};

/// A room's whole timeline, refused events included, as the store holds it.
///
/// # Errors
///
/// Returns the store's error when the read fails.
pub async fn timeline<S: ConversationStore>(
    kit: &RoomKit<S>,
    room_id: &RoomId,
) -> Result<Vec<RoomEvent>, StoreError> {
    kit.store().list_events(room_id, every_row()).await
}

/// Prints the timeline, one event per line: its index, source, depth,
/// status and text, then whether it was edited or deleted, and the channels
/// it is visible to when it is not visible to all.
pub fn print_timeline(events: &[RoomEvent]) {
    println!("Timeline:");
    for event in events {
        let status = match (&event.status, &event.blocked_by) {
            (EventStatus::Blocked, Some(blocked_by)) => format!("blocked by {blocked_by}"),
            (status, _) => format!("{status:?}").to_lowercase(),
        };
        let changed = if is_deleted(event) {
            " (deleted)"
        } else if is_edited(event) {
            " (edited)"
        } else {
            ""
        };
        let visibility = if event.visibility == Visibility::All {
            String::new()
        } else {
            format!(" (visible to {})", event.visibility)
        };
        println!(
            "  #{} {:<12} depth {} {:<32} {:?}{changed}{visibility}",
            event.index,
            event.source.channel_id,
            event.chain_depth,
            status,
            text_of(&event.content)
        );
    }
}

/// Every row of a room's timeline, the refused ones included: the read of
/// [`timeline`], for a store read without a kit.
pub fn every_row() -> roomkit::store::ListEventsOptions {
    roomkit::store::ListEventsOptions {
        limit: u64::MAX,
        event_filter: Some(roomkit::EventFilter {
            include_blocked: true,
            ..roomkit::EventFilter::default()
        }),
        ..roomkit::store::ListEventsOptions::default()
    }
}

/// Whether `event` was deleted: an accepted delete marks its metadata
/// `deleted` (RFC §10.3), as Python's `metadata.get("deleted")` reads it.
pub fn is_deleted(event: &RoomEvent) -> bool {
    marked(event, "deleted")
}

/// Whether `event` was edited: an accepted edit marks its metadata `edited`
/// (RFC §10.3).
pub fn is_edited(event: &RoomEvent) -> bool {
    marked(event, "edited")
}

fn marked(event: &RoomEvent, mark: &str) -> bool {
    event
        .metadata
        .get(mark)
        .and_then(serde_json::Value::as_bool)
        == Some(true)
}
