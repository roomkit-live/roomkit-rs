//! In-app threads (RFC §5.2, §10.1 step 8): a thread parent names its
//! thread's root, one level deep.

use crate::model::{EventId, RoomEvent, RoomId};

/// The parent an event of `room_id` keeps once its thread is rooted, for
/// `parent`, the event its parent names as the store holds it (RFC §10.1
/// step 8): that event's own parent when it is a reply, so a reply to a
/// reply joins the thread's root; that event itself otherwise; none when
/// the store holds no such event, or it is another room's, so the event is
/// posted at the top level.
pub fn root(parent: Option<&RoomEvent>, room_id: &RoomId) -> Option<EventId> {
    let parent = parent.filter(|parent| &parent.room_id == room_id)?;
    Some(
        parent
            .parent_event_id
            .clone()
            .unwrap_or_else(|| parent.id.clone()),
    )
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{
        ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventType, Timestamp,
    };

    fn event(id: &str, room: &str, parent: Option<&str>) -> RoomEvent {
        let mut event = RoomEvent::new(
            EventId::new(id).unwrap(),
            RoomId::new(room).unwrap(),
            EventType::Message,
            EventSource::new(
                ChannelId::new("sms").unwrap(),
                ChannelType::Sms,
                EventDirection::Inbound,
            ),
            EventContent::text("hi"),
            Timestamp::UNIX_EPOCH,
        );
        event.parent_event_id = parent.map(|parent| EventId::new(parent).unwrap());
        event
    }

    #[test]
    fn a_parent_is_rooted_one_level_up_and_dropped_out_of_the_room() {
        let room = RoomId::new("room_1").unwrap();
        let root_event = event("evt_root", "room_1", None);
        let reply = event("evt_reply", "room_1", Some("evt_root"));
        let elsewhere = event("evt_there", "room_2", None);
        let rooted = [
            root(Some(&root_event), &room),
            root(Some(&reply), &room),
            root(Some(&elsewhere), &room),
            root(None, &room),
        ];
        let root_id = Some(EventId::new("evt_root").unwrap());
        assert_eq!(rooted, [root_id.clone(), root_id, None, None]);
    }
}
