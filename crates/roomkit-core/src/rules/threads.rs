//! In-app threads (RFC §5.2, §10.1 step 8): a thread parent names its
//! thread's root, one level deep, and an event the room holds.
//!
//! Every path that commits an event roots its parent so (RFC §10.1 step 8):
//! the event the parent names is read, a reply replaced by the root it
//! names, which is read in turn; a parent that, once rooted, names no event
//! of the room (none, another room's, one a hard delete removed) is
//! dropped, the event posted at the top level.

use crate::model::{EventId, RoomEvent, RoomId};

/// What the event a parent names, as the store holds it, says of the
/// thread.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Parent {
    /// A root: the event of the room the parent names, which replies to
    /// none.
    Root(EventId),
    /// A reply of the room: its own parent, the thread's root, is read in
    /// turn.
    Reply {
        /// The root the reply names.
        root: EventId,
    },
    /// No event of the room: the store holds none with the id, or another
    /// room's.
    Missing,
}

/// What `read`, the event a parent names as the store holds it, says of an
/// event of `room_id`'s thread (RFC §10.1 step 8).
pub(crate) fn parent(read: Option<&RoomEvent>, room_id: &RoomId) -> Parent {
    let Some(read) = read.filter(|read| &read.room_id == room_id) else {
        return Parent::Missing;
    };
    match &read.parent_event_id {
        Some(root) => Parent::Reply { root: root.clone() },
        None => Parent::Root(read.id.clone()),
    }
}

/// The root an event of `room_id` keeps once its parent named a reply,
/// for `read`, the root that reply names as the store holds it: that root,
/// or none when the room holds no such event (RFC §10.1 step 8: "rooting
/// reads the root it lands on").
pub(crate) fn root(read: Option<&RoomEvent>, room_id: &RoomId) -> Option<EventId> {
    read.filter(|read| &read.room_id == room_id)
        .map(|read| read.id.clone())
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
                ChannelType::SMS,
                EventDirection::Inbound,
            ),
            EventContent::text("hi"),
            Timestamp::UNIX_EPOCH,
        );
        event.parent_event_id = parent.map(|parent| EventId::new(parent).unwrap());
        event
    }

    fn id(id: &str) -> EventId {
        EventId::new(id).unwrap()
    }

    #[test]
    fn a_parent_names_a_root_a_reply_or_nothing_of_the_room() {
        let room = RoomId::new("room_1").unwrap();
        let root_event = event("evt_root", "room_1", None);
        let reply = event("evt_reply", "room_1", Some("evt_root"));
        let elsewhere = event("evt_there", "room_2", None);
        let read = [
            parent(Some(&root_event), &room),
            parent(Some(&reply), &room),
            parent(Some(&elsewhere), &room),
            parent(None, &room),
        ];
        assert_eq!(
            read,
            [
                Parent::Root(id("evt_root")),
                Parent::Reply {
                    root: id("evt_root")
                },
                Parent::Missing,
                Parent::Missing
            ]
        );
    }

    /// RFC §10.1 step 8: the root a reply names is kept only when the room
    /// holds it.
    #[test]
    fn a_reply_s_root_is_kept_only_when_the_room_holds_it() {
        let room = RoomId::new("room_1").unwrap();
        let root_event = event("evt_root", "room_1", None);
        let elsewhere = event("evt_root", "room_2", None);
        assert_eq!(
            [
                root(Some(&root_event), &room),
                root(Some(&elsewhere), &room),
                root(None, &room)
            ],
            [Some(id("evt_root")), None, None]
        );
    }
}
