//! Creating a room, attaching a channel and closing a room (RFC §5.1, §8.2).

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelType, Room, RoomStatus, Timestamp,
};
use roomkit_core::ops::{AttachChannel, CloseRoom, CreateRoom, FrameworkEvent, LifecycleError};
use roomkit_core::rules::hook_registry::HookTrigger;

use crate::support::{Sim, channel, room_id};

fn sms_binding() -> ChannelBinding {
    ChannelBinding::new(
        channel("sms"),
        room_id(),
        ChannelType::Sms,
        ChannelCategory::Transport,
        ChannelDirection::Bidirectional,
        Timestamp::UNIX_EPOCH,
    )
}

mod create_room {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn stores_the_room_and_emits_room_created() {
        let mut sim = Sim::new();
        let room = Room::new(room_id(), Timestamp::UNIX_EPOCH);
        let created = sim.run(CreateRoom::new(sim.shared_config(), room.clone()));
        assert_eq!(
            (created, sim.framework_event_names()),
            (Ok(room), vec!["room_created".to_owned()])
        );
    }

    #[test]
    fn fires_on_room_created() {
        let mut sim = Sim::new();
        sim.observer("welcome", HookTrigger::OnRoomCreated);
        let room = Room::new(room_id(), Timestamp::UNIX_EPOCH);
        sim.run(CreateRoom::new(sim.shared_config(), room)).unwrap();
        assert_eq!(sim.async_triggers(), [HookTrigger::OnRoomCreated]);
    }
}

mod attach_channel {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn stores_the_binding_under_the_room_lock() {
        let mut sim = Sim::new();
        sim.run(AttachChannel::new(sim.shared_config(), sms_binding()))
            .unwrap();
        assert_eq!(
            sim.trace,
            [
                "lock room_1",
                "load_context room_1",
                "store binding sms",
                "release room_1",
            ]
        );
    }

    #[test]
    fn emits_room_channel_attached_with_the_binding_s_permissions() {
        let mut sim = Sim::new();
        sim.run(AttachChannel::new(sim.shared_config(), sms_binding()))
            .unwrap();
        assert_eq!(
            sim.framework_events(),
            [&FrameworkEvent::RoomChannelAttached {
                room_id: room_id(),
                channel_id: channel("sms"),
                access: sms_binding().access,
                visibility: sms_binding().visibility,
            }]
        );
    }

    #[test]
    fn to_a_missing_room_fails_and_releases_the_lock() {
        let mut sim = Sim::new();
        sim.room_exists = false;
        let attached = sim.run(AttachChannel::new(sim.shared_config(), sms_binding()));
        assert_eq!(attached, Err(LifecycleError::RoomNotFound(room_id())));
    }
}

mod close_room {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn closes_the_room_under_its_lock() {
        let mut sim = Sim::new();
        let closed = sim
            .run(CloseRoom::new(sim.shared_config(), room_id()))
            .unwrap();
        assert_eq!(
            (closed.status, closed.closed_at.is_some(), sim.room.status),
            (RoomStatus::Closed, true, RoomStatus::Closed)
        );
    }

    #[test]
    fn emits_room_closed_and_fires_on_room_closed() {
        let mut sim = Sim::new();
        sim.observer("archive", HookTrigger::OnRoomClosed);
        sim.run(CloseRoom::new(sim.shared_config(), room_id()))
            .unwrap();
        assert_eq!(
            (sim.framework_event_names(), sim.async_triggers()),
            (
                vec!["room_closed".to_owned()],
                vec![HookTrigger::OnRoomClosed]
            )
        );
    }

    #[test]
    fn changes_nothing_when_the_room_is_already_closed() {
        let mut sim = Sim::new();
        sim.room.status = RoomStatus::Archived;
        sim.run(CloseRoom::new(sim.shared_config(), room_id()))
            .unwrap();
        assert_eq!(
            (sim.room.status, sim.framework_event_names()),
            (RoomStatus::Archived, Vec::<String>::new())
        );
    }

    #[test]
    fn then_refuses_the_next_message() {
        let mut sim = Sim::new();
        sim.transport("sms");
        sim.run(CloseRoom::new(sim.shared_config(), room_id()))
            .unwrap();
        let result = sim.send(Sim::message("sms", "Hello?")).unwrap();
        assert_eq!(result.reason.as_deref(), Some("room_closed"));
    }
}
