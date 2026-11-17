//! Public provider context fields (Python `AIContext`, RFC §6.7).

use roomkit::model::{ChannelCapabilities, ChannelMediaType, Room, RoomContext, RoomId};
use roomkit_ai::AiContext;

#[test]
fn provider_room_and_target_fields_are_independent_and_kept_by_a_copy() {
    let empty = AiContext::default();
    assert!(empty.room.is_none());
    assert!(empty.target_capabilities.is_none());
    assert_eq!(empty.target_media_types, []);
    let room = RoomContext::new(Room::new(RoomId::new("room").unwrap()));
    let caps = ChannelCapabilities::default();
    let context = AiContext {
        room: Some(room.clone()),
        target_capabilities: Some(caps.clone()),
        target_media_types: vec![ChannelMediaType::Audio],
        ..empty
    };
    let copy = AiContext {
        system_prompt: Some("updated".into()),
        ..context.clone()
    };
    assert_eq!(copy.room, Some(room));
    assert_eq!(copy.target_capabilities, Some(caps));
    assert_eq!(copy.target_media_types, [ChannelMediaType::Audio]);
    assert!(context.system_prompt.is_none());
}
