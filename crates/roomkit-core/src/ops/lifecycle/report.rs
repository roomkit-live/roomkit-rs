//! What a lifecycle change reports: its framework event (RFC §8.2) and its
//! trigger's hooks (RFC §9.2). The transition's own record is not subject to
//! the status gate (RFC §5.1).

use crate::model::{
    ChannelBinding, ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventType,
    Metadata, Room, RoomContext, RoomEvent, RoomId, SystemContent,
};
use crate::ops::io::Io;
use crate::ops::{Environment, FrameworkEvent, KitConfig, Notice};
use crate::rules::hook_registry::HookTrigger;

/// A lifecycle change to report.
#[derive(Clone, Copy, Debug)]
pub(super) enum Reported<'change> {
    RoomCreated(&'change Room),
    RoomClosed(&'change Room),
    ChannelAttached(&'change ChannelBinding),
}

/// Emits the change's framework event and fires its trigger's hooks.
pub(super) fn report(
    io: &mut Io,
    config: &KitConfig,
    environment: &mut dyn Environment,
    reported: Reported<'_>,
) {
    let (event, trigger, room_id, channel_id) = match reported {
        Reported::RoomCreated(room) => (
            FrameworkEvent::RoomCreated {
                room_id: room.id.clone(),
            },
            HookTrigger::OnRoomCreated,
            &room.id,
            None,
        ),
        Reported::RoomClosed(room) => (
            FrameworkEvent::RoomClosed {
                room_id: room.id.clone(),
            },
            HookTrigger::OnRoomClosed,
            &room.id,
            None,
        ),
        Reported::ChannelAttached(binding) => (
            FrameworkEvent::RoomChannelAttached {
                room_id: binding.room_id.clone(),
                channel_id: binding.channel_id.clone(),
                access: binding.access,
                visibility: binding.visibility.clone(),
            },
            HookTrigger::OnChannelAttached,
            &binding.room_id,
            Some(&binding.channel_id),
        ),
    };
    io.notify(Notice::FrameworkEvent(event));
    fire_hooks(io, config, environment, trigger, room_id, channel_id);
}

/// Fires `trigger`'s hooks, if it has any, with the change as their payload.
fn fire_hooks(
    io: &mut Io,
    config: &KitConfig,
    environment: &mut dyn Environment,
    trigger: HookTrigger,
    room_id: &RoomId,
    channel_id: Option<&ChannelId>,
) {
    let Some(payload) = payload(environment, trigger, room_id, channel_id) else {
        return;
    };
    let hooks = config.hooks.observers(trigger, &payload.source);
    if hooks.is_empty() {
        return;
    }
    let context = RoomContext::new(Room::new(room_id.clone(), payload.created_at));
    io.notify(Notice::AsyncHooks {
        trigger,
        hooks,
        event: Box::new(payload),
        context: Box::new(context),
    });
}

/// The payload of a lifecycle hook: a system event whose code names the
/// change and whose data names the room and the channel.
fn payload(
    environment: &mut dyn Environment,
    trigger: HookTrigger,
    room_id: &RoomId,
    channel_id: Option<&ChannelId>,
) -> Option<RoomEvent> {
    let code = match trigger {
        HookTrigger::OnRoomCreated => "room_created",
        HookTrigger::OnRoomClosed => "room_closed",
        HookTrigger::OnChannelAttached => "channel_attached",
        HookTrigger::BeforeBroadcast
        | HookTrigger::AfterBroadcast
        | HookTrigger::OnError
        | HookTrigger::OnTranscription
        | HookTrigger::BeforeTts
        | HookTrigger::BeforeToolUse => return None,
    };
    let system = ChannelId::new("system").ok()?;
    let source = EventSource::new(system, ChannelType::System, EventDirection::Outbound);
    let mut data = Metadata::new();
    data.insert("room_id".to_owned(), room_id.as_str().into());
    if let Some(channel_id) = channel_id {
        data.insert("channel_id".to_owned(), channel_id.as_str().into());
    }
    let content = EventContent::System(SystemContent {
        body: code.replace('_', " "),
        code: Some(code.to_owned()),
        data,
    });
    let now = environment.now();
    Some(RoomEvent::new(
        environment.new_event_id(),
        room_id.clone(),
        EventType::System,
        source,
        content,
        now,
    ))
}
