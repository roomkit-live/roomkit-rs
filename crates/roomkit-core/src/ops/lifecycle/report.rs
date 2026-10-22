//! What a lifecycle change reports: its framework event (RFC §8.2) and its
//! trigger's hooks (RFC §9.2). The transition's own record is not subject to
//! the status gate (RFC §5.1).

use crate::model::{
    ChannelBinding, ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventType,
    Metadata, RoomContext, RoomEvent, RoomId, SystemContent,
};
use crate::ops::io::Io;
use crate::ops::{Environment, FrameworkEvent, KitConfig, Notice};
use crate::rules::hook_registry::HookTrigger;

/// A lifecycle change to report.
#[derive(Clone, Copy, Debug)]
pub(super) enum Reported<'change> {
    RoomCreated,
    RoomClosed,
    ChannelAttached(&'change ChannelBinding),
}

/// Emits the change's framework event and fires its trigger's hooks, which
/// receive `context`: the room as the change left it, with its bindings,
/// participants and recent events, as any hook's context has them (gap
/// G-027). A room just created has the bindings its creation made.
pub(super) fn report(
    io: &mut Io,
    config: &KitConfig,
    environment: &mut dyn Environment,
    reported: Reported<'_>,
    context: &RoomContext,
) {
    let room_id = &context.room.id;
    let (event, trigger, channel_id) = match reported {
        Reported::RoomCreated => (
            FrameworkEvent::RoomCreated {
                room_id: room_id.clone(),
            },
            HookTrigger::OnRoomCreated,
            None,
        ),
        Reported::RoomClosed => (
            FrameworkEvent::RoomClosed {
                room_id: room_id.clone(),
            },
            HookTrigger::OnRoomClosed,
            None,
        ),
        Reported::ChannelAttached(binding) => (
            FrameworkEvent::RoomChannelAttached {
                room_id: room_id.clone(),
                channel_id: binding.channel_id.clone(),
                access: binding.access,
                visibility: binding.visibility.clone(),
            },
            HookTrigger::OnChannelAttached,
            Some(&binding.channel_id),
        ),
    };
    io.notify(Notice::FrameworkEvent(event));
    fire_hooks(io, config, environment, trigger, context, channel_id);
}

/// Fires `trigger`'s hooks, if it has any, with the change as their payload.
fn fire_hooks(
    io: &mut Io,
    config: &KitConfig,
    environment: &mut dyn Environment,
    trigger: HookTrigger,
    context: &RoomContext,
    channel_id: Option<&ChannelId>,
) {
    let Some(payload) = payload(environment, trigger, &context.room.id, channel_id) else {
        return;
    };
    let hooks = config.hooks.observers(trigger, &payload.source);
    if hooks.is_empty() {
        return;
    }
    io.notify(Notice::AsyncHooks {
        trigger,
        hooks,
        event: Box::new(payload),
        context: Box::new(context.clone()),
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
