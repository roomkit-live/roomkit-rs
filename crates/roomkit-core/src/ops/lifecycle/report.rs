//! What a lifecycle change reports: its framework event (RFC §8.2) and its
//! trigger's hooks (RFC §9.2), as the table of RFC §26.1 pairs them. The
//! transition's own record is not subject to the status gate (RFC §5.1).

use crate::model::{
    ChannelBinding, ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventType,
    Metadata, RoomContext, RoomEvent, RoomId, SystemContent,
};
use crate::ops::{Cx, FrameworkEvent, Notice, TransitionReason};
use crate::rules::hook_registry::HookTrigger;

/// A lifecycle change to report.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Reported<'change> {
    RoomCreated,
    /// The reason is `None` for the integrator's pause.
    RoomPaused(Option<TransitionReason>),
    /// The reason is `None` for the integrator's close.
    RoomClosed(Option<TransitionReason>),
    RoomArchived,
    ChannelAttached(&'change ChannelBinding),
    ChannelDetached(&'change ChannelBinding),
    ChannelMuted(&'change ChannelBinding),
    ChannelUnmuted(&'change ChannelBinding),
}

impl Reported<'_> {
    /// The framework event the change emits, if any (RFC §26.1): a pause
    /// emits one only when its timer paused the room, a mute none.
    fn framework_event(self, room_id: &RoomId) -> Option<FrameworkEvent> {
        let room_id = room_id.clone();
        match self {
            Self::RoomCreated => Some(FrameworkEvent::RoomCreated { room_id }),
            Self::RoomPaused(reason) => {
                reason.map(|reason| FrameworkEvent::RoomPaused { room_id, reason })
            }
            Self::RoomClosed(reason) => Some(FrameworkEvent::RoomClosed { room_id, reason }),
            Self::RoomArchived => Some(FrameworkEvent::RoomArchived { room_id }),
            Self::ChannelAttached(binding) => Some(FrameworkEvent::RoomChannelAttached {
                room_id,
                channel_id: binding.channel_id.clone(),
                access: binding.access,
                visibility: binding.visibility.clone(),
            }),
            Self::ChannelDetached(binding) => Some(FrameworkEvent::RoomChannelDetached {
                room_id,
                channel_id: binding.channel_id.clone(),
            }),
            Self::ChannelMuted(_) | Self::ChannelUnmuted(_) => None,
        }
    }

    /// The trigger the change fires, if any (RFC §26.1): an archive fires
    /// none.
    fn trigger(self) -> Option<HookTrigger> {
        match self {
            Self::RoomCreated => Some(HookTrigger::OnRoomCreated),
            Self::RoomPaused(_) => Some(HookTrigger::OnRoomPaused),
            Self::RoomClosed(_) => Some(HookTrigger::OnRoomClosed),
            Self::RoomArchived => None,
            Self::ChannelAttached(_) => Some(HookTrigger::OnChannelAttached),
            Self::ChannelDetached(_) => Some(HookTrigger::OnChannelDetached),
            Self::ChannelMuted(_) => Some(HookTrigger::OnChannelMuted),
            Self::ChannelUnmuted(_) => Some(HookTrigger::OnChannelUnmuted),
        }
    }

    /// The code of the change's hook payload.
    fn code(self) -> &'static str {
        match self {
            Self::RoomCreated => "room_created",
            Self::RoomPaused(_) => "room_paused",
            Self::RoomClosed(_) => "room_closed",
            Self::RoomArchived => "room_archived",
            Self::ChannelAttached(_) => "channel_attached",
            Self::ChannelDetached(_) => "channel_detached",
            Self::ChannelMuted(_) => "channel_muted",
            Self::ChannelUnmuted(_) => "channel_unmuted",
        }
    }

    /// The channel the change is about, if it is a channel's.
    fn channel_id(&self) -> Option<&ChannelId> {
        match self {
            Self::ChannelAttached(binding)
            | Self::ChannelDetached(binding)
            | Self::ChannelMuted(binding)
            | Self::ChannelUnmuted(binding) => Some(&binding.channel_id),
            Self::RoomCreated | Self::RoomPaused(_) | Self::RoomClosed(_) | Self::RoomArchived => {
                None
            }
        }
    }

    /// What made the room transition on its own, if anything did.
    fn reason(self) -> Option<TransitionReason> {
        match self {
            Self::RoomPaused(reason) | Self::RoomClosed(reason) => reason,
            Self::RoomCreated
            | Self::RoomArchived
            | Self::ChannelAttached(_)
            | Self::ChannelDetached(_)
            | Self::ChannelMuted(_)
            | Self::ChannelUnmuted(_) => None,
        }
    }
}

/// Emits the change's framework event and fires its trigger's hooks, which
/// receive `context`: the room as the change left it, with its bindings,
/// participants and recent events, as any hook's context has them (gap
/// G-027). A room just created has the bindings its creation made.
pub(crate) fn report(cx: &mut Cx<'_>, reported: Reported<'_>, context: &RoomContext) {
    if let Some(event) = reported.framework_event(&context.room.id) {
        cx.io.notify(Notice::FrameworkEvent(event));
    }
    let Some(trigger) = reported.trigger() else {
        return;
    };
    let payload = payload(cx, reported, &context.room.id);
    let hooks = cx.config.hooks.observers(trigger, &payload.source);
    if hooks.is_empty() {
        return;
    }
    cx.io.notify(Notice::AsyncHooks {
        trigger,
        hooks,
        event: Box::new(payload),
        context: Box::new(context.clone()),
    });
}

/// The payload of a lifecycle hook: a system event whose code names the
/// change and whose data names the room, the channel, and the reason of a
/// transition its timer made.
fn payload(cx: &mut Cx<'_>, reported: Reported<'_>, room_id: &RoomId) -> RoomEvent {
    let source = EventSource::new(
        ChannelId::system(),
        ChannelType::System,
        EventDirection::Outbound,
    );
    let mut data = Metadata::new();
    data.insert("room_id".to_owned(), room_id.as_str().into());
    if let Some(channel_id) = reported.channel_id() {
        data.insert("channel_id".to_owned(), channel_id.as_str().into());
    }
    if let Some(TransitionReason::Timer) = reported.reason() {
        data.insert("reason".to_owned(), "timer".into());
    }
    let code = reported.code();
    let content = EventContent::System(SystemContent {
        body: code.replace('_', " "),
        code: Some(code.to_owned()),
        data,
    });
    let now = cx.env.now();
    RoomEvent::new(
        cx.env.new_event_id(),
        room_id.clone(),
        EventType::System,
        source,
        content,
        now,
    )
}
