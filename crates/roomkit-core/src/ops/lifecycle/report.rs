//! What a lifecycle change reports: its framework event (RFC §8.2) and its
//! trigger's hooks (RFC §9.2), as the table of RFC §26.1 pairs them. The
//! transition's own record is not subject to the status gate (RFC §5.1).

use super::payload::{LifecycleChange, LifecyclePayload};
use crate::model::{
    ChannelBinding, ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventType,
    RoomContext, RoomEvent, RoomId,
};
use crate::ops::protocol::{EffectId, HookRequest};
use crate::ops::{Cx, FrameworkEvent, Notice, TransitionReason};
use crate::rules::hook_registry::{HookId, HookTrigger};

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
    /// The inbound pipeline created the room for a message on this channel
    /// (RFC §10.1 step 2).
    SessionStarted {
        channel_id: &'change ChannelId,
        channel_type: ChannelType,
    },
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
            Self::SessionStarted {
                channel_id,
                channel_type,
            } => Some(FrameworkEvent::SessionStarted {
                room_id,
                channel_id: channel_id.clone(),
                channel_type,
            }),
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
            Self::SessionStarted { .. } => Some(HookTrigger::OnSessionStarted),
        }
    }

    /// The change the hook payload names.
    fn change(self) -> LifecycleChange {
        match self {
            Self::RoomCreated => LifecycleChange::RoomCreated,
            Self::RoomPaused(_) => LifecycleChange::RoomPaused,
            Self::RoomClosed(_) => LifecycleChange::RoomClosed,
            Self::RoomArchived => LifecycleChange::RoomArchived,
            Self::ChannelAttached(_) => LifecycleChange::ChannelAttached,
            Self::ChannelDetached(_) => LifecycleChange::ChannelDetached,
            Self::ChannelMuted(_) => LifecycleChange::ChannelMuted,
            Self::ChannelUnmuted(_) => LifecycleChange::ChannelUnmuted,
            Self::SessionStarted { .. } => LifecycleChange::SessionStarted,
        }
    }

    /// The channel the change is about, if it is a channel's.
    fn channel_id(&self) -> Option<&ChannelId> {
        match self {
            Self::ChannelAttached(binding)
            | Self::ChannelDetached(binding)
            | Self::ChannelMuted(binding)
            | Self::ChannelUnmuted(binding) => Some(&binding.channel_id),
            Self::SessionStarted { channel_id, .. } => Some(*channel_id),
            Self::RoomCreated | Self::RoomPaused(_) | Self::RoomClosed(_) | Self::RoomArchived => {
                None
            }
        }
    }

    /// The type of the channel a session started on.
    fn channel_type(self) -> Option<ChannelType> {
        match self {
            Self::SessionStarted { channel_type, .. } => Some(channel_type),
            Self::RoomCreated
            | Self::RoomPaused(_)
            | Self::RoomClosed(_)
            | Self::RoomArchived
            | Self::ChannelAttached(_)
            | Self::ChannelDetached(_)
            | Self::ChannelMuted(_)
            | Self::ChannelUnmuted(_) => None,
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
            | Self::ChannelUnmuted(_)
            | Self::SessionStarted { .. } => None,
        }
    }
}

/// The hooks a change's trigger runs, with their payload.
struct Observers {
    trigger: HookTrigger,
    hooks: Vec<HookId>,
    payload: RoomEvent,
}

/// Emits the change's framework event and fires its trigger's hooks, which
/// receive `context`: the room as the change left it, with its bindings,
/// participants and recent events, as any hook's context has them (gap
/// G-027). A room just created has the bindings its creation made.
pub(crate) fn report(cx: &mut Cx<'_>, reported: Reported<'_>, context: &RoomContext) {
    announce(cx, reported, &context.room.id);
    let Some(trigger) = reported.trigger() else {
        return;
    };
    let payload = payload(cx, reported, &context.room.id);
    cx.fire_observers(trigger, &payload, context);
}

/// Emits the change's framework event now, and hands back its trigger's
/// hooks to fire once the room's lock is released, as an ASYNC trigger
/// fires (RFC §5.1 When): a timer transition the gate applies under the
/// lock reports so. `None` when no hook observes the trigger.
pub(crate) fn report_after_release(
    cx: &mut Cx<'_>,
    reported: Reported<'_>,
    context: &RoomContext,
) -> Option<Notice> {
    announce(cx, reported, &context.room.id);
    let Observers {
        trigger,
        hooks,
        payload,
    } = observers(cx, reported, context)?;
    Some(Notice::AsyncHooks {
        trigger,
        hooks,
        event: Box::new(payload),
        context: Box::new(context.clone()),
    })
}

/// Asks for the change's trigger's hooks to be run and awaited, as every
/// creation of a room does (gap G-050). The change's framework event comes
/// once they are over ([`announce`]), as RFC §10.1 step 2 orders a
/// creation's: `ON_ROOM_CREATED`'s hooks, then `room_created`. Returns the
/// effect to wait for; `None` when no hook observes the trigger, the event
/// emitted at once.
pub(crate) fn report_awaited(
    cx: &mut Cx<'_>,
    reported: Reported<'_>,
    context: &RoomContext,
) -> Option<EffectId> {
    let Some(Observers {
        trigger,
        hooks,
        payload,
    }) = observers(cx, reported, context)
    else {
        announce(cx, reported, &context.room.id);
        return None;
    };
    Some(cx.io.issue(HookRequest::RunObservers {
        trigger,
        hooks,
        event: Box::new(payload),
        context: Box::new(context.clone()),
    }))
}

/// Emits the framework event of a change to the room `room_id`, if it has
/// one: at once for a change whose hooks run after the fact, once its
/// awaited hooks are over for a creation ([`report_awaited`]).
pub(crate) fn announce(cx: &mut Cx<'_>, reported: Reported<'_>, room_id: &RoomId) {
    if let Some(event) = reported.framework_event(room_id) {
        cx.io.notify(Notice::FrameworkEvent(event));
    }
}

/// The hooks the change's trigger runs and their payload; `None` when it
/// fires no trigger or no hook observes it.
fn observers(cx: &mut Cx<'_>, reported: Reported<'_>, context: &RoomContext) -> Option<Observers> {
    let trigger = reported.trigger()?;
    let payload = payload(cx, reported, &context.room.id);
    let hooks = cx.config.hooks.observers(trigger, &payload.source);
    (!hooks.is_empty()).then_some(Observers {
        trigger,
        hooks,
        payload,
    })
}

/// The payload of a lifecycle hook: a system event from the framework,
/// whose content's code names the change and whose data names the room, the
/// channel, and the reason of a transition its timer made, as
/// [`LifecyclePayload`] writes and reads them.
fn payload(cx: &mut Cx<'_>, reported: Reported<'_>, room_id: &RoomId) -> RoomEvent {
    let source = EventSource::new(
        ChannelId::system(),
        ChannelType::System,
        EventDirection::Outbound,
    );
    let lifecycle = LifecyclePayload {
        change: reported.change(),
        room_id: room_id.clone(),
        channel_id: reported.channel_id().cloned(),
        channel_type: reported.channel_type(),
        reason: reported.reason(),
    };
    let content = EventContent::System(lifecycle.content());
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
