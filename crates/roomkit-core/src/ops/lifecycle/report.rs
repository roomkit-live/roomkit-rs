//! What a lifecycle change reports: its framework event (RFC §8.2) and its
//! trigger's hooks (RFC §9.2), as the table of RFC §26.1 pairs them. The
//! transition's own record is not subject to the status gate (RFC §5.1).

use serde_json::{Value, json};

use crate::model::{
    ChannelBinding, ChannelId, ChannelType, EventContent, EventSource, EventStatus, EventType,
    Metadata, RoomContext, RoomEvent, RoomId, SystemContent, Timestamp, Visibility,
};
use crate::ops::protocol::{EffectId, HookRequest};
use crate::ops::{Cx, FrameworkEvent, Notice, TransitionReason};
use crate::rules::chain_depth::Draw;
use crate::rules::hook_registry::{HookId, HookTrigger};

/// A timer's transition (RFC §5.1 When): the timer's threshold, in
/// seconds, and the instant the transition fell due.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TimerDue {
    pub(crate) threshold: u64,
    pub(crate) at: Timestamp,
}

/// A lifecycle change to report.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Reported<'change> {
    RoomCreated,
    /// The timer's, `None` for the integrator's pause.
    RoomPaused(Option<TimerDue>),
    /// The timer's, `None` for the integrator's close.
    RoomClosed(Option<TimerDue>),
    RoomArchived,
    ChannelAttached(&'change ChannelBinding),
    ChannelDetached(&'change ChannelBinding),
    ChannelMuted(&'change ChannelBinding),
    ChannelUnmuted(&'change ChannelBinding),
    /// The inbound pipeline created the room for a message on this channel
    /// (RFC §10.1 step 2).
    SessionStarted {
        channel_id: &'change ChannelId,
        channel_type: &'change ChannelType,
        /// The message's sender.
        participant_id: &'change str,
    },
}

impl Reported<'_> {
    /// The framework event the change emits, if any (RFC §26.1): a pause
    /// emits one only when its timer paused the room, a mute none.
    fn framework_event(self, room_id: &RoomId) -> Option<FrameworkEvent> {
        let room_id = room_id.clone();
        match self {
            Self::RoomCreated => Some(FrameworkEvent::RoomCreated { room_id }),
            Self::RoomPaused(timer) => timer.map(|_| FrameworkEvent::RoomPaused {
                room_id,
                reason: TransitionReason::Timer,
            }),
            Self::RoomClosed(timer) => Some(FrameworkEvent::RoomClosed {
                room_id,
                reason: timer.map(|_| TransitionReason::Timer),
            }),
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
                ..
            } => Some(FrameworkEvent::SessionStarted {
                room_id,
                channel_id: channel_id.clone(),
                channel_type: channel_type.clone(),
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

/// Emits the change's framework event and fires its trigger's hooks as
/// [`report`] does, their bodies the room's work the change was made in,
/// `work`: they draw their calls on its budget, at least its floor deep
/// (RFC §8.3, §10.3).
pub(crate) fn report_within(
    cx: &mut Cx<'_>,
    reported: Reported<'_>,
    context: &RoomContext,
    work: &(Draw, u32),
) {
    announce(cx, reported, &context.room.id);
    let Some(trigger) = reported.trigger() else {
        return;
    };
    let payload = payload(cx, reported, &context.room.id);
    let (draw, floor) = work;
    cx.fire_within(trigger, &payload, context, draw.budget(), *floor);
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
        after_delivery: None,
        then: None,
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
    let hooks = cx.config.hooks.observers(trigger, &payload);
    (!hooks.is_empty()).then_some(Observers {
        trigger,
        hooks,
        payload,
    })
}

/// The payload of a lifecycle hook, as Python's `_fire_lifecycle_hook`
/// writes it: an event of the change's lifecycle type (`SYSTEM` for a
/// room's), from the framework's `system` source, inbound, `DELIVERED` and
/// `INTERNAL`, whose system content's code names the change and whose data
/// holds what it changed (RFC §5.1 Lifecycle records, §9.2).
fn payload(cx: &mut Cx<'_>, reported: Reported<'_>, room_id: &RoomId) -> RoomEvent {
    let (event_type, content) = described(reported, room_id);
    let source = EventSource::new(ChannelId::system(), ChannelType::SYSTEM);
    let now = cx.env.now();
    let mut event = RoomEvent::at(
        cx.env.new_event_id(),
        room_id.clone(),
        event_type,
        source,
        EventContent::System(content),
        now,
    );
    event.status = EventStatus::Delivered;
    event.visibility = Visibility::Internal;
    event
}

/// The type and the content of a change's payload: Python's code, message
/// and data for it (`room_lifecycle.py`, `channel_ops.py`,
/// `_timer_transitions.py`).
fn described(reported: Reported<'_>, room_id: &RoomId) -> (EventType, SystemContent) {
    let room = || json!({ "room_id": room_id });
    let timer = |due: TimerDue| json!({ "threshold": due.threshold, "at": due.at.to_iso_offset() });
    let channel = |binding: &ChannelBinding| json!({ "channel_id": binding.channel_id });
    let (event_type, code, body, data) = match reported {
        Reported::RoomCreated => (
            EventType::System,
            "room_created",
            format!("Room {room_id} created"),
            room(),
        ),
        Reported::RoomPaused(None) => (
            EventType::System,
            "room_paused",
            format!("Room {room_id} paused"),
            room(),
        ),
        Reported::RoomPaused(Some(due)) => (
            EventType::System,
            "room_paused_by_timer",
            format!("Room {room_id} paused by timer"),
            timer(due),
        ),
        Reported::RoomClosed(None) => (
            EventType::System,
            "room_closed",
            format!("Room {room_id} closed"),
            room(),
        ),
        Reported::RoomClosed(Some(due)) => (
            EventType::System,
            "room_closed_by_timer",
            format!("Room {room_id} closed by timer"),
            timer(due),
        ),
        Reported::RoomArchived => (
            EventType::System,
            "room_archived",
            format!("Room {room_id} archived"),
            room(),
        ),
        Reported::ChannelAttached(binding) => (
            EventType::ChannelAttached,
            "channel_attached",
            format!("Channel {} attached", binding.channel_id),
            json!({
                "channel_id": binding.channel_id,
                "access": binding.access,
                "visibility": binding.visibility,
            }),
        ),
        Reported::ChannelDetached(binding) => (
            EventType::ChannelDetached,
            "channel_detached",
            format!("Channel {} detached", binding.channel_id),
            channel(binding),
        ),
        Reported::ChannelMuted(binding) => (
            EventType::ChannelMuted,
            "channel_muted",
            format!("Channel {} muted", binding.channel_id),
            channel(binding),
        ),
        Reported::ChannelUnmuted(binding) => (
            EventType::ChannelUnmuted,
            "channel_unmuted",
            format!("Channel {} unmuted", binding.channel_id),
            channel(binding),
        ),
        Reported::SessionStarted {
            channel_id,
            channel_type,
            participant_id,
        } => (
            EventType::System,
            "session_started",
            format!("Session started on channel {channel_id}"),
            json!({
                "room_id": room_id,
                "channel_id": channel_id,
                "channel_type": channel_type,
                "participant_id": participant_id,
            }),
        ),
    };
    let data = if let Value::Object(members) = data {
        members
    } else {
        Metadata::new()
    };
    let content = SystemContent {
        body,
        code: Some(code.to_owned()),
        data,
    };
    (event_type, content)
}
