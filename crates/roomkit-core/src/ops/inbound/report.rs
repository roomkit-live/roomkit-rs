//! What the cascade reports as it goes: framework events (RFC §8.2) and the
//! firing of observer hooks (RFC §9.3).

use super::delivery::SetOutcome;
use super::pass::PassOrigin;
use crate::model::{
    ChannelBinding, ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventType,
    Metadata, RoomContext, RoomEvent, RoomStatus, SystemContent, Task,
};
use crate::ops::{ChainDepthData, Cx, FrameworkEvent, Notice, RefusedOperation};
use crate::rules::hook_chain::{HookIncident, HookProblem};
use crate::rules::hook_registry::HookTrigger;

/// Reports the deliveries of the inbound event itself: each success and
/// failure, a partial failure when any target failed, then the event's end
/// (RFC §8.2). A response's deliveries are not reported.
pub(super) fn root_deliveries(cx: &mut Cx<'_>, event: &RoomEvent, outcome: &SetOutcome) {
    let room_id = &event.room_id;
    for channel_id in &outcome.delivered {
        notify(
            cx,
            FrameworkEvent::DeliverySucceeded {
                room_id: room_id.clone(),
                event_id: event.id.clone(),
                channel_id: channel_id.clone(),
            },
        );
    }
    for (channel_id, error) in &outcome.failures {
        notify(
            cx,
            FrameworkEvent::DeliveryFailed {
                room_id: room_id.clone(),
                event_id: event.id.clone(),
                channel_id: channel_id.clone(),
                error: error.clone(),
            },
        );
    }
    if !outcome.failures.is_empty() {
        notify(
            cx,
            FrameworkEvent::BroadcastPartialFailure {
                room_id: room_id.clone(),
                event_id: event.id.clone(),
                failed: outcome.failures.len(),
                total: outcome.targets,
                errors: outcome.failures.clone(),
            },
        );
    }
    notify(
        cx,
        FrameworkEvent::EventProcessed {
            room_id: room_id.clone(),
            event_id: event.id.clone(),
        },
    );
}

/// Fires the `AFTER_BROADCAST` observers of an event whose delivery set
/// completed (RFC §10.1 step 16).
pub(super) fn after_broadcast(cx: &mut Cx<'_>, event: &RoomEvent, context: &RoomContext) {
    cx.fire_observers(HookTrigger::AfterBroadcast, event, context);
}

/// Fires the mutation trigger of an edit or a delete committed as `change`,
/// once the room lock is released (RFC §10.3): `ON_EVENT_UPDATED` for an
/// edit, `ON_EVENT_DELETED` for a delete, with `target` as the commit left
/// it. A blocked change never reaches here.
pub(super) fn target_changed(
    cx: &mut Cx<'_>,
    change: &RoomEvent,
    target: &RoomEvent,
    context: &RoomContext,
) {
    let trigger = if change.event_type == EventType::Delete {
        HookTrigger::OnEventDeleted
    } else {
        HookTrigger::OnEventUpdated
    };
    cx.fire_observers(trigger, target, context);
}

/// Fires the `ON_TASK_CREATED` observers of each stored task (RFC §9.2),
/// its payload a system event naming the task.
pub(super) fn tasks_created(cx: &mut Cx<'_>, tasks: &[Task], context: &RoomContext) {
    for task in tasks {
        let payload = task_payload(cx, task);
        cx.fire_observers(HookTrigger::OnTaskCreated, &payload, context);
    }
}

/// The payload of `ON_TASK_CREATED`: a system event whose data names the
/// task, as a lifecycle hook's names the change.
fn task_payload(cx: &mut Cx<'_>, task: &Task) -> RoomEvent {
    let source = EventSource::new(
        ChannelId::system(),
        ChannelType::System,
        EventDirection::Outbound,
    );
    let mut data = Metadata::new();
    data.insert("task_id".to_owned(), task.id.as_str().into());
    data.insert("title".to_owned(), task.title.clone().into());
    let content = EventContent::System(SystemContent {
        body: "task created".to_owned(),
        code: Some("task_created".to_owned()),
        data,
    });
    let now = cx.env.now();
    RoomEvent::new(
        cx.env.new_event_id(),
        task.room_id.clone(),
        EventType::TaskCreated,
        source,
        content,
        now,
    )
}

/// Reports `event`, from `origin`, refused by its room's `status`: a refused
/// event is observable (RFC §5.1), an inbound message or a direct injection
/// as `inbound`, a response or a hook's injected event as `reentry`, the
/// framework's own re-injection into the room (gap G-043).
pub(super) fn room_refused(
    cx: &mut Cx<'_>,
    event: &RoomEvent,
    origin: &PassOrigin,
    status: RoomStatus,
) {
    let operation = if origin.is_entry() {
        RefusedOperation::Inbound
    } else {
        RefusedOperation::Reentry
    };
    room_refused_as(cx, event, operation, status);
}

/// Reports `event` refused by its room's `status`, met on `operation`'s
/// way (RFC §8.2 `room_refused_event`).
pub(super) fn room_refused_as(
    cx: &mut Cx<'_>,
    event: &RoomEvent,
    operation: RefusedOperation,
    status: RoomStatus,
) {
    notify(
        cx,
        FrameworkEvent::RoomRefusedEvent {
            room_id: event.room_id.clone(),
            event_id: event.id.clone(),
            status,
            operation,
            event_type: event.event_type,
        },
    );
}

/// Reports the record of a channel not asked at the chain-depth limit (RFC
/// §8.3).
pub(super) fn chain_depth_exceeded(cx: &mut Cx<'_>, record: &RoomEvent, agent: &ChannelBinding) {
    let max_chain_depth = cx.config.max_chain_depth;
    notify(
        cx,
        FrameworkEvent::ChainDepthExceeded {
            room_id: record.room_id.clone(),
            event_id: record.id.clone(),
            channel_id: agent.channel_id.clone(),
            data: ChainDepthData {
                chain_depth: record.chain_depth,
                max_chain_depth,
            },
        },
    );
}

/// Reports the `BEFORE_BROADCAST` hooks that produced no usable result for
/// `event` (RFC §8.2 `hook_error`, `hook_timeout`).
pub(super) fn hook_incidents(cx: &mut Cx<'_>, event: &RoomEvent, incidents: &[HookIncident]) {
    for incident in incidents {
        tracing::warn!(
            target: "roomkit::core::hooks",
            room_id = %event.room_id,
            event_id = %event.id,
            hook = %incident.hook,
            blocked = incident.blocked,
            problem = ?incident.problem,
            "a BEFORE_BROADCAST hook produced no usable result"
        );
        let room_id = event.room_id.clone();
        let error = match &incident.problem {
            HookProblem::TimedOut => {
                let timeout = cx
                    .config
                    .hooks
                    .find(&incident.hook)
                    .map_or(0.0, |spec| spec.timeout.as_secs_f64());
                notify(
                    cx,
                    FrameworkEvent::HookTimeout {
                        room_id: room_id.clone(),
                        hook_name: incident.hook.clone(),
                        trigger: HookTrigger::BeforeBroadcast,
                        timeout,
                    },
                );
                format!("timed out after {timeout}s")
            }
            HookProblem::Raised(error) => error.message.clone(),
            HookProblem::Invalid(what) => what.clone(),
        };
        // Whatever made the result unusable, the commit gate reports it as
        // a hook error (RFC §8.2), a timeout as a timeout as well.
        notify(
            cx,
            FrameworkEvent::HookError {
                room_id,
                event_id: event.id.clone(),
                hook: incident.hook.clone(),
                error,
            },
        );
    }
}

fn notify(cx: &mut Cx<'_>, event: FrameworkEvent) {
    cx.io.notify(Notice::FrameworkEvent(event));
}
