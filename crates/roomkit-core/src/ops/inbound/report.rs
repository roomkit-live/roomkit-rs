//! What the cascade reports as it goes: framework events (RFC §8.2) and the
//! firing of observer hooks (RFC §9.3).

use std::collections::HashMap;

use super::delivery::SetOutcome;
use super::pass::PassOrigin;
use crate::model::{
    ChannelBinding, ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventStatus,
    EventType, Metadata, RoomContext, RoomEvent, RoomStatus, SystemContent, Task, TaskId,
    Visibility,
};
use crate::ops::{ChainDepthData, Cx, FrameworkEvent, Notice, RefusedOperation};
use crate::rules::chain_depth::ReentryBudget;
use crate::rules::hook_chain::{HookIncident, HookProblem};
use crate::rules::hook_registry::HookTrigger;

/// Reports the deliveries of the inbound event itself, in the Python
/// implementation's order (RMK-1095): a partial failure first when any
/// target failed, then each success, then each failure (RFC §8.2); its end
/// follows its `AFTER_BROADCAST` hooks ([`after_broadcast`]). A response's
/// deliveries are not reported.
pub(super) fn root_deliveries(cx: &mut Cx<'_>, event: &RoomEvent, outcome: &SetOutcome) {
    let room_id = &event.room_id;
    if !outcome.failures.is_empty() {
        notify(
            cx,
            FrameworkEvent::BroadcastPartialFailure {
                room_id: room_id.clone(),
                event_id: event.id.clone(),
                failed: outcome.failures.len(),
                // The deliveries that ended, as Python counts them: neither
                // an agent's turn nor a target that is not delivered to.
                total: outcome.delivered.len() + outcome.failures.len(),
                errors: outcome.failures.clone(),
            },
        );
    }
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
}

/// Fires the `AFTER_BROADCAST` observers of an event whose delivery set
/// completed (RFC §10.1 step 16), their bodies the work of the cascade
/// drawing on `budget` while they run (gap G-087). For the inbound event,
/// `processed`, its `event_processed` follows once they are over, at once
/// when none observes it, as the Python implementation emits it (RFC §8.2).
pub(super) fn after_broadcast(
    cx: &mut Cx<'_>,
    event: &RoomEvent,
    context: &RoomContext,
    budget: &ReentryBudget,
    processed: bool,
) {
    let then = processed.then(|| FrameworkEvent::EventProcessed {
        room_id: event.room_id.clone(),
        event_id: event.id.clone(),
    });
    let floor = event.chain_depth.saturating_add(1);
    cx.fire_within_then(
        HookTrigger::AfterBroadcast,
        event,
        context,
        (budget, floor),
        then,
    );
}

/// Fires the mutation trigger of an edit or a delete committed as `change`,
/// once the room lock is released (RFC §10.3 "Mutation triggers"):
/// `ON_EVENT_UPDATED` for an edit, `ON_EVENT_DELETED` for a delete, its
/// payload `target` as the commit left it, which its observers' filters
/// read, and `context` the room as the commit left it, the change in its
/// history (gap G-062), their bodies the work of the cascade drawing on
/// `budget` (gap G-087). A blocked change never reaches here.
pub(super) fn target_changed(
    cx: &mut Cx<'_>,
    change: &RoomEvent,
    target: &RoomEvent,
    context: &RoomContext,
    budget: &ReentryBudget,
) {
    let trigger = if change.r#type == EventType::Delete {
        HookTrigger::OnEventDeleted
    } else {
        HookTrigger::OnEventUpdated
    };
    cx.fire_as_cascade_work(trigger, target, context, budget, 0);
}

/// Fires the `ON_TASK_CREATED` observers of each stored task (RFC §9.2),
/// its payload the task's event, as Python's `_persist_side_effects`
/// builds it, for a task whose event `made_for` holds; a system event
/// naming the task otherwise.
pub(super) fn tasks_created(
    cx: &mut Cx<'_>,
    tasks: &[Task],
    made_for: &HashMap<TaskId, RoomEvent>,
    context: &RoomContext,
) {
    for task in tasks {
        let payload = match made_for.get(&task.id) {
            Some(event) => task_event(cx, task, event),
            None => task_payload(cx, task),
        };
        cx.fire_observers(HookTrigger::OnTaskCreated, &payload, context);
    }
}

/// The payload of `ON_TASK_CREATED` for `task`, made for `event`: an
/// internal `task_created` event of the room, delivered, carrying the
/// event's source and content, the task named in its metadata (Python's
/// `RoomEvent(type=TASK_CREATED, source=event.source,
/// content=event.content, status=DELIVERED, visibility=INTERNAL,
/// metadata={"task_id", "task_title"})`).
fn task_event(cx: &mut Cx<'_>, task: &Task, event: &RoomEvent) -> RoomEvent {
    let now = cx.env.now();
    let mut payload = RoomEvent::at(
        cx.env.new_event_id(),
        task.room_id.clone(),
        EventType::TaskCreated,
        event.source.clone(),
        event.content.clone(),
        now,
    );
    payload.status = EventStatus::Delivered;
    payload.visibility = Visibility::Internal;
    payload
        .metadata
        .insert("task_id".to_owned(), task.id.as_str().into());
    payload
        .metadata
        .insert("task_title".to_owned(), task.title.clone().into());
    payload
}

/// The payload of `ON_TASK_CREATED` for a task whose event the call does
/// not hold (a streamed response's targets'): a system event whose data
/// names the task, as a lifecycle hook's names the change.
fn task_payload(cx: &mut Cx<'_>, task: &Task) -> RoomEvent {
    let source = EventSource {
        direction: EventDirection::Outbound,
        ..EventSource::new(ChannelId::system(), ChannelType::SYSTEM)
    };
    let mut data = Metadata::new();
    data.insert("task_id".to_owned(), task.id.as_str().into());
    data.insert("title".to_owned(), task.title.clone().into());
    let content = EventContent::System(SystemContent {
        body: "task created".to_owned(),
        code: Some("task_created".to_owned()),
        data,
    });
    let now = cx.env.now();
    RoomEvent::at(
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
            event_type: event.r#type,
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
                let timeout = incident.timeout.as_secs_f64();
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
