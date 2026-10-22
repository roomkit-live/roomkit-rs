//! What the cascade reports as it goes: framework events (RFC §8.2) and the
//! firing of observer hooks (RFC §9.3).

use super::delivery::SetOutcome;
use crate::model::{ChannelBinding, RoomContext, RoomEvent};
use crate::ops::{ChainDepthData, Cx, FrameworkEvent, Notice};
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
    let hooks = cx
        .config
        .hooks
        .observers(HookTrigger::AfterBroadcast, &event.source);
    if hooks.is_empty() {
        return;
    }
    cx.io.notify(Notice::AsyncHooks {
        trigger: HookTrigger::AfterBroadcast,
        hooks,
        event: Box::new(event.clone()),
        context: Box::new(context.clone()),
    });
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
        let reported = match &incident.problem {
            HookProblem::TimedOut => FrameworkEvent::HookTimeout {
                room_id,
                hook_name: incident.hook.clone(),
                trigger: HookTrigger::BeforeBroadcast,
                timeout: cx
                    .config
                    .hooks
                    .find(&incident.hook)
                    .map_or(0.0, |spec| spec.timeout.as_secs_f64()),
            },
            HookProblem::Raised(error) => FrameworkEvent::HookError {
                room_id,
                event_id: event.id.clone(),
                hook: incident.hook.clone(),
                error: error.message.clone(),
            },
            HookProblem::Invalid(what) => FrameworkEvent::HookError {
                room_id,
                event_id: event.id.clone(),
                hook: incident.hook.clone(),
                error: what.clone(),
            },
        };
        notify(cx, reported);
    }
}

fn notify(cx: &mut Cx<'_>, event: FrameworkEvent) {
    cx.io.notify(Notice::FrameworkEvent(event));
}
