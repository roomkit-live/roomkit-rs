//! An agent's greeting, its text path (RFC §19.1): the agent's first words in
//! a room, committed by a path of their own (RFC §7.5 rule 2).
//!
//! It takes the room's lock, its due timers and its status gate as any event,
//! then no `BEFORE_BROADCAST` hook and no check of its source's right to
//! write: nothing blocks it. It is committed `DELIVERED`, its visibility
//! narrowed by its agent's binding (RFC §7.3), and delivered on the room's
//! lane, the answers of the other agents re-entering. An agent that cannot
//! write has an empty delivery set, and an agent the room does not bind has
//! none at all: the greeting is then a record of the timeline only. A room
//! whose status refuses events does nothing to it, and only logs: its caller
//! never sees that as a failure, and no `room_refused_event` is emitted.

use super::job::Job;
use super::pass::{CommitPass, PassOrigin, PassResult};
use super::{Machine, Stage};
use crate::model::{
    ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventStatus, EventType,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use crate::ops::Cx;
use crate::ops::io::Bound;
use crate::rules::permissions::{self, DeliveryPlan};

/// The metadata key that marks a greeting among the agent's events.
const AUTO_GREETING: &str = "auto_greeting";

/// The greeting `text` of `agent_id` in `room_id`, as Python's
/// `_make_greeting_event` writes it: a message of an AI channel, outbound,
/// whatever the agent's channel type.
pub(super) fn event(
    cx: &mut Cx<'_>,
    room_id: RoomId,
    agent_id: ChannelId,
    text: String,
) -> RoomEvent {
    let source = EventSource {
        direction: EventDirection::Outbound,
        ..EventSource::new(agent_id, ChannelType::AI)
    };
    let id = cx.env.new_event_id();
    let now = cx.env.now();
    let content = EventContent::Text(TextContent::new(text));
    let mut event = RoomEvent::at(id, room_id, EventType::Message, source, content, now);
    event.status = EventStatus::Delivered;
    event
        .metadata
        .insert(AUTO_GREETING.to_owned(), serde_json::Value::Bool(true));
    event
}

/// Whom the greeting reaches, its visibility narrowed by its agent's binding
/// (RFC §7.3): nobody when the agent cannot write, or the room binds no such
/// source ([`stored_when_unbound`]).
pub(super) fn plan(cx: &Cx<'_>, event: &mut RoomEvent, context: &RoomContext) -> DeliveryPlan {
    let bindings = &context.bindings;
    let Some(source) = context.get_binding(&event.source.channel_id) else {
        return DeliveryPlan::default();
    };
    event.visibility = permissions::effective_visibility(&event.visibility, source, bindings);
    if permissions::write_refusal(source).is_some() {
        return DeliveryPlan::default();
    }
    permissions::plan_delivery(
        event,
        bindings,
        context.room.agent_response_policy,
        cx.config.max_chain_depth,
    )
}

/// `result`, a greeting's pass committed: one the room binds no source for
/// has no delivery set at all, not even an empty one, so that its
/// `AFTER_BROADCAST` does not fire.
pub(super) fn stored_when_unbound(result: PassResult) -> PassResult {
    let PassResult::Delivered {
        event,
        plan,
        context,
        changed,
    } = result
    else {
        return result;
    };
    if context.get_binding(&event.source.channel_id).is_none() {
        return PassResult::Stored { event, context };
    }
    PassResult::Delivered {
        event,
        plan,
        context,
        changed,
    }
}

/// Logs a greeting its room's status refused: what the integrator hears of
/// it, as Python's `_log_refused`.
pub(super) fn log_refused(event: &RoomEvent) {
    tracing::debug!(
        target: "roomkit::core::inbound",
        room_id = %event.room_id,
        channel_id = %event.source.channel_id,
        "the room's status refuses new events: the greeting is not committed"
    );
}

impl Machine {
    /// Starts a greeting's pass, with no deadline and drawing on no budget:
    /// Python's `_commit_and_deliver` bounds neither.
    pub(super) fn begin_greeting(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: RoomId,
        agent_id: ChannelId,
        text: String,
    ) {
        self.call.channel_id = Some(agent_id.clone());
        self.call.room_id = Some(room_id.clone());
        self.call.bound = Bound::Unbounded;
        let event = event(cx, room_id, agent_id, text);
        let pass = CommitPass::trigger(event, Bound::Unbounded, None).greeting();
        self.cascade
            .lead(Job::Pass(Box::new(pass), PassOrigin::Trigger));
        self.stage.enter(Stage::Cascade);
    }
}
