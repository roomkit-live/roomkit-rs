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
//! never sees that as a failure, and within its budget no `room_refused_event`
//! is emitted.
//!
//! Made in the room's own work, a hook's body or a delivery, it is that work's
//! write as a direct injection is (RFC §8.3, §9.5.1): at the work's depth and
//! on its budget, so that a hook greeting on every answer, or on its own
//! greeting, stops at the limit. A top-level caller's budget is its own.

use super::job::{Job, RecordNote};
use super::pass::{CommitPass, PassOrigin, PassResult};
use super::record::RecordPass;
use super::{Machine, Stage};
use crate::model::{
    ChannelId, ChannelType, EventContent, EventDirection, EventSource, EventStatus, EventType,
    RoomContext, RoomEvent, RoomId, TextContent,
};
use crate::ops::io::Bound;
use crate::ops::{Cx, RefusedOperation};
use crate::rules::chain_depth;
use crate::rules::permissions::{self, DeliveryPlan};

/// The metadata key that marks a greeting among the agent's events.
const AUTO_GREETING: &str = "auto_greeting";

/// The greeting `text` of `agent_id` in `room_id` at `chain_depth`, as
/// Python's `_make_greeting_event` writes it: a message of an AI channel,
/// outbound, whatever the agent's channel type.
pub(super) fn event(
    cx: &mut Cx<'_>,
    room_id: RoomId,
    agent_id: ChannelId,
    text: String,
    chain_depth: u32,
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
    event.chain_depth = chain_depth;
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
    /// Starts a greeting's pass, under no deadline, as Python's
    /// `_commit_and_deliver`, though within the call: a caller gone before
    /// its commit takes it along (`Bound::Call`). Made in a cascade's work,
    /// it is that work's write, as a direct injection is: it draws what the
    /// call draws on that cascade's budget, and past it is stored `BLOCKED`
    /// with `reentry_loop_cap`, no hook run and no delivery (RFC §8.3). What
    /// answers it draws on the same budget.
    pub(super) fn begin_greeting(
        &mut self,
        cx: &mut Cx<'_>,
        room_id: RoomId,
        agent_id: ChannelId,
        text: String,
        chain_depth: u32,
    ) {
        self.call.channel_id = Some(agent_id.clone());
        self.call.room_id = Some(room_id.clone());
        let within_its_caller = Bound::Call(None);
        self.call.bound = within_its_caller;
        let mut event = event(cx, room_id, agent_id, text, chain_depth);
        if self.cascade.drawn_on().is_some_and(|draw| !draw.take()) {
            event.blocked_by = Some(chain_depth::REENTRY_LOOP_CAP.to_owned());
            let record = RecordPass::refusing(event, RefusedOperation::Inbound).deciding();
            self.cascade
                .lead(Job::Record(Box::new(record), RecordNote::Trigger));
            self.stage.enter(Stage::Cascade);
            return;
        }
        let pass = CommitPass::trigger(event, within_its_caller, None).greeting();
        self.cascade
            .lead(Job::Pass(Box::new(pass), PassOrigin::Trigger));
        self.stage.enter(Stage::Cascade);
    }
}
