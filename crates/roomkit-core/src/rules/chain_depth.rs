//! How far a chain of responses may go (RFC §8.3).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::model::{
    ChannelBinding, EventDirection, EventId, EventSource, EventStatus, EventType, Observation,
    ObservationId, RoomEvent, TextContent, Timestamp,
};

/// The default `max_chain_depth` (RFC §8.3).
pub(crate) const DEFAULT_MAX_CHAIN_DEPTH: u32 = 5;

/// The `blocked_by` of a channel not asked at the chain-depth limit.
pub(crate) const CHAIN_DEPTH_LIMIT: &str = "event_chain_depth_limit";

/// The `blocked_by` of a response past its cascade's reentry budget.
pub(crate) const REENTRY_LOOP_CAP: &str = "reentry_loop_cap";

/// The chain depth of a response to `trigger`: one more than the trigger's,
/// whichever path produces it (RFC §8.3).
pub fn response_depth(trigger: &RoomEvent) -> u32 {
    trigger.chain_depth.saturating_add(1)
}

/// Whether a response to an event of `trigger_depth` would reach
/// `max_chain_depth`, so that no intelligence channel may be asked for it
/// (RFC §8.3: `chain_depth + 1 >= max_chain_depth`).
pub(crate) fn reaches_limit(trigger_depth: u32, max_chain_depth: u32) -> bool {
    trigger_depth.saturating_add(1) >= max_chain_depth
}

/// The record stored in place of the response of `agent`, not asked because
/// of the limit (RFC §8.3): blocked, sourced from the agent, with empty text
/// and the depth the response would have had.
///
/// The record answers `trigger` (RFC §8.5) and travels outbound, as a
/// response does (gap G-013).
pub(crate) fn not_asked_record(
    id: EventId,
    trigger: &RoomEvent,
    agent: &ChannelBinding,
    now: Timestamp,
) -> RoomEvent {
    let source = EventSource::new(
        agent.channel_id.clone(),
        agent.channel_type.clone(),
        EventDirection::Outbound,
    );
    let mut record = RoomEvent::new(
        id,
        trigger.room_id.clone(),
        EventType::Message,
        source,
        TextContent::new("").into(),
        now,
    );
    record.status = EventStatus::Blocked;
    record.blocked_by = Some(CHAIN_DEPTH_LIMIT.to_owned());
    record.chain_depth = response_depth(trigger);
    record.responds_to = Some(trigger.id.clone());
    record
}

/// The observation that goes with a not-asked record (RFC §8.3: "its own
/// observation"), whose content and category the RFC leaves open (gap
/// G-016).
pub(crate) fn not_asked_observation(
    id: ObservationId,
    trigger: &RoomEvent,
    agent: &ChannelBinding,
    max_chain_depth: u32,
    now: Timestamp,
) -> Observation {
    let content = format!(
        "{} was not asked: its response would reach chain depth {}, the limit is {}",
        agent.channel_id,
        response_depth(trigger),
        max_chain_depth
    );
    let mut observation = Observation::new(
        id,
        trigger.room_id.clone(),
        agent.channel_id.clone(),
        content,
        now,
    );
    // The category and the fields are the ones the Python implementation
    // stores, which consumers already read (gap G-016).
    observation.category = Some("event_chain_depth_exceeded".to_owned());
    observation
        .metadata
        .insert("chain_depth".to_owned(), response_depth(trigger).into());
    observation
        .metadata
        .insert("max_chain_depth".to_owned(), max_chain_depth.into());
    observation.metadata.insert(
        "source_channel".to_owned(),
        agent.channel_id.as_str().into(),
    );
    observation
}

/// The reentry passes one cascade may still take (RFC §8.3): ten times the
/// chain-depth limit, shared by every response the cascade re-enters, the
/// events its hooks inject, the entries its operation adopts, and the calls
/// its delivery sets make into a room, a turn's or a transport's delivery's
/// (gap G-070); and, on a counter of the same size kept apart, the calls
/// the bodies of the ASYNC hooks it fires once an event's delivery set has
/// run may still make into the room, every hook of the cascade together
/// (RFC §8.3 "The hook calls of a cascade", gap G-087): a hook's notices
/// cost the agents no answer, and their answers block no notice below the
/// cap.
///
/// A clone draws on the same counters: the operation a delivery's call
/// starts draws on the budget of the cascade that delivers, so that a
/// channel that calls into a room on every delivery stops at the cap. The
/// counts are shared across threads, as the operations that draw on them
/// may run on several. What a call draws, a pass or a hook call, is its
/// [`Draw`].
#[derive(Clone, Debug)]
pub struct ReentryBudget {
    passes: Arc<AtomicU32>,
    hook_calls: Arc<AtomicU32>,
}

impl ReentryBudget {
    /// The budget of a cascade under `max_chain_depth`: as many passes as
    /// hook calls.
    pub fn new(max_chain_depth: u32) -> Self {
        let size = max_chain_depth.saturating_mul(10);
        Self {
            passes: Arc::new(AtomicU32::new(size)),
            hook_calls: Arc::new(AtomicU32::new(size)),
        }
    }

    /// Spends one pass, or answers `false` when none is left: the response
    /// or the injection is then stored blocked with `REENTRY_LOOP_CAP`
    /// instead.
    pub fn take_pass(&self) -> bool {
        take_one(&self.passes)
    }

    /// Spends one hook call, a call an ASYNC hook's body makes into the
    /// room (RFC §8.3 "The hook calls of a cascade"), or answers `false`
    /// when none is left.
    pub fn take_hook_call(&self) -> bool {
        take_one(&self.hook_calls)
    }

    /// The passes left.
    pub fn remaining(&self) -> u32 {
        self.passes.load(Ordering::Relaxed)
    }

    /// The hook calls left.
    pub fn hook_calls_left(&self) -> u32 {
        self.hook_calls.load(Ordering::Relaxed)
    }
}

/// Spends one of `counter`, or answers `false` when none is left.
fn take_one(counter: &AtomicU32) -> bool {
    // The count guards nothing else: no ordering with other memory is
    // needed.
    let mut remaining = counter.load(Ordering::Relaxed);
    loop {
        let Some(left) = remaining.checked_sub(1) else {
            return false;
        };
        match counter.compare_exchange_weak(remaining, left, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(now) => remaining = now,
        }
    }
}

/// Two budgets are equal when they are one budget, shared.
impl PartialEq for ReentryBudget {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.passes, &other.passes)
    }
}

impl Eq for ReentryBudget {}

/// What a call made in a cascade's work draws on the cascade's budget
/// (RFC §8.3, gaps G-070, G-087).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Draw {
    /// One of its passes: a call a delivery's code makes into the room, a
    /// turn's or a transport's. A direct change made from the cascade's
    /// delivery, or from one of its sections (a hook under the room's lock
    /// or in its admission order), draws none (RFC §10.3).
    Pass(ReentryBudget),
    /// One of its hook calls: a call the body of an ASYNC hook it fired
    /// makes.
    HookCall(ReentryBudget),
}

impl Draw {
    /// Spends what the call draws, or answers `false` when none is left.
    pub fn take(&self) -> bool {
        match self {
            Self::Pass(budget) => budget.take_pass(),
            Self::HookCall(budget) => budget.take_hook_call(),
        }
    }

    /// The budget it draws on.
    pub fn budget(&self) -> &ReentryBudget {
        match self {
            Self::Pass(budget) | Self::HookCall(budget) => budget,
        }
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::model::{ChannelCategory, ChannelDirection, ChannelId, ChannelType, RoomId};

    fn trigger(chain_depth: u32) -> RoomEvent {
        let mut event = RoomEvent::new(
            EventId::new("evt_4").unwrap(),
            RoomId::new("room_1").unwrap(),
            EventType::Message,
            EventSource::new(
                ChannelId::new("analyst").unwrap(),
                ChannelType::AI,
                EventDirection::Outbound,
            ),
            TextContent::new("Good report.").into(),
            Timestamp::UNIX_EPOCH,
        );
        event.chain_depth = chain_depth;
        event
    }

    fn writer() -> ChannelBinding {
        ChannelBinding::new(
            ChannelId::new("writer").unwrap(),
            RoomId::new("room_1").unwrap(),
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
            Timestamp::UNIX_EPOCH,
        )
    }

    #[test]
    fn a_response_is_one_hop_deeper_than_its_trigger() {
        assert_eq!(response_depth(&trigger(2)), 3);
    }

    #[test]
    fn a_response_reaching_the_limit_is_at_the_limit() {
        assert!(reaches_limit(4, DEFAULT_MAX_CHAIN_DEPTH));
    }

    #[test]
    fn a_response_below_the_limit_is_not_at_the_limit() {
        assert!(!reaches_limit(3, DEFAULT_MAX_CHAIN_DEPTH));
    }

    #[test]
    fn the_not_asked_record_is_blocked_by_the_chain_depth_limit() {
        let record = not_asked_record(
            EventId::new("evt_5").unwrap(),
            &trigger(4),
            &writer(),
            Timestamp::UNIX_EPOCH,
        );
        assert_eq!(
            (record.status, record.blocked_by.as_deref()),
            (EventStatus::Blocked, Some(CHAIN_DEPTH_LIMIT))
        );
    }

    #[test]
    fn the_not_asked_record_carries_the_depth_the_response_would_have_had() {
        let record = not_asked_record(
            EventId::new("evt_5").unwrap(),
            &trigger(4),
            &writer(),
            Timestamp::UNIX_EPOCH,
        );
        assert_eq!(record.chain_depth, 5);
    }

    #[test]
    fn the_not_asked_record_is_sourced_from_the_agent_with_empty_text() {
        let record = not_asked_record(
            EventId::new("evt_5").unwrap(),
            &trigger(4),
            &writer(),
            Timestamp::UNIX_EPOCH,
        );
        assert_eq!(
            (record.source.channel_id.as_str(), record.content),
            ("writer", TextContent::new("").into())
        );
    }

    fn observation_at_limit() -> Observation {
        not_asked_observation(
            ObservationId::new("obs_1").unwrap(),
            &trigger(4),
            &writer(),
            DEFAULT_MAX_CHAIN_DEPTH,
            Timestamp::UNIX_EPOCH,
        )
    }

    #[test]
    fn the_not_asked_observation_is_categorized_as_event_chain_depth_exceeded() {
        assert_eq!(
            observation_at_limit().category.as_deref(),
            Some("event_chain_depth_exceeded")
        );
    }

    #[test]
    fn the_not_asked_observation_carries_the_depth_the_limit_and_the_channel() {
        assert_eq!(
            serde_json::Value::Object(observation_at_limit().metadata),
            serde_json::json!({
                "chain_depth": 5,
                "max_chain_depth": 5,
                "source_channel": "writer",
            })
        );
    }

    #[test]
    fn the_reentry_budget_is_ten_times_the_chain_depth_limit() {
        assert_eq!(ReentryBudget::new(5).remaining(), 50);
    }

    #[test]
    fn an_exhausted_reentry_budget_refuses_a_pass() {
        let budget = ReentryBudget::new(0);
        assert!(!budget.take_pass());
    }

    #[test]
    fn taking_a_pass_spends_the_budget() {
        let budget = ReentryBudget::new(1);
        budget.take_pass();
        assert_eq!(budget.remaining(), 9);
    }

    /// RFC §8.3: a hook's body draws on the hook calls, kept apart from the
    /// passes: neither spends the other.
    #[test]
    fn a_hook_call_spends_no_pass() {
        let budget = ReentryBudget::new(1);
        let hooks = Draw::HookCall(budget.clone());
        let spent = (0..10).all(|_| hooks.take());
        assert_eq!(
            (spent, hooks.take(), budget.remaining(), budget.take_pass()),
            (true, false, 10, true)
        );
    }

    /// Gap G-070: the operation a delivery's call starts draws on the
    /// passes of the cascade that delivers.
    #[test]
    fn a_clone_spends_the_same_passes() {
        let budget = ReentryBudget::new(1);
        Draw::Pass(budget.clone()).take();
        assert_eq!(budget.remaining(), 9);
    }
}
