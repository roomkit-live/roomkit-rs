//! How far a chain of responses may go (RFC §8.3).

use crate::model::{
    ChannelBinding, EventContent, EventDirection, EventId, EventSource, EventStatus, EventType,
    Observation, ObservationId, RoomEvent, Timestamp,
};

/// The default `max_chain_depth` (RFC §8.3).
pub const DEFAULT_MAX_CHAIN_DEPTH: u32 = 5;

/// The `blocked_by` of a channel not asked at the chain-depth limit.
pub const CHAIN_DEPTH_LIMIT: &str = "event_chain_depth_limit";

/// The `blocked_by` of a response past its cascade's reentry budget.
pub const REENTRY_LOOP_CAP: &str = "reentry_loop_cap";

/// The chain depth of a response to `trigger`: one more than the trigger's,
/// whichever path produces it (RFC §8.3).
pub fn response_depth(trigger: &RoomEvent) -> u32 {
    trigger.chain_depth.saturating_add(1)
}

/// Whether a response to an event of `trigger_depth` would reach
/// `max_chain_depth`, so that no intelligence channel may be asked for it
/// (RFC §8.3: `chain_depth + 1 >= max_chain_depth`).
pub fn reaches_limit(trigger_depth: u32, max_chain_depth: u32) -> bool {
    trigger_depth.saturating_add(1) >= max_chain_depth
}

/// The record stored in place of the response of `agent`, not asked because
/// of the limit (RFC §8.3): blocked, sourced from the agent, with empty text
/// and the depth the response would have had.
///
/// The record answers `trigger` (RFC §8.5) and travels outbound, as a
/// response does (gap G-013).
pub fn not_asked_record(
    id: EventId,
    trigger: &RoomEvent,
    agent: &ChannelBinding,
    now: Timestamp,
) -> RoomEvent {
    let source = EventSource::new(
        agent.channel_id.clone(),
        agent.channel_type,
        EventDirection::Outbound,
    );
    let mut record = RoomEvent::new(
        id,
        trigger.room_id.clone(),
        EventType::Message,
        source,
        EventContent::text(""),
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
pub fn not_asked_observation(
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
    observation.category = Some("chain_depth_exceeded".to_owned());
    observation
}

/// The reentry passes one cascade may still take (RFC §8.3): ten times the
/// chain-depth limit, shared by every response the cascade re-enters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReentryBudget {
    remaining: u32,
}

impl ReentryBudget {
    /// The budget of a cascade under `max_chain_depth`.
    pub fn new(max_chain_depth: u32) -> Self {
        Self {
            remaining: max_chain_depth.saturating_mul(10),
        }
    }

    /// Spends one pass, or answers `false` when none is left: the response
    /// is then stored blocked with [`REENTRY_LOOP_CAP`] instead.
    pub fn take(&mut self) -> bool {
        match self.remaining.checked_sub(1) {
            Some(remaining) => {
                self.remaining = remaining;
                true
            }
            None => false,
        }
    }

    /// The passes left.
    pub fn remaining(self) -> u32 {
        self.remaining
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
                ChannelType::Ai,
                EventDirection::Outbound,
            ),
            EventContent::text("Good report."),
            Timestamp::UNIX_EPOCH,
        );
        event.chain_depth = chain_depth;
        event
    }

    fn writer() -> ChannelBinding {
        ChannelBinding::new(
            ChannelId::new("writer").unwrap(),
            RoomId::new("room_1").unwrap(),
            ChannelType::Ai,
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
            ("writer", EventContent::text(""))
        );
    }

    #[test]
    fn the_not_asked_observation_is_categorized_as_chain_depth_exceeded() {
        let observation = not_asked_observation(
            ObservationId::new("obs_1").unwrap(),
            &trigger(4),
            &writer(),
            DEFAULT_MAX_CHAIN_DEPTH,
            Timestamp::UNIX_EPOCH,
        );
        assert_eq!(
            observation.category.as_deref(),
            Some("chain_depth_exceeded")
        );
    }

    #[test]
    fn the_reentry_budget_is_ten_times_the_chain_depth_limit() {
        assert_eq!(ReentryBudget::new(5).remaining(), 50);
    }

    #[test]
    fn an_exhausted_reentry_budget_refuses_a_pass() {
        let mut budget = ReentryBudget::new(0);
        assert!(!budget.take());
    }

    #[test]
    fn taking_a_pass_spends_the_budget() {
        let mut budget = ReentryBudget::new(1);
        budget.take();
        assert_eq!(budget.remaining(), 9);
    }
}
