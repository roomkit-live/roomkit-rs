//! The observed plan payload, separate from the private planner's validation.

use roomkit::model::{ChannelId, Metadata, RoomId, Timestamp};
use roomkit::{HookTrigger, TurnPayload};
use roomkit_core::ops::{Environment, SystemEnvironment};

/// A room's newly stored plan (RFC §9.2), observed by `ON_PLAN_UPDATED` on
/// the turn executor. Python's `PlanUpdatedEvent`; the hook also runs when
/// no realtime backend is configured.
#[derive(Clone, Debug, PartialEq)]
pub struct PlanUpdatedEvent {
    /// The room whose plan changed.
    pub room_id: RoomId,
    /// The channel that stored the plan.
    pub channel_id: ChannelId,
    /// The plan's tasks. The public payload preserves arbitrary JSON object
    /// members; only the private planning tool normalizes title and status.
    pub tasks: Vec<Metadata>,
    /// When the new plan was observed.
    pub timestamp: Timestamp,
}

impl PlanUpdatedEvent {
    /// An empty plan for `channel_id` in `room_id`, now. Other values can
    /// be supplied by struct update, as for [`crate::ThinkingEvent`].
    pub fn new(room_id: RoomId, channel_id: ChannelId) -> Self {
        Self {
            room_id,
            channel_id,
            tasks: Vec::new(),
            timestamp: SystemEnvironment.now(),
        }
    }
}

impl TurnPayload for PlanUpdatedEvent {
    const TRIGGER: HookTrigger = HookTrigger::OnPlanUpdated;
}
