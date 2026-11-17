//! The observed plan payload, separate from the private planner's validation.

use roomkit::model::Timestamp;
use roomkit::{HookTrigger, TurnPayload};
use roomkit_core::ops::{Environment, SystemEnvironment};

use crate::PlanTaskHandle;

/// A room's newly stored plan (RFC §9.2), observed by `ON_PLAN_UPDATED` on
/// the turn executor. Python's `PlanUpdatedEvent`; the hook also runs when
/// no realtime backend is configured.
#[derive(Clone, Debug, PartialEq)]
pub struct PlanUpdatedEvent {
    /// The room whose plan changed; this payload does not validate record ids.
    pub room_id: String,
    /// The channel that stored the plan.
    pub channel_id: String,
    /// A list of shared task objects. Editing an object changes the stored
    /// plan; editing this list does not. Cloning the event copies its list
    /// and shares its objects, as Python's observer payload does.
    pub tasks: Vec<PlanTaskHandle>,
    /// When the new plan was observed.
    pub timestamp: Timestamp,
}

impl PlanUpdatedEvent {
    /// An empty plan for `channel_id` in `room_id`, now, without validating ids.
    /// Other values can be supplied by struct update, as for [`crate::ThinkingEvent`].
    pub fn new(room_id: String, channel_id: String) -> Self {
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
