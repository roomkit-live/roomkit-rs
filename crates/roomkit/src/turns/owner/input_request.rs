//! Preserve an open request on the owner, with its turn's scope (RFC §10.6.5).

use std::sync::Arc;

use super::OwnerState;
use crate::human_input::PendingInputEvent;
use crate::turns::InputTurn;
use crate::turns::owner_inputs::OwnerInputs;
use crate::turns::wire::InputRequest;

pub(super) fn opened(
    owner: &OwnerState,
    inputs: &OwnerInputs,
    turn: Arc<dyn InputTurn>,
    request: InputRequest,
) {
    let facts = &owner.facts;
    let event = PendingInputEvent {
        pending_id: request.request_id,
        tool_name: request.tool_name,
        arguments: request.arguments,
        room_id: request
            .room_id
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| facts.room_id.to_string()),
        channel_id: request
            .channel_id
            .filter(|id| !id.is_empty())
            .unwrap_or_else(|| facts.channel_id.to_string()),
        tool_call_id: request.tool_call_id.unwrap_or_default(),
        channel_type: facts.channel_type.clone(),
        timestamp: request.created_at.unwrap_or(facts.event_created_at),
        actor_id: request.actor_id,
    };
    inputs.opened(turn, event, facts.organization_id.clone());
}
