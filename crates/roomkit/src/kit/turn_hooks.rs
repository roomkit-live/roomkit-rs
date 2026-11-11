//! The hooks of the triggers an AI channel's loop fires (RFC §6.4):
//! `AFTER_TOOL_ROUND`, `BEFORE_AI_GENERATION` and `ON_AI_RESPONSE`, whose
//! payloads roomkit-ai defines, found for a turn's agent and room.

use std::sync::Arc;

use roomkit_core::model::{ChannelId, ChannelType, RoomId};
use roomkit_core::rules::hook_registry::{HookSpec, HookSubject, HookTrigger};

use super::{Inner, read};
use crate::hooks::{Kept, TurnHook};
use crate::store::ConversationStore;

impl<S: ConversationStore> Inner<S> {
    /// The hooks of `trigger`, one a turn's payload of its own hands, that
    /// fire for the agent `channel_id` of `channel_type` in the room
    /// `room_id`, the kit's and the room's: its SYNC chain in chain order,
    /// then its observers (RFC §9.1, §9.2).
    pub(crate) fn turn_hooks(
        &self,
        trigger: HookTrigger,
        (room_id, channel_id): (&RoomId, &ChannelId),
        channel_type: &ChannelType,
    ) -> Vec<(HookSpec, TurnHook)> {
        let config = Arc::clone(&read(&self.config));
        let handlers = read(&self.hooks);
        let registry = super::named_hooks::registry_for_turn(&config.hooks, room_id);
        let subject = HookSubject::of_channel(channel_id, channel_type)
            .typed_for(trigger)
            .in_room(room_id);
        registry
            .chain(trigger, subject)
            .into_iter()
            .chain(registry.observers(trigger, subject))
            .filter_map(|id| {
                let Kept::Turn(hook) = handlers.get(&id)? else {
                    return None;
                };
                Some((registry.get(id)?.clone(), hook.clone()))
            })
            .collect()
    }
}
