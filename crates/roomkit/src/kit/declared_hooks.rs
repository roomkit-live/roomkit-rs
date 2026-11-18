//! Turn hooks registered on the kit run on the local executor (RFC §10.6.6).

use roomkit_core::model::{ChannelId, ChannelType, RoomId};
use roomkit_core::rules::hook_registry::{HookRegistry, HookSpec};

use super::{Inner, read};
use crate::error::{RoomKitError, repr};

pub(super) fn check_declaration(
    registry: &HookRegistry,
    id: &ChannelId,
    kind: &ChannelType,
) -> Result<(), RoomKitError> {
    for (hook, room) in registry.kit_turn_hooks() {
        if conflicts(hook, room, id, kind) {
            return Err(unreachable_hook(hook, id, room, true));
        }
    }
    Ok(())
}

impl<S> Inner<S> {
    pub(super) fn check_kit_hook(
        &self,
        hook: &HookSpec,
        room: Option<&RoomId>,
    ) -> Result<(), RoomKitError> {
        if !hook.trigger.is_turn_trigger() {
            return Ok(());
        }
        let order = read(&self.channel_order);
        let channels = read(&self.channels);
        for id in order.iter() {
            if let Some(channel) = channels.get(id)
                && channel.is_declaration_only()
                && conflicts(hook, room, id, &channel.declaration().channel_type)
            {
                return Err(unreachable_hook(hook, id, room, false));
            }
        }
        Ok(())
    }
}

fn conflicts(hook: &HookSpec, room: Option<&RoomId>, id: &ChannelId, kind: &ChannelType) -> bool {
    // A room hook must remain valid in any binding the room later acquires.
    room.is_some()
        || (hook
            .filter
            .channel_ids
            .as_ref()
            .is_none_or(|ids| ids.contains(id))
            && (!hook.trigger.carries_channel_type()
                || hook
                    .filter
                    .channel_types
                    .as_ref()
                    .is_none_or(|types| types.contains(kind))))
}

fn unreachable_hook(
    hook: &HookSpec,
    id: &ChannelId,
    room: Option<&RoomId>,
    registering_channel: bool,
) -> RoomKitError {
    let id = repr(id.as_str());
    let name = repr(&hook.name);
    let trigger = super::engine::trigger_name(hook.trigger);
    let scope = if room.is_some() {
        "for a room"
    } else {
        "globally"
    };
    let alternative = if room.is_some() {
        ""
    } else {
        ", or leave the channel out of channel_ids"
    };
    let message = if registering_channel {
        format!(
            "Channel {id} is held by its declaration alone, its turns on another executor, where \
             hook {name} on {trigger}, registered {scope} on the kit, never fires (RFC §10.6.6): \
             register that hook on the executor that runs the channel (executor.hook(...)){alternative}"
        )
    } else {
        format!(
            "Hook {name} on {trigger}: channel {id} is held by its declaration alone, its turns \
             on another executor, where a hook on the kit never fires (RFC §10.6.6): register it \
             on that executor (executor.hook(...)){alternative}"
        )
    };
    RoomKitError::Value(message)
}
