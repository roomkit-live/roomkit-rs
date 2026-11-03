//! The tools a strategy's install sets up for its agent in the room it is
//! installed in, served by the room's owner (RFC §10.6.2, §19.7): Python's
//! `owner_tools_of(kit, agent).register_all(entries, room_id=..., owner=...)`.

use roomkit_core::model::{ChannelId, RoomId};

use super::RoomKit;
use super::background_tools::collision;
use crate::error::RoomKitError;
use crate::store::ConversationStore;
use crate::turns::served::OwnerToolEntry;

impl<S: ConversationStore> RoomKit<S> {
    /// Sets up `entries` for `agent`'s turns in `room_id` alone, all or
    /// none, for the install at `owner`: the same install setting them up
    /// again replaces its own. An uninstall takes them back with the rest
    /// of what the install added.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::ChannelNotRegistered`] for an agent the kit does not
    /// hold; [`RoomKitError::Value`] for a channel whose turns never declare
    /// the tools their owner serves (RFC §10.6.5);
    /// [`RoomKitError::ToolNameCollision`] for a name one of the agent's own
    /// tools takes, or that something else serves it already where the
    /// entry would be declared, none set up.
    pub(crate) fn register_room_tools(
        &self,
        agent: &ChannelId,
        room_id: &RoomId,
        entries: Vec<OwnerToolEntry<S>>,
        owner: usize,
    ) -> Result<(), RoomKitError> {
        let channel =
            self.inner
                .channel(agent)
                .ok_or_else(|| RoomKitError::ChannelNotRegistered {
                    channel_id: agent.clone(),
                    message: format!("Channel '{agent}' is not registered"),
                })?;
        if !channel.declaration().takes_owner_tools {
            return Err(RoomKitError::Value(format!(
                "Channel '{agent}' does not take the tools its room owner serves (RFC §10.6.5): \
                 its turns would never declare them"
            )));
        }
        let own = channel.own_tool_names();
        if let Some(entry) = entries
            .iter()
            .find(|entry| own.iter().any(|name| name == entry.name()))
        {
            return Err(collision(
                entry.name(),
                "one of the channel's own tools",
                agent,
            ));
        }
        self.inner
            .owner_tools
            .register_all_in_room(agent, room_id, entries, owner)
            .map_err(|(name, clash)| collision(&name, &clash, agent))
    }
}
