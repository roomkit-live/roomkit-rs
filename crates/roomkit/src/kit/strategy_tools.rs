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
        for entry in &entries {
            self.check_own_tool(agent, entry.name())?;
        }
        self.inner
            .owner_tools
            .register_all_in_room(agent, room_id, entries, owner)
            .map_err(|(name, clash)| collision(&name, &clash, agent))
    }

    /// Refuses what [`register_room_tools`](Self::register_room_tools) would
    /// refuse of a tool `name`, nothing set up: all of a strategy's agents
    /// checked before any is served (Python's registry's `check`).
    pub(crate) fn check_room_tool(
        &self,
        agent: &ChannelId,
        room_id: &RoomId,
        name: &str,
        owner: usize,
    ) -> Result<(), RoomKitError> {
        self.check_own_tool(agent, name)?;
        match self
            .inner
            .owner_tools
            .clash_in_room(agent, room_id, name, owner)
        {
            Some(clash) => Err(collision(name, &clash, agent)),
            None => Ok(()),
        }
    }

    /// Refuses a channel the kit does not hold, one whose turns never
    /// declare the tools their owner serves (RFC §10.6.5), and a name one of
    /// its own tools takes.
    fn check_own_tool(&self, agent: &ChannelId, name: &str) -> Result<(), RoomKitError> {
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
        if channel.own_tool_names().iter().any(|own| own == name) {
            return Err(collision(name, "one of the channel's own tools", agent));
        }
        Ok(())
    }
}
