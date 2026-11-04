//! A handler's greeting (RFC §19.1, §19.6): the room's active agent says the
//! line the handler's greeting map gives it. Python's
//! `HandoffHandler.send_greeting`, its text path: the realtime sessions it
//! injects into come with the voice channels.

use roomkit_core::model::{ChannelId, RoomId};

use crate::error::RoomKitError;
use crate::kit::{self, GetRoomOptions};
use crate::orchestration::get_conversation_state;
use crate::orchestration::handoff::HandoffHandler;
use crate::store::ConversationStore;

/// What [`HandoffHandler::send_greeting`] takes beside the room, Python's
/// `send_greeting` keyword.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SendGreetingOptions {
    /// What allows the greeting, the handler's event channel when unset:
    /// with neither, nothing is said. The channel itself is read only for a
    /// realtime session: the greeting reaches the whole room.
    pub channel_id: Option<ChannelId>,
}

impl<S: ConversationStore> HandoffHandler<S> {
    /// Has the room's active agent say its greeting: the line the handler's
    /// [`greeting_map`](Self::greeting_map) gives it, as written, through
    /// [`RoomKit::send_greeting`](crate::RoomKit::send_greeting), which stores
    /// it as the agent's message and delivers it as one. A greeting is never
    /// sent as something the user said, which the agent would answer. Python's
    /// `send_greeting(room_id, channel_id=None)`.
    ///
    /// Call it once the room's conversation state is set. With no active
    /// agent, no line for it, whatever its own greeting, or no channel named
    /// by the call or the handler, nothing is said, and nothing fails.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::KitClosed`] once the kit is gone, and what reading the
    /// room, which a room missing refuses, or its conversation state fails
    /// with, or what [`RoomKit::send_greeting`](crate::RoomKit::send_greeting)
    /// fails with.
    pub async fn send_greeting(
        &self,
        room_id: &RoomId,
        options: SendGreetingOptions,
    ) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        let room = kit.get_room(room_id, GetRoomOptions::default()).await?;
        let Some(agent_id) = get_conversation_state(&room)?.active_agent_id else {
            return Ok(());
        };
        let line = self.greeting_map().remove(&agent_id);
        let Some(greeting) = line.filter(|line| !line.is_empty()) else {
            return Ok(());
        };
        if options.channel_id.is_none() && self.event_channel().is_none() {
            return Ok(());
        }
        kit.send_greeting(
            room_id,
            kit::SendGreetingOptions {
                agent_id: Some(agent_id),
                greeting: Some(greeting),
                ..kit::SendGreetingOptions::default()
            },
        )
        .await
    }
}
