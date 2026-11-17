//! The local executor's coverage of each room's agents (RFC §10.6.7).

mod call;
pub(in crate::kit) mod detach;
pub(crate) mod watch;

use roomkit_core::model::{ChannelCategory, ChannelId, RoomId};

use super::{GetRoomOptions, RoomKit};
use crate::turns::{AgentPresence, ExecutorInfo, TakesOptions};
use crate::{ConversationStore, RoomKitError};

/// What [`RoomKit::agent_presence`] takes beside the room id.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentPresenceOptions {
    /// Python `organization_id=None`: the organization the read acts for.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// The turn backend's executors, excluding the kit's own executor
    /// (RFC §10.6.7, §10.6.8). A local kit lists no other executors.
    ///
    /// # Errors
    /// The local backend has no executor registry read that can fail.
    #[expect(
        clippy::unused_async,
        clippy::unused_async_trait_impl,
        reason = "Python's public registry reader is asynchronous, including the empty local backend"
    )]
    pub async fn executors(&self) -> Result<Vec<ExecutorInfo>, RoomKitError> {
        Ok(Vec::new())
    }

    /// Each intelligence binding's presence, sorted by channel id, as
    /// Python's `agent_presence`. The returned values own their lists.
    ///
    /// # Errors
    /// [`RoomKitError::RoomNotFound`] for a missing room or another
    /// organization's room; propagates store failures.
    pub async fn agent_presence(
        &self,
        room_id: &RoomId,
        options: AgentPresenceOptions,
    ) -> Result<Vec<AgentPresence>, RoomKitError> {
        let room = self
            .get_room(
                room_id,
                GetRoomOptions {
                    organization_id: options.organization_id,
                },
            )
            .await?;
        let mut bindings = self.store().list_bindings(room_id).await?;
        bindings.retain(|binding| binding.category == ChannelCategory::Intelligence);
        bindings.sort_by(|left, right| left.channel_id.cmp(&right.channel_id));
        Ok(bindings
            .into_iter()
            .map(|binding| {
                self.presence_of(&binding.channel_id, room_id, room.organization_id.clone())
            })
            .collect())
    }

    pub(super) fn presence_of(
        &self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        organization_id: Option<String>,
    ) -> AgentPresence {
        let executor = self.turn_executor();
        let takes = executor.takes(
            channel_id,
            TakesOptions {
                room_hooks: self.config().hooks.enabled_room_hooks(room_id),
                organization_id,
            },
        );
        let covering = if takes {
            vec![executor.executor_id().to_owned()]
        } else {
            Vec::new()
        };
        AgentPresence::new(channel_id.as_str(), takes, covering)
    }
}
