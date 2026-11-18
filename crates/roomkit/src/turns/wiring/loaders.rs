//! Readers a channel can retain after an executor wires it.
use super::TurnWiring;
use crate::RoomKitError;
use crate::history::TOOL_ROWS;
use crate::model::{ChannelId, Metadata, RoomEvent, RoomId};
use crate::turns::TurnFuture;
use roomkit_core::rules::transcript::remembered_calls;
use std::sync::Arc;

impl TurnWiring {
    /// Read the channel's most recent remembered tool calls in execution order.
    pub fn build_tool_usage_loader(
        &self,
        channel_id: &ChannelId,
    ) -> impl Fn(RoomId) -> TurnFuture<'static, Result<Vec<Metadata>, RoomKitError>> + Send + Sync + use<>
    {
        let env = Arc::clone(&self.env);
        let channel = channel_id.clone();
        move |room| {
            let env = Arc::clone(&env);
            let channel = channel.clone();
            Box::pin(async move {
                let rows = env.tool_rows(&room, &channel, TOOL_ROWS).await?;
                Ok(remembered_calls(&rows))
            })
        }
    }

    /// Read the channel's replayable rows since the message preceding a history's first index.
    /// A negative index starts from the beginning of the room.
    pub fn build_tool_history_loader(
        &self,
        channel_id: &ChannelId,
    ) -> impl Fn(RoomId, i64) -> TurnFuture<'static, Result<Vec<RoomEvent>, RoomKitError>>
    + Send
    + Sync
    + use<> {
        let wiring = self.clone();
        let channel = channel_id.clone();
        move |room, first| {
            let wiring = wiring.clone();
            let channel = channel.clone();
            Box::pin(async move {
                wiring
                    .tool_history(&channel, &room, u64::try_from(first).ok())
                    .await
            })
        }
    }

    /// Read the owner's current background tasks inside a turn and the bound environment's outside one.
    pub fn build_room_tasks_loader(
        &self,
    ) -> impl Fn(RoomId) -> TurnFuture<'static, Result<Vec<Metadata>, RoomKitError>> + Send + Sync + use<>
    {
        let wiring = self.clone();
        move |room| {
            let wiring = wiring.clone();
            Box::pin(async move { wiring.room_tasks(&room).await })
        }
    }
}
