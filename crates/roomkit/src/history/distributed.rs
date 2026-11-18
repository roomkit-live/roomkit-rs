//! Separate the conversation a distributed channel receives from the agent's
//! private tool and instruction rows (RFC §10.6.3, Python's `TurnOwner._channel_rows`).

use roomkit_core::model::{
    ChannelBinding, ChannelCategory, EventType, RoomContext, RoomEvent, RoomId,
};
use roomkit_core::rules::transcript::TOOL_MEMORY_ONLY;
use roomkit_core::rules::withheld_history;

use crate::store::{ConversationStore, StoreError};
use crate::turn::TurnKit;

pub(crate) async fn distributed_rows(
    kit: &dyn TurnKit,
    binding: &ChannelBinding,
    event: &RoomEvent,
    context: &mut RoomContext,
) -> Vec<RoomEvent> {
    if binding.category != ChannelCategory::Intelligence {
        return Vec::new();
    }
    let first = take_conversation(context, &binding.channel_id, event);
    match kit
        .read_channel_rows(binding.clone(), event.room_id.clone(), first)
        .await
    {
        Ok(rows) => rows,
        Err(error) => {
            tracing::warn!(target: "roomkit::turns", room_id = %event.room_id, channel_id = %binding.channel_id, %error, "could not read the channel's rows for its turn");
            Vec::new()
        }
    }
}

/// Separate the channel's private replay rows from the room's conversation.
pub(crate) fn take_conversation(
    context: &mut RoomContext,
    channel: &roomkit_core::model::ChannelId,
    event: &RoomEvent,
) -> Option<u64> {
    // A turn context loaded by this SDK adds the agent's replay rows to its
    // conversation. Its triggering event, even a tool row, is part of that
    // conversation in its own right; other channels' rows are untouched.
    context.recent_events.retain(|row| {
        row.id == event.id
            || row.source.channel_id != *channel
            || !matches!(
                row.r#type,
                EventType::ToolCallStart | EventType::ToolCallEnd | EventType::InstructionRead
            )
    });
    history_start(event, context)
}

pub(crate) fn history_start(event: &RoomEvent, context: &RoomContext) -> Option<u64> {
    context
        .recent_events
        .iter()
        .map(|row| row.index)
        .chain(super::answered_index(event))
        .min()
}

pub(crate) async fn read_distributed_rows<S: ConversationStore>(
    store: &S,
    binding: &ChannelBinding,
    room: &RoomId,
    first: Option<u64>,
) -> Result<Vec<RoomEvent>, StoreError> {
    let mut rows = super::private_rows(store, binding, room, first).await?;
    for row in &mut rows {
        row.metadata.shift_remove(TOOL_MEMORY_ONLY);
    }
    Ok(withheld_history(&rows))
}
