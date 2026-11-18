//! The same per-channel readers, whether backed by a store or by a turn.
use super::TurnWiring;
use crate::RoomKitError;
use crate::history::{HISTORY_ROWS_PAGE, TOOL_ROWS, merged_rows, replay_pages, replay_start};
use crate::model::{ChannelId, RoomContext, RoomEvent, RoomId};
use roomkit_core::rules::withheld_history;

impl TurnWiring {
    /// The channel's own tool and instruction history since the message preceding
    /// `first`. Use `None` to start from the beginning of the room.
    ///
    /// # Errors
    /// Propagates errors reading the store or application environment.
    pub async fn tool_history(
        &self,
        channel: &ChannelId,
        room: &RoomId,
        first: Option<u64>,
    ) -> Result<Vec<RoomEvent>, RoomKitError> {
        let after = match first {
            Some(first) => self.env.message_before(room, first).await?,
            None => None,
        };
        Ok(withheld_history(
            &self.replay_rows(room, channel, after).await?,
        ))
    }

    async fn replay_rows(
        &self,
        room: &RoomId,
        channel: &ChannelId,
        after: Option<u64>,
    ) -> Result<Vec<RoomEvent>, RoomKitError> {
        replay_pages(after, |after| {
            self.env
                .history_rows(room, channel, after, HISTORY_ROWS_PAGE)
        })
        .await
    }

    async fn rows(
        &self,
        room: &RoomId,
        channel: &ChannelId,
        first: Option<u64>,
    ) -> Result<Vec<RoomEvent>, RoomKitError> {
        let latest = self.env.tool_rows(room, channel, TOOL_ROWS).await?;
        let from = match first {
            Some(first) => Some(self.env.message_before(room, first).await?),
            None => None,
        };
        let replayed = match from {
            Some(after) => self.replay_rows(room, channel, after).await?,
            None => Vec::new(),
        };
        Ok(withheld_history(&merged_rows(
            latest,
            replayed,
            from.and_then(replay_start),
        )))
    }

    /// Add the SDK's private replay rows to an AI channel's turn context.
    #[doc(hidden)]
    pub async fn channel_context(
        &self,
        event: &RoomEvent,
        channel: &ChannelId,
        mut context: RoomContext,
    ) -> RoomContext {
        let host = crate::turn::turn_in_scope();
        let rows = if self.kit_defaults
            && let Some(host) = host
        {
            // Local native turns already read the store under the room lock.
            // A supplied owner may have added separate rows to its request.
            host.channel_rows().to_vec()
        } else {
            let first =
                crate::history::distributed::take_conversation(&mut context, channel, event);
            match self.rows(&context.room.id, channel, first).await {
                Ok(rows) => rows,
                Err(error) => {
                    tracing::warn!(room_id=%context.room.id,channel_id=%channel,%error,"could not read the channel's private rows");
                    Vec::new()
                }
            }
        };
        if !rows.is_empty() {
            let mut seen: std::collections::HashSet<_> = context
                .recent_events
                .iter()
                .map(|row| row.id.clone())
                .collect();
            context
                .recent_events
                .extend(rows.into_iter().filter(|row| seen.insert(row.id.clone())));
            context.recent_events.sort_by_key(|row| row.index);
        }
        context
    }
}
