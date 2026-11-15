//! Preparation ends before consuming a turn, including a direct buffered call.

use std::sync::Arc;

use roomkit::model::{ChannelBinding, ChannelOutput, EventSource, RoomContext, RoomEvent};
use roomkit::{ChannelError, Scoped, TurnHost, TurnRecord};

use super::{AiChannel, AiProvider, turn_actor};
use crate::rows;
use crate::tool_loop::TurnItems;
use crate::tools::{ToolTurnContextOptions, within_ai_turn};

/// What preparation hands its consumer. The stream already captured its
/// room and record, but its own live depth starts only when it is polled.
pub(super) struct PreparedAnswer {
    pub(super) items: Scoped<TurnItems<'static>>,
    pub(super) record: TurnRecord,
    pub(super) turn: Option<TurnHost>,
    pub(super) source: EventSource,
    pub(super) event: RoomEvent,
}

impl PreparedAnswer {
    async fn consume(self) -> Result<ChannelOutput, ChannelError> {
        match self.turn {
            Some(turn) => Ok(ChannelOutput {
                response_metadata: self.record.current(),
                ..turn.respond_with_stream(rows::response(self.items))
            }),
            None => Ok(rows::collect(self.source, &self.event, self.items, self.record).await?),
        }
    }
}

impl<P: AiProvider> AiChannel<P> {
    /// Install after ingestion and before configuration. Consumption inherits
    /// the snapshot and record with a new lifetime, on both public call paths.
    pub(super) async fn answer_in_context(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        let options = ToolTurnContextOptions {
            room: Some(Arc::new(context.room.clone())),
            actor_id: turn_actor(&event, &context.participants),
            chain_depth: roomkit_core::rules::chain_depth::response_depth(&event),
            ..ToolTurnContextOptions::default()
        };
        let record = TurnRecord::default();
        let prepared = record
            .clone()
            .scope(within_ai_turn(
                options,
                self.answer(event, binding, context, record),
            ))
            .await?;
        match prepared {
            Some(answer) => answer.consume().await,
            None => Ok(ChannelOutput::empty()),
        }
    }
}
