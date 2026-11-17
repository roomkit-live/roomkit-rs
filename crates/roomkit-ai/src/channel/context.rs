//! Preparation ends before consuming a turn, including a direct buffered call.

use std::sync::Arc;

use roomkit::model::{ChannelBinding, ChannelOutput, EventSource, RoomContext, RoomEvent};
use roomkit::{ChannelError, Scoped, TurnHost, TurnRecord};
use roomkit_core::rules::transcript::Message;

use super::{AiChannel, AiProvider, turn_actor};
use crate::tool_loop::{Loop, TurnItems};
use crate::tools::{ToolTurnContextOptions, within_ai_turn};
use crate::{AiError, ToolPolicy, rows};

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
        let role = roomkit_core::rules::roles::participant_role(&event, &context.participants);
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
                self.answer(event, binding, context, record, role),
            ))
            .await?;
        match prepared {
            Some(answer) => answer.consume().await,
            None => Ok(ChannelOutput::empty()),
        }
    }

    /// Own the policy for this prepared turn; another turn never changes it.
    pub(super) fn turn_policy(&self, role: Option<&str>) -> Result<ToolPolicy, AiError> {
        self.policy.resolve(role).map(std::borrow::Cow::into_owned)
    }

    /// Generation hooks finish preparation; a block hands no stream to the
    /// consumer (RFC §6.4). The prepared loop owns its turn and record.
    pub(super) async fn finish_preparation(
        &self,
        mut looped: Loop,
        event: RoomEvent,
        messages: Vec<Message>,
    ) -> Option<PreparedAnswer> {
        if !looped.before_generation(&event, &messages).await {
            return None;
        }
        let record = looped.record.clone();
        let turn = looped.turn.clone();
        let items = record.clone().scope_stream(looped.run(messages));
        Some(PreparedAnswer {
            items,
            record,
            turn,
            source: self.source(),
            event,
        })
    }
}

#[cfg(test)]
mod served_tests;
