//! Preparation ends before consuming a turn, including a direct buffered call.

use std::sync::Arc;

use roomkit::model::{ChannelBinding, ChannelOutput, EventSource, RoomContext, RoomEvent};
use roomkit::{ChannelError, Scoped, TurnHost, TurnRecord};
use roomkit_core::rules::transcript::Message;

use super::{
    AiChannel, AiProvider, KitMedia, SentImages, turn_actor, with_attached_media,
    with_discussion_turn, with_ending_texts,
};
use crate::tool_loop::{Loop, TurnItems};
use crate::tools::toolbox::Toolbox;
use crate::tools::{ToolTurnContextOptions, within_ai_turn};
use crate::{AiError, ToolPolicy, rows};

/// The AI's private rows restore its tool memory and earlier rounds. They are
/// presented to neither the channel factory nor the room's conversation memory.
pub(super) fn with_channel_rows(mut context: RoomContext, rows: &[RoomEvent]) -> RoomContext {
    if !rows.is_empty() {
        let mut seen: std::collections::HashSet<_> = context
            .recent_events
            .iter()
            .map(|event| event.id.clone())
            .collect();
        context.recent_events.extend(
            rows.iter()
                .filter(|row| seen.insert(row.id.clone()))
                .cloned(),
        );
        context.recent_events.sort_by_key(|row| row.index);
    }
    context
}

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
    pub(super) fn channel_callbacks(
        &self,
        room: &roomkit::model::RoomId,
        host: Option<&TurnHost>,
    ) -> Option<roomkit::turns::ChannelTurnHooks> {
        host.cloned().map(Into::into).or_else(|| {
            self.wiring
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map(|wiring| {
                    wiring.channel_hooks(room, &self.channel_id, &roomkit::model::ChannelType::AI)
                })
        })
    }

    /// The registered environment supplies the channel's own replay rows.
    pub(super) async fn wired_rows(
        &self,
        event: &RoomEvent,
        context: RoomContext,
        host: Option<&TurnHost>,
    ) -> RoomContext {
        let wiring = self
            .wiring
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(wiring) = wiring {
            return wiring
                .channel_context(event, &self.channel_id, context)
                .await;
        }
        with_channel_rows(context, host.map_or(&[], TurnHost::channel_rows))
    }

    /// Refresh exact ownership before per-turn host admission.
    pub(super) fn admission_toolbox(&self) -> Result<Arc<Toolbox>, ChannelError> {
        let Some(sandbox) = &self.sandbox else {
            return Ok(self.host_toolbox(&self.toolbox));
        };
        let names = sandbox
            .names()
            .map_err(crate::sandbox::tools::channel_error)?;
        Ok(self.host_toolbox(&self.toolbox.with_sandbox(names)))
    }

    /// Schemas are read as context is built, after selecting the turn's provider.
    pub(super) fn sandbox_definitions(
        &self,
        toolbox: Arc<Toolbox>,
    ) -> Result<Arc<Toolbox>, ChannelError> {
        let Some(sandbox) = &self.sandbox else {
            return Ok(toolbox);
        };
        let definitions = sandbox
            .definitions()
            .map_err(crate::sandbox::tools::channel_error)?;
        Ok(Arc::new(toolbox.with_sandbox(definitions)))
    }

    /// Install after ingestion and before configuration. Consumption inherits
    /// the snapshot and record with a new lifetime, on both public call paths.
    pub(super) async fn answer_in_context(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
        notes: Vec<String>,
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
                self.answer(event, binding, context, record, role, notes),
            ))
            .await?;
        match prepared {
            Some(answer) => answer.consume().await,
            None => Ok(ChannelOutput::empty()),
        }
    }

    pub(super) async fn answer(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
        record: TurnRecord,
        role: Option<&str>,
        notes: Vec<String>,
    ) -> Result<Option<PreparedAnswer>, ChannelError> {
        let (looped, messages) = Box::pin(self.prepare_turn(
            event.clone(),
            binding,
            context,
            record,
            (role, notes, false),
        ))
        .await?;
        Ok(Box::pin(self.finish_preparation(looped, event, messages)).await)
    }

    pub(super) async fn thinking_context(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<Option<crate::SharedAiContext>, ChannelError> {
        let role = roomkit_core::rules::roles::participant_role(&event, &context.participants);
        let options = ToolTurnContextOptions {
            room: Some(Arc::new(context.room.clone())),
            actor_id: turn_actor(&event, &context.participants),
            chain_depth: roomkit_core::rules::chain_depth::response_depth(&event),
            ..ToolTurnContextOptions::default()
        };
        let record = TurnRecord::default();
        record
            .clone()
            .scope(within_ai_turn(options, async {
                let (looped, messages) = Box::pin(self.prepare_turn(
                    event.clone(),
                    binding,
                    context,
                    record,
                    (role, Vec::new(), true),
                ))
                .await?;
                Ok(looped
                    .generation_context(&event, &messages, crate::GenerationPurpose::Thought)
                    .await
                    .map(|(_, context)| context))
            }))
            .await
    }

    /// Read memory, attribution and attachments before placing this turn's input.
    pub(super) async fn read_turn(
        &self,
        looped: &mut Loop,
        (event, context, settings): (&RoomEvent, &RoomContext, &super::TurnSettings),
        (stands_alone, vision, thinking): (bool, bool, bool),
        notes: Vec<String>,
    ) -> Result<Vec<Message>, ChannelError> {
        let footprint = self.measure_turn(looped, context, stands_alone, &notes);
        // The event as the model reads it, the media the application keeps
        // beside its content among its parts, as its history (RFC §6.4).
        let kit = KitMedia::of(looped.turn.as_ref());
        let read = with_attached_media(event, kit.attached.as_ref());
        let recalled = self
            .recalled(
                &read,
                context,
                (stands_alone, footprint),
                kit.attached.as_ref(),
            )
            .await
            .map_err(rows::provider_failure)?;
        let images = (self.author_images((&read, &recalled.history), &kit, vision)).await;
        looped.sent_images = Some(SentImages {
            refused: Arc::clone(&self.media.refused),
            room: context.room.id.clone(),
            keys: images.sent(),
        });
        let memory = with_discussion_turn(event, recalled.memory, notes);
        let conversation = self.conversation(
            (&read, &recalled.history),
            &memory,
            &images,
            (vision, thinking),
        );
        // Who a task this turn delegates names as having asked (RFC §19.7).
        looped.context.requester = conversation.requester;
        looped.remembered = with_ending_texts(recalled.messages, conversation.ends_on);
        let (messages, input) = (conversation.messages, conversation.input);
        // The context takes the settings once the conversation is read, as
        // Python's AIContext validates them.
        settings.validated()?;
        looped.place_turn_input(&messages, input.filter(|_| memory.in_place));
        Ok(messages)
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
