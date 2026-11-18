//! Per-room thoughts join only decided answer turns, never instructions.

use std::sync::{Arc, PoisonError};

use roomkit::ChannelError;
use roomkit::model::{ChannelBinding, RoomContext, RoomEvent, RoomId};

use super::{
    AiChannel, AiProvider,
    mind::{Mind, report},
};
use crate::{SpeakDecision, Thought};

impl<P: AiProvider> AiChannel<P> {
    pub(super) fn thought_of(&self, room: &RoomId) -> Option<Thought> {
        self.speech.thinker.as_ref()?;
        let minds = self
            .speech
            .minds
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Some(
            minds
                .get(room)
                .map_or_else(Thought::default, |mind| mind.thought()),
        )
    }

    pub(super) async fn think_while_listening(
        &self,
        event: &RoomEvent,
        binding: &ChannelBinding,
        context: &RoomContext,
        decision: SpeakDecision,
    ) -> Result<SpeakDecision, ChannelError> {
        let Some(thinker) = &self.speech.thinker else {
            return Ok(decision);
        };
        let mind = {
            let mut minds = self
                .speech
                .minds
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            Arc::clone(minds.entry(context.room.id.clone()).or_insert_with(|| {
                Arc::new(Mind::new(
                    context.room.id.to_string(),
                    self.channel_id.to_string(),
                    Arc::clone(thinker),
                ))
            }))
        };
        let Some(input) = self
            .thinking_context(event.clone(), binding.clone(), context.clone())
            .await?
        else {
            return Ok(decision);
        };
        let (channel, said, room) = (self.clone(), event.clone(), context.clone());
        let late = Box::new(
            move |thought| -> std::pin::Pin<Box<dyn Future<Output = ()> + Send>> {
                Box::pin(async move {
                    channel
                        .speak_decision(&said, &room, Some(thought), true, true)
                        .await;
                })
            },
        );
        let callbacks =
            self.channel_callbacks(&context.room.id, roomkit::current_turn_host().as_ref());
        let (ticket, classified) = mind.ask(
            input,
            self.loops.answering_in(&context.room.id),
            callbacks,
            late,
        );
        if decision.r#final
            || self.speech.wait.is_zero()
            || !mind.settled(ticket, self.speech.wait).await
        {
            let _ = classified.send(true);
            return Ok(decision);
        }
        let _ = classified.send(false);
        let thought = mind.thought();
        if thought.want_to_say.is_empty() {
            return Ok(decision);
        }
        Ok(self
            .speak_decision(event, context, Some(thought), true, false)
            .await
            .unwrap_or(decision))
    }

    pub(super) fn thought_notes(&self, room: &RoomId) -> Vec<String> {
        let mind = self
            .speech
            .minds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(room)
            .cloned();
        let Some(mind) = mind else {
            return Vec::new();
        };
        let spoken = mind.spoke();
        let now = spoken.said();
        if now != spoken {
            report(
                self.channel_callbacks(room, roomkit::current_turn_host().as_ref())
                    .as_ref(),
                room.as_str(),
                self.channel_id.as_str(),
                now,
                spoken.clone(),
                None,
            );
        }
        let note = crate::speaking::thought_note(&spoken);
        if note.is_empty() {
            Vec::new()
        } else {
            vec![note]
        }
    }

    pub(super) async fn forget_thought(&self, room: &RoomId) {
        let mind = self
            .speech
            .minds
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(room);
        if let Some(mind) = mind {
            mind.close().await;
        }
    }

    pub(super) async fn close_minds(&self) {
        let minds = std::mem::take(
            &mut *self
                .speech
                .minds
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for mind in minds.into_values() {
            mind.close().await;
        }
    }
}
