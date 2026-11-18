//! Executor callbacks whose event types belong to the AI channel.
use std::sync::Arc;

use roomkit::RoomKitError;
use roomkit::model::{ChannelId, ChannelType, Metadata, RoomId};
use roomkit::turns::{ChannelTurnHooks, EmitOptions, SyncPipelineResult, TurnFuture, TurnWiring};

use crate::{
    AiGenerationEvent, AiResponseEvent, SpeakDecision, SpeakDecisionEvent, ThinkingEvent,
    ThoughtEvent, ToolRoundEvent,
};

type Callback<E, R> = Arc<dyn Fn(E) -> TurnFuture<'static, R> + Send + Sync>;
type ThinkingCallback = Arc<dyn Fn(RoomId, String, u32) -> TurnFuture<'static, ()> + Send + Sync>;

/// AI callbacks obtained from an executor's wiring. Import this trait to call the
/// builders on [`TurnWiring`], including when the channel has not been registered.
pub trait TurnWiringExt {
    /// Create the channel's nonblocking reasoning observer.
    fn build_thinking_hook(&self, channel_id: &ChannelId) -> ThinkingCallback;
    /// Create a callback returning the speaking decision left by the synchronous hooks.
    fn build_speak_decision_hook(&self) -> Callback<SpeakDecisionEvent, SpeakDecision>;
    /// Create a callback scheduling observers of a thought.
    fn build_thought_hook(&self) -> Callback<ThoughtEvent, ()>;
    /// Apply a completed round's tool withdrawals and new messages in place.
    /// Rust handlers receive an owned event and return its edits via MODIFY (RFC §6.4).
    fn build_after_tool_round_hook(
        &self,
    ) -> impl for<'a> Fn(&'a mut ToolRoundEvent) -> TurnFuture<'a, ()> + Send + Sync + use<Self>;
    /// Create the response observer, followed by the framework event.
    fn build_after_response_hook(
        &self,
        channel_id: &ChannelId,
    ) -> Callback<AiResponseEvent, Result<(), RoomKitError>>;
    /// Create the synchronous generation gate and publish its decision.
    fn build_before_generation_hook(
        &self,
        channel_id: &ChannelId,
    ) -> Callback<AiGenerationEvent, Result<SyncPipelineResult<AiGenerationEvent>, RoomKitError>>;
}

impl TurnWiringExt for TurnWiring {
    fn build_thinking_hook(&self, channel_id: &ChannelId) -> ThinkingCallback {
        let (wiring, channel) = (self.clone(), channel_id.clone());
        Arc::new(move |room, thinking, round| {
            let hooks = wiring.channel_hooks(&room, &channel, &ChannelType::AI);
            let channel = channel.clone();
            Box::pin(async move {
                if hooks.has_turn_hooks::<ThinkingEvent>() {
                    let mut event = ThinkingEvent::new(room, channel, thinking);
                    event.round_index = round;
                    hooks.observe(&event, || {});
                }
            })
        })
    }

    fn build_speak_decision_hook(&self) -> Callback<SpeakDecisionEvent, SpeakDecision> {
        let wiring = self.clone();
        Arc::new(move |event| {
            let hooks = for_event(&wiring, &event.room_id, &event.channel_id);
            Box::pin(crate::channel::speaking::hooked(event, hooks))
        })
    }

    fn build_thought_hook(&self) -> Callback<ThoughtEvent, ()> {
        let wiring = self.clone();
        Arc::new(move |event| {
            let hooks = for_event(&wiring, &event.room_id, &event.channel_id);
            Box::pin(async move {
                if let Some(hooks) = hooks {
                    hooks.observe_after_context(&event).await;
                }
            })
        })
    }

    fn build_after_tool_round_hook(
        &self,
    ) -> impl for<'a> Fn(&'a mut ToolRoundEvent) -> TurnFuture<'a, ()> + Send + Sync + use<> {
        let wiring = self.clone();
        move |event| {
            let hooks = event
                .room_id
                .as_ref()
                .map(|room| wiring.channel_hooks(room, &event.channel_id, &ChannelType::AI));
            Box::pin(async move {
                if let Some(hooks) = hooks {
                    let left = hooks.run(event.clone()).await.payload;
                    event.withdrawn = left.withdrawn;
                    event.messages = left.messages;
                }
            })
        }
    }

    fn build_after_response_hook(
        &self,
        channel_id: &ChannelId,
    ) -> Callback<AiResponseEvent, Result<(), RoomKitError>> {
        let (wiring, channel) = (self.clone(), channel_id.clone());
        Arc::new(move |event| {
            let (wiring, channel) = (wiring.clone(), channel.clone());
            Box::pin(async move {
                let Some(room) = event.room_id.as_ref() else {
                    return Ok(());
                };
                let hooks = wiring.channel_hooks(room, &event.channel_id, &ChannelType::AI);
                let data = response_data(&event);
                if hooks.has_turn_hooks::<AiResponseEvent>() {
                    let outlet = wiring.channel_hooks(room, &channel, &ChannelType::AI);
                    hooks
                        .observe_after_context_then(&event, move || {
                            outlet.emit_later("ai_response", data);
                        })
                        .await;
                } else {
                    wiring
                        .emit(
                            "ai_response",
                            EmitOptions {
                                room_id: Some(room.clone()),
                                channel_id: Some(channel),
                                data: Some(data),
                                ..Default::default()
                            },
                        )
                        .await?;
                }
                Ok(())
            })
        })
    }

    fn build_before_generation_hook(
        &self,
        channel_id: &ChannelId,
    ) -> Callback<AiGenerationEvent, Result<SyncPipelineResult<AiGenerationEvent>, RoomKitError>>
    {
        let (wiring, channel) = (self.clone(), channel_id.clone());
        Arc::new(move |event| {
            let (wiring, channel) = (wiring.clone(), channel.clone());
            Box::pin(async move {
                let Some(room) = event.room_id.clone() else {
                    return Ok(SyncPipelineResult::default());
                };
                let hooks = wiring.channel_hooks(&room, &event.channel_id, &ChannelType::AI);
                let result = hooks.sync_pipeline(event).await;
                if result.event.is_some() {
                    let data = Metadata::from_iter([
                        ("allowed".into(), result.allowed.into()),
                        ("blocked_by".into(), result.blocked_by.clone().into()),
                    ]);
                    wiring
                        .emit(
                            "before_ai_generation",
                            EmitOptions {
                                room_id: Some(room),
                                channel_id: Some(channel),
                                data: Some(data),
                                ..Default::default()
                            },
                        )
                        .await?;
                }
                Ok(result)
            })
        })
    }
}

fn for_event(wiring: &TurnWiring, room: &str, channel: &str) -> Option<ChannelTurnHooks> {
    Some(wiring.channel_hooks(
        &RoomId::new(room).ok()?,
        &ChannelId::new(channel).ok()?,
        &ChannelType::AI,
    ))
}

pub(crate) fn response_data(event: &AiResponseEvent) -> Metadata {
    Metadata::from_iter([
        ("tool_calls_count".into(), event.tool_calls_count.into()),
        ("latency_ms".into(), event.latency_ms.into()),
        ("streaming".into(), event.streaming.into()),
    ])
}
