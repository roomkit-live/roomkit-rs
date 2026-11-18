//! Exercise the public, directly wired AI channel, outside a submitted turn.

use std::sync::Arc;

use roomkit::Channel;
use roomkit::model::{ChannelBinding, ChannelId, RoomContext, RoomEvent, RoomId};
use roomkit::turns::{TurnFuture, TurnWiring};
use roomkit_ai::{AiChannel, AiProvider};
use serde_json::{Value, json};

use super::Scripted;
use crate::calls::{Called, Unreadable, field};
use crate::run::{Run, lock};
use crate::templates::complete_event;

/// The callable channel retained by the scenario when its concrete type is erased at registration.
pub(crate) trait Prompt: Send + Sync {
    fn invoke(
        &self,
        wiring: TurnWiring,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> TurnFuture<'_, Result<bool, String>>;
}

impl<P: AiProvider> Prompt for AiChannel<P> {
    fn invoke(
        &self,
        wiring: TurnWiring,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> TurnFuture<'_, Result<bool, String>> {
        Box::pin(async move {
            wiring.wire(self).map_err(|e| e.to_string())?;
            let output = self
                .on_event(event, binding, context)
                .await
                .map_err(|e| e.to_string())?;
            Ok(output.responded)
        })
    }
}

pub(crate) fn channel(scripted: &Scripted) -> Arc<dyn Prompt> {
    match scripted {
        Scripted::Channel(channel) => Arc::new(channel.as_ref().clone()),
        Scripted::Agent(agent) => Arc::new(agent.channel().clone()),
        Scripted::RoutedChannel(channel) => Arc::new(channel.as_ref().clone()),
        Scripted::RoutedAgent(agent) => Arc::new(agent.channel().clone()),
    }
}

pub(crate) async fn call(run: &Run, spec: &Value) -> Result<Called, Unreadable> {
    let id: ChannelId = field(spec, "channel_id")?;
    let room: RoomId = field(spec, "room_id")?;
    let agent = lock(&run.direct_channels)
        .get(id.as_str())
        .cloned()
        .ok_or_else(|| format!("agent.direct_prompt requires a live scripted AI channel: {id}"))?;
    let wiring = run.kit().turn_executor().wiring();
    let context = wiring
        .env
        .context(&room, None)
        .await
        .map_err(|e| e.to_string())?;
    let binding = context
        .get_binding(&id)
        .ok_or_else(|| format!("{id} is not bound to {room}"))?;
    let event = serde_json::from_value(complete_event(&spec["event"], room.as_str(), &json!({})))
        .map_err(|e| e.to_string())?;
    let responded = agent
        .invoke(wiring, event, binding.clone(), context)
        .await?;
    Ok(Ok(json!(responded)))
}
