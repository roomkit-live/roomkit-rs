//! Observe a text channel's updated plan on the executor that wired it.
use super::TurnWiring;
use crate::model::{ChannelId, ChannelType, RoomId};
use crate::turns::{PlanTaskHandle, PlanUpdatedEvent, TurnFuture};

impl TurnWiring {
    /// Build the callback that reads the room context and schedules its plan observers.
    pub fn build_plan_updated_hook(
        &self,
        channel_id: &ChannelId,
    ) -> impl Fn(RoomId, Vec<PlanTaskHandle>) -> TurnFuture<'static, ()> + Send + Sync + use<> {
        let wiring = self.clone();
        let channel = channel_id.clone();
        move |room, tasks| {
            let wiring = wiring.clone();
            let channel = channel.clone();
            Box::pin(async move {
                let hooks = wiring.channel_hooks(&room, &channel, &ChannelType::AI);
                let event = PlanUpdatedEvent {
                    tasks,
                    ..PlanUpdatedEvent::new(room.to_string(), channel.to_string())
                };
                hooks.observe_after_context(&event).await;
            })
        }
    }
}
