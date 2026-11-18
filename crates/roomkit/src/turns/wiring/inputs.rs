//! Human-input callbacks constructed by an executor's consumer or its registered channel.
use super::{EmitOptions, TurnFuture, TurnWiring};
use crate::human_input::PendingInputEvent;
use crate::model::{ChannelId, RoomId};
use crate::turn::InputNotification;
use crate::{HookTrigger, RoomKitError};

impl TurnWiring {
    /// Bind the input request gate and the framework notification for a channel.
    /// A request without a room or whose hook context cannot be built is allowed.
    pub fn build_on_user_input_required_hook(
        &self,
        channel_id: &ChannelId,
    ) -> impl Fn(PendingInputEvent) -> TurnFuture<'static, Result<bool, RoomKitError>>
    + Send
    + Sync
    + use<> {
        let (wiring, registered) = (self.clone(), channel_id.clone());
        move |event| {
            let (wiring, registered) = (wiring.clone(), registered.clone());
            Box::pin(async move {
                let Some(notification) = wiring.input_notification(&registered, &event).await
                else {
                    return Ok(true);
                };
                let (allowed, data) = notification.verdict(&event).await;
                let (room, channel) = notification.ids;
                wiring
                    .emit(
                        "user_input_required",
                        EmitOptions {
                            room_id: Some(room),
                            channel_id: Some(channel),
                            data: Some(data),
                            ..Default::default()
                        },
                    )
                    .await?;
                Ok(allowed)
            })
        }
    }

    pub(crate) async fn input_notification(
        &self,
        registered: &ChannelId,
        event: &PendingInputEvent,
    ) -> Option<InputNotification> {
        let room = RoomId::new(&event.room_id).ok()?;
        let channel = ChannelId::new(&event.channel_id).unwrap_or_else(|_| registered.clone());
        let hooks = self.kit.input_hooks((&room, &channel), &event.channel_type);
        let hooks = if hooks.is_empty() {
            None
        } else {
            Some((
                hooks,
                self.hook_context(&room, HookTrigger::OnUserInputRequired, None)
                    .await?,
            ))
        };
        Some(InputNotification {
            kit: std::sync::Arc::clone(&self.kit),
            ids: (room, channel),
            hooks,
            turn: crate::turn::turn_in_scope(),
            wiring: Some(self.clone()),
        })
    }
}
