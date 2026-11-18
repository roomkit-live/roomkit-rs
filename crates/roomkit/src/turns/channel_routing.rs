//! The owner's route for channel control outside a turn (RFC §10.6.3).
//! Live channels stay on the kit's executor. A declaration reaches the
//! executors hosting it when the kit uses a distributed backend.

use std::sync::Arc;

use roomkit_core::model::ChannelId;

use super::Turns;
use super::wire::ChannelControl;
use crate::RoomKitError;
use crate::channel::DynChannel;

impl Turns {
    #[cfg(feature = "redis")]
    pub(crate) fn remote_for_channel(
        &self,
        channel: Option<&dyn DynChannel>,
    ) -> Option<&super::redis::RedisTurnBackend> {
        self.remote
            .as_ref()
            .filter(|_| channel.is_some_and(DynChannel::is_declaration_only))
    }

    pub(crate) fn carries_controls(&self, channel: Option<&dyn DynChannel>) -> bool {
        #[cfg(feature = "redis")]
        if self.remote_for_channel(channel).is_some() {
            return true;
        }
        let _ = channel;
        self.application.is_some()
    }

    pub(super) async fn carry_control(
        &self,
        message: ChannelControl,
    ) -> Result<Option<super::ChannelResult>, RoomKitError> {
        if let Some(app) = &self.application {
            app.ready().await?;
            return app.backend.control(message).await;
        }
        #[cfg(feature = "redis")]
        if let Some(backend) = &self.remote {
            return backend.control_remote(&message).await;
        }
        Err(super::TurnError::link_reason("no_executor").into_kit_error())
    }

    pub(crate) async fn release(
        &self,
        channel_id: &ChannelId,
        channel: Arc<dyn DynChannel>,
    ) -> Result<(), RoomKitError> {
        self.send_channel_notification(
            ChannelControl::ChannelReleased {
                channel_id: channel_id.clone(),
            },
            Some(channel),
        )
        .await
    }

    pub(super) async fn send_channel_notification(
        &self,
        message: ChannelControl,
        channel: Option<Arc<dyn DynChannel>>,
    ) -> Result<(), RoomKitError> {
        #[cfg(feature = "redis")]
        self.identify_redis_owner();
        if self.carries_controls(channel.as_deref()) {
            self.carry_control(message).await?;
            return Ok(());
        }
        if let Some(message) = self.control(&message) {
            super::apply(channel, message).await;
        }
        Ok(())
    }
}
