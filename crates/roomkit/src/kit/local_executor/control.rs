//! An owner's messages to a worker outside a running turn (RFC §10.6.3).

use crate::RoomKitError;
use crate::turns::ChannelResult;
use crate::turns::wire::{ChannelControl, ChannelControlEnvelope};
use crate::{ConversationStore, TurnExecutor};

impl<S: ConversationStore> TurnExecutor<S> {
    pub(crate) async fn control(
        &self,
        envelope: ChannelControlEnvelope,
    ) -> Result<Option<ChannelResult>, RoomKitError> {
        if self.closed() {
            return Err(RoomKitError::TurnLink {
                reason: "no_executor".into(),
                message: Some(format!("Executor {} has closed", self.executor_id())),
            });
        }
        let message = envelope.message;
        match &message {
            ChannelControl::ChannelCall(call) => {
                let answer = match self
                    .worker_channel(call.channel_id.clone(), call.organization_id.as_deref())
                    .await
                {
                    Ok(channel) => {
                        self.kit
                            .inner
                            .turns
                            .worker_call(&channel.channel, call, self.executor_id())
                            .await
                    }
                    Err(error) => ChannelResult {
                        result: serde_json::Value::Null,
                        error: Some(*error),
                        room_state: None,
                        executor_id: Some(self.executor_id().to_owned()),
                    },
                };
                Ok(Some(answer))
            }
            ChannelControl::ChannelReleased { channel_id } => {
                if self.standalone.is_some() {
                    self.release_channel(channel_id).await?;
                }
                Ok(None)
            }
            ChannelControl::RoomAttached {
                channel_id,
                organization_id,
                ..
            } => {
                if self.serves(organization_id.as_deref()) {
                    let channel = self.built_channel(channel_id).await;
                    crate::turns::apply(
                        channel
                            .as_ref()
                            .map(|built| std::sync::Arc::clone(&built.channel)),
                        message,
                    )
                    .await;
                }
                Ok(None)
            }
            ChannelControl::RoomDetached {
                channel_id,
                room_id,
                ..
            } => {
                self.kit.inner.turns.forget_worker_room(channel_id, room_id);
                let channel = self.built_channel(channel_id).await;
                crate::turns::apply(
                    channel
                        .as_ref()
                        .map(|built| std::sync::Arc::clone(&built.channel)),
                    message,
                )
                .await;
                Ok(None)
            }
            ChannelControl::CommittedEvent {
                channel_id,
                organization_id,
                room_hooks,
                ..
            } => {
                if self.serves(organization_id.as_deref())
                    && self.hooks().defines_room_hooks(room_hooks)
                {
                    match self
                        .worker_channel(channel_id.clone(), organization_id.as_deref())
                        .await
                    {
                        Ok(channel) => {
                            crate::turns::apply(
                                Some(std::sync::Arc::clone(&channel.channel)),
                                message,
                            )
                            .await;
                        }
                        Err(error) => {
                            tracing::warn!(%channel_id, message = %error.message, "could not deliver a committed event");
                        }
                    }
                }
                Ok(None)
            }
        }
    }
}
