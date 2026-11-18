//! Route application channel calls to the room holder; notify every eligible
//! executor when its declaration was attached, detached or released.
use std::collections::BTreeSet;

use super::RedisTurnBackend;
use super::registry::Target;
use crate::RoomKitError;
use crate::turns::channel_calls::{ChannelCall, ChannelResult};
use crate::turns::wire::{ChannelControl, ChannelControlEnvelope};

struct Placement<'a> {
    channel: &'a str,
    room: &'a str,
    hooks: &'a [String],
    organization: Option<&'a str>,
    call: Option<&'a ChannelCall>,
}

enum Audience<'a> {
    Room(Option<&'a str>),
    AllOrganizations,
}

impl RedisTurnBackend {
    pub(crate) async fn control_remote(
        &self,
        message: &ChannelControl,
    ) -> Result<Option<ChannelResult>, RoomKitError> {
        self.attend().await?;
        let raw = serde_json::to_string(&ChannelControlEnvelope {
            message: message.clone(),
        })
        .map_err(|e| RoomKitError::Value(e.to_string()))?;
        match message {
            ChannelControl::ChannelCall(call) => {
                self.apply_one(
                    &raw,
                    Placement {
                        channel: call.channel_id.as_str(),
                        room: call.room_id.as_str(),
                        hooks: &[],
                        organization: call.organization_id.as_deref(),
                        call: Some(call),
                    },
                )
                .await
            }
            ChannelControl::CommittedEvent {
                channel_id,
                room_id,
                organization_id,
                room_hooks,
                ..
            } => {
                self.apply_one(
                    &raw,
                    Placement {
                        channel: channel_id.as_str(),
                        room: room_id.as_str(),
                        hooks: room_hooks,
                        organization: organization_id.as_deref(),
                        call: None,
                    },
                )
                .await?;
                Ok(None)
            }
            ChannelControl::RoomAttached {
                channel_id,
                organization_id,
                ..
            } => {
                self.tell_all(
                    &raw,
                    channel_id.as_str(),
                    Audience::Room(organization_id.as_deref()),
                )
                .await?;
                Ok(None)
            }
            ChannelControl::RoomDetached {
                channel_id,
                room_id,
                organization_id,
            } => {
                self.forget_holder(channel_id.as_str(), room_id.as_str())
                    .await?;
                self.tell_all(
                    &raw,
                    channel_id.as_str(),
                    Audience::Room(organization_id.as_deref()),
                )
                .await?;
                Ok(None)
            }
            ChannelControl::ChannelReleased { channel_id } => {
                self.tell_all(&raw, channel_id.as_str(), Audience::AllOrganizations)
                    .await?;
                Ok(None)
            }
        }
    }

    async fn apply_one(
        &self,
        raw: &str,
        placement: Placement<'_>,
    ) -> Result<Option<ChannelResult>, RoomKitError> {
        let mut refused = BTreeSet::new();
        loop {
            let target = self
                .place(
                    placement.channel,
                    placement.room,
                    placement.hooks,
                    placement.organization,
                    &refused,
                    placement.call.is_some(),
                )
                .await?;
            if let Some(call) = placement.call {
                call.hand_over();
            }
            if let Some(answer) = self.apply_to(&target, raw).await? {
                return Ok(Some(answer));
            }
            if let Some(call) = placement.call {
                call.refused();
            }
            refused.insert(target.member());
        }
    }

    async fn apply_to(
        &self,
        target: &Target,
        raw: &str,
    ) -> Result<Option<ChannelResult>, RoomKitError> {
        let answer = self
            .ask_living(&target.node, raw.to_owned(), &target.executor_id)
            .await?;
        if answer == "refused" {
            return Ok(None);
        }
        serde_json::from_str(&answer)
            .map(Some)
            .map_err(|error| RoomKitError::TurnLink {
                reason: "frame_invalid".into(),
                message: Some(error.to_string()),
            })
    }

    async fn tell_all(
        &self,
        raw: &str,
        channel: &str,
        audience: Audience<'_>,
    ) -> Result<(), RoomKitError> {
        let mut pending = tokio::task::JoinSet::new();
        for info in self.executors().await? {
            let matches_org = match audience {
                Audience::Room(org) => org,
                Audience::AllOrganizations => info
                    .organizations
                    .as_ref()
                    .and_then(|names| names.first())
                    .map(String::as_str),
            };
            if !info.covers(channel, &[], matches_org)? {
                continue;
            }
            if let Some(node) = info.node {
                let backend = self.clone();
                let message = raw.to_owned();
                let target = Target {
                    node,
                    executor_id: info.executor_id,
                };
                pending.spawn(async move { backend.apply_to(&target, &message).await });
            }
        }
        while let Some(result) = pending.join_next().await {
            match result {
                Ok(Ok(Some(_))) => {}
                Ok(Ok(None)) => tracing::warn!("A closing executor refused channel control"),
                Ok(Err(error)) => {
                    tracing::warn!(%error, "Executor failed to apply channel control");
                }
                Err(error) => tracing::warn!(%error, "Channel control task failed"),
            }
        }
        Ok(())
    }
}
