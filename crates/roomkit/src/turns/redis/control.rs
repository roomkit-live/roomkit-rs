//! Route application channel calls to the room holder; notify every eligible
//! executor when its declaration was attached, detached or released.
use std::collections::BTreeSet;

use super::RedisTurnBackend;
use super::placement::Placed;
use super::registry::Target;
use crate::RoomKitError;
use crate::TurnExecutor;
use crate::turns::channel_calls::{ChannelCall, ChannelResult};
use crate::turns::wire::{ChannelControl, ChannelControlEnvelope};
use roomkit_core::model::ChannelId;

struct Placement<'a> {
    channel: &'a ChannelId,
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
                        channel: &call.channel_id,
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
                        channel: channel_id,
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
                self.tell_all(&raw, channel_id, Audience::Room(organization_id.as_deref()))
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
                self.tell_all(&raw, channel_id, Audience::Room(organization_id.as_deref()))
                    .await?;
                Ok(None)
            }
            ChannelControl::ChannelReleased { channel_id } => {
                self.tell_all(&raw, channel_id, Audience::AllOrganizations)
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
            let applied = match &target {
                Placed::Here(worker) if worker.closed() => None,
                Placed::Here(worker) => Some(apply_here(worker, raw).await?),
                Placed::Elsewhere(peer) => self.apply_to(peer, raw).await?,
            };
            if let Some(answer) = applied {
                return Ok(Some(answer));
            }
            if let Some(call) = placement.call {
                call.refused();
            }
            refused.insert(target.member(self.node_id()));
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
        channel: &ChannelId,
        audience: Audience<'_>,
    ) -> Result<(), RoomKitError> {
        let workers = crate::turns::lock(&self.0.serving).workers.clone();
        for worker in workers {
            if worker.declares(channel)
                && match audience {
                    Audience::Room(org) => worker.serves(org),
                    Audience::AllOrganizations => true,
                }
            {
                apply_here(&worker, raw).await?;
            }
        }
        let mut pending = tokio::task::JoinSet::new();
        for info in self.executors().await? {
            if info.node.as_deref() == Some(self.node_id()) {
                continue;
            }
            let matches_org = match audience {
                Audience::Room(org) => org,
                Audience::AllOrganizations => info
                    .organizations
                    .as_ref()
                    .and_then(|names| names.first())
                    .map(String::as_str),
            };
            if !info.covers(channel.as_str(), &[], matches_org)? {
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

/// The same serialized control and result as a worker on another node.
async fn apply_here(worker: &TurnExecutor, raw: &str) -> Result<ChannelResult, RoomKitError> {
    let parse_error = |error: serde_json::Error| RoomKitError::Value(error.to_string());
    let envelope: ChannelControlEnvelope = serde_json::from_str(raw).map_err(parse_error)?;
    let answer = worker.control(envelope.message).await?;
    let raw = encode_answer(answer).map_err(parse_error)?;
    serde_json::from_str(&raw).map_err(parse_error)
}

pub(super) fn encode_answer(answer: Option<ChannelResult>) -> Result<String, serde_json::Error> {
    match answer {
        Some(answer) => serde_json::to_string(&answer),
        None => Ok(r#"{"type":"channel_result","result":null,"room_state":null,"error":null,"executor_id":null}"#.to_owned()),
    }
}
