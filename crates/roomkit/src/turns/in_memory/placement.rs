//! Local affinity and routing of channel lifecycle notifications (RFC §10.6.7).
use super::{InMemoryTurnBackend, Worker, closed};
use crate::RoomKitError;
use crate::model::{ChannelId, RoomId};
use crate::turns::{ChannelControl, ChannelResult, TakesOptions, lock, no_executor};

impl InMemoryTurnBackend {
    pub(super) fn select(
        &self,
        channel: &ChannelId,
        room: &RoomId,
        hooks: &[String],
        organization: Option<&str>,
        claim: bool,
    ) -> Result<Worker, RoomKitError> {
        let key = (channel.clone(), room.clone());
        let options = TakesOptions {
            room_hooks: hooks.to_owned(),
            organization_id: organization.map(str::to_owned),
        };
        loop {
            let (workers, owner, holder, version) = {
                let state = lock(&self.state);
                if state.closed {
                    return Err(closed());
                }
                (
                    state.workers.clone(),
                    state.owner.clone(),
                    state.holders.get(&key).cloned(),
                    state.revision,
                )
            };
            let own = owner
                .and_then(|p| p.executor())
                .filter(|e| e.hosts_channel(channel));
            let pick = if let Some(own) = own {
                let own_id = own.native().lifecycle();
                workers.into_iter().find(|e| {
                    e.native().lifecycle().ptr_eq(&own_id) && e.takes(channel, options.clone())
                })
            } else {
                holder
                    .into_iter()
                    .chain(workers)
                    .find(|e| e.takes(channel, options.clone()))
            };
            let mut state = lock(&self.state);
            if state.closed {
                return Err(closed());
            }
            if version != state.revision {
                continue;
            }
            let chosen = pick.ok_or_else(|| no_executor(channel.as_str(), hooks, organization))?;
            if claim
                && state
                    .holders
                    .get(&key)
                    .is_none_or(|prior| !std::sync::Arc::ptr_eq(prior, &chosen))
            {
                state.holders.insert(key, std::sync::Arc::clone(&chosen));
                state.revision = state.revision.wrapping_add(1);
            }
            return Ok(chosen);
        }
    }

    pub(super) async fn apply(
        &self,
        message: ChannelControl,
    ) -> Result<Option<ChannelResult>, RoomKitError> {
        self.open()?;
        match &message {
            ChannelControl::ChannelCall(call) => {
                let target = self.select(
                    &call.channel_id,
                    &call.room_id,
                    &[],
                    call.organization_id.as_deref(),
                    true,
                )?;
                call.hand_over();
                let answer = target
                    .control(crossed(&message)?)
                    .await?
                    .unwrap_or(ChannelResult {
                        result: serde_json::Value::Null,
                        error: None,
                        room_state: None,
                        executor_id: None,
                    });
                Ok(Some(crossed(&answer)?))
            }
            ChannelControl::CommittedEvent {
                channel_id,
                room_id,
                organization_id,
                room_hooks,
                ..
            } => {
                let target = self.select(
                    channel_id,
                    room_id,
                    room_hooks,
                    organization_id.as_deref(),
                    false,
                )?;
                target.control(crossed(&message)?).await?;
                Ok(None)
            }
            ChannelControl::RoomDetached {
                channel_id,
                room_id,
                organization_id,
            } => {
                {
                    let mut state = lock(&self.state);
                    state.holders.remove(&(channel_id.clone(), room_id.clone()));
                    state.revision = state.revision.wrapping_add(1);
                }
                self.tell(channel_id, organization_id.as_deref(), &message)
                    .await
            }
            ChannelControl::RoomAttached {
                channel_id,
                organization_id,
                ..
            } => {
                self.tell(channel_id, organization_id.as_deref(), &message)
                    .await
            }
            ChannelControl::ChannelReleased { channel_id } => {
                self.tell(channel_id, None, &message).await
            }
        }
    }

    async fn tell(
        &self,
        channel: &ChannelId,
        organization: Option<&str>,
        message: &ChannelControl,
    ) -> Result<Option<ChannelResult>, RoomKitError> {
        let (workers, owner) = {
            let state = lock(&self.state);
            (state.workers.clone(), state.owner.clone())
        };
        let own = owner
            .and_then(|p| p.executor())
            .filter(|e| e.hosts_channel(channel))
            .map(|e| e.native().lifecycle());
        for worker in workers {
            if own
                .as_ref()
                .is_none_or(|id| id.ptr_eq(&worker.native().lifecycle()))
                && worker.declares(channel)
                && (matches!(message, ChannelControl::ChannelReleased { .. })
                    || worker.serves(organization))
            {
                worker.control(crossed(message)?).await?;
            }
        }
        Ok(None)
    }
}

pub(super) fn crossed<T: serde::Serialize + serde::de::DeserializeOwned>(
    message: &T,
) -> Result<T, RoomKitError> {
    let raw =
        serde_json::to_vec(message).map_err(|error| RoomKitError::Value(error.to_string()))?;
    serde_json::from_slice(&raw).map_err(|error| RoomKitError::Value(error.to_string()))
}
