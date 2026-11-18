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
                    state.holders.get(channel, room).cloned(),
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
            if claim {
                state
                    .holders
                    .put(channel, room, std::sync::Arc::clone(&chosen));
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
                    state.holders.pop(channel_id, room_id);
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

#[cfg(test)]
mod tests {
    use super::InMemoryTurnBackend;
    use crate::model::{ChannelId, RoomId};
    use crate::testing::{EchoAgent, EchoAgentOptions};
    use crate::turns::records::ROOMS_KEPT;
    use crate::turns::{ChannelCall, ChannelControl, TurnBackend};
    use crate::{TurnExecutor, TurnExecutorOptions};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    async fn call(backend: &InMemoryTurnBackend, channel: &ChannelId, room: &RoomId) -> String {
        let call = ChannelCall::new(channel.clone(), room.clone(), "get_status");
        backend
            .control(ChannelControl::ChannelCall(Box::new(call)))
            .await
            .unwrap()
            .unwrap()
            .executor_id
            .unwrap()
    }

    #[tokio::test]
    async fn a_room_call_refreshes_affinity_and_a_quiet_rooms_holder_expires() {
        let backend = InMemoryTurnBackend::new();
        let enabled = Arc::new(AtomicBool::new(false));
        for (id, eligible) in [
            ("first", Arc::clone(&enabled)),
            ("second", Arc::new(AtomicBool::new(true))),
        ] {
            let worker = TurnExecutor::new_with_host_predicate(
                move |id| {
                    std::future::ready(Ok(Some(EchoAgent::new(
                        id,
                        "hi",
                        EchoAgentOptions::default(),
                    ))))
                },
                move |_| eligible.load(Ordering::Relaxed),
                TurnExecutorOptions {
                    executor_id: Some(id.into()),
                    ..Default::default()
                },
            )
            .unwrap();
            backend.serve(Arc::new(worker)).await.unwrap();
        }
        let channel = ChannelId::new("agent").unwrap();
        let active = RoomId::new("active").unwrap();
        let quiet = RoomId::new("quiet").unwrap();
        assert_eq!(call(&backend, &channel, &active).await, "second");
        assert_eq!(call(&backend, &channel, &quiet).await, "second");
        enabled.store(true, Ordering::Relaxed);
        for id in 0..ROOMS_KEPT - 2 {
            let room = RoomId::new(format!("other-{id}")).unwrap();
            backend.select(&channel, &room, &[], None, true).unwrap();
        }
        assert_eq!(call(&backend, &channel, &active).await, "second");
        // A committed event only looks up the holder; the next new room evicts the quiet one.
        backend.select(&channel, &quiet, &[], None, false).unwrap();
        assert_eq!(
            call(&backend, &channel, &RoomId::new("another").unwrap()).await,
            "first"
        );
        assert_eq!(call(&backend, &channel, &active).await, "second");
        assert_eq!(call(&backend, &channel, &quiet).await, "first");
    }
}
