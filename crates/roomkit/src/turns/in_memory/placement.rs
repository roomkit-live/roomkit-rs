//! Local affinity and routing of channel lifecycle notifications (RFC §10.6.7).
use super::{InMemoryTurnBackend, Worker, closed};
use crate::RoomKitError;
use crate::model::{ChannelId, RoomId};
use crate::turns::{
    ChannelControl, ChannelResult, ExecutorInfo, TakesOptions, lock, names, no_executor, not_taken,
};

/// What a selection is for: an event held, which leaves the room's holder as
/// it is; a turn or a call placed, whose taker holds the room from now; or a
/// turn submitted to the one executor its caller names, which holds it too.
#[derive(Clone, Copy)]
pub(super) enum Claim<'a> {
    None,
    Holder,
    Named(&'a ExecutorInfo),
}

impl InMemoryTurnBackend {
    pub(super) fn select(
        &self,
        channel: &ChannelId,
        room: &RoomId,
        hooks: &[String],
        organization: Option<&str>,
        claim: Claim<'_>,
    ) -> Result<Worker, RoomKitError> {
        // Every executor is of this process, which names no node.
        let named = |e: &Worker| match claim {
            Claim::Named(only) => names(only, e.executor_id(), None),
            Claim::None | Claim::Holder => true,
        };
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
                    e.native().lifecycle().ptr_eq(&own_id)
                        && named(e)
                        && e.takes(channel, options.clone())
                })
            } else {
                holder
                    .into_iter()
                    .chain(workers)
                    .find(|e| named(e) && e.takes(channel, options.clone()))
            };
            let mut state = lock(&self.state);
            if state.closed {
                return Err(closed());
            }
            if version != state.revision {
                continue;
            }
            let chosen = pick.ok_or_else(|| match claim {
                Claim::Named(only) => not_taken(only, channel.as_str(), hooks, organization),
                Claim::None | Claim::Holder => no_executor(channel.as_str(), hooks, organization),
            })?;
            if !matches!(claim, Claim::None) {
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
                    Claim::Holder,
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
                    Claim::None,
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
    use super::{Claim, InMemoryTurnBackend};
    use crate::model::{ChannelId, RoomId};
    use crate::testing::{EchoAgent, EchoAgentOptions};
    use crate::turns::records::ROOMS_KEPT;
    use crate::turns::{
        ChannelCall, ChannelControl, ExecutorInfo, ExecutorInfoOptions, TurnBackend,
    };
    use crate::{RoomKitError, TurnExecutor, TurnExecutorOptions};
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

    fn worker(id: &str) -> Arc<TurnExecutor> {
        Arc::new(
            TurnExecutor::new(
                |id| {
                    std::future::ready(Ok(Some(EchoAgent::new(
                        id,
                        "hi",
                        EchoAgentOptions::default(),
                    ))))
                },
                TurnExecutorOptions {
                    executor_id: Some(id.into()),
                    hosts: Some(["agent".into()].into()),
                    ..Default::default()
                },
            )
            .unwrap(),
        )
    }

    fn named(id: &str, node: Option<&str>) -> ExecutorInfo {
        ExecutorInfo::new(
            id,
            ExecutorInfoOptions {
                node: node.map(str::to_owned),
                ..Default::default()
            },
        )
    }

    #[tokio::test]
    async fn a_named_executor_takes_the_turn_whoever_holds_the_room_and_then_holds_it() {
        let backend = InMemoryTurnBackend::new();
        let second = worker("second");
        backend.serve(worker("first")).await.unwrap();
        backend
            .serve(Arc::<TurnExecutor>::clone(&second))
            .await
            .unwrap();
        let channel = ChannelId::new("agent").unwrap();
        let room = RoomId::new("room").unwrap();
        let taker = |claim| {
            backend
                .select(&channel, &room, &[], None, claim)
                .map(|worker| worker.executor_id().to_owned())
        };
        assert_eq!(taker(Claim::Holder).unwrap(), "first");
        // No worker of that id, a node this one process has none of, a room
        // hook it does not define: refused, the room still on its holder.
        for (only, hooks) in [
            (named("ghost", None), &[][..]),
            (named("second", Some("elsewhere")), &[][..]),
            (named("second", None), &["audit".to_owned()][..]),
        ] {
            let refused = backend.select(&channel, &room, hooks, None, Claim::Named(&only));
            assert!(
                matches!(&refused, Err(RoomKitError::TurnLink { reason, .. }) if reason == "no_executor"),
                "{:?}",
                refused.err()
            );
            assert_eq!(taker(Claim::None).unwrap(), "first");
        }
        let second_named = named("second", None);
        assert_eq!(taker(Claim::Named(&second_named)).unwrap(), "second");
        assert_eq!(taker(Claim::Holder).unwrap(), "second");
        // Closed, it takes no turn, named or not.
        second.close().await;
        assert!(taker(Claim::Named(&second_named)).is_err());
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
            backend
                .select(&channel, &room, &[], None, Claim::Holder)
                .unwrap();
        }
        assert_eq!(call(&backend, &channel, &active).await, "second");
        // A committed event only looks up the holder; the next new room evicts the quiet one.
        backend
            .select(&channel, &quiet, &[], None, Claim::None)
            .unwrap();
        assert_eq!(
            call(&backend, &channel, &RoomId::new("another").unwrap()).await,
            "first"
        );
        assert_eq!(call(&backend, &channel, &active).await, "second");
        assert_eq!(call(&backend, &channel, &quiet).await, "first");
    }
}
