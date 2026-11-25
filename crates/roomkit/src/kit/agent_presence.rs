//! The local executor's coverage of each room's agents (RFC §10.6.7).

mod call;
#[cfg(test)]
mod catalogue_tests;
pub(in crate::kit) mod detach;
pub(crate) mod watch;

use std::collections::BTreeSet;

use roomkit_core::model::{ChannelCategory, ChannelId, RoomId};

use super::{GetRoomOptions, RoomKit};
use crate::turns::{AgentPresence, ExecutorInfo, TakesOptions};
use crate::{ConversationStore, RoomKitError};

/// What [`RoomKit::agent_presence`] takes beside the room id.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentPresenceOptions {
    /// Python `organization_id=None`: the organization the read acts for.
    pub organization_id: Option<String>,
}

impl<S: ConversationStore> RoomKit<S> {
    /// The turn backend's executors, excluding the kit's own executor
    /// (RFC §10.6.7, §10.6.8). A local kit lists no other executors.
    ///
    /// # Errors
    /// The distributed executor registry could not be read.
    pub async fn executors(&self) -> Result<Vec<ExecutorInfo>, RoomKitError> {
        if let Some(backend) = self.inner.turns.configured_backend() {
            return backend.executors().await;
        }
        Ok(Vec::new())
    }

    /// Each intelligence binding's presence, sorted by channel id, as
    /// Python's `agent_presence`. The returned values own their lists.
    ///
    /// # Errors
    /// [`RoomKitError::RoomNotFound`] for a missing room or another
    /// organization's room; propagates store failures.
    pub async fn agent_presence(
        &self,
        room_id: &RoomId,
        options: AgentPresenceOptions,
    ) -> Result<Vec<AgentPresence>, RoomKitError> {
        let room = self
            .get_room(
                room_id,
                GetRoomOptions {
                    organization_id: options.organization_id,
                },
            )
            .await?;
        let executors = self.executors().await?;
        let mut bindings = self.store().list_bindings(room_id).await?;
        bindings.retain(|binding| binding.category == ChannelCategory::Intelligence);
        bindings.sort_by(|left, right| left.channel_id.cmp(&right.channel_id));
        bindings
            .into_iter()
            .map(|binding| {
                self.presence_of(
                    &binding.channel_id,
                    room_id,
                    room.organization_id.clone(),
                    &executors,
                )
            })
            .collect()
    }

    /// Only declared agents absent from a fresh registry read can miss work (§10.6.7).
    pub(crate) async fn known_offline(
        &self,
        channels: &[ChannelId],
        room: &RoomId,
        organization: Option<&str>,
    ) -> BTreeSet<ChannelId> {
        let declared: BTreeSet<_> = channels
            .iter()
            .filter(|id| {
                self.inner
                    .channel(id)
                    .is_some_and(|c| c.is_declaration_only())
            })
            .cloned()
            .collect();
        let absent = match self.inner.turns.configured_backend() {
            Some(backend) => {
                self.confirm_offline(&**backend, declared, room, organization)
                    .await
            }
            // The implicit local transport lists no other executors.
            None => self.absent_from_listing(declared, room, organization, &[]),
        };
        absent.unwrap_or_else(|error| {
            tracing::warn!(%error, %room, "Could not establish executor coverage");
            BTreeSet::new()
        })
    }

    /// A cached absence may have missed a new worker. Confirm only the still
    /// missing channels before the discussion drops their work (§19.7.5).
    async fn confirm_offline(
        &self,
        backend: &dyn crate::turns::TurnBackend,
        mut candidates: BTreeSet<ChannelId>,
        room: &RoomId,
        organization: Option<&str>,
    ) -> Result<BTreeSet<ChannelId>, RoomKitError> {
        use crate::turns::TurnBackend;

        for read in [
            <dyn TurnBackend>::executors,
            <dyn TurnBackend>::executors_now,
        ] {
            if candidates.is_empty() {
                break;
            }
            let Some(read) = read(backend) else {
                return Ok(BTreeSet::new());
            };
            candidates = self.absent_from_listing(candidates, room, organization, &read.await?)?;
        }
        Ok(candidates)
    }

    fn absent_from_listing(
        &self,
        candidates: BTreeSet<ChannelId>,
        room: &RoomId,
        organization: Option<&str>,
        infos: &[ExecutorInfo],
    ) -> Result<BTreeSet<ChannelId>, RoomKitError> {
        let mut absent = BTreeSet::new();
        for id in candidates {
            if !self
                .presence_of(&id, room, organization.map(str::to_owned), infos)?
                .online
            {
                absent.insert(id);
            }
        }
        Ok(absent)
    }

    pub(super) fn presence_of(
        &self,
        channel_id: &ChannelId,
        room_id: &RoomId,
        organization_id: Option<String>,
        infos: &[ExecutorInfo],
    ) -> Result<AgentPresence, RoomKitError> {
        let hooks = self.config().hooks.enabled_room_hooks(room_id);
        if self
            .inner
            .channel(channel_id)
            .is_none_or(|channel| channel.is_declaration_only())
        {
            let mut covering = Vec::new();
            for info in infos {
                if info.covers(channel_id.as_str(), &hooks, organization_id.as_deref())? {
                    covering.push(info.executor_id.clone());
                }
            }
            covering.sort();
            return Ok(AgentPresence::new(
                channel_id.as_str(),
                !covering.is_empty(),
                covering,
            ));
        }
        let executor = self.turn_executor();
        let takes = executor.takes(
            channel_id,
            TakesOptions {
                room_hooks: hooks,
                organization_id,
            },
        );
        let covering = if takes {
            vec![executor.executor_id().to_owned()]
        } else {
            Vec::new()
        };
        Ok(AgentPresence::new(channel_id.as_str(), takes, covering))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turns::{
        Beat, BeatWriter, Beats, DistributedTurnBackend, DistributedTurnBackendOptions,
        InMemoryTurnBroker, RegistryView, TurnFuture, TurnRegistry,
    };
    use crate::{ChannelDeclaration, RoomKitOptions};
    use std::sync::Arc;

    /// A registry no call reaches: each one fails.
    struct Unreachable;

    fn unreachable<'a, T: Send + 'a>() -> TurnFuture<'a, Result<T, RoomKitError>> {
        Box::pin(async { Err(RoomKitError::Runtime("the registry is unreachable".into())) })
    }

    impl TurnRegistry for Unreachable {
        fn connect(&self) -> TurnFuture<'_, Result<(), RoomKitError>> {
            unreachable()
        }

        fn connected(&self) -> bool {
            false
        }

        fn beat<'a>(&'a self, _: &'a Beat) -> TurnFuture<'a, Result<(), RoomKitError>> {
            unreachable()
        }

        fn beater(&self) -> TurnFuture<'_, Result<Box<dyn BeatWriter>, RoomKitError>> {
            unreachable()
        }

        fn view(&self, _: i64) -> TurnFuture<'_, Result<RegistryView, RoomKitError>> {
            unreachable()
        }

        fn forget_owner<'a>(&'a self, _: &'a str) -> TurnFuture<'a, Result<(), RoomKitError>> {
            unreachable()
        }

        fn beats<'a>(
            &'a self,
            _: &'a [&'a str],
            _: &'a [(&'a str, Vec<&'a str>)],
        ) -> TurnFuture<'a, Result<Beats, RoomKitError>> {
            unreachable()
        }

        fn holder<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
        ) -> TurnFuture<'a, Result<Option<String>, RoomKitError>> {
            unreachable()
        }

        fn hold<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: &'a str,
        ) -> TurnFuture<'a, Result<(), RoomKitError>> {
            unreachable()
        }

        fn forget_holder<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
        ) -> TurnFuture<'a, Result<(), RoomKitError>> {
            unreachable()
        }

        fn claim_presence<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: Option<bool>,
        ) -> TurnFuture<'a, Result<bool, RoomKitError>> {
            unreachable()
        }
    }

    #[tokio::test]
    async fn an_unreadable_registry_cannot_confirm_any_absent_agent() {
        let backend = DistributedTurnBackend::new(
            Arc::new(InMemoryTurnBroker::new()),
            Arc::new(Unreachable),
            DistributedTurnBackendOptions::default(),
        );
        let kit = RoomKit::new(RoomKitOptions {
            turn_backend: Some(crate::turns::TurnBackendHandle::new(Arc::new(backend))),
            ..Default::default()
        });
        let candidates = ["ana", "bob"].map(|name| ChannelId::new(name).unwrap());
        for id in &candidates {
            kit.register_channel(ChannelDeclaration::new(id.clone()))
                .unwrap();
        }
        let room = RoomId::new("without-a-stored-room").unwrap();
        assert!(kit.executors().await.is_err());
        assert!(kit.known_offline(&candidates, &room, None).await.is_empty());
        kit.close().await.unwrap();
    }
}
