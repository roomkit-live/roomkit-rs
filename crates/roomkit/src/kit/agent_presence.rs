//! The local executor's coverage of each room's agents (RFC §10.6.7).

mod call;
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
        let declared: Vec<_> = channels
            .iter()
            .filter(|id| {
                self.inner
                    .channel(id)
                    .is_some_and(|c| c.is_declaration_only())
            })
            .collect();
        if declared.is_empty() {
            return BTreeSet::new();
        }
        // Both built-in registries read current coverage. A failed read proves nothing.
        let infos = if let Some(app) = &self.inner.turns.application {
            if !app.backend.lists_executors() {
                return BTreeSet::new();
            }
            app.backend.executors_now().await
        } else {
            self.executors().await
        };
        let confirmed = infos.and_then(|infos| {
            let mut offline = BTreeSet::new();
            for id in declared {
                if !self
                    .presence_of(id, room, organization.map(str::to_owned), &infos)?
                    .online
                {
                    offline.insert(id.clone());
                }
            }
            Ok(offline)
        });
        confirmed.unwrap_or_else(|error| {
            tracing::warn!(%error, %room, "Could not establish executor coverage");
            BTreeSet::new()
        })
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

#[cfg(all(test, feature = "redis"))]
mod tests {
    use super::*;
    use crate::{
        ChannelDeclaration, RedisTurnBackend, RedisTurnBackendOptions, RoomKitOptions, TurnTimeouts,
    };
    use std::time::Duration;
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn an_unreadable_registry_cannot_confirm_any_absent_agent() {
        // The listener owns its port but never answers Redis's handshake.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("redis://{}", listener.local_addr().unwrap());
        let backend = RedisTurnBackend::new(
            Some(&url),
            RedisTurnBackendOptions {
                timeouts: TurnTimeouts {
                    heartbeat_timeout: Duration::from_millis(40),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap();
        let kit = RoomKit::new(RoomKitOptions {
            turn_backend: Some(std::sync::Arc::new(backend)),
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
