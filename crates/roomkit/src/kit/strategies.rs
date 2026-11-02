//! One orchestration strategy per room, installed while the room lives (RFC
//! §19.7): Python's `StrategyMixin`.
//!
//! An install claims the room for one strategy, registers and attaches the
//! strategy's agents, runs its install, and records what the install added
//! for the room, from the room before and after it: its room hooks, the
//! agents it attached and the room metadata keys it wrote. An uninstall runs
//! the strategy's own uninstall step, then takes back exactly what was
//! recorded; the room can then take another strategy, its timeline kept.
//! Recording what changed, rather than asking each strategy what it set up,
//! holds for the host's own strategies too.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{ChannelCategory, ChannelId, Metadata, RoomId};

use super::RoomKit;
use crate::error::RoomKitError;
use crate::kit::{AttachChannelOptions, DetachChannelOptions, GetRoomOptions};
use crate::orchestration::{Orchestration, StrategyAgent};
use crate::store::{ConversationStore, PatchRoomMetadataOptions, StoreError};

/// What [`RoomKit::install_strategy`] takes beside the room and the
/// strategy: Python's `install_strategy(room_id, strategy, *,
/// organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InstallStrategyOptions {
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2); a room of another organization is not found.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::uninstall_strategy`] takes beside the room: Python's
/// `uninstall_strategy(room_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UninstallStrategyOptions {
    /// Python `organization_id=None`: the organization the call acts for
    /// (RFC §17.2); a room of another organization is not found.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::claim_room_strategy`] takes beside the room and the
/// strategy: Python's `claim_room_strategy(room_id, strategy, *,
/// organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClaimRoomStrategyOptions {
    /// Python `organization_id=None`: the organization the caller acts for
    /// (RFC §17.2); set, the room is read with it, and another
    /// organization's room is not found.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::room_strategy`] takes beside the room: Python's
/// `room_strategy(room_id, *, organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoomStrategyOptions {
    /// Python `organization_id=None`: scopes the read to one tenant (RFC
    /// §17.2): a room of another organization reads as holding none.
    pub organization_id: Option<String>,
}

/// The strategies installed through the kit, by room.
pub(crate) struct RoomStrategies<S> {
    rooms: Mutex<HashMap<RoomId, Installed<S>>>,
}

impl<S> Default for RoomStrategies<S> {
    fn default() -> Self {
        Self {
            rooms: Mutex::new(HashMap::new()),
        }
    }
}

impl<S> fmt::Debug for RoomStrategies<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoomStrategies")
            .finish_non_exhaustive()
    }
}

impl<S> RoomStrategies<S> {
    fn rooms(&self) -> MutexGuard<'_, HashMap<RoomId, Installed<S>>> {
        self.rooms.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A room's strategy, and what its install added for the room.
struct Installed<S> {
    strategy: Arc<dyn Orchestration<S>>,
    organization_id: Option<String>,
    added: Added,
    /// The record claims the room while the install runs.
    installing: bool,
}

/// What an install added for a room: what an uninstall takes back.
#[derive(Clone, Debug, Default)]
struct Added {
    hooks: Vec<String>,
    attached: Vec<ChannelId>,
    keys: Vec<String>,
}

/// What a room holds that a strategy's install may add to.
#[derive(Clone)]
struct Snapshot {
    hooks: BTreeSet<String>,
    bindings: BTreeSet<ChannelId>,
    keys: BTreeSet<String>,
}

impl Snapshot {
    /// What `after` holds that this snapshot does not.
    fn added(&self, after: &Self) -> Added {
        Added {
            hooks: after.hooks.difference(&self.hooks).cloned().collect(),
            attached: after.bindings.difference(&self.bindings).cloned().collect(),
            keys: after.keys.difference(&self.keys).cloned().collect(),
        }
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// The strategy installed in `room_id` through this kit, if any:
    /// Python's `room_strategy(room_id, *, organization_id=None)`. A room of
    /// another organization than `options` names reads as holding none
    /// (RFC §17.2).
    pub fn room_strategy(
        &self,
        room_id: &RoomId,
        options: RoomStrategyOptions,
    ) -> Option<Arc<dyn Orchestration<S>>> {
        let RoomStrategyOptions { organization_id } = options;
        let rooms = self.inner.room_strategies.rooms();
        let installed = rooms.get(room_id)?;
        if organization_id.is_some() && installed.organization_id != organization_id {
            return None;
        }
        Some(Arc::clone(&installed.strategy))
    }

    /// Installs `strategy` in a room, new or already in use (RFC §19.7):
    /// Python's `install_strategy(room_id, strategy, *,
    /// organization_id=None)`. The strategy's agents are registered and
    /// attached as intelligence channels, then its install runs; the room's
    /// timeline stays, and the agents' turns read it.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] for a room the call does not
    /// reach (RFC §17.2); [`RoomKitError::Value`] when the room holds a
    /// strategy already (uninstall it first), or the strategy refuses the
    /// room, what its install added taken back;
    /// [`RoomKitError::ChannelNotRegistered`] for an agent named by an id
    /// the kit does not hold; and what the strategy's install failed with.
    /// When what the install added could not be read, or not all of it
    /// taken back, the store's error or the install's is returned and the
    /// room stays the strategy's: `uninstall_strategy` takes back the rest.
    pub async fn install_strategy(
        &self,
        room_id: &RoomId,
        strategy: Arc<dyn Orchestration<S>>,
        options: InstallStrategyOptions,
    ) -> Result<(), RoomKitError> {
        let room = self
            .get_room(
                room_id,
                GetRoomOptions {
                    organization_id: options.organization_id,
                },
            )
            .await?;
        self.install_on(room_id, strategy, room.organization_id)
            .await
    }

    /// Takes the room's strategy back out (RFC §19.7): its own uninstall
    /// step, then what its install added for the room. Python's
    /// `uninstall_strategy(room_id, *, organization_id=None)`: whether the
    /// room held a strategy. The room answers by its
    /// `agent_response_policy` again and may take another strategy.
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] for a room the call does not
    /// reach (RFC §17.2); what the strategy's uninstall step failed with;
    /// and [`RoomKitError::Runtime`] when an agent the install attached
    /// could not be detached: the room stays the strategy's, and a call
    /// again finishes the rest.
    pub async fn uninstall_strategy(
        &self,
        room_id: &RoomId,
        options: UninstallStrategyOptions,
    ) -> Result<bool, RoomKitError> {
        self.get_room(
            room_id,
            GetRoomOptions {
                organization_id: options.organization_id,
            },
        )
        .await?;
        let Some(strategy) = self.recorded_strategy(room_id) else {
            return Ok(false);
        };
        strategy.uninstall(self, room_id).await?;
        // Released only once all of it is taken back: a room left with part
        // of a strategy is still that strategy's, never open to a second.
        let added = self.recorded_added(room_id, &strategy);
        let stayed = self.take_back(room_id, added, &strategy).await?;
        if !stayed.is_empty() {
            return Err(still_binds(room_id, &stayed));
        }
        self.drop_record(room_id, &strategy);
        Ok(true)
    }

    /// Claims `room_id` for `strategy`, or refuses: a room holds one
    /// strategy (RFC §19.7). Python's `claim_room_strategy(room_id,
    /// strategy, *, organization_id=None)`. [`install_strategy`]
    /// claims for the strategy it installs; a strategy installed by calling
    /// its `install` directly calls this first.
    ///
    /// [`install_strategy`]: Self::install_strategy
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::RoomNotFound`] when `options` names an
    /// organization and the room is missing or another organization's, and
    /// [`RoomKitError::Value`] when the room holds another strategy.
    pub async fn claim_room_strategy(
        &self,
        room_id: &RoomId,
        strategy: Arc<dyn Orchestration<S>>,
        options: ClaimRoomStrategyOptions,
    ) -> Result<(), RoomKitError> {
        if options.organization_id.is_some() {
            self.get_room(
                room_id,
                GetRoomOptions {
                    organization_id: options.organization_id,
                },
            )
            .await?;
        }
        check_claim(&self.inner.room_strategies.rooms(), room_id, &strategy)
    }

    /// Installs `strategy` in `room_id`, of `organization_id`, the room
    /// read: claimed before any await, then its agents attached, its install
    /// run and what it added recorded; a failed install is taken back.
    pub(crate) async fn install_on(
        &self,
        room_id: &RoomId,
        strategy: Arc<dyn Orchestration<S>>,
        organization_id: Option<String>,
    ) -> Result<(), RoomKitError> {
        self.claim_install(room_id, &strategy, organization_id)?;
        // An install cut short is taken back as a failed one is (Python's
        // `except BaseException`), once its future is dropped.
        let mut guard = InstallGuard {
            kit: self.clone(),
            room_id: room_id.clone(),
            strategy: Arc::clone(&strategy),
            before: None,
            armed: true,
        };
        let before = match self.snapshot(room_id).await {
            Ok(before) => before,
            Err(error) => {
                guard.armed = false;
                self.drop_record(room_id, &strategy);
                return Err(error);
            }
        };
        guard.before = Some(before.clone());
        let installed = match self.attach_agents(room_id, strategy.as_ref()).await {
            Ok(()) => strategy.install(self, room_id).await,
            Err(error) => Err(error),
        };
        let (added, unread) = self.added_since(room_id, &before).await;
        let outcome = match (installed, unread) {
            (Err(error), unread) => {
                self.undo_install(room_id, &strategy, added, unread.is_none())
                    .await;
                Err(error)
            }
            (Ok(()), Some(error)) => {
                // What the read missed stays the strategy's: the room stays
                // claimed, for uninstall_strategy to take back.
                self.record(room_id, &strategy, added);
                Err(error)
            }
            (Ok(()), None) => {
                self.record(room_id, &strategy, added);
                Ok(())
            }
        };
        guard.armed = false;
        outcome
    }

    /// What an install added to `room_id` since `before`; with the read's
    /// error when the room could not be read, its room hooks alone then.
    async fn added_since(
        &self,
        room_id: &RoomId,
        before: &Snapshot,
    ) -> (Added, Option<RoomKitError>) {
        match self.snapshot(room_id).await {
            Ok(after) => (before.added(&after), None),
            Err(error) => {
                tracing::error!(
                    target: "roomkit::orchestration",
                    room_id = %room_id,
                    error = %error,
                    "what an install added could not be read whole"
                );
                let hooks = self.hook_engine().room_hook_names(room_id);
                let added = Added {
                    hooks: hooks
                        .into_iter()
                        .filter(|name| !before.hooks.contains(name))
                        .collect(),
                    ..Added::default()
                };
                (added, Some(error))
            }
        }
    }

    /// Takes back an install cut short: what it added since `before`, or
    /// nothing when it was cut before the room was read.
    async fn undo_cancelled(
        &self,
        room_id: &RoomId,
        strategy: &Arc<dyn Orchestration<S>>,
        before: Option<Snapshot>,
    ) {
        let Some(before) = before else {
            self.drop_record(room_id, strategy);
            return;
        };
        let (added, unread) = self.added_since(room_id, &before).await;
        self.undo_install(room_id, strategy, added, unread.is_none())
            .await;
    }

    /// Takes back the strategy recorded for `room_id` when the store has
    /// just created the room anew: the record outlived its room (deleted
    /// from the store), and what its install set up would serve the new
    /// room, whose bindings and metadata hold none of the old one's.
    pub(crate) async fn forget_strategy(&self, room_id: &RoomId) -> Result<(), RoomKitError> {
        let Some(strategy) = self.recorded_strategy(room_id) else {
            return Ok(());
        };
        let held = self
            .store()
            .get_room(room_id)
            .await
            .map_err(|error| self.read_failed(error))?;
        if held.is_some() {
            // The room lives: its creation is refused, its strategy stays.
            return Ok(());
        }
        strategy.uninstall(self, room_id).await?;
        let Added { hooks, .. } = self.recorded_added(room_id, &strategy);
        let added = Added {
            hooks,
            ..Added::default()
        };
        self.take_back(room_id, added, &strategy).await?;
        self.drop_record(room_id, &strategy);
        Ok(())
    }

    /// Claims `room_id` for an install of `strategy`, before any await: two
    /// installs at once are one too many.
    fn claim_install(
        &self,
        room_id: &RoomId,
        strategy: &Arc<dyn Orchestration<S>>,
        organization_id: Option<String>,
    ) -> Result<(), RoomKitError> {
        let mut rooms = self.inner.room_strategies.rooms();
        check_claim(&rooms, room_id, strategy)?;
        if rooms.contains_key(room_id) {
            return Err(RoomKitError::Value(format!(
                "Room {room_id} holds this strategy already"
            )));
        }
        rooms.insert(
            room_id.clone(),
            Installed {
                strategy: Arc::clone(strategy),
                organization_id,
                added: Added::default(),
                installing: true,
            },
        );
        Ok(())
    }

    /// Registers the strategy's agents given live that the kit does not
    /// hold, all or none, and attaches each agent the room does not bind as
    /// an intelligence channel.
    async fn attach_agents(
        &self,
        room_id: &RoomId,
        strategy: &dyn Orchestration<S>,
    ) -> Result<(), RoomKitError> {
        let agents = strategy.agents();
        self.register_strategy_agents(&agents)?;
        let bound = self.bound(room_id).await?;
        for agent in &agents {
            if bound.contains(agent.channel_id()) {
                continue;
            }
            let options = AttachChannelOptions {
                category: Some(ChannelCategory::Intelligence),
                ..AttachChannelOptions::default()
            };
            self.attach_channel(room_id, agent.channel_id(), options)
                .await?;
        }
        Ok(())
    }

    /// Registers each of `agents` given live that the kit does not hold
    /// yet: all of them, or none when an agent named by its id, or given
    /// live under the id of a channel the kit holds, is no intelligence
    /// channel the kit holds.
    fn register_strategy_agents(&self, agents: &[StrategyAgent]) -> Result<(), RoomKitError> {
        for agent in agents {
            let held = self.inner.channel(agent.channel_id());
            // A live agent the kit holds no channel for is registered below;
            // one under a held id is the kit's channel, an agent only when it
            // is an intelligence channel (decided 2026-11-02, HITL 91d5d8e9).
            if agent.live().is_some() && held.is_none() {
                continue;
            }
            let declaration = held.map(|channel| channel.declaration()).ok_or_else(|| {
                RoomKitError::ChannelNotRegistered {
                    channel_id: agent.channel_id().clone(),
                    message: format!(
                        "Channel '{}' is not registered: a strategy names by its id an agent \
                         the kit holds, live or by its declaration",
                        agent.channel_id()
                    ),
                }
            })?;
            if declaration.category != ChannelCategory::Intelligence {
                return Err(RoomKitError::Value(format!(
                    "Channel '{}' is not an intelligence channel",
                    agent.channel_id()
                )));
            }
        }
        for agent in agents {
            let Some(live) = agent.live() else { continue };
            if self.inner.channel(agent.channel_id()).is_some() {
                continue;
            }
            self.register_dyn(
                Arc::clone(&live.channel),
                live.config.clone(),
                live.human_input.clone(),
            )?;
        }
        Ok(())
    }

    /// What `room_id` holds now that an install may add to.
    async fn snapshot(&self, room_id: &RoomId) -> Result<Snapshot, RoomKitError> {
        let room = self
            .store()
            .get_room(room_id)
            .await
            .map_err(|error| self.read_failed(error))?;
        Ok(Snapshot {
            hooks: self
                .hook_engine()
                .room_hook_names(room_id)
                .into_iter()
                .collect(),
            bindings: self.bound(room_id).await?,
            keys: room
                .map(|room| room.metadata.keys().cloned().collect())
                .unwrap_or_default(),
        })
    }

    /// The channels `room_id` binds.
    async fn bound(&self, room_id: &RoomId) -> Result<BTreeSet<ChannelId>, RoomKitError> {
        let bindings = self
            .store()
            .list_bindings(room_id)
            .await
            .map_err(|error| self.read_failed(error))?;
        Ok(bindings
            .into_iter()
            .map(|binding| binding.channel_id)
            .collect())
    }

    /// Takes back what a failed install added, `added`, read whole when
    /// `complete`; the room stays claimed for what could not be, so
    /// `uninstall_strategy` can finish it. The record holds `added` first,
    /// and loses what is taken back.
    async fn undo_install(
        &self,
        room_id: &RoomId,
        strategy: &Arc<dyn Orchestration<S>>,
        added: Added,
        complete: bool,
    ) {
        self.update_record(room_id, strategy, |record| *record = added);
        let added = self.recorded_added(room_id, strategy);
        match self.take_back(room_id, added, strategy).await {
            Ok(stayed) if stayed.is_empty() && complete => self.drop_record(room_id, strategy),
            outcome => {
                let error = outcome.err().map(|error| error.to_string());
                tracing::error!(
                    target: "roomkit::orchestration",
                    room_id = %room_id,
                    error = ?error,
                    "a failed install was not taken back whole: the room stays its strategy's"
                );
                self.settle(room_id, strategy);
            }
        }
    }

    /// Removes what `added` holds for `room_id`: everything it can, the
    /// agents that stayed bound returned. What was taken back is dropped
    /// from the room's record, so a retry finishes the rest.
    async fn take_back(
        &self,
        room_id: &RoomId,
        added: Added,
        strategy: &Arc<dyn Orchestration<S>>,
    ) -> Result<Vec<ChannelId>, RoomKitError> {
        let engine = self.hook_engine();
        for name in &added.hooks {
            engine.remove_room_hook(room_id, name);
        }
        self.update_record(room_id, strategy, |record| record.hooks.clear());
        let stayed = self.detach_agents(room_id, &added.attached).await;
        self.update_record(room_id, strategy, |record| {
            record.attached.clone_from(&stayed);
        });
        if !added.keys.is_empty() {
            let patch: Metadata = added
                .keys
                .iter()
                .map(|key| (key.clone(), serde_json::Value::Null))
                .collect();
            self.store()
                .patch_room_metadata(room_id, patch, PatchRoomMetadataOptions::default())
                .await
                .map_err(|error: StoreError| self.read_failed(error))?;
            self.update_record(room_id, strategy, |record| record.keys.clear());
        }
        Ok(stayed)
    }

    /// Detaches each of `channel_ids` from `room_id`, whatever one of them
    /// fails with; the ones still bound.
    async fn detach_agents(&self, room_id: &RoomId, channel_ids: &[ChannelId]) -> Vec<ChannelId> {
        let mut stayed = Vec::new();
        for channel_id in channel_ids {
            let detached = self
                .detach_channel(room_id, channel_id, DetachChannelOptions::default())
                .await;
            if let Err(error) = detached {
                tracing::error!(
                    target: "roomkit::orchestration",
                    room_id = %room_id,
                    channel_id = %channel_id,
                    error = %error,
                    "a strategy's agent was not detached"
                );
                stayed.push(channel_id.clone());
            }
        }
        stayed
    }

    /// The strategy recorded for `room_id`, once its install is over.
    fn recorded_strategy(&self, room_id: &RoomId) -> Option<Arc<dyn Orchestration<S>>> {
        let rooms = self.inner.room_strategies.rooms();
        rooms
            .get(room_id)
            .filter(|installed| !installed.installing)
            .map(|installed| Arc::clone(&installed.strategy))
    }

    /// What the record of `strategy` in `room_id` says its install added.
    fn recorded_added(&self, room_id: &RoomId, strategy: &Arc<dyn Orchestration<S>>) -> Added {
        let rooms = self.inner.room_strategies.rooms();
        rooms
            .get(room_id)
            .filter(|installed| Arc::ptr_eq(&installed.strategy, strategy))
            .map(|installed| installed.added.clone())
            .unwrap_or_default()
    }

    /// Records what the install of `strategy` added to `room_id`: the room
    /// is its from now on.
    fn record(&self, room_id: &RoomId, strategy: &Arc<dyn Orchestration<S>>, added: Added) {
        let mut rooms = self.inner.room_strategies.rooms();
        if let Some(installed) = rooms
            .get_mut(room_id)
            .filter(|installed| Arc::ptr_eq(&installed.strategy, strategy))
        {
            installed.added = added;
            installed.installing = false;
        }
    }

    /// Ends the install of the record of `strategy` in `room_id`, keeping
    /// what it holds: a failed install's, for what could not be taken back.
    fn settle(&self, room_id: &RoomId, strategy: &Arc<dyn Orchestration<S>>) {
        let mut rooms = self.inner.room_strategies.rooms();
        if let Some(installed) = rooms
            .get_mut(room_id)
            .filter(|installed| Arc::ptr_eq(&installed.strategy, strategy))
        {
            installed.installing = false;
        }
    }

    fn update_record(
        &self,
        room_id: &RoomId,
        strategy: &Arc<dyn Orchestration<S>>,
        change: impl FnOnce(&mut Added),
    ) {
        let mut rooms = self.inner.room_strategies.rooms();
        if let Some(installed) = rooms
            .get_mut(room_id)
            .filter(|installed| Arc::ptr_eq(&installed.strategy, strategy))
        {
            change(&mut installed.added);
        }
    }

    /// Forgets the record of `strategy` in `room_id`.
    fn drop_record(&self, room_id: &RoomId, strategy: &Arc<dyn Orchestration<S>>) {
        let mut rooms = self.inner.room_strategies.rooms();
        if rooms
            .get(room_id)
            .is_some_and(|installed| Arc::ptr_eq(&installed.strategy, strategy))
        {
            rooms.remove(room_id);
        }
    }
}

/// An install under way: dropped before its end, it takes the install
/// back in a task of its own, as a failed install is.
struct InstallGuard<S: ConversationStore> {
    kit: RoomKit<S>,
    room_id: RoomId,
    strategy: Arc<dyn Orchestration<S>>,
    /// The room before the install, once read.
    before: Option<Snapshot>,
    armed: bool,
}

impl<S: ConversationStore> Drop for InstallGuard<S> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (kit, room_id) = (self.kit.clone(), self.room_id.clone());
        let (strategy, before) = (Arc::clone(&self.strategy), self.before.take());
        let tasks = Arc::clone(&kit.inner.tasks);
        let undo: crate::channel::BoxFuture<'static, ()> = Box::pin(async move {
            kit.undo_cancelled(&room_id, &strategy, before).await;
        });
        // The kit's close waits for the undo; once it spawns no more work,
        // the undo runs apart all the same, as a held task's end does.
        let Err(refused) = crate::tasks::spawn_apart(&tasks, undo) else {
            return;
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn(refused));
        }
    }
}

/// Refuses `strategy` a room holding another strategy (RFC §19.7).
fn check_claim<S>(
    rooms: &HashMap<RoomId, Installed<S>>,
    room_id: &RoomId,
    strategy: &Arc<dyn Orchestration<S>>,
) -> Result<(), RoomKitError> {
    match rooms.get(room_id) {
        Some(installed) if !Arc::ptr_eq(&installed.strategy, strategy) => Err(RoomKitError::Value(
            format!("Room {room_id} holds a strategy already: uninstall_strategy() first"),
        )),
        _ => Ok(()),
    }
}

fn still_binds(room_id: &RoomId, stayed: &[ChannelId]) -> RoomKitError {
    let stayed: Vec<&str> = stayed.iter().map(ChannelId::as_str).collect();
    RoomKitError::Runtime(format!(
        "Room {room_id} still binds {stayed:?} its strategy attached"
    ))
}
