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

use std::any::{Any, TypeId};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::pin::pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit_core::model::{ChannelCategory, ChannelId, Metadata, RoomId};
use tokio::sync::Notify;

use super::RoomKit;
use crate::context::{ExecutionContext, Section};
use crate::error::RoomKitError;
use crate::kit::{AttachChannelOptions, DetachChannelOptions, GetRoomOptions};
use crate::orchestration::{Discussion, Orchestration, StrategyAgent};
use crate::store::{ConversationStore, PatchRoomMetadataOptions, StoreError};
use roomkit_core::rules::discussion::config::DiscussionConfig;

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

/// What [`RoomKit::room_strategy_is_current`] takes beside the room and the
/// kind: Python's `room_strategy_is_current(room_id, kind, *,
/// organization_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RoomStrategyIsCurrentOptions {
    /// Python `organization_id=None`: scopes the call to one tenant (RFC
    /// §17.2): a room of another organization is not found.
    pub organization_id: Option<String>,
}

/// The strategies installed through the kit, by room.
pub(crate) struct RoomStrategies<S> {
    rooms: Mutex<HashMap<RoomId, Installed<S>>>,
    /// Told whenever an install ends, whatever its end: what an uninstall
    /// called meanwhile waits for.
    installs_ended: Notify,
}

impl<S> Default for RoomStrategies<S> {
    fn default() -> Self {
        Self {
            rooms: Mutex::new(HashMap::new()),
            installs_ended: Notify::new(),
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
    /// Running installs are waited for; failed initial reads must be uninstalled.
    state: InstallState,
    /// The install takes the place of the discussion the room holds (RFC
    /// §19.7.5 rule 1), and this is the record of that discussion's
    /// strategy, when this process recorded one.
    replacing: Option<Replaced<S>>,
}

/// An install's progress is separate from its right to be replaced.
#[derive(Clone, Copy, PartialEq, Eq)]
enum InstallState {
    Running,
    /// The initial snapshot failed: uninstall can run, but replacement cannot.
    AwaitingUninstall,
    Ready,
}

/// The record of a discussion's strategy another install takes the place
/// of, which the room's record joins once it has.
struct Replaced<S> {
    strategy: Option<Arc<dyn Orchestration<S>>>,
    organization_id: Option<String>,
    added: Added,
}

/// What an install added for a room: what an uninstall takes back.
#[derive(Clone, Debug, Default)]
struct Added {
    hooks: Vec<String>,
    /// The tools set up for the room alone, by channel and name.
    tools: Vec<(ChannelId, String)>,
    runners: Vec<ChannelId>,
    attached: Vec<ChannelId>,
    keys: Vec<String>,
}

/// What a room holds that a strategy's install may add to.
#[derive(Clone)]
struct Snapshot {
    hooks: BTreeSet<String>,
    tools: BTreeMap<ChannelId, BTreeSet<String>>,
    runners: BTreeSet<ChannelId>,
    bindings: BTreeSet<ChannelId>,
    keys: BTreeSet<String>,
}

impl Added {
    /// What both installs set up, this one's taking the place of `held`'s,
    /// but agents the room no longer binds: Python's `_joined`.
    fn joined(held: &Self, added: Self, bound: &BTreeSet<ChannelId>) -> Self {
        let union = |a: &[String], b: Vec<String>| -> Vec<String> {
            a.iter()
                .cloned()
                .chain(b)
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        };
        let mut tools: BTreeSet<(ChannelId, String)> = held.tools.iter().cloned().collect();
        tools.extend(added.tools);
        let mut runners: BTreeSet<ChannelId> = held.runners.iter().cloned().collect();
        runners.extend(added.runners);
        let attached: BTreeSet<ChannelId> = held
            .attached
            .iter()
            .cloned()
            .chain(added.attached)
            .filter(|agent| bound.contains(agent))
            .collect();
        Self {
            hooks: union(&held.hooks, added.hooks),
            tools: tools.into_iter().collect(),
            runners: runners.into_iter().collect(),
            attached: attached.into_iter().collect(),
            keys: union(&held.keys, added.keys),
        }
    }
}

impl Snapshot {
    /// What `after` holds that this snapshot does not.
    fn added(&self, after: &Self) -> Added {
        Added {
            hooks: after.hooks.difference(&self.hooks).cloned().collect(),
            tools: added_tools(&self.tools, &after.tools),
            runners: after.runners.difference(&self.runners).cloned().collect(),
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
        // A discussion another process stored is this room's here too (RFC
        // §19.7.5 rule 16).
        self.follow_discussion(&room);
        // Boxed: the install, under its section, makes a large future.
        Box::pin(self.install_on(room_id, strategy, room.organization_id)).await
    }

    /// Takes the room's strategy back out (RFC §19.7): its own uninstall
    /// step, then what its install added for the room. Python's
    /// `uninstall_strategy(room_id, *, organization_id=None)`: whether the
    /// room held a strategy. The room answers by its
    /// `agent_response_policy` again and may take another strategy. An
    /// install under way is waited for, then taken back. A discussion is
    /// uninstalled from any process serving the room, one where no host
    /// installed it included, for every process, the agents its installs
    /// attached detached (RFC §19.7.5 rule 16).
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
        let room = self
            .get_room(
                room_id,
                GetRoomOptions {
                    organization_id: options.organization_id,
                },
            )
            .await?;
        self.install_over(room_id).await;
        let Some(strategy) = self.recorded_strategy(room_id) else {
            // A discussion is uninstalled from any process serving the room,
            // one where no host installed it included (RFC §19.7.5 rule 16).
            return self.uninstall_stored_discussion(&room).await;
        };
        strategy.uninstall(self, room_id).await?;
        // Released only once all of it is taken back: a room left with part
        // of a strategy is still that strategy's, never open to a second.
        let mut added = self.recorded_added(room_id, &strategy);
        // A discussion's agents another process's install attached too.
        if let Some(stored) = DiscussionConfig::stored(&room.metadata) {
            added.attached.extend(stored.attached);
            added.attached.sort();
            added.attached.dedup();
        }
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
    /// [`RoomKitError::Value`] when the room holds another strategy, or a
    /// discussion, another process's included, that `strategy` does not
    /// take the place of. The room is read first, with no organization
    /// named too (RFC §19.7.5 rule 16).
    pub async fn claim_room_strategy(
        &self,
        room_id: &RoomId,
        strategy: Arc<dyn Orchestration<S>>,
        options: ClaimRoomStrategyOptions,
    ) -> Result<(), RoomKitError> {
        // The room is read first, as an install reads it: a discussion
        // another process stored is this room's here too (RFC §19.7.5 rule
        // 16).
        let room = if options.organization_id.is_some() {
            let options = GetRoomOptions {
                organization_id: options.organization_id,
            };
            Some(self.get_room(room_id, options).await?)
        } else {
            self.store()
                .get_room(room_id)
                .await
                .map_err(|error: StoreError| self.read_failed(error))?
        };
        if let Some(room) = &room {
            self.follow_discussion(room);
        }
        let rooms = self.inner.room_strategies.rooms();
        check_claim(&rooms, room_id, &strategy)?;
        self.check_discussion_claim(&rooms, room_id, strategy.as_ref())
    }

    /// Refuses `strategy` a room holding a discussion it does not take the
    /// place of, with no install of this process recorded (RFC §19.7.5
    /// rule 1): Python's `_check_claim`, its second refusal.
    fn check_discussion_claim(
        &self,
        rooms: &HashMap<RoomId, Installed<S>>,
        room_id: &RoomId,
        strategy: &dyn Orchestration<S>,
    ) -> Result<(), RoomKitError> {
        if rooms.contains_key(room_id)
            || self.inner.discussions.get(room_id).is_none()
            || is_discussion(strategy)
        {
            return Ok(());
        }
        Err(RoomKitError::Value(format!(
            "Room {room_id} holds a discussion: no other strategy can join it"
        )))
    }

    /// Whether the discussion this process installed in `room_id` is the
    /// one the room stores now (RFC §19.7.5 rule 16): Python's
    /// `room_strategy_is_current(room_id, kind, *, organization_id=None)`,
    /// `kind` the [`TypeId`] of a strategy stored with the room, a
    /// [`Discussion`]. False when this process installed none, when
    /// another process's install replaced it with another configuration,
    /// and once it was uninstalled: an uninstall another process made is
    /// taken here at once, so [`room_strategy`](Self::room_strategy)
    /// answers `None` from then on. The room's stored state is read when
    /// called. A host that finds it false installs again only while its own
    /// state still wants the discussion, else it would bring back one
    /// another process took out.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Type`], Python's `TypeError`, when `kind` is not a
    /// strategy stored with the room; [`RoomKitError::RoomNotFound`] for a
    /// room the call does not reach (RFC §17.2).
    pub async fn room_strategy_is_current(
        &self,
        room_id: &RoomId,
        kind: TypeId,
        options: RoomStrategyIsCurrentOptions,
    ) -> Result<bool, RoomKitError> {
        if kind != TypeId::of::<Discussion>() {
            return Err(RoomKitError::Type(
                "room_strategy_is_current() takes a strategy class stored with the room \
                 (Discussion), not another type: it is not a strategy stored with the room"
                    .to_owned(),
            ));
        }
        let RoomStrategyIsCurrentOptions { organization_id } = options;
        self.discussion_is_current(room_id, organization_id).await
    }

    /// Whether an install of a strategy runs in `room_id` now.
    pub(super) fn installing_strategy(&self, room_id: &RoomId) -> bool {
        self.inner
            .room_strategies
            .rooms()
            .get(room_id)
            .is_some_and(|installed| installed.state == InstallState::Running)
    }

    /// Drops the record of `room_id`'s strategy when it is the discussion
    /// `held` runs, which another process uninstalled (RFC §19.7.5 rule
    /// 16): an install of another strategy claiming the room meanwhile
    /// keeps its claim. Python's `_drop_record(room_id,
    /// discussion.strategy)`.
    pub(super) fn drop_discussion_record(
        &self,
        room_id: &RoomId,
        held: &Arc<dyn super::discussions::DiscussionSeam>,
    ) {
        let mut rooms = self.inner.room_strategies.rooms();
        if rooms
            .get(room_id)
            .is_some_and(|installed| held.runs(strategy_address(installed.strategy.as_ref())))
        {
            rooms.remove(room_id);
        }
    }

    /// Installs `strategy` in `room_id`, of `organization_id`, the room
    /// read: claimed before any await, then, under the room's lock (RFC
    /// §13.5), its agents attached, its install run and what it added
    /// recorded; a failed install is taken back. What changes in the room
    /// meanwhile is the install's: any other caller waits for its end.
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
            section: None,
            armed: true,
        };
        let section = match Section::enter(&self.inner.locks, room_id).await {
            Ok(section) => Arc::new(section),
            Err(error) => {
                guard.armed = false;
                self.drop_claim(room_id, &strategy);
                return Err(error);
            }
        };
        guard.section = Some(Arc::clone(&section));
        let before = match section.scope(self.snapshot(room_id)).await {
            Ok(before) => before,
            Err(error) => {
                guard.armed = false;
                // Python ends the install but keeps its claim when the
                // initial snapshot fails. An explicit uninstall releases
                // it, and callers waiting for the install may now proceed.
                self.settle(room_id, &strategy, InstallState::AwaitingUninstall);
                return Err(error);
            }
        };
        guard.before = Some(before.clone());
        let outcome = section
            .scope(self.install_steps(room_id, &strategy, &before))
            .await;
        guard.armed = false;
        outcome
    }

    /// Attaches the strategy's agents, runs its install and records what
    /// it added to `room_id` since `before`, or takes it back.
    async fn install_steps(
        &self,
        room_id: &RoomId,
        strategy: &Arc<dyn Orchestration<S>>,
        before: &Snapshot,
    ) -> Result<(), RoomKitError> {
        // A discussion taking another's place binds its new agents itself,
        // once it holds the room (RFC §19.7.5 rule 1).
        let replacing = self.replacing(room_id, strategy);
        let attached = if replacing {
            Ok(())
        } else {
            self.attach_agents(room_id, strategy.as_ref()).await
        };
        let installed = match attached {
            Ok(()) => strategy.install(self, room_id).await,
            Err(error) => Err(error),
        };
        let (added, bound, unread) = self.added_and_bound_since(room_id, before).await;
        match (installed, unread) {
            (Err(error), unread) => {
                self.undo_install(room_id, strategy, added, bound, unread.is_none())
                    .await;
                Err(error)
            }
            (Ok(()), Some(error)) => {
                // What the read missed stays the strategy's: the room stays
                // claimed, for uninstall_strategy to take back.
                let added = self.joined_record(room_id, strategy, added, bound.as_ref());
                self.record(room_id, strategy, added);
                Err(error)
            }
            (Ok(()), None) => {
                let attached = added.attached.clone();
                let added = self.joined_record(room_id, strategy, added, bound.as_ref());
                self.record(room_id, strategy, added);
                self.hold_attached(room_id, strategy.as_ref(), attached)
                    .await
            }
        }
    }

    /// Whether the install of `strategy` claiming `room_id` takes the place
    /// of the discussion the room holds (RFC §19.7.5 rule 1).
    fn replacing(&self, room_id: &RoomId, strategy: &Arc<dyn Orchestration<S>>) -> bool {
        self.inner
            .room_strategies
            .rooms()
            .get(room_id)
            .is_some_and(|installed| {
                Arc::ptr_eq(&installed.strategy, strategy) && installed.replacing.is_some()
            })
    }

    /// What the record of `strategy` in `room_id` holds once its install
    /// added `added`: joined with the record of the discussion's strategy it
    /// took the place of, but the agents the room no longer binds, `bound`.
    fn joined_record(
        &self,
        room_id: &RoomId,
        strategy: &Arc<dyn Orchestration<S>>,
        added: Added,
        bound: Option<&BTreeSet<ChannelId>>,
    ) -> Added {
        let rooms = self.inner.room_strategies.rooms();
        let held = rooms
            .get(room_id)
            .filter(|installed| Arc::ptr_eq(&installed.strategy, strategy))
            .and_then(|installed| installed.replacing.as_ref())
            .filter(|replaced| replaced.strategy.is_some());
        match (held, bound) {
            (Some(held), Some(bound)) => Added::joined(&held.added, added, bound),
            _ => added,
        }
    }

    /// Stores `attached`, the agents an install of the discussion
    /// `strategy` attached to `room_id`, with the configuration the room
    /// stores, for any process uninstalling it to detach them (RFC §19.7):
    /// Python's `_hold_attached`.
    async fn hold_attached(
        &self,
        room_id: &RoomId,
        strategy: &dyn Orchestration<S>,
        attached: Vec<ChannelId>,
    ) -> Result<(), RoomKitError> {
        if attached.is_empty() || !is_discussion(strategy) {
            return Ok(());
        }
        match self.inner.discussions.get(room_id) {
            Some(seam) if seam.installed() => seam.hold_attached(attached).await,
            _ => Ok(()),
        }
    }

    /// Waits until no install runs in `room_id`: an uninstall called
    /// meanwhile takes back what the install recorded, rather than let it
    /// record itself again (decided 2026-11-02, RMK-1247). Code under a
    /// live lease on the room's lock waits for no install, which waits for
    /// that lock or runs under it (RFC §19.7: neither waits on work that
    /// needs it): nothing is installed yet.
    async fn install_over(&self, room_id: &RoomId) {
        if ExecutionContext::current().lease(room_id).is_some() {
            return;
        }
        let strategies = &self.inner.room_strategies;
        loop {
            let mut ended = pin!(strategies.installs_ended.notified());
            // Listening before looking: an install that ends between the two
            // still wakes this one.
            ended.as_mut().enable();
            let installing = strategies
                .rooms()
                .get(room_id)
                .is_some_and(|installed| installed.state == InstallState::Running);
            if !installing {
                return;
            }
            ended.await;
        }
    }

    /// What an install added to `room_id` since `before`, and the channels
    /// the room binds now; with the read's error when the room could not be
    /// read, its room hooks alone then, and no channels.
    async fn added_and_bound_since(
        &self,
        room_id: &RoomId,
        before: &Snapshot,
    ) -> (Added, Option<BTreeSet<ChannelId>>, Option<RoomKitError>) {
        match self.snapshot(room_id).await {
            Ok(after) => (before.added(&after), Some(after.bindings), None),
            Err(error) => {
                tracing::error!(
                    target: "roomkit::orchestration",
                    room_id = %room_id,
                    error = %error,
                    "what an install added could not be read whole"
                );
                let hooks = self.hook_engine().room_hook_names(room_id);
                let runners = self.inner.turn_runners.channels(room_id);
                let tools = self.inner.owner_tools.room_names(room_id);
                let added = Added {
                    hooks: hooks
                        .into_iter()
                        .filter(|name| !before.hooks.contains(name))
                        .collect(),
                    tools: added_tools(&before.tools, &tools),
                    runners: runners.difference(&before.runners).cloned().collect(),
                    ..Added::default()
                };
                (added, None, Some(error))
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
            self.drop_claim(room_id, strategy);
            return;
        };
        let (added, bound, unread) = self.added_and_bound_since(room_id, &before).await;
        self.undo_install(room_id, strategy, added, bound, unread.is_none())
            .await;
    }

    /// Takes back the strategy recorded for `room_id` once the store has
    /// just created the room anew: the record outlived its room (deleted
    /// from the store), and what its install set up would serve the new
    /// room, whose bindings and metadata hold none of the old one's.
    pub(crate) async fn forget_strategy(&self, room_id: &RoomId) -> Result<(), RoomKitError> {
        let Some(strategy) = self.recorded_strategy(room_id) else {
            return Ok(());
        };
        strategy.uninstall(self, room_id).await?;
        // The room's bindings and metadata are the new room's; what the
        // install set up in this process, its hooks, its tools and its turn
        // runners, is the old one's (Python's `_forget_strategy`).
        let Added {
            hooks,
            tools,
            runners,
            ..
        } = self.recorded_added(room_id, &strategy);
        let added = Added {
            hooks,
            tools,
            runners,
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
        // A discussion takes the place of the discussion the room holds here,
        // installed or followed (RFC §19.7.5 rule 1).
        let replacing =
            is_discussion(strategy.as_ref()) && self.inner.discussions.get(room_id).is_some();
        if !replacing {
            check_claim(&rooms, room_id, strategy)?;
            self.check_discussion_claim(&rooms, room_id, strategy.as_ref())?;
            if rooms.contains_key(room_id) {
                return Err(RoomKitError::Value(format!(
                    "Room {room_id} holds this strategy already"
                )));
            }
        } else if rooms
            .get(room_id)
            .is_some_and(|installed| installed.state != InstallState::Ready)
        {
            return Err(RoomKitError::Value(format!(
                "Room {room_id} holds a discussion another install is replacing"
            )));
        }
        let replacing = replacing.then(|| {
            let held = rooms.remove(room_id);
            Replaced {
                organization_id: held.as_ref().and_then(|held| held.organization_id.clone()),
                added: held
                    .as_ref()
                    .map(|held| held.added.clone())
                    .unwrap_or_default(),
                strategy: held.map(|held| held.strategy),
            }
        });
        rooms.insert(
            room_id.clone(),
            Installed {
                strategy: Arc::clone(strategy),
                organization_id,
                added: Added::default(),
                state: InstallState::Running,
                replacing,
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
    pub(crate) fn register_strategy_agents(
        &self,
        agents: &[StrategyAgent],
    ) -> Result<(), RoomKitError> {
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
            tools: self.inner.owner_tools.room_names(room_id),
            runners: self.inner.turn_runners.channels(room_id),
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
        bound: Option<BTreeSet<ChannelId>>,
        complete: bool,
    ) {
        if self.replacing(room_id, strategy) {
            self.settle_replacement(room_id, strategy, added, bound.as_ref());
            return;
        }
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
                self.settle(room_id, strategy, InstallState::Ready);
            }
        }
    }

    /// Drops the claim of an install of `strategy` that changed nothing in
    /// `room_id`: the record of the discussion's strategy it was to take the
    /// place of is the room's again.
    fn drop_claim(&self, room_id: &RoomId, strategy: &Arc<dyn Orchestration<S>>) {
        if self.replacing(room_id, strategy) {
            self.settle_replacement(room_id, strategy, Added::default(), None);
        } else {
            self.drop_record(room_id, strategy);
        }
    }

    /// Records what a failed replacement leaves `room_id` holding (RFC
    /// §19.7.5 rule 1): `strategy` once the discussion runs it, installing
    /// it again finishing the install; else the record of the strategy it
    /// was to replace, while the discussion still runs that one; else none.
    /// Python's `_settle_replacement`.
    fn settle_replacement(
        &self,
        room_id: &RoomId,
        strategy: &Arc<dyn Orchestration<S>>,
        added: Added,
        bound: Option<&BTreeSet<ChannelId>>,
    ) {
        let seam = self.inner.discussions.get(room_id);
        let runs = |strategy: &Arc<dyn Orchestration<S>>| {
            seam.as_ref()
                .is_some_and(|seam| seam.runs(strategy_address(strategy.as_ref())))
        };
        if runs(strategy) {
            let added = self.joined_record(room_id, strategy, added, bound);
            self.record(room_id, strategy, added);
            return;
        }
        let mut rooms = self.inner.room_strategies.rooms();
        let Some(installed) = rooms
            .get_mut(room_id)
            .filter(|installed| Arc::ptr_eq(&installed.strategy, strategy))
        else {
            return;
        };
        match installed.replacing.take() {
            Some(Replaced {
                strategy: Some(held),
                organization_id,
                added,
            }) if runs(&held) => {
                *installed = Installed {
                    strategy: held,
                    organization_id,
                    added,
                    state: InstallState::Ready,
                    replacing: None,
                };
            }
            _ => {
                rooms.remove(room_id);
            }
        }
        drop(rooms);
        self.inner.room_strategies.installs_ended.notify_waiters();
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
        for (channel_id, name) in &added.tools {
            self.inner
                .owner_tools
                .unregister_in_room(channel_id, room_id, name);
        }
        for channel_id in &added.runners {
            self.inner.turn_runners.release(room_id, channel_id);
        }
        self.update_record(room_id, strategy, |record| {
            record.hooks.clear();
            record.tools.clear();
            record.runners.clear();
        });
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
    pub(super) async fn detach_agents(
        &self,
        room_id: &RoomId,
        channel_ids: &[ChannelId],
    ) -> Vec<ChannelId> {
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
            .filter(|installed| installed.state != InstallState::Running)
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
            installed.state = InstallState::Ready;
            installed.replacing = None;
        }
        drop(rooms);
        self.inner.room_strategies.installs_ended.notify_waiters();
    }

    /// Ends the install of the record of `strategy` in `room_id`, keeping
    /// what it holds: a failed install's, for what could not be taken back.
    fn settle(&self, room_id: &RoomId, strategy: &Arc<dyn Orchestration<S>>, state: InstallState) {
        let mut rooms = self.inner.room_strategies.rooms();
        if let Some(installed) = rooms
            .get_mut(room_id)
            .filter(|installed| Arc::ptr_eq(&installed.strategy, strategy))
        {
            installed.state = state;
        }
        drop(rooms);
        self.inner.room_strategies.installs_ended.notify_waiters();
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
        drop(rooms);
        self.inner.room_strategies.installs_ended.notify_waiters();
    }

    /// Claims `room_id` for the strategy at `owner` from its own install,
    /// or refuses: Python's `claim_room_strategy(room_id, self)` a built-in
    /// strategy's install calls first, which an install the kit runs passes
    /// and a direct one claims nothing by (RFC §19.7).
    pub(crate) fn claim_for(&self, room_id: &RoomId, owner: usize) -> Result<(), RoomKitError> {
        check_claim_of(&self.inner.room_strategies.rooms(), room_id, owner)
    }

    /// Whether `room_id` holds a strategy recorded through this kit, its
    /// install over or not.
    pub(crate) fn holds_strategy_record(&self, room_id: &RoomId) -> bool {
        self.inner.room_strategies.rooms().contains_key(room_id)
    }
}

/// An install under way: dropped before its end, it takes the install
/// back in a task of its own, as a failed install is, the room's lock
/// still held.
struct InstallGuard<S: ConversationStore> {
    kit: RoomKit<S>,
    room_id: RoomId,
    strategy: Arc<dyn Orchestration<S>>,
    /// The room before the install, once read.
    before: Option<Snapshot>,
    /// The room's lock the install holds, once taken.
    section: Option<Arc<Section>>,
    armed: bool,
}

impl<S: ConversationStore> Drop for InstallGuard<S> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (kit, room_id) = (self.kit.clone(), self.room_id.clone());
        let (strategy, before) = (Arc::clone(&self.strategy), self.before.take());
        let section = self.section.take();
        let tasks = Arc::clone(&kit.inner.tasks);
        let undo: crate::channel::BoxFuture<'static, ()> = Box::pin(async move {
            let Some(section) = section else {
                kit.undo_cancelled(&room_id, &strategy, before).await;
                return;
            };
            // The writes the cut calls had decided run to their end: the
            // room is read once they landed, the lock held meanwhile.
            kit.inner.tasks.calls_over_within(section.unit()).await;
            section
                .scope(kit.undo_cancelled(&room_id, &strategy, before))
                .await;
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
    check_claim_of(rooms, room_id, strategy_address(strategy.as_ref()))
}

/// Refuses the strategy at `owner` a room holding another strategy.
fn check_claim_of<S>(
    rooms: &HashMap<RoomId, Installed<S>>,
    room_id: &RoomId,
    owner: usize,
) -> Result<(), RoomKitError> {
    match rooms.get(room_id) {
        Some(installed) if strategy_address(installed.strategy.as_ref()) != owner => {
            Err(RoomKitError::Value(format!(
                "Room {room_id} holds a strategy already: uninstall_strategy() first"
            )))
        }
        _ => Ok(()),
    }
}

/// The address of `strategy`'s object: what tells one strategy from
/// another, as Python's `is`.
/// The tools `after` holds for a room that `before` did not, by channel
/// and name.
fn added_tools(
    before: &BTreeMap<ChannelId, BTreeSet<String>>,
    after: &BTreeMap<ChannelId, BTreeSet<String>>,
) -> Vec<(ChannelId, String)> {
    after
        .iter()
        .flat_map(|(channel_id, names)| {
            let held = before.get(channel_id);
            names
                .iter()
                .filter(move |name| !held.is_some_and(|held| held.contains(*name)))
                .map(move |name| (channel_id.clone(), name.clone()))
        })
        .collect()
}

/// Whether `strategy` is a [`Discussion`], a strategy stored with the room
/// (RFC §19.7.5 rule 16): Python's `isinstance(strategy, Discussion)`.
pub(crate) fn is_discussion<S>(strategy: &dyn Orchestration<S>) -> bool {
    let strategy: &dyn Any = strategy;
    strategy.is::<Discussion>()
}

pub(crate) fn strategy_address<S>(strategy: &dyn Orchestration<S>) -> usize {
    std::ptr::from_ref(strategy).cast::<()>() as usize
}

fn still_binds(room_id: &RoomId, stayed: &[ChannelId]) -> RoomKitError {
    let stayed: Vec<&str> = stayed.iter().map(ChannelId::as_str).collect();
    RoomKitError::Runtime(format!(
        "Room {room_id} still binds {stayed:?} its strategy attached"
    ))
}
