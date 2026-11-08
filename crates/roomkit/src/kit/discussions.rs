//! The seam between the pipeline and the discussion a room holds (RFC
//! §19.7.5): what the core asks under the room's lock, carried out by the
//! discussion this process runs for the room, and a turn's call.
//!
//! A room is held when its metadata stores a discussion, whichever process
//! installed it (rule 16). The core reads that itself; here is only what it
//! cannot do, the discussion's own queue. A room held with no discussion
//! running here is answered as told, with no turn to give.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use roomkit_core::model::{
    ChannelCategory, ChannelId, ChannelType, Room, RoomContext, RoomEvent, RoomId,
};
use roomkit_core::ops::{Inbound, NextTurn};
use roomkit_core::rules::discussion::config::DiscussionConfig;
use roomkit_core::rules::discussion::names::SilentToken;

use super::RoomKit;
use crate::InboundResult;
use crate::channel::BoxFuture;
use crate::delivery_keys::NamedGuard;
use crate::driver;
use crate::error::RoomKitError;
use crate::orchestration::SpeakQueue;
use crate::orchestration::strategies::discussion::room::{
    DiscussionRoom, Install, NAMES, SILENT, binding_refusal,
};
use crate::store::ConversationStore;

/// What a discussion running in this process does when the pipeline tells
/// it, always under the room's lock: it edits its stored queue without taking
/// that lock again. An error on a committed message is logged, and never
/// leaves it undelivered; on an instruction or a turn it fails the call.
pub(crate) trait DiscussionSeam: Send + Sync + 'static {
    /// A message was committed and took its place in the room's lane: queue
    /// whom it asks for, within `reach` (rule 8).
    fn on_committed(
        &self,
        event: RoomEvent,
        reach: Vec<ChannelId>,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<(), RoomKitError>>;

    /// An instruction addressed to its agents: a turn of its own each (rule
    /// 7).
    fn queue_instruction(&self, event: RoomEvent) -> BoxFuture<'_, Result<(), RoomKitError>>;

    /// The turn to give now, if any, its queue edited.
    fn next_turn(&self, context: RoomContext) -> BoxFuture<'_, Result<NextTurn, RoomKitError>>;

    /// The speak queue as last read, in `organization_id`'s scope (rule 17).
    fn view(&self, _organization_id: Option<&str>) -> Option<SpeakQueue> {
        None
    }

    /// The silence token a stream of `channel_id` is held back on, when it
    /// is one of its agents (rule 13).
    fn silence_for(&self, _channel_id: &ChannelId) -> Option<SilentToken> {
        None
    }

    /// Whether the room is `organization_id`'s, any caller's when `None`
    /// (RFC §17.2).
    fn in_scope(&self, organization_id: Option<&str>) -> bool {
        organization_id.is_none()
    }

    /// Has `channel_ids`, those of its agents, only listen (rule 12).
    fn listen_only(&self, _channel_ids: Vec<ChannelId>) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    /// Ends the listening state of `channel_ids` (rule 12).
    fn talk_again(&self, _channel_ids: Vec<ChannelId>) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    /// Whether it has no turn to give for now, and no wake to read.
    fn settled(&self) -> bool {
        true
    }

    /// Has its driver look at the stored queue now, which another process
    /// may have changed (rule 16).
    fn rouse(&self) {}

    /// Whether this process may give the turns of the discussion the room
    /// stores, `stored`: its host installed that one (rule 16).
    fn drives(&self, _stored: &DiscussionConfig) -> bool {
        false
    }

    /// Whether the host installed a discussion in this process: one that
    /// only follows the discussion another process installed did not (rule
    /// 16).
    fn installed(&self) -> bool {
        true
    }

    /// Whether the discussion the host installed here is the strategy at
    /// `owner`, an address (Python's `strategy is`).
    fn runs(&self, _owner: usize) -> bool {
        false
    }

    /// Whether this process read it uninstalled, by another process: it
    /// gives no turn again (rule 16).
    fn forgotten(&self) -> bool {
        false
    }

    /// Routes and queues by `stored`, the configuration the room stores
    /// now, which another process's install may have replaced (rule 16).
    fn follow(&self, _stored: DiscussionConfig) {}

    /// Takes the place of the discussion it routes by with `install`, the
    /// queue kept but for the agents `install` lets go (rule 1).
    fn take_over(&self, _install: Install) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    /// Gives the turns from this process: its driver started once, a room it
    /// only followed until now included.
    fn drive(&self) {}

    /// Stores `agents`, which an install attached to the room, with the
    /// stored configuration (RFC §19.7).
    fn hold_attached(&self, _agents: Vec<ChannelId>) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    /// Leaves the discussion another process uninstalled, writing nothing
    /// back (rule 16).
    fn leave(&self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }

    /// Stops taking part: its turns stopped, its queue left stored (rule 16).
    fn stop(&self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::ready(()))
    }

    /// Leaves the discussion, for every process: its queue and configuration
    /// forgotten (rule 1).
    fn forget(&self) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    /// Why the room cannot bind `channel_id`, if it cannot (rule 1).
    fn binding_refusal(
        &self,
        _channel_id: &ChannelId,
        _category: ChannelCategory,
        _channel_type: &ChannelType,
    ) -> Option<String> {
        None
    }
}

/// What [`RoomKit::speak_queue`] takes: Python's `speak_queue(room_id, *,
/// organization_id=None)`.
#[derive(Clone, Debug, Default)]
pub struct SpeakQueueOptions {
    /// Scopes the read to one tenant (RFC §17.2): a room of another reads as
    /// holding none.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::listen_only`] takes beside the room and the agents:
/// Python's `listen_only(room_id, channel_ids, *, organization_id=None)`.
#[derive(Clone, Debug, Default)]
pub struct ListenOnlyOptions {
    /// Scopes the call to one tenant (RFC §17.2): a room of another holds
    /// no discussion for it.
    pub organization_id: Option<String>,
}

/// What [`RoomKit::talk_again`] takes beside the room and the agents:
/// Python's `talk_again(room_id, channel_ids, *, organization_id=None)`.
#[derive(Clone, Debug, Default)]
pub struct TalkAgainOptions {
    /// Scopes the call to one tenant (RFC §17.2): a room of another holds
    /// no discussion for it.
    pub organization_id: Option<String>,
}

/// The discussion this process runs for each room, and who this process is
/// as a discussion's lease names it (rule 16).
#[derive(Default)]
pub(crate) struct DiscussionSeams {
    rooms: Mutex<HashMap<RoomId, Arc<dyn DiscussionSeam>>>,
    /// The rooms whose discussion this process is uninstalling: one its
    /// own work reads stored meanwhile is not followed (rule 16).
    leaving: Mutex<HashSet<RoomId>>,
    me: OnceLock<String>,
}

/// A room whose discussion this process is uninstalling, until dropped.
pub(crate) struct Leaving<'seams> {
    seams: &'seams DiscussionSeams,
    room_id: RoomId,
}

impl Drop for Leaving<'_> {
    fn drop(&mut self) {
        self.seams.leaving().remove(&self.room_id);
    }
}

impl fmt::Debug for DiscussionSeams {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DiscussionSeams")
            .finish_non_exhaustive()
    }
}

impl DiscussionSeams {
    fn rooms(&self) -> MutexGuard<'_, HashMap<RoomId, Arc<dyn DiscussionSeam>>> {
        self.rooms.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn leaving(&self) -> MutexGuard<'_, HashSet<RoomId>> {
        self.leaving.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `room_id`'s discussion is being uninstalled here until the guard is
    /// dropped: until its configuration is forgotten, a context of the room
    /// this process builds, its own driver's turn's included, follows none.
    pub(crate) fn leaving_room(&self, room_id: &RoomId) -> Leaving<'_> {
        self.leaving().insert(room_id.clone());
        Leaving {
            seams: self,
            room_id: room_id.clone(),
        }
    }

    /// Who this process is, as a lease names it: Python's `kit._instance_id`.
    pub(crate) fn me(&self) -> String {
        self.me
            .get_or_init(|| uuid::Uuid::new_v4().simple().to_string())
            .clone()
    }

    /// Whether a discussion runs here, in any room.
    pub(crate) fn any(&self) -> bool {
        !self.rooms().is_empty()
    }

    /// The discussion this process runs for `room_id`.
    pub(crate) fn get(&self, room_id: &RoomId) -> Option<Arc<dyn DiscussionSeam>> {
        self.rooms().get(room_id).cloned()
    }

    /// `seam` runs the discussion of `room_id` from now on.
    pub(crate) fn hold(&self, room_id: RoomId, seam: Arc<dyn DiscussionSeam>) {
        self.rooms().insert(room_id, seam);
    }

    /// `seam` runs the discussion of `room_id` from now on, unless one runs
    /// there already or this process is uninstalling it; whether it does.
    fn hold_if_none(&self, room_id: RoomId, seam: Arc<dyn DiscussionSeam>) -> bool {
        let mut rooms = self.rooms();
        if rooms.contains_key(&room_id) || self.leaving().contains(&room_id) {
            return false;
        }
        rooms.insert(room_id, seam);
        true
    }

    /// `room_id`'s discussion runs here no longer; the one it ran.
    pub(crate) fn release(&self, room_id: &RoomId) -> Option<Arc<dyn DiscussionSeam>> {
        self.rooms().remove(room_id)
    }

    /// `held`, the discussion of `room_id`, runs here no longer, unless it
    /// was replaced meanwhile; whether it was released.
    fn release_held(&self, room_id: &RoomId, held: &Arc<dyn DiscussionSeam>) -> bool {
        let mut rooms = self.rooms();
        if !same_seam(rooms.get(room_id), Some(held)) {
            return false;
        }
        rooms.remove(room_id);
        true
    }

    /// Whether every discussion here has no turn to give for now.
    pub(crate) fn settled(&self) -> bool {
        self.rooms().values().all(|seam| seam.settled())
    }

    /// Has every discussion's driver here look at its stored queue now.
    pub(crate) fn rouse_all(&self) {
        let seams: Vec<_> = self.rooms().values().cloned().collect();
        for seam in seams {
            seam.rouse();
        }
    }

    /// Stops every discussion's turns; their queues stay stored (rule 16).
    pub(crate) async fn stop_all(&self) {
        let seams: Vec<_> = self.rooms().values().cloned().collect();
        for seam in seams {
            seam.stop().await;
        }
    }
}

/// Whether `seam` and `other` are the same discussion, or both none.
fn same_seam(
    seam: Option<&Arc<dyn DiscussionSeam>>,
    other: Option<&Arc<dyn DiscussionSeam>>,
) -> bool {
    match (seam, other) {
        (Some(seam), Some(other)) => {
            Arc::as_ptr(seam).cast::<()>() == Arc::as_ptr(other).cast::<()>()
        }
        (None, None) => true,
        _ => false,
    }
}

impl<S: ConversationStore> RoomKit<S> {
    /// Joins, follows or leaves the discussion `room` stores (RFC §19.7.5
    /// rule 16): a room another process gave a discussion is followed here,
    /// one whose discussion another process replaced is routed by the new
    /// configuration, an installed one included, and one whose discussion is
    /// gone is left when this process only followed it. Run for every
    /// context the pipeline builds, before a `process_inbound` naming its
    /// room, and by the strategy calls: Python's `_follow_discussion`, which
    /// every context the kit builds runs. Whether this process joined the
    /// discussion now, its hooks registered for the room.
    pub(crate) fn follow_discussion(&self, room: &Room) -> bool {
        let config = DiscussionConfig::stored(&room.metadata);
        let held = self.inner.discussions.get(&room.id);
        match (config, held) {
            (Some(config), None) => return self.join_stored(room, config),
            (Some(config), Some(held)) => held.follow(config),
            (None, Some(held)) if !held.installed() => {
                self.leave_discussion(&room.id, &held);
            }
            _ => {}
        }
        false
    }

    /// Reads `room_id` and follows the discussion it stores now, when this
    /// process only follows one there: a host call routes by the
    /// configuration another process's install may have replaced, and a
    /// discussion another process uninstalled is left (rule 16, Python's
    /// `editing`, which follows the stored configuration).
    async fn follow_stored(&self, room_id: &RoomId) {
        let follows = self
            .inner
            .discussions
            .get(room_id)
            .is_some_and(|seam| !seam.installed());
        if !follows {
            return;
        }
        if let Ok(Some(room)) = self.store().get_room(room_id).await {
            self.follow_discussion(&room);
        }
    }

    /// Reads `room_id` and follows the discussion it stores when this
    /// process holds none for it (rule 16); a room the store cannot read is
    /// left to the call that reads it next.
    pub(crate) async fn follow_stored_discussion(&self, room_id: &RoomId) {
        if self.inner.discussions.get(room_id).is_some() {
            return;
        }
        if let Ok(Some(room)) = self.store().get_room(room_id).await {
            self.follow_discussion(&room);
        }
    }

    /// Follows the discussion `room` stores, `config`, another process's:
    /// this process routes and queues by it (rule 16). Python's
    /// `_join_stored`. Whether it joined it: not when another call of this
    /// process did first.
    fn join_stored(&self, room: &Room, config: DiscussionConfig) -> bool {
        let room_id = room.id.clone();
        let organization_id = room.organization_id.clone();
        let follower = DiscussionRoom::following(self, room_id.clone(), organization_id, config);
        let seam: Arc<dyn DiscussionSeam> = Arc::<DiscussionRoom<S>>::clone(&follower);
        if !self.inner.discussions.hold_if_none(room_id.clone(), seam) {
            return false;
        }
        for hook in follower.hooks() {
            if let Err(error) = self.hook_engine().replace_room_hook(&room_id, hook) {
                tracing::warn!(
                    target: "roomkit::orchestration::discussion",
                    %room_id,
                    %error,
                    "a followed discussion's hook was not registered"
                );
            }
        }
        true
    }

    /// Stops following `held`, the discussion of `room_id`: its hooks
    /// removed. Python's `_leave_discussion`.
    fn leave_discussion(&self, room_id: &RoomId, held: &Arc<dyn DiscussionSeam>) -> bool {
        if !self.inner.discussions.release_held(room_id, held) {
            return false;
        }
        for hook in [SILENT, NAMES] {
            self.hook_engine().remove_room_hook(room_id, hook);
        }
        true
    }

    /// Uninstalls the discussion `room` stores with no install of this
    /// process recorded, one another process stored included (rule 16), the
    /// agents its installs attached detached; whether it held one. Python's
    /// `_uninstall_stored_discussion`.
    pub(crate) async fn uninstall_stored_discussion(
        &self,
        room: &Room,
    ) -> Result<bool, RoomKitError> {
        self.follow_discussion(room);
        let Some(held) = self.inner.discussions.get(&room.id) else {
            return Ok(false);
        };
        let leaving = self.inner.discussions.leaving_room(&room.id);
        if !self.leave_discussion(&room.id, &held) {
            return Ok(false);
        }
        held.forget().await?;
        drop(leaving);
        let attached = DiscussionConfig::stored(&room.metadata)
            .map(|config| config.attached)
            .unwrap_or_default();
        let stayed = self.detach_agents(&room.id, &attached).await;
        if !stayed.is_empty() {
            let stayed: Vec<&str> = stayed.iter().map(ChannelId::as_str).collect();
            return Err(RoomKitError::Runtime(format!(
                "Room {} still binds {stayed:?}, which its installs attached",
                room.id
            )));
        }
        Ok(true)
    }

    /// Whether the discussion this process installed is the one the room
    /// stores now, the one it gives turns by (rule 16). One another process
    /// uninstalled is left here at once, its record with it, so that
    /// `room_strategy` tells the host it is gone. Python's
    /// `_discussion_is_current`.
    pub(crate) async fn discussion_is_current(
        &self,
        room_id: &RoomId,
        organization_id: Option<String>,
    ) -> Result<bool, RoomKitError> {
        let seen = self.inner.discussions.get(room_id);
        let room = self
            .get_room(room_id, crate::kit::GetRoomOptions { organization_id })
            .await?;
        self.follow_discussion(&room);
        let Some(held) = self.inner.discussions.get(&room.id) else {
            return Ok(false);
        };
        // Gone when the room stores none, or once this process read it
        // uninstalled, one installed again since included (Python's
        // `_uninstalled`).
        let stored = DiscussionConfig::stored(&room.metadata).filter(|_| !held.forgotten());
        let Some(stored) = stored else {
            // An install of this process under way has not stored its
            // configuration yet: it is not current, and stays.
            if same_seam(seen.as_ref(), Some(&held)) && !self.installing_strategy(&room.id) {
                self.leave_uninstalled(&room.id, &held).await;
            }
            return Ok(false);
        };
        Ok(held.drives(&stored))
    }

    /// Leaves the discussion of `room_id` another process uninstalled when
    /// `this`, a discussion's address, is the one this process holds for
    /// it, its driver having read
    /// it so: Python's `_forget_discussion` from `_next_turn`. Whether it was
    /// left.
    pub(crate) async fn leave_uninstalled_here(&self, room_id: &RoomId, this: usize) -> bool {
        let Some(held) = self.inner.discussions.get(room_id) else {
            return false;
        };
        if Arc::as_ptr(&held).cast::<()>() as usize != this || self.installing_strategy(room_id) {
            return false;
        }
        self.leave_uninstalled(room_id, &held).await
    }

    /// Leaves `held`, the discussion of `room_id` another process
    /// uninstalled, and the strategy record with it, writing nothing back.
    /// Python's `_leave_uninstalled`.
    async fn leave_uninstalled(&self, room_id: &RoomId, held: &Arc<dyn DiscussionSeam>) -> bool {
        if !self.leave_discussion(room_id, held) {
            return false;
        }
        self.drop_discussion_record(room_id, held);
        held.leave().await;
        true
    }

    /// The speak queue of the discussion `room_id` holds in this process, or
    /// `None` when it holds none (RFC §19.7.5 rule 17): Python's
    /// `speak_queue(room_id, *, organization_id=None)`. `options` scopes the
    /// read to one tenant (RFC §17.2): a room of another reads as holding none.
    #[must_use]
    pub fn speak_queue(&self, room_id: &RoomId, options: SpeakQueueOptions) -> Option<SpeakQueue> {
        let SpeakQueueOptions { organization_id } = options;
        self.inner
            .discussions
            .get(room_id)?
            .view(organization_id.as_deref())
    }

    /// Has agents of the discussion `room_id` holds only listen (RFC §19.7.5
    /// rule 12): Python's `listen_only(room_id, channel_ids, *,
    /// organization_id=None)`.
    ///
    /// An agent that only listens keeps its place in the queue and takes no
    /// turn an agent's message asks for; a person's message that names it
    /// gives it one turn. Setting the state on the agent whose turn runs cuts
    /// that turn, as a `Cancel` does; what the turn committed stays. A
    /// channel id that is not one of its agents is left out.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`], Python's `ValueError`, when the room holds no
    /// discussion in this process, or none in `organization_id`'s scope; a
    /// store failure as it comes.
    pub async fn listen_only(
        &self,
        room_id: &RoomId,
        channel_ids: &[ChannelId],
        options: ListenOnlyOptions,
    ) -> Result<(), RoomKitError> {
        let ListenOnlyOptions { organization_id } = options;
        self.follow_stored(room_id).await;
        self.discussion_of(room_id, organization_id.as_deref())?
            .listen_only(channel_ids.to_vec())
            .await
    }

    /// Ends the listening state of agents of the discussion `room_id` holds
    /// (RFC §19.7.5 rule 12): Python's `talk_again(room_id, channel_ids, *,
    /// organization_id=None)`. A wait that was for them ends, and a turn they
    /// are owed is given.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Value`], Python's `ValueError`, when the room holds no
    /// discussion in this process, or none in `organization_id`'s scope; a
    /// store failure as it comes.
    pub async fn talk_again(
        &self,
        room_id: &RoomId,
        channel_ids: &[ChannelId],
        options: TalkAgainOptions,
    ) -> Result<(), RoomKitError> {
        let TalkAgainOptions { organization_id } = options;
        self.follow_stored(room_id).await;
        self.discussion_of(room_id, organization_id.as_deref())?
            .talk_again(channel_ids.to_vec())
            .await
    }

    /// The discussion `room_id` holds in this process, in `organization_id`'s
    /// scope: Python's `_discussion`.
    fn discussion_of(
        &self,
        room_id: &RoomId,
        organization_id: Option<&str>,
    ) -> Result<Arc<dyn DiscussionSeam>, RoomKitError> {
        self.inner
            .discussions
            .get(room_id)
            .filter(|seam| seam.in_scope(organization_id))
            .ok_or_else(|| RoomKitError::Value(format!("Room {room_id} holds no discussion")))
    }

    /// Gives `room` to the discussion `install` configures, or puts it in
    /// place of the discussion the room holds, here or stored by another
    /// process (RFC §19.7.5 rules 1 and 16): Python's `_install_discussion`.
    pub(crate) async fn install_discussion(
        &self,
        room: Room,
        install: Install,
    ) -> Result<(), RoomKitError> {
        self.follow_discussion(&room);
        let mut held = self.inner.discussions.get(&room.id);
        if let Some(seam) = held.clone()
            && seam.installed()
            && (seam.forgotten() || DiscussionConfig::stored(&room.metadata).is_none())
        {
            // Gone since this process installed it: left here, never reused.
            // The install takes the place of the discussion the room stores
            // now, another process's, else starts afresh.
            self.leave_uninstalled(&room.id, &seam).await;
            held = None;
            if let Some(config) = DiscussionConfig::stored(&room.metadata) {
                self.join_stored(&room, config);
                held = self.inner.discussions.get(&room.id);
            }
        }
        match held {
            Some(held) => self.replace_discussion(&room.id, &held, install).await,
            None => self.start_discussion(room, install).await,
        }
    }

    /// Gives `room`, which holds no discussion here, to the one `install`
    /// configures: held before its configuration is stored, its agents
    /// bound, its queue read, its hooks and turns started. Python's
    /// `_start_discussion`.
    async fn start_discussion(&self, room: Room, install: Install) -> Result<(), RoomKitError> {
        let room_id = room.id;
        self.check_discussion_room(&room_id, &install.config, &[])
            .await?;
        let agents = install.config.agents.clone();
        let running = DiscussionRoom::new(self, room_id.clone(), room.organization_id, install);
        // Before the configuration is stored: from then on a message is told
        // to it, and to nobody without it.
        self.inner
            .discussions
            .hold(room_id.clone(), Arc::<DiscussionRoom<S>>::clone(&running));
        let started = async {
            self.bind_discussion_agents(&room_id, &agents).await?;
            running.load().await?;
            for hook in running.hooks() {
                self.hook_engine().replace_room_hook(&room_id, hook)?;
            }
            Ok::<(), RoomKitError>(())
        }
        .await;
        if let Err(error) = started {
            self.inner.discussions.release(&room_id);
            return Err(error);
        }
        running.start();
        Ok(())
    }

    /// Puts the configuration `install` gives in place of the one `held`
    /// routes by, keeping the queue (rule 1): the agents that leave and an
    /// install attached detached, the new ones bound, this process giving
    /// the turns. Python's `_replace_discussion`.
    async fn replace_discussion(
        &self,
        room_id: &RoomId,
        held: &Arc<dyn DiscussionSeam>,
        install: Install,
    ) -> Result<(), RoomKitError> {
        let room = self.store().get_room(room_id).await?;
        let stored = room.and_then(|room| DiscussionConfig::stored(&room.metadata));
        let leaving: Vec<ChannelId> = stored
            .map(|stored| {
                held.follow(stored.clone());
                stored
                    .attached
                    .into_iter()
                    .filter(|agent| !install.config.agents.contains(agent))
                    .collect()
            })
            .unwrap_or_default();
        self.check_discussion_room(room_id, &install.config, &leaving)
            .await?;
        let stayed = self.detach_agents(room_id, &leaving).await;
        if !stayed.is_empty() {
            let stayed: Vec<&str> = stayed.iter().map(ChannelId::as_str).collect();
            return Err(RoomKitError::Runtime(format!(
                "Room {room_id} still binds {stayed:?}, which left its discussion"
            )));
        }
        let agents = install.config.agents.clone();
        held.take_over(install).await?;
        self.bind_discussion_agents(room_id, &agents).await?;
        held.drive();
        Ok(())
    }

    /// Refuses what the room binds that a discussion of `config` cannot share
    /// it with (rule 1), but for the agents `leaving` the room with the
    /// discussion it replaces: Python's `_discussion_refusal`.
    pub(crate) async fn check_discussion_room(
        &self,
        room_id: &RoomId,
        config: &DiscussionConfig,
        leaving: &[ChannelId],
    ) -> Result<(), RoomKitError> {
        let refused = |refusal: String| {
            RoomKitError::Value(format!(
                "Room {room_id} cannot hold a discussion: {refusal}"
            ))
        };
        for binding in self.store().list_bindings(room_id).await? {
            if leaving.contains(&binding.channel_id) {
                continue;
            }
            let channel = self.inner.channel(&binding.channel_id);
            let refusal = binding_refusal(
                &config.agents,
                &binding.channel_id,
                binding.category,
                channel
                    .map(|channel| channel.declaration().channel_type)
                    .as_ref(),
            );
            if let Some(refusal) = refusal {
                return Err(refused(refusal));
            }
        }
        for agent in &config.agents {
            // A strategy's runner already takes this agent's turns here.
            if self.inner.turn_runners.get(room_id, agent).is_some() {
                return Err(refused(format!("another strategy runs {agent}'s turns")));
            }
        }
        if self.hook_engine().has_router_hook(room_id) {
            return Err(refused("a router is installed".to_owned()));
        }
        Ok(())
    }

    /// Binds the agents of a discussion the room does not bind: an install
    /// called directly attaches them as `install_strategy` does.
    pub(crate) async fn bind_discussion_agents(
        &self,
        room_id: &RoomId,
        agents: &[ChannelId],
    ) -> Result<(), RoomKitError> {
        let bound = self.store().list_bindings(room_id).await?;
        for agent in agents {
            if bound.iter().any(|binding| &binding.channel_id == agent) {
                continue;
            }
            let options = crate::kit::AttachChannelOptions {
                category: Some(ChannelCategory::Intelligence),
                ..Default::default()
            };
            self.attach_channel(room_id, agent, options).await?;
        }
        Ok(())
    }

    /// Refuses a binding the room's discussion cannot share it with (rule 1):
    /// Python's `_refuse_discussion_binding`.
    pub(crate) fn refuse_discussion_binding(
        &self,
        room_id: &RoomId,
        channel_id: &ChannelId,
        category: ChannelCategory,
        channel_type: &ChannelType,
    ) -> Result<(), RoomKitError> {
        let refusal = self
            .inner
            .discussions
            .get(room_id)
            .and_then(|seam| seam.binding_refusal(channel_id, category, channel_type));
        match refusal {
            Some(refusal) => Err(RoomKitError::Value(format!(
                "Room {room_id} holds a discussion: {refusal}"
            ))),
            None => Ok(()),
        }
    }

    /// Gives the turn the discussion of `room_id` names now, if any, and
    /// waits for it: its agent's answers are the result's, as a new
    /// message's. The result names no event when no turn was given.
    /// `named` names the turn's delivery, which its cut cancels.
    pub(crate) async fn run_discussion_turn(
        &self,
        room_id: &RoomId,
        named: NamedGuard,
    ) -> Result<InboundResult, RoomKitError> {
        let turn = Inbound::discussion_turn(self.config(), room_id.clone());
        let ran =
            driver::run_rerun(&self.inner, "discussion_turn", room_id, turn, Some(named)).await;
        Ok(ran.output??.into())
    }
}

#[cfg(test)]
mod tests {
    use std::future::{self, Future};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use roomkit_core::model::{
        ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelOutput, ChannelType,
        EventStatus, EventType, InboundMessage, Metadata, TextContent,
    };
    use roomkit_core::ops::DiscussionTurn;
    use roomkit_core::rules::discussion::{CONFIG_KEY, STATE_KEY};
    use roomkit_core::rules::hook_registry::HookTrigger;
    use serde_json::json;
    use tokio::sync::Notify;

    use super::*;
    use crate::HookOptions;
    use crate::channel::{Channel, ChannelError};
    use crate::discussion_turn::discussion_notes;
    use crate::kit::WeakRoomKit;
    use crate::kit::{AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions};
    use crate::testing::{
        EchoAgent, EchoAgentOptions, RecordingTransport, RecordingTransportOptions,
    };

    fn id(channel: &str) -> ChannelId {
        ChannelId::new(channel).unwrap()
    }

    /// A turn's delivery, named by no key.
    fn unnamed(kit: &RoomKit) -> NamedGuard {
        kit.inner.delivery_keys.naming(None, None)
    }

    fn room_id() -> RoomId {
        RoomId::new("r1").unwrap()
    }

    /// Stands for a discussion holding the room, as Python's `_Held`: records
    /// what it is told, and gives the turn it is handed.
    #[derive(Default)]
    struct Held {
        committed: Mutex<Vec<(RoomEvent, Vec<ChannelId>)>>,
        instructed: Mutex<Vec<RoomEvent>>,
        turn: Mutex<Option<DiscussionTurn>>,
        fails: bool,
    }

    impl DiscussionSeam for Held {
        fn on_committed(
            &self,
            event: RoomEvent,
            reach: Vec<ChannelId>,
            _context: RoomContext,
        ) -> BoxFuture<'_, Result<(), RoomKitError>> {
            self.committed.lock().unwrap().push((event, reach));
            let fails = self.fails;
            Box::pin(async move {
                if fails {
                    return Err(RoomKitError::Value("it fails".to_owned()));
                }
                Ok(())
            })
        }

        fn queue_instruction(&self, event: RoomEvent) -> BoxFuture<'_, Result<(), RoomKitError>> {
            self.instructed.lock().unwrap().push(event);
            let fails = self.fails;
            Box::pin(async move {
                if fails {
                    return Err(RoomKitError::Value("it fails".to_owned()));
                }
                Ok(())
            })
        }

        fn next_turn(
            &self,
            _context: RoomContext,
        ) -> BoxFuture<'_, Result<NextTurn, RoomKitError>> {
            let turn = self.turn.lock().unwrap().take();
            let fails = self.fails;
            Box::pin(async move {
                if fails {
                    return Err(RoomKitError::Value("it fails".to_owned()));
                }
                Ok(NextTurn {
                    turn: turn.map(Box::new),
                    stopped: Vec::new(),
                })
            })
        }
    }

    /// A room storing a discussion of `a` and `b`, the transport `ops`
    /// recorded, the seam `held` running it.
    async fn held_room(held: &Arc<Held>) -> (RoomKit, RecordingTransport) {
        held_room_of(held, crate::kit::RoomKitOptions::default()).await
    }

    async fn held_room_of(
        held: &Arc<Held>,
        options: crate::kit::RoomKitOptions,
    ) -> (RoomKit, RecordingTransport) {
        let kit = RoomKit::new(options);
        let ops = RecordingTransport::new(
            id("ops"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(ops.clone()).unwrap();
        for agent in ["a", "b"] {
            let reply = format!("{agent} answers");
            kit.register_channel(EchoAgent::new(
                id(agent),
                reply,
                EchoAgentOptions::default(),
            ))
            .unwrap();
        }
        let options = CreateRoomOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        let mut room = kit.create_room(options).await.unwrap();
        room.metadata = Metadata::from_iter([(
            CONFIG_KEY.to_owned(),
            json!({"agents": ["a", "b"], "max_depth": 5}),
        )]);
        kit.store().update_room(room).await.unwrap();
        for (channel, category) in [
            ("ops", ChannelCategory::Transport),
            ("a", ChannelCategory::Intelligence),
            ("b", ChannelCategory::Intelligence),
        ] {
            let options = AttachChannelOptions {
                category: Some(category),
                ..Default::default()
            };
            kit.attach_channel(&room_id(), &id(channel), options)
                .await
                .unwrap();
        }
        kit.inner
            .discussions
            .hold(room_id(), Arc::<Held>::clone(held));
        (kit, ops)
    }

    fn said(body: &str) -> InboundMessage {
        InboundMessage::new(id("ops"), "oncall", TextContent::new(body))
    }

    async fn say(kit: &RoomKit, message: InboundMessage) -> InboundResult {
        kit.process_inbound(message, ProcessInboundOptions::default())
            .await
            .unwrap()
    }

    fn turn_on(
        answered: RoomEvent,
        agent: &str,
        chain_depth: u32,
        notes: &[&str],
    ) -> DiscussionTurn {
        DiscussionTurn {
            agent: id(agent),
            answered: Box::new(RoomEvent {
                chain_depth,
                ..answered
            }),
            mark: crate::issue_mark(notes.iter().map(|note| (*note).to_owned()).collect()),
            max_depth: 10,
        }
    }

    // Python: tests/test_discussion_core.py::test_a_room_a_discussion_holds_asks_no_agent_at_broadcast
    #[tokio::test]
    async fn a_committed_message_goes_to_the_discussion_within_its_reach() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;
        let mut message = said("@a hello");
        message.addressed_to = Some(vec![id("a"), id("ghost")]);

        let result = say(&kit, message).await;

        assert_eq!(result.response_events, Vec::new());
        assert_eq!(result.unavailable_targets, Some(vec![id("ghost")]));
        let committed = held.committed.lock().unwrap();
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].0.r#type, EventType::Message);
        assert_eq!(committed[0].1, vec![id("a"), id("b")]);
    }

    // Python: core/mixins/lane_execution.py `_queue_for_discussion`
    #[tokio::test]
    async fn a_failure_of_the_discussion_leaves_the_event_delivered() {
        let held = Arc::new(Held {
            fails: true,
            ..Held::default()
        });
        let (kit, ops) = held_room(&held).await;
        let other = RecordingTransport::new(
            id("sms"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(other.clone()).unwrap();
        kit.attach_channel(&room_id(), &id("sms"), AttachChannelOptions::default())
            .await
            .unwrap();

        let result = say(&kit, said("hello")).await;

        assert!(!result.blocked);
        assert!(result.event.is_some());
        crate::testing::idle(&kit).await;
        assert_eq!(other.texts(), vec!["hello".to_owned()]);
        assert_eq!(ops.texts(), Vec::<String>::new());
    }

    // Python: core/mixins/lane_execution.py `_commit`, an instruction to its agents
    #[tokio::test]
    async fn an_instruction_to_an_agent_goes_to_the_discussion_undelivered() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;
        let mut message = said("Introduce yourself");
        message.event_type = EventType::Instruction;
        message.addressed_to = Some(vec![id("a")]);

        let result = say(&kit, message).await;

        assert!(!result.blocked);
        assert_eq!(result.response_events, Vec::new());
        assert_eq!(result.event.unwrap().status, EventStatus::Delivered);
        assert_eq!(held.instructed.lock().unwrap().len(), 1);
        assert_eq!(held.committed.lock().unwrap().len(), 0);
    }

    // Python: core/mixins/lane_execution.py, `queue_instruction` is not caught
    #[tokio::test]
    async fn an_instruction_the_discussion_did_not_take_fails_its_call() {
        let held = Arc::new(Held {
            fails: true,
            ..Held::default()
        });
        let (kit, _ops) = held_room(&held).await;
        let mut message = said("Introduce yourself");
        message.event_type = EventType::Instruction;
        message.addressed_to = Some(vec![id("a")]);

        let result = kit
            .process_inbound(message, ProcessInboundOptions::default())
            .await;

        assert!(result.is_err(), "{result:?}");
    }

    // Python: core/mixins/discussion.py `_drive` backs off on what the
    // discussion raises: a failed look is no quiet "no turn".
    #[tokio::test]
    async fn a_failure_to_name_the_turn_fails_the_turn_s_call() {
        let held = Arc::new(Held {
            fails: true,
            ..Held::default()
        });
        let (kit, _ops) = held_room(&held).await;

        let result = kit.run_discussion_turn(&room_id(), unnamed(&kit)).await;

        assert!(result.is_err(), "{result:?}");
    }

    // Python: core/mixins/discussion.py `_plan_discussion_turn`
    #[tokio::test]
    async fn a_turn_on_an_event_whose_source_cannot_write_is_no_turn() {
        let held = Arc::new(Held::default());
        let (kit, ops) = held_room(&held).await;
        let asked = say(&kit, said("hello")).await.event.unwrap();
        crate::testing::idle(&kit).await;
        kit.mute(&room_id(), &id("ops"), crate::kit::MuteOptions::default())
            .await
            .unwrap();

        *held.turn.lock().unwrap() = Some(turn_on(asked, "a", 1, &[]));
        let result = kit
            .run_discussion_turn(&room_id(), unnamed(&kit))
            .await
            .unwrap();
        crate::testing::idle(&kit).await;

        assert_eq!(result, InboundResult::default());
        assert_eq!(ops.texts(), Vec::<String>::new());
    }

    /// A transport whose first delivery waits for its gate.
    #[derive(Clone, Default)]
    struct Gated {
        reached: Arc<Notify>,
        gate: Arc<Notify>,
        delivered: Arc<AtomicU32>,
    }

    impl Channel for Gated {
        fn channel_id(&self) -> ChannelId {
            id("slow")
        }

        fn channel_type(&self) -> ChannelType {
            ChannelType::SMS
        }

        fn category(&self) -> ChannelCategory {
            ChannelCategory::Transport
        }

        fn capabilities(&self) -> ChannelCapabilities {
            ChannelCapabilities::default()
        }

        fn handle_inbound(
            &self,
            _message: InboundMessage,
            _context: RoomContext,
        ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
            future::ready(Err("no inbound".into()))
        }

        async fn deliver(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> Result<ChannelOutput, ChannelError> {
            if self.delivered.fetch_add(1, Ordering::Relaxed) == 0 {
                self.reached.notify_one();
                self.gate.notified().await;
            }
            Ok(ChannelOutput::default())
        }
    }

    // Python: core/mixins/discussion.py `_plan_discussion_turn`, its
    // `after_index`; RFC §10.2, §19.7.5 rule 9.
    #[tokio::test]
    async fn a_turn_waits_for_its_event_s_own_delivery() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;
        let gated = Gated::default();
        kit.register_channel(gated.clone()).unwrap();
        kit.attach_channel(&room_id(), &id("slow"), AttachChannelOptions::default())
            .await
            .unwrap();
        let kit = Arc::new(kit);
        let delivering = {
            let kit = Arc::clone(&kit);
            tokio::spawn(async move { say(&kit, said("hello")).await })
        };
        gated.reached.notified().await;
        let options = crate::store::ListEventsOptions::default();
        let stored = kit.store().list_events(&room_id(), options).await.unwrap();
        let asked = stored.last().unwrap().clone();

        *held.turn.lock().unwrap() = Some(turn_on(asked, "a", 1, &[]));
        let mut turn = {
            let kit = Arc::clone(&kit);
            tokio::spawn(async move { kit.run_discussion_turn(&room_id(), unnamed(&kit)).await })
        };

        let early = tokio::time::timeout(Duration::from_millis(30), &mut turn).await;
        assert!(
            early.is_err(),
            "the turn's call ended before its event was delivered"
        );
        // Its answer's own set would wait behind the event's place as well:
        // only the store tells whether the agent was asked.
        let options = crate::store::ListEventsOptions::default();
        let stored = kit.store().list_events(&room_id(), options).await.unwrap();
        let answers = stored
            .iter()
            .filter(|event| event.source.channel_id == id("a"));
        assert_eq!(
            answers.count(),
            0,
            "the agent answered before its event was delivered"
        );
        gated.gate.notify_one();
        delivering.await.unwrap();
        let answered = turn.await.unwrap().unwrap();
        assert_eq!(answered.response_events.len(), 1);
    }

    /// An agent whose first turn waits for its gate, then each answers.
    #[derive(Clone, Default)]
    struct GatedAgent {
        reached: Arc<Notify>,
        gate: Arc<Notify>,
        turns: Arc<AtomicU32>,
    }

    impl Channel for GatedAgent {
        fn channel_id(&self) -> ChannelId {
            id("a")
        }

        fn channel_type(&self) -> ChannelType {
            ChannelType::AI
        }

        fn category(&self) -> ChannelCategory {
            ChannelCategory::Intelligence
        }

        fn capabilities(&self) -> ChannelCapabilities {
            ChannelCapabilities::default()
        }

        fn handle_inbound(
            &self,
            _message: InboundMessage,
            _context: RoomContext,
        ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
            future::ready(Err("no inbound".into()))
        }

        fn deliver(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
            future::ready(Ok(ChannelOutput::default()))
        }

        async fn on_event(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> Result<ChannelOutput, ChannelError> {
            if self.turns.fetch_add(1, Ordering::Relaxed) == 0 {
                self.reached.notify_one();
                self.gate.notified().await;
            }
            Ok(ChannelOutput::default())
        }
    }

    /// A room binding `ops`, a real discussion of `a`, gated, and `b`.
    async fn discussion_room() -> (RoomKit, GatedAgent) {
        let kit = RoomKit::new(crate::kit::RoomKitOptions::default());
        let ops = RecordingTransport::new(
            id("ops"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(ops).unwrap();
        let agent = GatedAgent::default();
        kit.register_channel(agent.clone()).unwrap();
        kit.register_channel(EchoAgent::new(
            id("b"),
            "b answers",
            EchoAgentOptions::default(),
        ))
        .unwrap();
        let options = CreateRoomOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        kit.create_room(options).await.unwrap();
        kit.attach_channel(&room_id(), &id("ops"), AttachChannelOptions::default())
            .await
            .unwrap();
        let discussion = crate::Discussion::new(
            vec![id("a").into(), id("b").into()],
            crate::orchestration::DiscussionOptions::default(),
        )
        .unwrap();
        kit.install_strategy(
            &room_id(),
            Arc::new(discussion),
            crate::kit::InstallStrategyOptions::default(),
        )
        .await
        .unwrap();
        (kit, agent)
    }

    /// [`discussion_room`], the first turn of `a` under way.
    async fn turn_under_way() -> (RoomKit, GatedAgent) {
        let (kit, agent) = discussion_room().await;
        say(&kit, said("@a hello")).await;
        agent.reached.notified().await;
        (kit, agent)
    }

    /// Every change of the queue, as `ON_SPEAK_QUEUE` tells it.
    fn following(kit: &RoomKit) -> Arc<Mutex<Vec<crate::SpeakQueueChange>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let told = Arc::clone(&seen);
        kit.hook(
            HookTrigger::OnSpeakQueue,
            move |event: crate::SpeakQueueEvent, _: RoomContext| {
                told.lock().unwrap().push(event.change);
                async {}
            },
            HookOptions::default(),
        )
        .unwrap();
        seen
    }

    // Python: tests/orchestration/test_discussion_edges.py::test_uninstalling_forgets_the_discussion_so_a_new_one_starts_fresh;
    // a turn running is cut (core/mixins/discussion.py `_uninstall_discussion`)
    #[tokio::test]
    async fn an_uninstall_cuts_the_turn_under_way_and_gives_the_room_back() {
        let (kit, agent) = turn_under_way().await;

        let room = room_id();
        let uninstalled =
            kit.uninstall_strategy(&room, crate::kit::UninstallStrategyOptions::default());
        let uninstalled = tokio::time::timeout(Duration::from_secs(5), uninstalled)
            .await
            .expect("an uninstall waits for no running turn");

        assert_eq!(uninstalled.unwrap(), true);
        assert_eq!(
            kit.speak_queue(&room_id(), SpeakQueueOptions::default()),
            None
        );
        crate::testing::idle(&kit).await;
        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        assert_eq!(room.metadata.get(STATE_KEY).filter(|v| !v.is_null()), None);
        assert_eq!(agent.turns.load(Ordering::Relaxed), 1);
    }

    // Python: core/mixins/discussion.py `_stop_discussions`, a close cuts its turn.
    #[tokio::test]
    async fn the_kit_s_close_cuts_a_discussion_s_turn_and_keeps_its_queue() {
        let (kit, _agent) = turn_under_way().await;

        tokio::time::timeout(Duration::from_secs(10), kit.close())
            .await
            .expect("a close waits for no running turn")
            .unwrap();

        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        assert!(room.metadata.contains_key(CONFIG_KEY));
    }

    // Python: orchestration/strategies/discussion/_room.py `forget`, what it held
    // told dropped.
    #[tokio::test]
    async fn an_uninstall_tells_the_instruction_it_drops() {
        let (kit, _agent) = turn_under_way().await;
        let seen = following(&kit);
        let mut instruction = said("Introduce yourself");
        instruction.event_type = EventType::Instruction;
        instruction.addressed_to = Some(vec![id("b")]);
        say(&kit, instruction).await;

        kit.uninstall_strategy(&room_id(), crate::kit::UninstallStrategyOptions::default())
            .await
            .unwrap();
        crate::testing::idle(&kit).await;

        let seen = seen.lock().unwrap();
        assert!(
            seen.contains(&crate::SpeakQueueChange::InstructionDropped),
            "{seen:?}"
        );
    }

    // Python: orchestration/strategies/discussion/_driver.py `_drive`'s cancel:
    // `_abandon_orphan`/`_ended`; the queue outlives the process (rule 16).
    #[tokio::test]
    async fn a_close_during_a_turn_records_its_end_and_whom_it_named() {
        let (kit, _agent) = turn_under_way().await;
        // Its row commits, and waits behind the turn in the room's lane.
        let sending = {
            let kit = kit.clone();
            tokio::spawn(async move {
                let content = TextContent::new("@b your turn");
                let options = crate::SendEventOptions::default();
                kit.send_event(&room_id(), &id("a"), content, options).await
            })
        };
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let options = crate::store::ListEventsOptions::default();
                let stored = kit.store().list_events(&room_id(), options).await.unwrap();
                if stored
                    .iter()
                    .any(|event| event.source.channel_id == id("a"))
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();

        kit.close().await.unwrap();
        drop(sending.await);

        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        let queue = room.metadata.get(STATE_KEY).unwrap();
        assert_eq!(queue["last_speaker"], json!("a"));
        assert_eq!(queue["entries"][0]["agent"], json!("b"));
        // What a test waits on still returns, the driver stopped.
        crate::testing::idle(&kit).await;
    }

    // Python: a close from a hook's body re-enters the lock it holds.
    #[tokio::test]
    async fn a_close_from_a_hook_under_the_room_s_lock_does_not_wait_for_that_lock() {
        let (kit, _agent) = discussion_room().await;
        let weak = kit.downgrade();
        kit.hook(
            HookTrigger::BeforeBroadcast,
            move |event: RoomEvent, _: RoomContext| {
                let closing = weak.upgrade();
                async move {
                    if let Some(kit) = closing {
                        drop(kit.close().await);
                    }
                    drop(event);
                    roomkit_core::model::HookResult::allow(
                        roomkit_core::model::hook::AllowOptions::default(),
                    )
                }
            },
            HookOptions {
                execution: crate::HookExecution::Sync,
                ..HookOptions::default()
            },
        )
        .unwrap();

        let saying = kit.process_inbound(said("hello"), ProcessInboundOptions::default());
        let said = tokio::time::timeout(Duration::from_secs(3), saying).await;

        assert!(said.is_ok(), "the close waited for the lock its hook holds");
    }

    // Python: tests/orchestration/test_discussion_refusals.py::test_installing_a_router_is_refused_in_the_room_and_globally
    #[tokio::test]
    async fn a_router_is_refused_for_every_room_once_a_discussion_holds_one() {
        let (kit, _agent) = discussion_room().await;
        let router = crate::ConversationRouter::new(
            crate::orchestration::ConversationRouterOptions::default(),
        );

        let refused = kit.hook(
            HookTrigger::BeforeBroadcast,
            router.as_hook(),
            HookOptions::default(),
        );

        assert!(
            matches!(refused, Err(RoomKitError::Value(_))),
            "{refused:?}"
        );
    }

    // Python: core/mixins/discussion.py `_fire_speak_queue` starts its hooks.
    #[tokio::test]
    async fn an_announcement_waits_for_the_start_of_the_hooks_not_their_end() {
        let kit = RoomKit::new(crate::kit::RoomKitOptions::default());
        let ops = RecordingTransport::new(
            id("ops"),
            ChannelType::SMS,
            RecordingTransportOptions::default(),
        );
        kit.register_channel(ops).unwrap();
        kit.register_channel(EchoAgent::new(
            id("a"),
            "a answers",
            EchoAgentOptions::default(),
        ))
        .unwrap();
        let options = CreateRoomOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        kit.create_room(options).await.unwrap();
        kit.attach_channel(&room_id(), &id("ops"), AttachChannelOptions::default())
            .await
            .unwrap();
        let given = Arc::new(Notify::new());
        let released = Arc::new(AtomicU32::new(0));
        let (telling, counted) = (Arc::clone(&given), Arc::clone(&released));
        kit.hook(
            HookTrigger::OnSpeakQueue,
            move |event: crate::SpeakQueueEvent, _: RoomContext| {
                let (given, released) = (Arc::clone(&telling), Arc::clone(&counted));
                async move {
                    match event.change {
                        crate::SpeakQueueChange::Queued => {
                            let wait =
                                tokio::time::timeout(Duration::from_secs(2), given.notified());
                            if wait.await.is_ok() {
                                released.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        crate::SpeakQueueChange::TurnGiven => given.notify_one(),
                        crate::SpeakQueueChange::TurnEnded
                        | crate::SpeakQueueChange::InstructionDropped
                        | crate::SpeakQueueChange::Listening
                        | crate::SpeakQueueChange::TalkingAgain
                        | crate::SpeakQueueChange::Waiting
                        | crate::SpeakQueueChange::Over => {}
                    }
                }
            },
            HookOptions::default(),
        )
        .unwrap();
        let discussion = crate::Discussion::new(
            vec![id("a").into()],
            crate::orchestration::DiscussionOptions::default(),
        )
        .unwrap();
        kit.install_strategy(
            &room_id(),
            Arc::new(discussion),
            crate::kit::InstallStrategyOptions::default(),
        )
        .await
        .unwrap();

        say(&kit, said("@a hello")).await;
        crate::testing::idle(&kit).await;

        assert_eq!(released.load(Ordering::SeqCst), 1);
    }

    // Python: core/mixins/discussion.py `_fire_speak_queue` starts its hooks: a
    // hook that panics at its first poll beside one that stays pending holds
    // no announcement back.
    #[tokio::test]
    async fn an_announcement_goes_on_past_a_hook_that_panics_at_its_first_poll() {
        let (kit, _agent) = discussion_room().await;
        let (telling, mut told) = tokio::sync::watch::channel(Vec::new());
        let held = Arc::new(Notify::new());
        let holding = Arc::clone(&held);
        kit.hook(
            HookTrigger::OnSpeakQueue,
            move |event: crate::SpeakQueueEvent, _: RoomContext| {
                telling.send_modify(|told| told.push(event.change));
                let holding = Arc::clone(&holding);
                async move {
                    if event.change == crate::SpeakQueueChange::Queued {
                        holding.notified().await;
                    }
                }
            },
            HookOptions {
                name: "holds".to_owned(),
                ..HookOptions::default()
            },
        )
        .unwrap();
        kit.hook(
            HookTrigger::OnSpeakQueue,
            |event: crate::SpeakQueueEvent, _: RoomContext| async move {
                assert_ne!(
                    event.change,
                    crate::SpeakQueueChange::Queued,
                    "this hook panics at its first poll"
                );
            },
            HookOptions {
                name: "panics".to_owned(),
                ..HookOptions::default()
            },
        )
        .unwrap();

        say(&kit, said("@a hello")).await;

        let given = told.wait_for(|told| told.contains(&crate::SpeakQueueChange::TurnGiven));
        tokio::time::timeout(Duration::from_secs(5), given)
            .await
            .expect("the turn's announcement goes on")
            .unwrap();
        held.notify_one();
    }

    // Python: strategy.py `Discussion.install`, called directly.
    #[tokio::test]
    async fn an_install_called_directly_binds_its_agents_and_refuses_no_room() {
        let kit = RoomKit::new(crate::kit::RoomKitOptions::default());
        kit.register_channel(EchoAgent::new(
            id("a"),
            "a answers",
            EchoAgentOptions::default(),
        ))
        .unwrap();
        let discussion = crate::Discussion::new(
            vec![id("a").into()],
            crate::orchestration::DiscussionOptions::default(),
        )
        .unwrap();
        let direct: &dyn crate::Orchestration = &discussion;

        let missing = direct.install(&kit, &room_id()).await;
        assert!(
            matches!(missing, Err(RoomKitError::RoomNotFound { .. })),
            "{missing:?}"
        );

        let options = CreateRoomOptions {
            room_id: Some(room_id()),
            ..Default::default()
        };
        kit.create_room(options).await.unwrap();
        direct.install(&kit, &room_id()).await.unwrap();
        let bindings = kit.store().list_bindings(&room_id()).await.unwrap();
        assert!(bindings.iter().any(|binding| binding.channel_id == id("a")));
        assert_eq!(kit.hook_engine().room_hook_names(&room_id()).len(), 2);

        direct.uninstall(&kit, &room_id()).await.unwrap();
        assert_eq!(kit.hook_engine().room_hook_names(&room_id()).len(), 0);
        assert_eq!(
            kit.speak_queue(&room_id(), SpeakQueueOptions::default()),
            None
        );
    }

    // Python: core/mixins/discussion.py `_planned_turn` has no bound on the room's lock.
    // The call's bound leaves the message before the turn time on a loaded
    // machine; the turn is held three times as long.
    #[tokio::test]
    async fn a_turn_waits_for_the_room_s_lock_past_the_call_s_bound() {
        let held = Arc::new(Held::default());
        let options = crate::kit::RoomKitOptions {
            process_timeout: Duration::from_millis(100),
            ..crate::kit::RoomKitOptions::default()
        };
        let (kit, _ops) = held_room_of(&held, options).await;
        let asked = say(&kit, said("hello")).await.event.unwrap();
        crate::testing::idle(&kit).await;
        *held.turn.lock().unwrap() = Some(turn_on(asked, "a", 1, &[]));
        let lock = kit.inner.locks.lock(&room_id()).await.unwrap();

        let mut turn = {
            let kit = kit.clone();
            tokio::spawn(async move { kit.run_discussion_turn(&room_id(), unnamed(&kit)).await })
        };
        let early = tokio::time::timeout(Duration::from_millis(300), &mut turn).await;
        assert!(
            early.is_err(),
            "the turn gave up waiting for the room's lock"
        );
        drop(lock);

        let answered = turn.await.unwrap().unwrap();
        assert_eq!(answered.response_events.len(), 1);
    }

    /// An agent whose turn calls into its room until a call is refused.
    #[derive(Clone, Default)]
    struct Calling {
        kit: Arc<OnceLock<WeakRoomKit>>,
        made: Arc<AtomicU32>,
    }

    impl Channel for Calling {
        fn channel_id(&self) -> ChannelId {
            id("calling")
        }

        fn channel_type(&self) -> ChannelType {
            ChannelType::AI
        }

        fn category(&self) -> ChannelCategory {
            ChannelCategory::Intelligence
        }

        fn capabilities(&self) -> ChannelCapabilities {
            ChannelCapabilities::default()
        }

        fn handle_inbound(
            &self,
            _message: InboundMessage,
            _context: RoomContext,
        ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
            future::ready(Err("no inbound".into()))
        }

        fn deliver(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
            future::ready(Ok(ChannelOutput::default()))
        }

        async fn on_event(
            &self,
            _event: RoomEvent,
            _binding: ChannelBinding,
            _context: RoomContext,
        ) -> Result<ChannelOutput, ChannelError> {
            let kit = self.kit.get().and_then(WeakRoomKit::upgrade).unwrap();
            loop {
                let options = ProcessInboundOptions {
                    room_id: Some(room_id()),
                    ..Default::default()
                };
                let called = kit
                    .process_inbound(said("follow-up"), options)
                    .await
                    .map_err(|error| ChannelError::from(error.to_string()))?;
                if called.event.is_none() {
                    return Ok(ChannelOutput::default());
                }
                self.made.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    // Python: core/mixins/discussion.py `_plan_discussion_turn`,
    // `reentry_budget=10 * max_depth`, one budget (rule 11).
    #[tokio::test]
    async fn a_turn_s_calls_draw_on_the_budget_its_limit_bounds() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;
        let calling = Calling::default();
        kit.register_channel(calling.clone()).unwrap();
        calling.kit.set(kit.downgrade()).unwrap();
        let options = AttachChannelOptions {
            category: Some(ChannelCategory::Intelligence),
            ..Default::default()
        };
        kit.attach_channel(&room_id(), &id("calling"), options)
            .await
            .unwrap();
        let asked = say(&kit, said("hello")).await.event.unwrap();
        crate::testing::idle(&kit).await;

        let mut turn = turn_on(asked, "calling", 0, &[]);
        turn.max_depth = 2;
        *held.turn.lock().unwrap() = Some(turn);
        kit.run_discussion_turn(&room_id(), unnamed(&kit))
            .await
            .unwrap();

        // Twice the discussion's limit, in tens; the kit's own would be 50.
        assert_eq!(calling.made.load(Ordering::Relaxed), 20);
    }

    // Python: core/mixins/inbound_locked.py, RFC §19.7.5 rule 15
    #[tokio::test]
    async fn an_instruction_to_a_discussion_that_is_over_is_blocked() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;
        let mut room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        room.metadata
            .insert(STATE_KEY.to_owned(), json!({"over": true}));
        kit.store().update_room(room).await.unwrap();
        let mut message = said("Introduce yourself");
        message.event_type = EventType::Instruction;
        message.addressed_to = Some(vec![id("a")]);

        let result = say(&kit, message).await;

        assert!(result.blocked);
        assert_eq!(result.reason.as_deref(), Some("discussion_over"));
        assert_eq!(result.event, None);
        assert_eq!(held.instructed.lock().unwrap().len(), 0);
    }

    // Python: tests/test_discussion_core.py::test_a_turn_asks_its_one_agent_against_the_discussions_depth_limit
    #[tokio::test]
    async fn test_a_turn_asks_its_one_agent_against_the_discussions_depth_limit() {
        let held = Arc::new(Held::default());
        let (kit, ops) = held_room(&held).await;
        let asked = say(&kit, said("@a @b hello")).await.event.unwrap();
        crate::testing::idle(&kit).await;

        // Deeper than the kit's limit, within the discussion's own.
        *held.turn.lock().unwrap() = Some(turn_on(asked.clone(), "a", 7, &["NOTES"]));
        let answered = kit
            .run_discussion_turn(&room_id(), unnamed(&kit))
            .await
            .unwrap();
        crate::testing::idle(&kit).await;

        let bodies: Vec<_> = answered
            .response_events
            .iter()
            .map(|e| e.source.channel_id.clone())
            .collect();
        assert_eq!(bodies, vec![id("a")]);
        // It goes on no transport twice, and is committed as an answer.
        assert_eq!(ops.texts(), vec!["a answers".to_owned()]);

        // Past the discussion's own limit: not asked, and recorded blocked.
        *held.turn.lock().unwrap() = Some(turn_on(asked.clone(), "b", 9, &[]));
        let stopped = kit
            .run_discussion_turn(&room_id(), unnamed(&kit))
            .await
            .unwrap();
        crate::testing::idle(&kit).await;
        assert_eq!(stopped.response_events, Vec::new());
        let timeline = kit
            .store()
            .list_events(
                &room_id(),
                crate::store::ListEventsOptions {
                    event_filter: Some(crate::store::EventFilter {
                        include_blocked: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let blocked: Vec<_> = timeline
            .iter()
            .filter(|event| event.status == EventStatus::Blocked)
            .map(|event| (event.source.channel_id.clone(), event.blocked_by.clone()))
            .collect();
        assert_eq!(
            blocked,
            vec![(id("b"), Some("event_chain_depth_limit".to_owned()))]
        );
    }

    #[tokio::test]
    async fn no_turn_runs_nothing() {
        let held = Arc::new(Held::default());
        let (kit, _ops) = held_room(&held).await;

        let result = kit
            .run_discussion_turn(&room_id(), unnamed(&kit))
            .await
            .unwrap();

        assert_eq!(result, InboundResult::default());
    }

    // Python: HookTrigger.ON_SPEAK_QUEUE takes a SpeakQueueEvent, and no other payload.
    #[tokio::test]
    async fn on_speak_queue_takes_its_own_payload() {
        let kit = RoomKit::new(crate::kit::RoomKitOptions::default());
        let speak_queue = |_: crate::SpeakQueueEvent, _: RoomContext| async {};
        kit.hook(
            HookTrigger::OnSpeakQueue,
            speak_queue,
            HookOptions::default(),
        )
        .unwrap();
        let event = |_: RoomEvent, _: RoomContext| async {};
        assert!(
            kit.hook(HookTrigger::OnSpeakQueue, event, HookOptions::default())
                .is_err()
        );
    }

    #[test]
    fn a_mark_is_read_until_it_is_retired() {
        let mark = crate::issue_mark(vec!["one".to_owned(), String::new()]);
        let mut event = RoomEvent::new(
            room_id(),
            roomkit_core::model::EventSource::new(id("ops"), ChannelType::SMS),
            TextContent::new("hello"),
        );
        event.metadata.insert(
            roomkit_core::rules::discussion::TURN_KEY.to_owned(),
            mark.clone(),
        );
        assert_eq!(discussion_notes(&event), vec!["one".to_owned()]);
        crate::retire_mark(&mark);
        assert_eq!(discussion_notes(&event), Vec::<String>::new());
    }

    // An uninstall releases the room's discussion, then forgets its stored
    // configuration: a context its own driver's turn reads in between is
    // followed by no new seam, which would outlive the uninstall.
    #[tokio::test]
    async fn a_room_whose_discussion_is_uninstalled_here_is_followed_by_none() {
        let held = Arc::new(Held::default());
        let (kit, _) = held_room(&held).await;
        let room = kit.store().get_room(&room_id()).await.unwrap().unwrap();
        kit.inner.discussions.release(&room_id());

        let leaving = kit.inner.discussions.leaving_room(&room_id());
        kit.follow_discussion(&room);
        assert!(kit.inner.discussions.get(&room_id()).is_none());
        drop(leaving);
        // Stored still once the uninstall gave up: followed again.
        kit.follow_discussion(&room);
        assert!(kit.inner.discussions.get(&room_id()).is_some());
        kit.close().await.unwrap();
    }
}
