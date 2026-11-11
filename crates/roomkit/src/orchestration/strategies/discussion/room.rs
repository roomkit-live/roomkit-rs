//! One room's discussion in this process, what fills its queue and its hooks
//! (RFC §19.7.5): Python's `orchestration/strategies/discussion/_room.py`, as
//! far as lot 8c. The kit tells it what the room commits, under the room's
//! lock; every change of the queue reads and writes the stored queue under
//! that lock, so that the queue moves in the order of the room's commits.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use roomkit_core::model::hook::{AllowOptions, BlockOptions, ModifyOptions};
use roomkit_core::model::{
    ChannelCategory, ChannelId, ChannelType, EventStatus, EventType, HookResult, Metadata,
    RoomContext, RoomEvent, RoomId,
};
use roomkit_core::ops::NextTurn;
use roomkit_core::rules::discussion::config::DiscussionConfig;
use roomkit_core::rules::discussion::names::{SilentToken, read_names};
use roomkit_core::rules::discussion::people::{
    PeopleIndex, is_person, names_of, one_person, people_index, person_label, sees,
};
use roomkit_core::rules::discussion::{CONFIG_KEY, STATE_KEY};
use roomkit_core::rules::transcoding::content_words;
use serde_json::Value;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use super::dispatch::{DispatchDecisionEvent, DispatchPolicy};
use super::driver::now;
use super::queue::{Ask, Entry, Pending, SpeakQueueState};
use super::{DoneFn, SpeakQueue, SpeakQueueChange, SpeakQueueEvent};
use crate::channel::BoxFuture;
use crate::context::{ExecutionContext, Section};
use crate::delivery_keys::Named;
use crate::error::RoomKitError;
use crate::hooks::HookRegistration;
use crate::kit::discussions::DiscussionSeam;
use crate::kit::{HasHooksOptions, RoomKit, SteerOptions, WeakRoomKit};
use crate::steering::Cancel;
use crate::store::{ConversationStore, PatchRoomMetadataOptions};
use crate::tasks::Kind;
use crate::{HookExecution, HookTrigger};

/// The `blocked_by` of an agent's row that says nothing (rule 13).
pub(crate) const SILENT: &str = "discussion_silent";

/// The hook that reads the names a message addresses (rule 4).
pub(crate) const NAMES: &str = "discussion_names";

/// How long a cut waits for the turn it cut to be over, as Python's
/// `cancel_and_wait`.
const CUT_WAIT: Duration = Duration::from_secs(5);

/// A turn given and not ended yet.
#[derive(Clone, Debug)]
pub(super) struct GivenTurn {
    pub(super) entry: Entry,
    pub(super) mark: Value,
    /// Its agent only listens, as last read: a person named it (rule 12),
    /// and only setting the state while the turn runs cuts it.
    pub(super) listening: bool,
    /// What another process stored cut it already.
    pub(super) cut: bool,
}

/// What this process holds of the room's discussion.
#[derive(Debug, Default)]
pub(super) struct Live {
    pub(super) queue: SpeakQueueState,
    pub(super) instructions: HashMap<String, RoomEvent>,
    /// What the speaking agent committed during its turn, read at its end.
    pub(super) turn_rows: Vec<RoomEvent>,
    /// The turn this process gave.
    pub(super) turn: Option<GivenTurn>,
    /// What `done` answered off the lock, for the turn the lock's section
    /// plans: `None` when the queue peeked held nothing to ask it about.
    pub(super) done: Option<bool>,
    /// The version of the queue the last look planned from.
    pub(super) seen_version: Option<u64>,
    /// What cuts the delivery of the look's turn, while it runs.
    pub(super) cutter: Option<Arc<Named>>,
    /// The message waiting for the dispatch policy the look's plan found
    /// first, which the look has decided off the room's lock (rule 18).
    pub(super) deciding: Option<Pending>,
    /// Whether the latest queue edit read a stored discussion.
    pub(super) stored: bool,
    pub(super) closed: bool,
    pub(super) forgotten: bool,
}

/// One change of the queue, told once the store holds it.
pub(super) type Change = (SpeakQueueChange, Vec<ChannelId>, Option<String>);

/// What a host installs in this process: the configuration it gives turns
/// by, its strategy's `done` and dispatch policy (its bound with it), and
/// the address of the strategy object, which tells one install from
/// another (Python's `strategy is`).
pub(crate) struct Install {
    pub(crate) config: DiscussionConfig,
    pub(crate) done: Option<DoneFn>,
    pub(crate) dispatch: Option<(Arc<dyn DispatchPolicy>, Duration)>,
    pub(crate) owner: usize,
}

/// The discussion a room holds, as this process takes part in it.
pub(crate) struct DiscussionRoom<S> {
    pub(super) kit: WeakRoomKit<S>,
    /// Itself, for the driver an install over a followed discussion starts.
    this: Weak<Self>,
    pub(super) room_id: RoomId,
    pub(super) organization_id: Option<String>,
    /// Who this process is, as the lease names it (rule 16).
    pub(super) me: String,
    /// The configuration it routes and queues by: the one the room stores,
    /// as last read, which another process's install may have replaced
    /// (rule 16).
    routed: Mutex<Arc<DiscussionConfig>>,
    /// What the host installed in this process, if it installed one: a
    /// process that only follows the discussion another process installed
    /// has none, and gives no turn (rule 16).
    own: Mutex<Option<Arc<Install>>>,
    /// A driver was started: it gives the turns while this process's
    /// install is the one the room stores.
    pub(super) driven: AtomicBool,
    pub(super) live: Mutex<Live>,
    pub(super) wake: Notify,
    /// A wake not read yet.
    pub(super) woken: AtomicBool,
    /// Its last look found nothing to do, and no wake came since.
    pub(super) settled: AtomicBool,
    pub(super) stopping: AtomicBool,
    pub(super) task: Mutex<Option<JoinHandle<()>>>,
    /// Told each time a look's turn is over, run or cut.
    pub(super) turn_over: Notify,
    announcing: Arc<Mutex<Announcing>>,
}

/// The changes still to tell, and whether a task tells them.
#[derive(Debug, Default)]
struct Announcing {
    queued: VecDeque<Announcement>,
    running: bool,
}

/// What the room tells its hooks, in the order it happened.
#[derive(Debug)]
enum Announcement {
    /// A change of the speak queue, to `ON_SPEAK_QUEUE`.
    Queue(SpeakQueueEvent),
    /// A dispatch decision applied, to `ON_DISPATCH_DECISION`.
    Decision(Box<DispatchDecisionEvent>),
}

impl<S: ConversationStore> DiscussionRoom<S> {
    /// The discussion the host installs in this process, `install`.
    pub(crate) fn new(
        kit: &RoomKit<S>,
        room_id: RoomId,
        organization_id: Option<String>,
        install: Install,
    ) -> Arc<Self> {
        let config = install.config.clone();
        Self::of(kit, room_id, organization_id, config, Some(install))
    }

    /// The discussion another process installed, `config` as the room
    /// stores it: this process queues and routes by it, and gives no turn
    /// (rule 16). Python's `DiscussionRoom.following`.
    pub(crate) fn following(
        kit: &RoomKit<S>,
        room_id: RoomId,
        organization_id: Option<String>,
        config: DiscussionConfig,
    ) -> Arc<Self> {
        Self::of(kit, room_id, organization_id, config, None)
    }

    fn of(
        kit: &RoomKit<S>,
        room_id: RoomId,
        organization_id: Option<String>,
        config: DiscussionConfig,
        install: Option<Install>,
    ) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            kit: kit.downgrade(),
            this: Weak::clone(this),
            room_id,
            organization_id,
            me: kit.inner.discussions.me(),
            routed: Mutex::new(Arc::new(config)),
            own: Mutex::new(install.map(Arc::new)),
            driven: AtomicBool::new(false),
            live: Mutex::new(Live::default()),
            wake: Notify::new(),
            woken: AtomicBool::new(false),
            settled: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            task: Mutex::new(None),
            turn_over: Notify::new(),
            announcing: Arc::default(),
        })
    }

    /// The configuration it routes and queues by, as last read.
    pub(super) fn config(&self) -> Arc<DiscussionConfig> {
        Arc::clone(&self.routed.lock().unwrap_or_else(PoisonError::into_inner))
    }

    /// What the host installed in this process, if it installed one.
    fn own(&self) -> Option<Arc<Install>> {
        self.own
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The silence token of the configuration it routes by (rule 13).
    pub(super) fn silent(&self) -> SilentToken {
        SilentToken::new(self.config().silent_token.clone())
    }

    /// The `done` of the strategy the host installed here.
    pub(super) fn done_fn(&self) -> Option<DoneFn> {
        self.own().and_then(|own| own.done.clone())
    }

    /// The dispatch policy of the strategy the host installed here, and its
    /// bound (rule 18).
    pub(super) fn dispatch_policy(&self) -> Option<(Arc<dyn DispatchPolicy>, Duration)> {
        self.own().and_then(|own| own.dispatch.clone())
    }

    /// Routes and queues by `stored`, the configuration the room stores now,
    /// which another process may have replaced (rule 16). Python's
    /// `follow`.
    pub(super) fn follow(&self, stored: DiscussionConfig) {
        let mut routed = self.routed.lock().unwrap_or_else(PoisonError::into_inner);
        if **routed != stored {
            *routed = Arc::new(stored);
        }
    }

    /// Whether this process may give the turns: its host installed the
    /// discussion it routes by, the one the room stores as last read (rule
    /// 16). Python's `drives`.
    pub(super) fn gives_turns(&self) -> bool {
        let config = self.config();
        self.own()
            .is_some_and(|own| own.config.same_as(Some(&config)))
    }

    pub(super) fn state(&self) -> MutexGuard<'_, Live> {
        self.live.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) fn kit(&self) -> Result<RoomKit<S>, RoomKitError> {
        self.kit
            .upgrade()
            .ok_or_else(|| RoomKitError::Runtime("the kit is gone".to_owned()))
    }

    /// Stores the configuration and reads the queue a previous install left:
    /// an instruction that names no process kept its text in one gone, and
    /// is told dropped; one another process holds is handed over when its
    /// turn comes.
    pub(crate) async fn load(&self) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        let config = self.config();
        // Taking the place of the discussion the room stores, it keeps the
        // queue and the agents its installs attached, but for the agents it
        // lets go (rule 1, Python's `take_over`).
        let room = kit.store().get_room(&self.room_id).await?;
        let stored = room.and_then(|room| DiscussionConfig::stored(&room.metadata));
        let (gone, attached) = stored.map_or_else(Default::default, |stored| {
            let gone: Vec<ChannelId> = stored
                .agents
                .into_iter()
                .filter(|agent| !config.agents.contains(agent))
                .collect();
            let attached = stored
                .attached
                .into_iter()
                .filter(|agent| !gone.contains(agent))
                .collect();
            (gone, attached)
        });
        let storing = DiscussionConfig {
            attached,
            ..(*config).clone()
        };
        let config = serde_json::to_value(&storing)
            .map_err(|error| RoomKitError::Value(error.to_string()))?;
        let patch = Metadata::from_iter([(CONFIG_KEY.to_owned(), config)]);
        kit.store()
            .patch_room_metadata(&self.room_id, patch, PatchRoomMetadataOptions::default())
            .await?;
        let (legacy, dropped, ended, state) = self
            .editing(&kit, |state, _| {
                let (legacy, kept): (Vec<Entry>, Vec<Entry>) = std::mem::take(&mut state.entries)
                    .into_iter()
                    .partition(|entry| entry.instruction.is_some() && entry.holder.is_none());
                state.entries = kept;
                let dropped = state.drop_agents(&gone);
                // The wait may have been for an agent that left.
                let ended = state.end_wait_for_nobody();
                (legacy, dropped, ended, state.clone())
            })
            .await?;
        let mut changes = Vec::new();
        self.drop_instructions(&legacy, &state, &mut changes);
        self.drop_instructions(&dropped, &state, &mut changes);
        if ended {
            changes.push((SpeakQueueChange::Waiting, Vec::new(), None));
        }
        self.fire_all(changes);
        Ok(())
    }

    /// Takes the place of the discussion this process routes by with
    /// `install`, keeping the stored queue but for the agents `install` no
    /// longer holds (rule 1); the agents installs attached stay recorded,
    /// but for those. From then on this process may give the turns.
    /// Python's `take_over`.
    async fn take_over_install(&self, install: Install) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        let section = Section::enter(&kit.inner.locks, &self.room_id).await?;
        let taking = async {
            let room = kit.store().get_room(&self.room_id).await?;
            let stored = room.and_then(|room| DiscussionConfig::stored(&room.metadata));
            let (gone, attached) = stored.map_or_else(Default::default, |stored| {
                let gone: Vec<ChannelId> = stored
                    .agents
                    .into_iter()
                    .filter(|agent| !install.config.agents.contains(agent))
                    .collect();
                let attached = stored
                    .attached
                    .into_iter()
                    .filter(|agent| !gone.contains(agent))
                    .collect();
                (gone, attached)
            });
            let config = DiscussionConfig {
                attached,
                ..install.config.clone()
            };
            let value = serde_json::to_value(&config)
                .map_err(|error| RoomKitError::Value(error.to_string()))?;
            let patch = Metadata::from_iter([(CONFIG_KEY.to_owned(), value)]);
            kit.store()
                .patch_room_metadata(&self.room_id, patch, PatchRoomMetadataOptions::default())
                .await?;
            let (dropped, ended, state) = self
                .edit(&kit, |state, _| {
                    let dropped = state.drop_agents(&gone);
                    // The wait may have been for an agent that left.
                    let ended = state.end_wait_for_nobody();
                    (dropped, ended, state.clone())
                })
                .await?;
            *self.own.lock().unwrap_or_else(PoisonError::into_inner) = Some(Arc::new(install));
            self.follow(config);
            Ok::<_, RoomKitError>((dropped, ended, state))
        };
        let (dropped, ended, state) = section.scope(taking).await?;
        drop(section);
        let mut changes = Vec::new();
        self.drop_instructions(&dropped, &state, &mut changes);
        if ended {
            changes.push((SpeakQueueChange::Waiting, Vec::new(), None));
        }
        self.fire_all(changes);
        Ok(())
    }

    /// The room's hooks, after every other: an agent's silence blocked, then
    /// the names read in the text the event commits with (rules 4, 13).
    pub(crate) fn hooks(self: &Arc<Self>) -> [HookRegistration; 2] {
        let room: Weak<Self> = Arc::downgrade(self);
        let silence = move |event: RoomEvent, _context: RoomContext| {
            let room = room.upgrade();
            async move {
                match room {
                    Some(room) if room.is_silence(&event) => {
                        HookResult::block("silent", BlockOptions::default())
                    }
                    _ => HookResult::allow(AllowOptions::default()),
                }
            }
        };
        let room: Weak<Self> = Arc::downgrade(self);
        let names = move |event: RoomEvent, context: RoomContext| {
            let room = room.upgrade();
            async move {
                match room.and_then(|room| room.named(&event, &context)) {
                    Some(address) => {
                        let named = RoomEvent {
                            addressed_to: Some(address),
                            ..event
                        };
                        HookResult::modify(named, ModifyOptions::default())
                    }
                    None => HookResult::allow(AllowOptions::default()),
                }
            }
        };
        let only_messages = [EventType::Message].into_iter().collect();
        [
            HookRegistration {
                priority: i32::MAX - 1,
                name: SILENT.to_owned(),
                event_types: Some(std::collections::BTreeSet::clone(&only_messages)),
                ..HookRegistration::new(HookTrigger::BeforeBroadcast, HookExecution::Sync, silence)
            },
            HookRegistration {
                priority: i32::MAX,
                name: NAMES.to_owned(),
                event_types: Some(only_messages),
                ..HookRegistration::new(HookTrigger::BeforeBroadcast, HookExecution::Sync, names)
            },
        ]
    }

    /// Whether `event` is an agent's row that says nothing (rule 13).
    fn is_silence(&self, event: &RoomEvent) -> bool {
        let roomkit_core::model::EventContent::Text(text) = &event.content else {
            return false;
        };
        self.config().agents.contains(&event.source.channel_id)
            && self.silent().is_silent(&text.body)
    }

    /// The address of `event` once its names are read, when they add to it.
    fn named(&self, event: &RoomEvent, context: &RoomContext) -> Option<Vec<ChannelId>> {
        let words = content_words(&event.content).filter(|words| !words.is_empty())?;
        if event.addressed_to.as_deref() == Some(&[]) {
            return None;
        }
        let speaker = &event.source.channel_id;
        let config = self.config();
        let names = read_names(
            &words,
            &config.by_handle(),
            &self.people(context),
            config.agents.contains(speaker).then_some(speaker.as_str()),
        );
        let mut address = event.addressed_to.clone().unwrap_or_default();
        let before = address.len();
        for agent in names.agents {
            let Ok(agent) = ChannelId::new(agent) else {
                continue;
            };
            if !address.contains(&agent) {
                address.push(agent);
            }
        }
        (address.len() > before).then_some(address)
    }

    fn handles(&self) -> Vec<String> {
        let config = self.config();
        config
            .agents
            .iter()
            .map(|agent| config.handle(agent).to_owned())
            .collect()
    }

    pub(super) fn people(&self, context: &RoomContext) -> Vec<String> {
        names_of(&self.people_index(context))
    }

    fn people_index(&self, context: &RoomContext) -> PeopleIndex {
        people_index(context, &self.handles(), self.config().people.as_deref())
    }

    /// Changes the stored queue, the room's lock held by the caller: read,
    /// handed to `change`, written back when it changed, its version moved
    /// when the queue itself did. A room that stores no discussion when read
    /// gets nothing back (rule 1).
    pub(super) async fn edit<R>(
        &self,
        kit: &RoomKit<S>,
        change: impl FnOnce(&mut SpeakQueueState, &mut Live) -> R,
    ) -> Result<R, RoomKitError> {
        let room = kit.store().get_room(&self.room_id).await?;
        let metadata = room.map(|room| room.metadata).unwrap_or_default();
        // Routed by the configuration the room stores (rule 16).
        let stored = DiscussionConfig::stored(&metadata)
            .map(|config| self.follow(config))
            .is_some();
        let before = read_queue(&metadata);
        let mut state = before.clone();
        let result = {
            let mut live = self.state();
            live.stored = stored;
            let result = change(&mut state, &mut live);
            if stored && state != before && state.queue_differs(&before) {
                state.version += 1;
            }
            live.queue = state.clone();
            result
        };
        if stored && state != before {
            let value = serde_json::to_value(&state)
                .map_err(|error| RoomKitError::Value(error.to_string()))?;
            let patch = Metadata::from_iter([(STATE_KEY.to_owned(), value)]);
            kit.store()
                .patch_room_metadata(&self.room_id, patch, PatchRoomMetadataOptions::default())
                .await?;
        }
        Ok(result)
    }

    /// [`edit`](Self::edit), taking the room's lock.
    pub(super) async fn editing<R: Send>(
        &self,
        kit: &RoomKit<S>,
        change: impl FnOnce(&mut SpeakQueueState, &mut Live) -> R + Send,
    ) -> Result<R, RoomKitError> {
        let section = Section::enter(&kit.inner.locks, &self.room_id).await?;
        section.scope(self.edit(kit, change)).await
    }

    // -- Listening only (rule 12) --

    /// Has `channel_ids`, those of its agents not listening yet, only
    /// listen; the turn one of them takes in this process is cut.
    async fn listen(&self, channel_ids: Vec<ChannelId>) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        let (agents, speaking) = self
            .editing(&kit, |state, _| {
                let time = now(&kit);
                let agents: Vec<ChannelId> = channel_ids
                    .into_iter()
                    .filter(|agent| {
                        self.config().agents.contains(agent) && !state.listening.contains(agent)
                    })
                    .collect();
                state.listening.extend(agents.iter().cloned());
                let speaking = state
                    .speaking
                    .clone()
                    .filter(|_| state.holds(&self.me, time));
                (agents, speaking)
            })
            .await?;
        if !agents.is_empty() {
            self.fire(SpeakQueueChange::Listening, agents.clone(), None);
        }
        // Here, the turn runs in this process; elsewhere, its holder cuts it
        // once it reads the queue again.
        if let Some(speaking) = speaking.filter(|speaking| agents.contains(speaking)) {
            self.cut(&kit, &speaking, "listen_only").await?;
        }
        Ok(())
    }

    /// Cuts `agent`'s running turn, as a `Cancel` does; a turn given but not
    /// started yet has no loop a `Cancel` reaches, and its delivery is
    /// cancelled.
    pub(super) async fn cut(
        &self,
        kit: &RoomKit<S>,
        agent: &ChannelId,
        reason: &str,
    ) -> Result<(), RoomKitError> {
        // Its turn runs through the turn backend: the owner steers it.
        let reached = if kit.inner.channel(agent).is_some() {
            let cancel = Cancel {
                reason: reason.to_owned(),
            };
            kit.steer(&self.room_id, agent, cancel, SteerOptions::default())
                .await?
        } else {
            0
        };
        if reached == 0 {
            self.cut_given(agent, reason).await;
        }
        Ok(())
    }

    /// Cancels the delivery of `agent`'s turn when this process gave it,
    /// and waits for it to be over, briefly: never from a caller the wait
    /// would wait for itself, one holding the room's lock or delivering in
    /// it (Python's `waiter_would_deadlock`).
    async fn cut_given(&self, agent: &ChannelId, reason: &str) {
        let over = self.turn_over.notified();
        let mut over = std::pin::pin!(over);
        over.as_mut().enable();
        let cutter = {
            let live = self.state();
            live.turn
                .as_ref()
                .filter(|turn| &turn.entry.agent == agent)
                .and(live.cutter.clone())
        };
        let Some(cutter) = cutter else {
            return;
        };
        cutter.cut(reason);
        if ExecutionContext::current().waits_for_itself(&self.room_id) {
            return;
        }
        if tokio::time::timeout(CUT_WAIT, over).await.is_err() {
            tracing::warn!(
                target: "roomkit::orchestration::discussion",
                room_id = %self.room_id,
                %agent,
                "a discussion's turn cut was not over in time"
            );
        }
    }

    /// Ends the listening state of `channel_ids`; a wait that was for them
    /// ends.
    async fn talk(&self, channel_ids: Vec<ChannelId>) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        let (agents, ended) = self
            .editing(&kit, |state, _| {
                let agents: Vec<ChannelId> = channel_ids
                    .into_iter()
                    .filter(|agent| state.listening.contains(agent))
                    .collect();
                state.listening.retain(|agent| !agents.contains(agent));
                // The wait may have been for an agent that only listened.
                (agents, state.end_wait_for_nobody())
            })
            .await?;
        if !agents.is_empty() {
            self.fire(SpeakQueueChange::TalkingAgain, agents, None);
        }
        if ended {
            self.fire(SpeakQueueChange::Waiting, Vec::new(), None);
        }
        self.rouse();
        Ok(())
    }

    // -- What the room commits --

    async fn committed(
        &self,
        event: RoomEvent,
        reach: Vec<ChannelId>,
        context: RoomContext,
    ) -> Result<(), RoomKitError> {
        if event.r#type != EventType::Message || event.status == EventStatus::Blocked {
            return Ok(());
        }
        if self.state().closed {
            return Ok(());
        }
        self.hand_to_memories(&event, &reach).await?;
        let speaker = event.source.channel_id.clone();
        {
            let mut live = self.state();
            if live
                .turn
                .as_ref()
                .is_some_and(|turn| turn.entry.agent == speaker)
            {
                live.turn_rows.push(event);
                return Ok(());
            }
        }
        if !self.config().agents.contains(&speaker) && is_person(&event, &context) {
            self.route_person(&event, &context, &reach).await?;
            // A person's message ends a wait even when it asks for no one.
            self.rouse();
        } else if self.queue_named(&event, &reach, &speaker).await? {
            self.rouse();
        }
        Ok(())
    }

    /// Hands `event` to the memory of every agent it reaches but its author,
    /// as a room with no discussion delivers it (rule 3): a memory that
    /// learns as messages arrive learns the whole conversation. It reaches
    /// each agent through the turn backend, where the agent runs (RFC
    /// §10.6.3); what an agent raises is logged there.
    async fn hand_to_memories(
        &self,
        event: &RoomEvent,
        reach: &[ChannelId],
    ) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        for agent in &self.config().agents {
            if *agent == event.source.channel_id || !reach.contains(agent) {
                continue;
            }
            let Some(channel) = kit.inner.channel(agent) else {
                continue;
            };
            kit.inner
                .turns
                .committed(agent, &self.room_id, event, Some(channel))
                .await;
        }
        Ok(())
    }

    /// A person's message: the agents it addresses first; with no address
    /// and no name, the agents that asked that person, else `everyone`, or
    /// those the dispatch policy picks; each only when the message reaches
    /// it.
    async fn route_person(
        &self,
        event: &RoomEvent,
        context: &RoomContext,
        reach: &[ChannelId],
    ) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        let who = person_label(event, context);
        let unaddressed = self.unaddressed(event, context);
        let changes = self
            .edit(&kit, |state, live| {
                if takes_nothing(state, live) {
                    return Vec::new();
                }
                let config = self.config();
                let mut changes: Vec<Change> = Vec::new();
                let dispatched = config.dispatch && unaddressed;
                let asks: Vec<ChannelId> = match &event.addressed_to {
                    None if dispatched => {
                        if let Some((agents, event_id)) =
                            self.leave_for_dispatch(state, event, &who, reach)
                            && !agents.is_empty()
                        {
                            changes.push((SpeakQueueChange::Queued, agents, Some(event_id)));
                        }
                        Vec::new()
                    }
                    Some(address) => address
                        .iter()
                        .filter(|agent| config.agents.contains(agent))
                        .cloned()
                        .collect(),
                    None if unaddressed => {
                        let asking = state.asking(Some(&who));
                        if asking.is_empty() {
                            self.everyone()
                        } else {
                            asking
                        }
                    }
                    None => Vec::new(),
                };
                let agents: Vec<ChannelId> = asks
                    .into_iter()
                    .filter(|agent| reach.contains(agent))
                    .collect();
                for agent in &agents {
                    let ask = Ask {
                        event_id: event.id.to_string(),
                        depth: event.chain_depth,
                        asker: who.clone(),
                        index: event.index,
                        person: true,
                        named: event.addressed_to.is_some(),
                    };
                    state.ask(agent, ask, true, None);
                }
                // Routed first, then what was asked of them is answered.
                let answered = config.addressed_only.then_some(agents.as_slice());
                state.clear_asked(Some(&who), answered);
                let was_waiting = state.person_wrote();
                if !agents.is_empty() {
                    changes.push((SpeakQueueChange::Queued, agents, Some(event.id.to_string())));
                }
                if was_waiting {
                    changes.push((SpeakQueueChange::Waiting, Vec::new(), None));
                }
                changes
            })
            .await?;
        self.fire_all(changes);
        Ok(())
    }

    /// Leaves an unaddressed message to the dispatch policy: the process
    /// holding the lease decides, off the lock, before its next turn (rule
    /// 18). The candidates are the agents that asked its author, then
    /// `everyone`. Once too many wait, the oldest asks what it asks with no
    /// policy; those agents and that message are returned.
    fn leave_for_dispatch(
        &self,
        state: &mut SpeakQueueState,
        event: &RoomEvent,
        who: &str,
        reach: &[ChannelId],
    ) -> Option<(Vec<ChannelId>, String)> {
        let asked: Vec<ChannelId> = state
            .asking(Some(who))
            .into_iter()
            .filter(|agent| reach.contains(agent))
            .collect();
        let mut candidates: Vec<ChannelId> = Vec::new();
        for agent in asked.iter().chain(&self.everyone()) {
            if reach.contains(agent) && !candidates.contains(agent) {
                candidates.push(agent.clone());
            }
        }
        if candidates.is_empty() {
            return None;
        }
        let pending = Pending {
            event_id: event.id.to_string(),
            depth: event.chain_depth,
            asker: who.to_owned(),
            index: event.index,
            candidates,
            asked,
            seq: state.reserve_seq(),
        };
        let oldest = state.wait_for_decision(pending)?;
        tracing::warn!(
            target: "roomkit::orchestration::discussion",
            room_id = %self.room_id,
            event_id = %oldest.event_id,
            "too many messages wait for a dispatch decision; the oldest goes undecided"
        );
        let agents: Vec<ChannelId> = oldest
            .undecided()
            .into_iter()
            .filter(|agent| !state.listening.contains(agent))
            .collect();
        state.queue_picked(&oldest, &agents);
        Some((agents, oldest.event_id))
    }

    /// A message with no address and no name, which `addressed_only` does
    /// not keep from every agent (rule 8).
    fn unaddressed(&self, event: &RoomEvent, context: &RoomContext) -> bool {
        let config = self.config();
        if event.addressed_to.is_some() || config.addressed_only {
            return false;
        }
        let Some(words) = content_words(&event.content).filter(|words| !words.is_empty()) else {
            return true;
        };
        let names = read_names(&words, &config.by_handle(), &self.people(context), None);
        names.people.is_empty()
    }

    pub(super) fn everyone(&self) -> Vec<ChannelId> {
        let config = self.config();
        config
            .everyone
            .clone()
            .unwrap_or_else(|| config.agents.clone())
    }

    /// An event from an agent outside its turn, or from any other sender:
    /// the agents its address names and it reaches, at the back; whether it
    /// queued one.
    async fn queue_named(
        &self,
        event: &RoomEvent,
        reach: &[ChannelId],
        asker: &ChannelId,
    ) -> Result<bool, RoomKitError> {
        let named: Vec<ChannelId> = event
            .addressed_to
            .iter()
            .flatten()
            .filter(|agent| {
                self.config().agents.contains(agent) && *agent != asker && reach.contains(agent)
            })
            .cloned()
            .collect();
        if named.is_empty() {
            return Ok(false);
        }
        let kit = self.kit()?;
        let queued = self
            .edit(&kit, |state, live| {
                if takes_nothing(state, live) {
                    return false;
                }
                for agent in &named {
                    let ask = Ask {
                        event_id: event.id.to_string(),
                        depth: event.chain_depth,
                        asker: asker.to_string(),
                        index: event.index,
                        person: false,
                        named: false,
                    };
                    state.ask(agent, ask, false, None);
                }
                true
            })
            .await?;
        if queued {
            self.fire(SpeakQueueChange::Queued, named, Some(event.id.to_string()));
        }
        Ok(queued)
    }

    /// An instruction addressed to agents: a turn of its own each, at the
    /// front, taking the instruction as its input (rule 7).
    async fn instruction(&self, event: RoomEvent) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        let agents: Vec<ChannelId> = event
            .addressed_to
            .iter()
            .flatten()
            .filter(|agent| self.config().agents.contains(agent))
            .cloned()
            .collect();
        let id = event.id.to_string();
        // This process keeps the instruction's text; one that gives no turn
        // (it only follows the discussion, or its host installed another)
        // tells it dropped.
        if !self.gives_turns() {
            self.fire(SpeakQueueChange::InstructionDropped, agents, Some(id));
            return Ok(());
        }
        let taken = self
            .edit(&kit, |state, live| {
                if takes_nothing(state, live) {
                    return false;
                }
                if !agents.is_empty() {
                    live.instructions.insert(id.clone(), event.clone());
                }
                for agent in &agents {
                    state.queue_instruction(
                        agent,
                        &id,
                        event.chain_depth,
                        Some(self.me.clone()),
                        false,
                    );
                }
                true
            })
            .await?;
        if !taken {
            self.fire(SpeakQueueChange::InstructionDropped, agents, Some(id));
        } else if !agents.is_empty() {
            self.fire(SpeakQueueChange::Queued, agents, Some(id));
            self.rouse();
        }
        Ok(())
    }

    /// What one of a turn's delivered rows asks, once the turn has ended: the
    /// agents it names, at the back, and the people it names, as asked (rules
    /// 5 and 10); the agents queued.
    pub(super) fn queue_turn_names(
        &self,
        state: &mut SpeakQueueState,
        agent: &ChannelId,
        event: &RoomEvent,
        context: &RoomContext,
    ) -> Vec<ChannelId> {
        if event.r#type != EventType::Message
            || event.status == EventStatus::Blocked
            || &event.source.channel_id != agent
        {
            return Vec::new();
        }
        let config = self.config();
        let asked: Vec<ChannelId> = event
            .addressed_to
            .iter()
            .flatten()
            .filter(|other| {
                config.agents.contains(other) && *other != agent && sees(other, event, context)
            })
            .cloned()
            .collect();
        for other in &asked {
            let ask = Ask {
                event_id: event.id.to_string(),
                depth: event.chain_depth,
                asker: agent.to_string(),
                index: event.index,
                person: false,
                named: false,
            };
            state.ask(other, ask, false, None);
        }
        if let Some(words) = content_words(&event.content).filter(|words| !words.is_empty()) {
            let index = self.people_index(context);
            let names = read_names(&words, &config.by_handle(), &names_of(&index), None);
            for name in names.people {
                if let Some(label) = one_person(&index, &name) {
                    state.record_asked(agent, label);
                }
            }
        }
        asked
    }

    // -- Announcements --

    pub(super) fn fire_all(&self, changes: Vec<Change>) {
        for (change, channel_ids, event_id) in changes {
            self.fire(change, channel_ids, event_id);
        }
    }

    /// Tells `ON_SPEAK_QUEUE` of a change of the queue, in the order the
    /// changes happened: each announcement waits for the previous.
    pub(super) fn fire(
        &self,
        change: SpeakQueueChange,
        channel_ids: Vec<ChannelId>,
        event_id: Option<String>,
    ) {
        let Some(kit) = self.kit.upgrade() else {
            return;
        };
        let options = HasHooksOptions {
            trigger: Some(HookTrigger::OnSpeakQueue),
        };
        if !kit.hook_engine().has_hooks(options) {
            return;
        }
        let queue = self.state().queue.view();
        let mut event = SpeakQueueEvent::new(self.room_id.clone(), queue, change);
        event.channel_ids = channel_ids;
        event.event_id = event_id.and_then(|id| roomkit_core::model::EventId::new(id).ok());
        self.announce(&kit, Announcement::Queue(event));
    }

    /// Tells `ON_DISPATCH_DECISION` of a decision applied, in the order of
    /// the queue's changes.
    pub(super) fn tell_decision(&self, event: DispatchDecisionEvent) {
        let Some(kit) = self.kit.upgrade() else {
            return;
        };
        let options = HasHooksOptions {
            trigger: Some(HookTrigger::OnDispatchDecision),
        };
        if kit.hook_engine().has_hooks(options) {
            self.announce(&kit, Announcement::Decision(Box::new(event)));
        }
    }

    /// Runs `announcement`'s hooks once the announcements before it have
    /// started, without holding up the turns.
    fn announce(&self, kit: &RoomKit<S>, announcement: Announcement) {
        let start = {
            let mut announcing = self
                .announcing
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            announcing.queued.push_back(announcement);
            !std::mem::replace(&mut announcing.running, true)
        };
        if start {
            let inner = Arc::clone(&kit.inner);
            let announcing = Arc::clone(&self.announcing);
            let telling = async move {
                while let Some(announcement) = next_announcement(&announcing) {
                    match announcement {
                        Announcement::Queue(event) => inner.observe_speak_queue(event).await,
                        Announcement::Decision(event) => {
                            inner.observe_dispatch_decision(*event).await;
                        }
                    }
                }
            };
            kit.inner.tasks.spawn(Kind::Observer, telling);
        }
    }

    /// Wakes the driver. In this order: a driver about to read itself settled
    /// reads the wake, or has its reading overwritten.
    pub(super) fn rouse(&self) {
        self.woken.store(true, Ordering::SeqCst);
        self.settled.store(false, Ordering::SeqCst);
        self.wake.notify_one();
    }

    /// Stores `agents`, which an install attached to the room, with the
    /// configuration, for any process uninstalling the discussion to detach
    /// them (RFC §19.7). Python's `hold_attached`.
    async fn attached(&self, agents: Vec<ChannelId>) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        let section = Section::enter(&kit.inner.locks, &self.room_id).await?;
        section
            .scope(async {
                let room = kit.store().get_room(&self.room_id).await?;
                let metadata = room.map(|room| room.metadata).unwrap_or_default();
                let Some(mut stored) = DiscussionConfig::stored(&metadata) else {
                    return Ok(());
                };
                stored.attached.extend(agents);
                stored.attached.sort();
                stored.attached.dedup();
                let config = serde_json::to_value(&stored)
                    .map_err(|error| RoomKitError::Value(error.to_string()))?;
                let patch = Metadata::from_iter([(CONFIG_KEY.to_owned(), config)]);
                kit.store()
                    .patch_room_metadata(&self.room_id, patch, PatchRoomMetadataOptions::default())
                    .await?;
                Ok(())
            })
            .await
    }

    /// Leaves the discussion another process uninstalled, writing nothing
    /// back: its queue is gone, so a turn cut on the way records nothing
    /// (rule 1), and no turn is given here again (rule 16). Python's
    /// `_leave_uninstalled`, the record dropped by the kit.
    async fn leave_uninstalled(&self) {
        {
            let mut live = self.state();
            live.closed = true;
            live.forgotten = true;
        }
        self.stop_driver().await;
    }

    fn category_refusal(
        &self,
        channel_id: &ChannelId,
        category: ChannelCategory,
        channel_type: &ChannelType,
    ) -> Option<String> {
        binding_refusal(
            &self.config().agents,
            channel_id,
            category,
            Some(channel_type),
        )
    }
}

/// Whether the queue, read as `state`, takes no turn: the discussion is over,
/// or this process left it.
fn takes_nothing(state: &SpeakQueueState, live: &Live) -> bool {
    state.over || live.closed
}

/// Why a room holding a discussion of `agents` cannot bind `channel_id`, of
/// `channel_type` when the kit holds it, if it cannot (rule 1).
pub(crate) fn binding_refusal(
    agents: &[ChannelId],
    channel_id: &ChannelId,
    category: ChannelCategory,
    channel_type: Option<&ChannelType>,
) -> Option<String> {
    let spoken = [
        ChannelType::VOICE,
        ChannelType::REALTIME_VOICE,
        ChannelType::REALTIME_AUDIO_VIDEO,
        ChannelType::AUDIO_VIDEO,
    ];
    if channel_type.is_some_and(|channel_type| spoken.contains(channel_type)) {
        return Some(format!("{channel_id} is a voice or realtime channel"));
    }
    if category == ChannelCategory::Intelligence && !agents.contains(channel_id) {
        return Some(format!(
            "{channel_id} is an intelligence channel that is not one of its agents"
        ));
    }
    None
}

/// The speak queue a room's `metadata` stores.
pub(super) fn read_queue(metadata: &Metadata) -> SpeakQueueState {
    metadata
        .get(STATE_KEY)
        .filter(|stored| !stored.is_null())
        .and_then(|stored| serde_json::from_value(stored.clone()).ok())
        .unwrap_or_default()
}

/// The next change to tell; none left, the telling ends.
fn next_announcement(announcing: &Mutex<Announcing>) -> Option<Announcement> {
    let mut announcing = announcing.lock().unwrap_or_else(PoisonError::into_inner);
    let next = announcing.queued.pop_front();
    if next.is_none() {
        announcing.running = false;
    }
    next
}

impl<S: ConversationStore> DiscussionSeam for DiscussionRoom<S> {
    fn on_committed(
        &self,
        event: RoomEvent,
        reach: Vec<ChannelId>,
        context: RoomContext,
    ) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(self.committed(event, reach, context))
    }

    fn queue_instruction(&self, event: RoomEvent) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(self.instruction(event))
    }

    fn queue_regeneration(
        &self,
        event: RoomEvent,
        context: RoomContext,
        floor: u32,
    ) -> BoxFuture<'_, Result<roomkit_core::ops::InboundResult, RoomKitError>> {
        Box::pin(self.regenerate(event, context, floor))
    }

    fn next_turn(&self, context: RoomContext) -> BoxFuture<'_, Result<NextTurn, RoomKitError>> {
        Box::pin(self.planned_turn(context))
    }

    fn view(&self, organization_id: Option<&str>) -> Option<SpeakQueue> {
        self.in_scope(organization_id)
            .then(|| self.state().queue.view())
    }

    fn silence_for(&self, channel_id: &ChannelId) -> Option<SilentToken> {
        self.config()
            .agents
            .contains(channel_id)
            .then(|| self.silent().clone())
    }

    fn in_scope(&self, organization_id: Option<&str>) -> bool {
        organization_id.is_none() || self.organization_id.as_deref() == organization_id
    }

    fn listen_only(&self, channel_ids: Vec<ChannelId>) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(self.listen(channel_ids))
    }

    fn talk_again(&self, channel_ids: Vec<ChannelId>) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(self.talk(channel_ids))
    }

    fn settled(&self) -> bool {
        // A process that only follows has no driver to settle.
        !self.driven.load(Ordering::SeqCst)
            || self.settled.load(Ordering::SeqCst)
            || self.stopping.load(Ordering::SeqCst)
    }

    fn rouse(&self) {
        DiscussionRoom::rouse(self);
    }

    fn drives(&self, stored: &DiscussionConfig) -> bool {
        self.own()
            .is_some_and(|own| own.config.same_as(Some(stored)))
    }

    fn installed(&self) -> bool {
        self.own().is_some()
    }

    fn runs(&self, owner: usize) -> bool {
        self.own().is_some_and(|own| own.owner == owner)
    }

    fn forgotten(&self) -> bool {
        self.state().forgotten
    }

    fn follow(&self, stored: DiscussionConfig) {
        DiscussionRoom::follow(self, stored);
    }

    fn take_over(&self, install: Install) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(self.take_over_install(install))
    }

    fn drive(&self) {
        if self.driven.load(Ordering::SeqCst) {
            self.rouse();
        } else if let Some(this) = self.this.upgrade() {
            this.start();
        }
    }

    fn hold_attached(&self, agents: Vec<ChannelId>) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(self.attached(agents))
    }

    fn leave(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.leave_uninstalled())
    }

    fn stop(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.stop_taking_part())
    }

    fn forget(&self) -> BoxFuture<'_, Result<(), RoomKitError>> {
        Box::pin(self.forget_discussion())
    }

    fn binding_refusal(
        &self,
        channel_id: &ChannelId,
        category: ChannelCategory,
        channel_type: &ChannelType,
    ) -> Option<String> {
        self.category_refusal(channel_id, category, channel_type)
    }
}
