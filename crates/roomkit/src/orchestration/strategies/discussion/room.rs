//! One room's discussion in this process, what fills its queue and its hooks
//! (RFC §19.7.5): Python's `orchestration/strategies/discussion/_room.py`, as
//! far as lot 8c. The kit tells it what the room commits, under the room's
//! lock; every change of the queue reads and writes the stored queue under
//! that lock, so that the queue moves in the order of the room's commits.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

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

use super::queue::{Ask, Entry, SpeakQueueState};
use super::{DoneFn, SpeakQueue, SpeakQueueChange, SpeakQueueEvent};
use crate::channel::BoxFuture;
use crate::context::Section;
use crate::error::RoomKitError;
use crate::hooks::HookRegistration;
use crate::kit::discussions::DiscussionSeam;
use crate::kit::{HasHooksOptions, RoomKit, WeakRoomKit};
use crate::store::{ConversationStore, PatchRoomMetadataOptions};
use crate::tasks::Kind;
use crate::{HookExecution, HookTrigger};

/// The `blocked_by` of an agent's row that says nothing (rule 13).
pub(super) const SILENT: &str = "discussion_silent";

/// The hook that reads the names a message addresses (rule 4).
pub(super) const NAMES: &str = "discussion_names";

/// A turn given and not ended yet.
#[derive(Clone, Debug)]
pub(super) struct GivenTurn {
    pub(super) entry: Entry,
    pub(super) mark: Value,
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
    pub(super) closed: bool,
    pub(super) forgotten: bool,
}

/// One change of the queue, told once the store holds it.
pub(super) type Change = (SpeakQueueChange, Vec<ChannelId>, Option<String>);

/// The discussion a room holds, as this process takes part in it.
pub(crate) struct DiscussionRoom<S> {
    pub(super) kit: WeakRoomKit<S>,
    pub(super) room_id: RoomId,
    pub(super) organization_id: Option<String>,
    /// Who this process is, as the lease names it (rule 16).
    pub(super) me: String,
    pub(super) config: DiscussionConfig,
    pub(super) silent: SilentToken,
    pub(super) done: Option<DoneFn>,
    pub(super) live: Mutex<Live>,
    pub(super) wake: Notify,
    /// A wake not read yet.
    pub(super) woken: AtomicBool,
    /// Its last look found nothing to do, and no wake came since.
    pub(super) settled: AtomicBool,
    pub(super) stopping: AtomicBool,
    pub(super) task: Mutex<Option<JoinHandle<()>>>,
    announcing: Arc<Mutex<Announcing>>,
}

/// The changes still to tell, and whether a task tells them.
#[derive(Debug, Default)]
struct Announcing {
    queued: VecDeque<SpeakQueueEvent>,
    running: bool,
}

impl<S: ConversationStore> DiscussionRoom<S> {
    /// The discussion the host installs in this process, configured by
    /// `config`, `done` the host's.
    pub(crate) fn new(
        kit: &RoomKit<S>,
        room_id: RoomId,
        organization_id: Option<String>,
        config: DiscussionConfig,
        done: Option<DoneFn>,
    ) -> Arc<Self> {
        Arc::new(Self {
            kit: kit.downgrade(),
            room_id,
            organization_id,
            me: kit.inner.discussions.me(),
            silent: SilentToken::new(config.silent_token.clone()),
            config,
            done,
            live: Mutex::new(Live::default()),
            wake: Notify::new(),
            woken: AtomicBool::new(false),
            settled: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            task: Mutex::new(None),
            announcing: Arc::default(),
        })
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
    /// an instruction's turn kept its text in a process gone.
    pub(crate) async fn load(&self) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        let config = serde_json::to_value(&self.config)
            .map_err(|error| RoomKitError::Value(error.to_string()))?;
        let patch = Metadata::from_iter([(CONFIG_KEY.to_owned(), config)]);
        kit.store()
            .patch_room_metadata(&self.room_id, patch, PatchRoomMetadataOptions::default())
            .await?;
        self.edit(&kit, |state, _| {
            state
                .entries
                .retain(|entry| entry.instruction.is_none() || entry.holder.is_some());
        })
        .await?;
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
        self.config.agents.contains(&event.source.channel_id) && self.silent.is_silent(&text.body)
    }

    /// The address of `event` once its names are read, when they add to it.
    fn named(&self, event: &RoomEvent, context: &RoomContext) -> Option<Vec<ChannelId>> {
        let words = content_words(&event.content).filter(|words| !words.is_empty())?;
        if event.addressed_to.as_deref() == Some(&[]) {
            return None;
        }
        let speaker = &event.source.channel_id;
        let names = read_names(
            &words,
            &self.config.by_handle(),
            &self.people(context),
            self.config
                .agents
                .contains(speaker)
                .then_some(speaker.as_str()),
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
        self.config
            .agents
            .iter()
            .map(|agent| self.config.handle(agent).to_owned())
            .collect()
    }

    pub(super) fn people(&self, context: &RoomContext) -> Vec<String> {
        names_of(&self.people_index(context))
    }

    fn people_index(&self, context: &RoomContext) -> PeopleIndex {
        people_index(context, &self.handles(), self.config.people.as_deref())
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
        let stored = DiscussionConfig::stored(&metadata).is_some();
        let before = read_queue(&metadata);
        let mut state = before.clone();
        let result = {
            let mut live = self.state();
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
        let speaker = event.source.channel_id.clone();
        {
            let mut live = self.state();
            if live.closed {
                return Ok(());
            }
            if live
                .turn
                .as_ref()
                .is_some_and(|turn| turn.entry.agent == speaker)
            {
                live.turn_rows.push(event);
                return Ok(());
            }
        }
        if !self.config.agents.contains(&speaker) && is_person(&event, &context) {
            self.route_person(&event, &context, &reach).await?;
            // A person's message ends a wait even when it asks for no one.
            self.rouse();
        } else if self.queue_named(&event, &reach, &speaker).await? {
            self.rouse();
        }
        Ok(())
    }

    /// A person's message: the agents it addresses first; with no address
    /// and no name, the agents that asked that person, else `everyone`; each
    /// only when the message reaches it.
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
                let asks: Vec<ChannelId> = match &event.addressed_to {
                    Some(address) => address
                        .iter()
                        .filter(|agent| self.config.agents.contains(agent))
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
                let answered = self.config.addressed_only.then_some(agents.as_slice());
                state.clear_asked(Some(&who), answered);
                let was_waiting = state.person_wrote();
                let mut changes: Vec<Change> = Vec::new();
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

    /// A message with no address and no name, which `addressed_only` does
    /// not keep from every agent (rule 8).
    fn unaddressed(&self, event: &RoomEvent, context: &RoomContext) -> bool {
        if event.addressed_to.is_some() || self.config.addressed_only {
            return false;
        }
        let Some(words) = content_words(&event.content).filter(|words| !words.is_empty()) else {
            return true;
        };
        let names = read_names(
            &words,
            &self.config.by_handle(),
            &self.people(context),
            None,
        );
        names.people.is_empty()
    }

    fn everyone(&self) -> Vec<ChannelId> {
        self.config
            .everyone
            .clone()
            .unwrap_or_else(|| self.config.agents.clone())
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
                self.config.agents.contains(agent) && *agent != asker && reach.contains(agent)
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
            .filter(|agent| self.config.agents.contains(agent))
            .cloned()
            .collect();
        let id = event.id.to_string();
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
        let asked: Vec<ChannelId> = event
            .addressed_to
            .iter()
            .flatten()
            .filter(|other| {
                self.config.agents.contains(other) && *other != agent && sees(other, event, context)
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
            let names = read_names(&words, &self.config.by_handle(), &names_of(&index), None);
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
        let start = {
            let mut announcing = self
                .announcing
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            announcing.queued.push_back(event);
            !std::mem::replace(&mut announcing.running, true)
        };
        if start {
            let inner = Arc::clone(&kit.inner);
            let announcing = Arc::clone(&self.announcing);
            let telling = async move {
                while let Some(event) = next_announcement(&announcing) {
                    inner.observe_speak_queue(event).await;
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

    fn category_refusal(
        &self,
        channel_id: &ChannelId,
        category: ChannelCategory,
        channel_type: &ChannelType,
    ) -> Option<String> {
        binding_refusal(
            &self.config.agents,
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
fn next_announcement(announcing: &Mutex<Announcing>) -> Option<SpeakQueueEvent> {
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

    fn next_turn(&self, context: RoomContext) -> BoxFuture<'_, Result<NextTurn, RoomKitError>> {
        Box::pin(self.planned_turn(context))
    }

    fn view(&self, organization_id: Option<&str>) -> Option<SpeakQueue> {
        if organization_id.is_some() && self.organization_id.as_deref() != organization_id {
            return None;
        }
        Some(self.state().queue.view())
    }

    fn settled(&self) -> bool {
        self.settled.load(Ordering::SeqCst) || self.stopping.load(Ordering::SeqCst)
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
