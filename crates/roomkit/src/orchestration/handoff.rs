//! The handoff protocol (RFC §19.6): Python's `orchestration/handoff.py`.
//!
//! An agent hands the conversation off through its `handoff_conversation`
//! tool: [`HandoffHandler::handle`] resolves the target, updates the room's
//! conversation state under its lock, records the handoff in the timeline,
//! says so on the status bus, and fires `ON_HANDOFF` and
//! `ON_PHASE_TRANSITION`, or refuses it and fires `ON_HANDOFF_REJECTED`.
//! The tool's definition stays JSON here: `AiTool`, `HANDOFF_TOOL`,
//! `build_handoff_tool` and `setup_handoff` are `roomkit_ai`'s (Python's
//! one package is two crates, `docs/architecture.md`).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, PoisonError, RwLock};

use roomkit_core::model::{ChannelCategory, ChannelId, Metadata, Room, RoomId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::channel::BoxFuture;
use crate::context::Section;
use crate::error::RoomKitError;
use crate::kit::{GetRoomOptions, RoomKit, WeakRoomKit};
use crate::orchestration::agents::HeldAgent;
use crate::orchestration::handoff_end::{accepted, moved_state, rejected};
pub(crate) use crate::orchestration::handoff_tool::{HANDOFF_TOOL_NAME, handoff_tool_definition};
use crate::orchestration::{
    ConversationRouter, ConversationState, StrategyAgent, get_conversation_state,
    save_conversation_state,
};
use crate::store::{ConversationStore, InMemoryStore};
use crate::turn::OwnerToolAnswer;
use crate::turns::served::OwnerToolEntry;

/// What an agent's handoff call asks: Python's `HandoffRequest`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HandoffRequest {
    /// The agent the conversation goes to, read through the aliases.
    pub target_agent_id: String,
    /// Why the handoff is needed.
    #[serde(default)]
    pub reason: String,
    /// What the next agent needs to know.
    #[serde(default)]
    pub summary: String,
    /// The call's arguments.
    #[serde(default)]
    pub context: Metadata,
    /// The channel to escalate to, `same` keeping the current one.
    #[serde(default)]
    pub channel_escalation: Option<String>,
    /// Whether the handoff is urgent.
    #[serde(default)]
    pub urgent: bool,
}

impl HandoffRequest {
    /// A request for `target_agent_id`, the rest Python's defaults.
    pub fn new(target_agent_id: impl Into<String>) -> Self {
        Self {
            target_agent_id: target_agent_id.into(),
            reason: String::new(),
            summary: String::new(),
            context: Metadata::new(),
            channel_escalation: None,
            urgent: false,
        }
    }
}

/// What the calling agent is answered: Python's `HandoffResult`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct HandoffResult {
    /// Whether the handoff went through.
    pub accepted: bool,
    /// The room's active agent now; `None` for a handoff to a human.
    pub new_agent_id: Option<String>,
    /// The room's phase now.
    pub new_phase: Option<String>,
    /// What the calling agent reads of an accepted handoff.
    pub message: String,
    /// Why it was refused.
    pub reason: String,
}

impl Default for HandoffResult {
    fn default() -> Self {
        Self {
            accepted: true,
            new_agent_id: None,
            new_phase: None,
            message: String::new(),
            reason: String::new(),
        }
    }
}

/// What runs after a handoff went through: Python's
/// `on_handoff_complete(room_id, result)`.
pub type OnHandoffComplete =
    Arc<dyn Fn(RoomId, HandoffResult) -> BoxFuture<'static, ()> + Send + Sync>;

/// What [`HandoffHandler::new`] takes beside the kit and the router:
/// Python's `HandoffHandler(kit, router, agent_aliases=None, phase_map=None,
/// allowed_transitions=None, known_agents=None, on_handoff_complete=None,
/// event_channel_id=None)`.
#[derive(Clone, Default)]
pub struct HandoffHandlerOptions {
    /// Each alias's agent id.
    pub agent_aliases: Option<HashMap<String, String>>,
    /// The phase each target leads to, `handling` for one it does not name.
    pub phase_map: Option<HashMap<String, String>>,
    /// Each phase's phases a handoff may lead to; none constrained when
    /// `None`.
    pub allowed_transitions: Option<HashMap<String, HashSet<String>>>,
    /// The targets taken though no binding of the room names them.
    pub known_agents: Option<HashSet<String>>,
    /// Runs after a handoff went through.
    pub on_handoff_complete: Option<OnHandoffComplete>,
    /// The channel the handoff's record is sent from, the calling agent's
    /// own when `None`.
    pub event_channel_id: Option<ChannelId>,
}

impl std::fmt::Debug for HandoffHandlerOptions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HandoffHandlerOptions")
            .field("agent_aliases", &self.agent_aliases)
            .field("phase_map", &self.phase_map)
            .field("allowed_transitions", &self.allowed_transitions)
            .field("known_agents", &self.known_agents)
            .field("event_channel_id", &self.event_channel_id)
            .finish_non_exhaustive()
    }
}

/// Processes handoff tool calls: Python's `HandoffHandler`. Cloning it is
/// cheap, the clones share its settings.
pub struct HandoffHandler<S = InMemoryStore> {
    inner: Arc<Inner<S>>,
}

impl<S> Clone for HandoffHandler<S> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<S> std::fmt::Debug for HandoffHandler<S> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("HandoffHandler")
            .field("aliases", &self.inner.aliases)
            .field("phase_map", &self.inner.phase_map)
            .finish_non_exhaustive()
    }
}

struct Inner<S> {
    kit: WeakRoomKit<S>,
    /// Its rules say the phases an agent holds.
    router: ConversationRouter,
    aliases: HashMap<String, String>,
    phase_map: HashMap<String, String>,
    allowed_transitions: Option<HashMap<String, HashSet<String>>>,
    known_agents: HashSet<String>,
    on_handoff_complete: RwLock<Option<OnHandoffComplete>>,
    event_channel_id: Option<ChannelId>,
    greeting_map: RwLock<HashMap<ChannelId, String>>,
    agents: RwLock<HashMap<ChannelId, StrategyAgent>>,
}

/// What the room's lock decided of a handoff.
enum Decided {
    Rejected(HandoffResult),
    Accepted {
        phase: String,
        agent: Option<String>,
    },
}

impl<S: ConversationStore> HandoffHandler<S> {
    /// A handler over `kit`, serving no agent yet: [`ConversationRouter::install`]
    /// or `roomkit_ai::setup_handoff` wires it to agents. The kit is held
    /// weakly: its owner tools hold the handler.
    pub fn new(
        kit: &RoomKit<S>,
        router: ConversationRouter,
        options: HandoffHandlerOptions,
    ) -> Self {
        let HandoffHandlerOptions {
            agent_aliases,
            phase_map,
            allowed_transitions,
            known_agents,
            on_handoff_complete,
            event_channel_id,
        } = options;
        Self {
            inner: Arc::new(Inner {
                kit: kit.downgrade(),
                router,
                aliases: agent_aliases.unwrap_or_default(),
                phase_map: phase_map.unwrap_or_default(),
                allowed_transitions,
                known_agents: known_agents.unwrap_or_default(),
                on_handoff_complete: RwLock::new(on_handoff_complete),
                event_channel_id,
                greeting_map: RwLock::default(),
                agents: RwLock::default(),
            }),
        }
    }

    /// The channel a handoff's record is sent from, when not the caller's.
    pub(super) fn event_channel(&self) -> Option<&ChannelId> {
        self.inner.event_channel_id.as_ref()
    }

    /// Each agent's greeting: Python's `greeting_map`.
    pub fn greeting_map(&self) -> HashMap<ChannelId, String> {
        read(&self.inner.greeting_map).clone()
    }

    /// Sets each agent's greeting: Python's `greeting_map = value`.
    pub fn set_greeting_map(&self, value: HashMap<ChannelId, String>) {
        *write(&self.inner.greeting_map) = value;
    }

    /// The agents the handler hands off between: Python's `agents`.
    pub fn agents(&self) -> HashMap<ChannelId, StrategyAgent> {
        read(&self.inner.agents).clone()
    }

    /// Sets the agents the handler hands off between: Python's
    /// `agents = value`.
    pub fn set_agents(&self, value: HashMap<ChannelId, StrategyAgent>) {
        *write(&self.inner.agents) = value;
    }

    /// What runs after a handoff went through: Python's
    /// `on_handoff_complete`.
    pub fn on_handoff_complete(&self) -> Option<OnHandoffComplete> {
        read(&self.inner.on_handoff_complete).clone()
    }

    /// Sets what runs after a handoff went through: Python's
    /// `on_handoff_complete = value`.
    pub fn set_on_handoff_complete(&self, value: Option<OnHandoffComplete>) {
        *write(&self.inner.on_handoff_complete) = value;
    }

    /// The room's effective language: the conversation state's, else
    /// `agent_id`'s own among the handler's agents. Python's
    /// `get_room_language(room, agent_id)`.
    ///
    /// # Errors
    ///
    /// What reading the room's conversation state fails with.
    pub fn get_room_language(
        &self,
        room: &Room,
        agent_id: Option<&ChannelId>,
    ) -> Result<Option<String>, RoomKitError> {
        let state = get_conversation_state(room)?;
        let language = state.context.get("language").and_then(Value::as_str);
        if let Some(language) = language.filter(|language| !language.is_empty()) {
            return Ok(Some(language.to_owned()));
        }
        let agent = agent_id.and_then(|id| read(&self.inner.agents).get(id).cloned());
        Ok(agent.and_then(|agent| self.agent_language(&agent)))
    }

    /// `agent`'s own language: a live one's, or the one its id names on the
    /// kit declares; none for an id nothing holds.
    fn agent_language(&self, agent: &StrategyAgent) -> Option<String> {
        if let Some(live) = agent.live() {
            return live.channel.declaration().identity?.language;
        }
        let kit = self.inner.kit.upgrade()?;
        let channel = kit.inner.channel(agent.channel_id())?;
        channel.declaration().identity?.language
    }

    /// Processes a handoff `calling_agent_id` asked in `room_id`, with the
    /// tool call's `arguments`: Python's `handle(room_id, calling_agent_id,
    /// arguments)`. A refused one is an `accepted: false` result.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::KitClosed`] once the kit is gone, what reading the room,
    /// saving its state or recording the handoff fails with, which a closing
    /// kit's send refuses; [`RoomKitError::Value`] for an argument that is no
    /// text.
    pub async fn handle(
        &self,
        room_id: &RoomId,
        calling_agent_id: &ChannelId,
        arguments: Metadata,
    ) -> Result<HandoffResult, RoomKitError> {
        let kit = self.kit()?;
        let request = self.request(arguments)?;
        match self
            .decide(&kit, room_id, calling_agent_id, &request)
            .await?
        {
            Decided::Rejected(result) => {
                rejected(&kit, room_id, calling_agent_id, &request, &result).await;
                Ok(result)
            }
            Decided::Accepted { phase, agent } => {
                let result = accepted(
                    &kit,
                    room_id,
                    calling_agent_id,
                    &request,
                    phase,
                    agent,
                    self,
                )
                .await?;
                if let Some(complete) = self.on_handoff_complete() {
                    complete(room_id.clone(), result.clone()).await;
                }
                Ok(result)
            }
        }
    }

    /// Keeps `language` in the room's conversation state: Python's
    /// `set_language(room_id, language)`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::KitClosed`] once the kit is gone, what reading the room
    /// or saving its state fails with.
    pub async fn set_language(
        &self,
        room_id: &RoomId,
        language: impl Into<String>,
    ) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        let section = Section::enter(&kit.inner.locks, room_id).await?;
        section
            .scope(async {
                let room = kit.get_room(room_id, GetRoomOptions::default()).await?;
                let mut state = get_conversation_state(&room)?;
                state
                    .context
                    .insert("language".to_owned(), Value::String(language.into()));
                save_conversation_state(kit.store(), room_id, &state).await
            })
            .await
    }

    /// The handoff tool `agent` serves with this handler, in the room of each
    /// call, declared as `definition`: Python's `handoff_entry`.
    pub(crate) fn entry(
        &self,
        agent: &ChannelId,
        definition: Value,
        owner: Option<usize>,
    ) -> OwnerToolEntry<S> {
        let (handler, calling) = (self.clone(), agent.clone());
        OwnerToolEntry {
            definition,
            serve: Arc::new(move |_kit, call| {
                let (handler, calling) = (handler.clone(), calling.clone());
                Box::pin(async move {
                    match handler
                        .handle(&call.room_id, &calling, call.arguments)
                        .await
                    {
                        Ok(result) => OwnerToolAnswer::Served(Value::String(
                            // Python's `result.model_dump_json()`.
                            serde_json::to_string(&result).unwrap_or_default(),
                        )),
                        Err(error) => OwnerToolAnswer::Failed {
                            kind: error.class_name(),
                            message: error.to_string(),
                        },
                    }
                })
            }),
            waits: false,
            owner,
        }
    }

    /// Serves the handoff tool on `agent`, in every room or in `room_id`'s
    /// only, the agent read as Python's `owner_tools_of` does, live or by the
    /// id of a channel the kit holds; a live one is given to the kit, which
    /// registers it. What `roomkit_ai::setup_handoff` and the
    /// router's install call. Hidden: Python's `setup_handoff` is public,
    /// `roomkit_ai` holds its `AiTool`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Runtime`] when it is set up already where it would be
    /// declared; [`RoomKitError::ChannelNotRegistered`] for an id naming no
    /// channel of the kit; [`RoomKitError::Value`] for a channel that is no
    /// intelligence channel, or whose turns never declare what its room's
    /// owner serves; [`RoomKitError::ToolNameCollision`] for a name its own
    /// tools take, or a room's handoff under a kit-wide one;
    /// [`RoomKitError::KitClosed`] once the kit is gone.
    #[doc(hidden)]
    pub fn serve_tool(
        &self,
        agent: &StrategyAgent,
        definition: Option<Value>,
        room_id: Option<&RoomId>,
    ) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        kit.takes_handoff(agent)?;
        kit.register_strategy_agents(std::slice::from_ref(agent))?;
        let channel = agent.channel_id();
        if kit.serves_handoff(channel, room_id) {
            return Err(RoomKitError::Runtime(format!(
                "setup_handoff() already called for channel '{channel}'"
            )));
        }
        let definition = definition.unwrap_or_else(|| handoff_tool_definition(&[]));
        kit.serve_handoff(channel, self.entry(channel, definition, None), room_id)
    }

    /// Serves each of `handoffs`, an agent and its tool's definition, in
    /// `room_id` for the install `owner`: all of them, or none when one is
    /// refused. Python's `set_up_handoffs`.
    pub(crate) fn serve_in_room(
        &self,
        kit: &RoomKit<S>,
        handoffs: &[(ChannelId, Value)],
        room_id: &RoomId,
        owner: usize,
    ) -> Result<(), RoomKitError> {
        for (agent, _) in handoffs {
            kit.check_room_tool(agent, room_id, HANDOFF_TOOL_NAME, owner)?;
        }
        for (agent, definition) in handoffs {
            let entry = self.entry(agent, definition.clone(), Some(owner));
            kit.register_room_tools(agent, room_id, vec![entry], owner)?;
        }
        Ok(())
    }

    /// Keeps the agents an install hands off between and their greetings,
    /// as a strategy reads them.
    pub(crate) fn keep_agents(&self, agents: &[StrategyAgent], held: &[HeldAgent]) {
        self.set_greeting_map(
            held.iter()
                .filter_map(|agent| {
                    let greeting = agent.greeting.clone()?;
                    Some((agent.channel_id.clone(), greeting))
                })
                .collect(),
        );
        self.set_agents(
            agents
                .iter()
                .map(|agent| (agent.channel_id().clone(), agent.clone()))
                .collect(),
        );
    }

    pub(super) fn kit(&self) -> Result<RoomKit<S>, RoomKitError> {
        self.inner.kit.upgrade().ok_or(RoomKitError::KitClosed)
    }

    /// The call's request: `target` through the aliases, `channel_escalation`
    /// `same` when absent.
    fn request(&self, arguments: Metadata) -> Result<HandoffRequest, RoomKitError> {
        let target = text(&arguments, "target")?;
        let target_agent_id = self.inner.aliases.get(&target).cloned().unwrap_or(target);
        let channel_escalation = match arguments.get("channel_escalation") {
            None => Some("same".to_owned()),
            Some(Value::Null) => None,
            Some(_) => Some(text(&arguments, "channel_escalation")?),
        };
        Ok(HandoffRequest {
            target_agent_id,
            reason: text(&arguments, "reason")?,
            summary: text(&arguments, "summary")?,
            context: arguments,
            channel_escalation,
            urgent: false,
        })
    }

    /// Validates the handoff and updates the room's state, under its lock so
    /// that no update is lost: hooks and the record run after it.
    async fn decide(
        &self,
        kit: &RoomKit<S>,
        room_id: &RoomId,
        calling_agent_id: &ChannelId,
        request: &HandoffRequest,
    ) -> Result<Decided, RoomKitError> {
        let section = Section::enter(&kit.inner.locks, room_id).await?;
        section
            .scope(self.decide_locked(kit, room_id, calling_agent_id, request))
            .await
    }

    /// [`decide`](Self::decide), under the room's lock. The state is patched
    /// into the room, which moves its `updated_at` where Python's
    /// `update_room` of the whole room does not.
    async fn decide_locked(
        &self,
        kit: &RoomKit<S>,
        room_id: &RoomId,
        calling_agent_id: &ChannelId,
        request: &HandoffRequest,
    ) -> Result<Decided, RoomKitError> {
        let target = request.target_agent_id.as_str();
        let room = kit.get_room(room_id, GetRoomOptions::default()).await?;
        if !self.finds(kit, room_id, target).await? {
            tracing::warn!(target: "roomkit::orchestration", "Handoff rejected: agent {target} not in room {room_id}");
            return Ok(Decided::Rejected(refusal(format!(
                "Agent {target} not found in room"
            ))));
        }
        let state = get_conversation_state(&room)?;
        // Only the host's transition to an agent takes a hold off, with no
        // model's tool on its way (RFC §19.6).
        if state.human_hold && target != HUMAN {
            tracing::warn!(target: "roomkit::orchestration", "Handoff rejected: room {room_id} is held for a human");
            return Ok(Decided::Rejected(refusal(format!(
                "Room {room_id} is held for a human"
            ))));
        }
        let phase = self.phase_of(request);
        if let Some(allowed) = &self.inner.allowed_transitions
            && !allowed
                .get(&state.phase)
                .is_some_and(|phases| phases.contains(&phase))
        {
            tracing::warn!(target: "roomkit::orchestration", "Handoff rejected: transition {} -> {phase} not allowed", state.phase);
            return Ok(Decided::Rejected(refusal(format!(
                "Transition from '{}' to '{phase}' is not allowed",
                state.phase
            ))));
        }
        if self.inner.allowed_transitions.is_some() && !self.holds_phase(target, &phase) {
            tracing::warn!(target: "roomkit::orchestration", "Handoff rejected: agent {target} does not hold phase {phase}");
            return Ok(Decided::Rejected(refusal(format!(
                "Agent {target} does not hold phase '{phase}'"
            ))));
        }
        if self.inner.allowed_transitions.is_some()
            && !self.caller_holds_room(calling_agent_id, &state)
        {
            tracing::warn!(target: "roomkit::orchestration", "Handoff rejected: caller {calling_agent_id} does not hold phase {}", state.phase);
            return Ok(Decided::Rejected(refusal(format!(
                "Agent {calling_agent_id} does not hold phase '{}'",
                state.phase
            ))));
        }
        if let Some(phase) = request.context.get("next_phase")
            && !phase.is_string()
            && truthy(phase)
        {
            return Err(RoomKitError::Value(format!(
                "next_phase: Input should be a valid string, not {phase}"
            )));
        }
        let agent = (target != HUMAN).then(|| target.to_owned());
        let moved = moved_state(&state, &phase, agent.as_deref(), calling_agent_id, request)?;
        save_conversation_state(kit.store(), room_id, &moved).await?;
        Ok(Decided::Accepted { phase, agent })
    }

    /// The phase a handoff leads to: the call's `next_phase`, else the phase
    /// map's for its target, else `handling`, one Python's truth test refuses
    /// read as none. A `next_phase` that is no text reads as written, for the
    /// refusal of a transition; the state refuses it further on.
    fn phase_of(&self, request: &HandoffRequest) -> String {
        match request.context.get("next_phase") {
            Some(Value::String(given)) if !given.is_empty() => return given.clone(),
            Some(given) if !given.is_string() && truthy(given) => return given.to_string(),
            _ => {}
        }
        self.inner
            .phase_map
            .get(&request.target_agent_id)
            .filter(|phase| !phase.is_empty())
            .map_or_else(|| "handling".to_owned(), Clone::clone)
    }

    /// Whether, under allowed transitions, `caller` may hand the room on
    /// (RFC §19.6): it holds the room's phase; or the map does not name it,
    /// and it is the room's active agent, or its default one while none is.
    fn caller_holds_room(&self, caller: &ChannelId, state: &ConversationState) -> bool {
        if self.holds_phase(caller.as_str(), &state.phase) {
            return true;
        }
        if self.inner.phase_map.contains_key(caller.as_str()) {
            return false;
        }
        let holder = state
            .active_agent_id
            .as_ref()
            .or_else(|| self.inner.router.default_agent());
        holder == Some(caller)
    }

    /// Whether, under allowed transitions, `agent_id` holds `phase`: as a
    /// handoff's target, the phase it is led to; as its caller, the room's
    /// (RFC §19.6). With no phase map nothing ties an agent to a phase. With
    /// one, a human or a known agent it does not name passes; any other holds
    /// its own phase and those the router's rules give it, and no phase
    /// otherwise.
    fn holds_phase(&self, agent_id: &str, phase: &str) -> bool {
        let inner = &self.inner;
        if inner.phase_map.is_empty() {
            return true;
        }
        let own = inner.phase_map.get(agent_id);
        if own.is_none() && (agent_id == HUMAN || inner.known_agents.contains(agent_id)) {
            return true;
        }
        own.is_some_and(|own| own == phase) || inner.router.routes_phase_to(phase, agent_id)
    }

    /// Whether `target` is a handoff's: an agent the room binds, a human, or
    /// known. A transport the room binds answers nothing: it is not found.
    async fn finds(
        &self,
        kit: &RoomKit<S>,
        room_id: &RoomId,
        target: &str,
    ) -> Result<bool, RoomKitError> {
        if target == HUMAN || self.inner.known_agents.contains(target) {
            return Ok(true);
        }
        let Ok(channel) = ChannelId::new(target) else {
            return Ok(false);
        };
        let binding = kit.store().get_binding(room_id, &channel).await?;
        Ok(binding.is_some_and(|binding| binding.category == ChannelCategory::Intelligence))
    }
}

/// The target that leaves the room with no active agent.
const HUMAN: &str = "human";

fn read<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

fn write<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

/// The text of the argument `key`, empty when absent; one that is no text
/// fails, as pydantic's validation does.
fn text(arguments: &Metadata, key: &str) -> Result<String, RoomKitError> {
    match arguments.get(key) {
        None => Ok(String::new()),
        Some(Value::String(text)) => Ok(text.clone()),
        Some(other) => Err(RoomKitError::Value(format!(
            "{key}: Input should be a valid string, not {other}"
        ))),
    }
}

/// Python's `bool(value)` of a JSON value.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64() != Some(0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    }
}

/// A refused handoff's result.
fn refusal(reason: String) -> HandoffResult {
    HandoffResult {
        accepted: false,
        reason,
        ..HandoffResult::default()
    }
}
