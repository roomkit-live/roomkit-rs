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

use roomkit_core::model::{ChannelId, Metadata, Room, RoomId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::channel::BoxFuture;
use crate::error::RoomKitError;
use crate::kit::{GetRoomOptions, RoomKit, WeakRoomKit};
use crate::orchestration::agents::held;
use crate::orchestration::handoff_end::{accepted, moved_state, rejected};
pub(crate) use crate::orchestration::handoff_tool::{HANDOFF_TOOL_NAME, handoff_tool_definition};
use crate::orchestration::{
    ConversationRouter, StrategyAgent, get_conversation_state, save_conversation_state,
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
    pub reason: String,
    /// What the next agent needs to know.
    pub summary: String,
    /// The call's arguments.
    pub context: Metadata,
    /// The channel to escalate to, `same` keeping the current one.
    pub channel_escalation: Option<String>,
    /// Whether the handoff is urgent.
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

    /// The router the handler was built with.
    pub fn router(&self) -> &ConversationRouter {
        &self.inner.router
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
        let Some(agent) = agent else {
            return Ok(None);
        };
        let Some(kit) = self.inner.kit.upgrade() else {
            return Ok(None);
        };
        Ok(held(&kit, &agent)?.language)
    }

    /// Processes a handoff `calling_agent_id` asked in `room_id`, with the
    /// tool call's `arguments`: Python's `handle(room_id, calling_agent_id,
    /// arguments)`. A refused one is an `accepted: false` result.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::KitClosed`] once the kit is gone or closing,
    /// [`RoomKitError::Value`] for an argument that is no text, what reading
    /// the room, saving its state or recording the handoff fails with.
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
    /// [`RoomKitError::KitClosed`] once the kit is gone or closing, what
    /// reading the room or saving its state fails with.
    pub async fn set_language(
        &self,
        room_id: &RoomId,
        language: impl Into<String>,
    ) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        let _lock = kit.inner.locks.lock(room_id).await?;
        let room = kit.get_room(room_id, GetRoomOptions::default()).await?;
        let mut state = get_conversation_state(&room)?;
        state
            .context
            .insert("language".to_owned(), Value::String(language.into()));
        save_conversation_state(kit.store(), room_id, &state).await
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
    /// only: what `roomkit_ai::setup_handoff` and the router's install call.
    /// Hidden: Python's `setup_handoff` is public, `roomkit_ai` holds its
    /// `AiTool`.
    ///
    /// # Errors
    ///
    /// [`RoomKitError::Runtime`] when it is set up already where it would be
    /// declared; [`RoomKitError::ChannelNotRegistered`] for an id naming no
    /// channel of the kit; [`RoomKitError::KitClosed`] once the kit is gone.
    #[doc(hidden)]
    pub fn serve_tool(
        &self,
        agent: &StrategyAgent,
        definition: Option<Value>,
        room_id: Option<&RoomId>,
    ) -> Result<(), RoomKitError> {
        let kit = self.kit()?;
        kit.register_strategy_agents(std::slice::from_ref(agent))?;
        let held = held(&kit, agent)?.channel_id;
        if kit.serves_handoff(&held, room_id) {
            return Err(RoomKitError::Runtime(format!(
                "setup_handoff() already called for channel '{held}'"
            )));
        }
        let definition = definition.unwrap_or_else(|| handoff_tool_definition(&[]));
        kit.serve_handoff(&held, self.entry(&held, definition, None), room_id)
    }

    fn kit(&self) -> Result<RoomKit<S>, RoomKitError> {
        let kit = self.inner.kit.upgrade().ok_or(RoomKitError::KitClosed)?;
        kit.inner.open()?;
        Ok(kit)
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
        let target = request.target_agent_id.as_str();
        let _lock = kit.inner.locks.lock(room_id).await?;
        let room = kit.get_room(room_id, GetRoomOptions::default()).await?;
        if !self.finds(kit, room_id, target).await? {
            tracing::warn!(target: "roomkit::orchestration", "Handoff rejected: agent {target} not in room {room_id}");
            return Ok(Decided::Rejected(refusal(format!(
                "Agent {target} not found in room"
            ))));
        }
        let phase = request
            .context
            .get("next_phase")
            .and_then(Value::as_str)
            .filter(|phase| !phase.is_empty())
            .or_else(|| self.inner.phase_map.get(target).map(String::as_str))
            .filter(|phase| !phase.is_empty())
            .unwrap_or("handling")
            .to_owned();
        let state = get_conversation_state(&room)?;
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
        let agent = (target != HUMAN).then(|| target.to_owned());
        let moved = moved_state(&state, &phase, agent.as_deref(), calling_agent_id, request)?;
        save_conversation_state(kit.store(), room_id, &moved).await?;
        Ok(Decided::Accepted { phase, agent })
    }

    /// Whether `target` is a handoff's: bound in the room, a human, or known.
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
        Ok(kit.store().get_binding(room_id, &channel).await?.is_some())
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

/// A refused handoff's result.
fn refusal(reason: String) -> HandoffResult {
    HandoffResult {
        accepted: false,
        reason,
        ..HandoffResult::default()
    }
}
