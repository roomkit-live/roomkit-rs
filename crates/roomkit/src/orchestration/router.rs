//! The conversation router (RFC §19.4): Python's
//! `roomkit.orchestration.router`. A `BEFORE_BROADCAST` sync hook that
//! selects the agent an unaddressed event goes to and stamps it on the
//! event's metadata, which the broadcast reads to ask that agent alone,
//! beside the router's supervisor.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::future::{Ready, ready};
use std::sync::Arc;

use roomkit_core::model::hook::{AllowOptions, ModifyOptions};
use roomkit_core::model::{
    ChannelCategory, ChannelId, ChannelType, HookResult, RoomContext, RoomEvent,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::agents::held;
use super::handoff::{HandoffHandler, HandoffHandlerOptions};
use super::state::{ConversationState, get_conversation_state};
use crate::error::RoomKitError;
use crate::hooks::HookError;
use crate::kit::RoomKit;
use crate::orchestration::StrategyAgent;
use crate::store::ConversationStore;

/// The metadata key the router stamps the agent it selected under.
const ROUTED_TO: &str = "_routed_to";

/// The metadata key the router lists the agents always asked under: its
/// supervisor (RFC §19.4 step 4), a list, as JSON keeps it.
const ALWAYS_PROCESS: &str = "_always_process";

/// What the router stamps on an agent's own output: no agent, so that no
/// agent answers another (RFC §19.4).
const NO_AGENT: &str = "_none_";

/// When a [`RoutingRule`] applies: each condition set must hold, and an
/// unset one holds always.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct RoutingConditions {
    /// The conversation state's phases it applies in.
    pub phases: Option<BTreeSet<String>>,
    /// The channel types of the sources it applies to.
    pub channel_types: Option<BTreeSet<ChannelType>>,
    /// The intents, the event metadata's `intent`, it applies to.
    pub intents: Option<BTreeSet<String>>,
    /// The source channels it applies to.
    pub source_channel_ids: Option<BTreeSet<ChannelId>>,
    /// The host's own predicate on the event, its room and the conversation
    /// state, as Python's callable; not serialized.
    #[serde(skip)]
    #[expect(
        clippy::type_complexity,
        reason = "Python's callable, written in full: no alias the Python API lacks"
    )]
    pub custom:
        Option<Arc<dyn Fn(&RoomEvent, &RoomContext, &ConversationState) -> bool + Send + Sync>>,
}

impl fmt::Debug for RoutingConditions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoutingConditions")
            .field("phases", &self.phases)
            .field("channel_types", &self.channel_types)
            .field("intents", &self.intents)
            .field("source_channel_ids", &self.source_channel_ids)
            .field("custom", &self.custom.as_ref().map(|_| "<predicate>"))
            .finish()
    }
}

impl RoutingConditions {
    /// No condition: they hold for every event.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether every condition set holds for `event` in `context` and
    /// `state`.
    fn hold(&self, event: &RoomEvent, context: &RoomContext, state: &ConversationState) -> bool {
        let within = |set: &Option<BTreeSet<String>>, value: Option<&str>| {
            set.as_ref()
                .is_none_or(|set| value.is_some_and(|value| set.contains(value)))
        };
        within(&self.phases, Some(&state.phase))
            && self
                .channel_types
                .as_ref()
                .is_none_or(|types| types.contains(&event.source.channel_type))
            && self
                .source_channel_ids
                .as_ref()
                .is_none_or(|ids| ids.contains(&event.source.channel_id))
            && within(
                &self.intents,
                event.metadata.get("intent").and_then(Value::as_str),
            )
            && self
                .custom
                .as_ref()
                .is_none_or(|custom| custom(event, context, state))
    }
}

/// A rule routing the events its conditions hold for to one agent.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RoutingRule {
    /// The agent it routes to.
    pub agent_id: ChannelId,
    /// When it applies.
    pub conditions: RoutingConditions,
    /// Python `priority=0`: rules are tried in ascending priority, the
    /// first that applies winning.
    #[serde(default)]
    pub priority: i32,
}

impl RoutingRule {
    /// The rule routing to `agent_id` when `conditions` hold, at priority 0.
    pub fn new(agent_id: ChannelId, conditions: RoutingConditions) -> Self {
        Self {
            agent_id,
            conditions,
            priority: 0,
        }
    }
}

/// What [`ConversationRouter::new`] takes: Python's
/// `rules=None, default_agent_id=None, supervisor_id=None`.
#[derive(Clone, Debug, Default)]
pub struct ConversationRouterOptions {
    /// The rules, tried in ascending priority.
    pub rules: Vec<RoutingRule>,
    /// The agent when no rule applies; `None`, none.
    pub default_agent_id: Option<ChannelId>,
    /// The agent asked about every event beside the selected one; `None`,
    /// none.
    pub supervisor_id: Option<ChannelId>,
}

/// Selects the agent each event goes to (RFC §19.4), installed as a
/// `BEFORE_BROADCAST` sync hook ([`as_hook`](Self::as_hook)) at priority
/// -100: it stamps the event, and the broadcast asks the agent it names
/// and the supervisor alone.
///
/// ```
/// use roomkit::model::ChannelId;
/// use roomkit::orchestration::ConversationRouterOptions;
/// use roomkit::{ConversationRouter, HookOptions, HookTrigger, RoomKit, RoomKitOptions};
///
/// let kit = RoomKit::new(RoomKitOptions::default());
/// let router = ConversationRouter::new(ConversationRouterOptions {
///     default_agent_id: Some(ChannelId::new("agent-triage").unwrap()),
///     ..Default::default()
/// });
/// let options = HookOptions { priority: -100, ..Default::default() };
/// kit.hook(HookTrigger::BeforeBroadcast, router.as_hook(), options).unwrap();
/// ```
#[derive(Clone, Debug)]
pub struct ConversationRouter {
    rules: Arc<[RoutingRule]>,
    default_agent_id: Option<ChannelId>,
    supervisor_id: Option<ChannelId>,
}

impl ConversationRouter {
    /// The router of `options`, its rules in ascending priority, those of
    /// one priority in their order.
    pub fn new(options: ConversationRouterOptions) -> Self {
        let mut rules = options.rules;
        rules.sort_by_key(|rule| rule.priority);
        Self {
            rules: rules.into(),
            default_agent_id: options.default_agent_id,
            supervisor_id: options.supervisor_id,
        }
    }

    /// The agent when no rule applies.
    pub(super) fn default_agent(&self) -> Option<&ChannelId> {
        self.default_agent_id.as_ref()
    }

    /// Whether a rule of this router asks `agent_id` in `phase`.
    pub(super) fn routes_phase_to(&self, phase: &str, agent_id: &str) -> bool {
        self.rules.iter().any(|rule| {
            rule.agent_id.as_str() == agent_id
                && rule
                    .conditions
                    .phases
                    .as_ref()
                    .is_some_and(|phases| phases.contains(phase))
        })
    }

    /// The agent an unaddressed `event` goes to: none for an agent's own
    /// output, or while the room is held for a human (RFC §19.6); the state's
    /// active agent while the room binds it (sticky
    /// affinity); else the first rule that applies; else the default agent.
    /// An addressed event never reaches here: its address decides (RFC
    /// §19.4 step 0).
    pub fn select_agent(
        &self,
        event: &RoomEvent,
        context: &RoomContext,
        state: &ConversationState,
    ) -> Option<ChannelId> {
        if from_agent(event, context) || state.human_hold {
            return None;
        }
        if let Some(active) = &state.active_agent_id
            && context.get_binding(active).is_some()
        {
            return Some(active.clone());
        }
        self.rules
            .iter()
            .find(|rule| rule.conditions.hold(event, context, state))
            .map(|rule| rule.agent_id.clone())
            .or_else(|| self.default_agent_id.clone())
    }

    /// The router's `BEFORE_BROADCAST` sync hook: it stamps the selected
    /// agent under `_routed_to` and the supervisor under `_always_process`,
    /// `_none_` on an agent's own output, and nothing when it selects no
    /// agent for another source. On an addressed event it stamps the
    /// supervisor alone, unless the address names it (RFC §19.4 steps 0
    /// and 4).
    pub fn as_hook(
        &self,
    ) -> impl Fn(RoomEvent, RoomContext) -> Ready<Result<HookResult, HookError>>
    + Clone
    + Send
    + Sync
    + 'static {
        let router = self.clone();
        move |event, context| ready(router.route(event, &context))
    }

    /// What the hook decides for `event`.
    fn route(&self, event: RoomEvent, context: &RoomContext) -> Result<HookResult, HookError> {
        if let Some(addressed) = &event.addressed_to {
            return Ok(match &self.supervisor_id {
                Some(supervisor) if !addressed.contains(supervisor) => {
                    let always = Value::from(vec![supervisor.as_str()]);
                    modified(stamped(event, [(ALWAYS_PROCESS, always)]))
                }
                _ => HookResult::allow(AllowOptions::default()),
            });
        }
        let state = get_conversation_state(&context.room)?;
        let routed_to = match self.select_agent(&event, context, &state) {
            Some(selected) => selected.as_str().to_owned(),
            None if from_agent(&event, context) || state.human_hold => NO_AGENT.to_owned(),
            None => return Ok(HookResult::allow(AllowOptions::default())),
        };
        tracing::debug!(
            target: "roomkit::orchestration::router",
            room_id = %event.room_id,
            event_id = %event.id,
            routed_to = %routed_to,
            "routed event"
        );
        let always: Vec<&str> = self.supervisor_id.iter().map(ChannelId::as_str).collect();
        let always = Value::from(always);
        let marks = [
            (ROUTED_TO, Value::from(routed_to)),
            (ALWAYS_PROCESS, always),
        ];
        Ok(modified(stamped(event, marks)))
    }
}

/// Whether `event` comes from an intelligence channel the room binds.
fn from_agent(event: &RoomEvent, context: &RoomContext) -> bool {
    context
        .get_binding(&event.source.channel_id)
        .is_some_and(|source| source.category == ChannelCategory::Intelligence)
}

/// `event` with `marks` written on its metadata, its other keys kept.
fn stamped<const N: usize>(mut event: RoomEvent, marks: [(&str, Value); N]) -> RoomEvent {
    for (key, value) in marks {
        event.metadata.insert(key.to_owned(), value);
    }
    event
}

/// The result that modifies the event into `event`.
fn modified(event: RoomEvent) -> HookResult {
    HookResult::modify(event, ModifyOptions::default())
}

/// What [`ConversationRouter::install`] takes beside the kit and the agents:
/// Python's `install(kit, agents, *, agent_aliases=None, phase_map=None,
/// hook_priority=-100)`.
#[derive(Clone, Debug)]
pub struct InstallOptions {
    /// Each alias's agent id.
    pub agent_aliases: Option<HashMap<String, String>>,
    /// The phase each target leads to.
    pub phase_map: Option<HashMap<String, String>>,
    /// The priority of the router's hook.
    pub hook_priority: i32,
}

impl Default for InstallOptions {
    fn default() -> Self {
        Self {
            agent_aliases: None,
            phase_map: None,
            hook_priority: -100,
        }
    }
}

impl ConversationRouter {
    /// Wires routing and handoff in one call: this router's hook on every
    /// `BEFORE_BROADCAST` of the kit, a [`HandoffHandler`], and the generic
    /// handoff tool on every agent, each live or named by the id of one the
    /// kit holds, a live one registered once every one was read, since it
    /// is given to the kit (`docs/architecture.md`).
    ///
    /// # Errors
    ///
    /// Before anything is installed, [`RoomKitError::ChannelNotRegistered`]
    /// for an id naming no channel of the kit, [`RoomKitError::Value`] or
    /// [`RoomKitError::Type`] for an agent that takes no handoff,
    /// [`RoomKitError::Runtime`] for one set up already or named twice, and
    /// [`RoomKitError::ToolNameCollision`] for a name its tools take.
    pub fn install<S: ConversationStore>(
        &self,
        kit: &RoomKit<S>,
        agents: &[StrategyAgent],
        options: InstallOptions,
    ) -> Result<HandoffHandler<S>, RoomKitError> {
        // Every agent read before anything is installed.
        for agent in agents {
            held(kit, agent)?;
        }
        kit.refuse_handoffs(agents)?;
        kit.register_strategy_agents(agents)?;
        kit.install_router_hook(self, options.hook_priority)?;
        let handler = HandoffHandler::new(
            kit,
            self.clone(),
            HandoffHandlerOptions {
                agent_aliases: options.agent_aliases,
                phase_map: options.phase_map,
                ..HandoffHandlerOptions::default()
            },
        );
        for agent in agents {
            handler.serve_tool(agent, None, None)?;
        }
        Ok(handler)
    }
}
