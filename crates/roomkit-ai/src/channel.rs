//! The AI channel (RFC §6.4, Appendix A.9): Python's `AIChannel`, an
//! intelligence channel whose turn runs the tool loop over a provider.

use std::fmt;
use std::future::{self, Future};
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use roomkit::human_input::CloseOptions;
use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelId, ChannelOutput, ChannelType,
    EventDirection, EventSource, EventType, InboundMessage, RoomContext, RoomEvent, RoomId,
    SteeringDirective,
};
use roomkit::{Channel, ChannelError, current_turn_host};
use roomkit_core::rules::transcript::{
    Message, TurnMemory, conversation_with_memory, replayed_rounds,
};
use serde_json::Value;

use crate::error::AiError;
use crate::human_input::HumanInputToolHandler;
use crate::images::ImageResolver;
use crate::memory::{
    IngestOptions, MemoryProvider, MemoryResult, RetrieveOptions, SlidingWindowMemory,
    SlidingWindowMemoryOptions, TurnFootprint, measured, runtime_or_cleaned,
};
use crate::provider::{AiMessage, AiProvider};
use crate::rows;
use crate::server_instructions::ServerInstructions;
use crate::skills::SkillRegistry;
use crate::skills::tools::{SkillTools, SkillTurn};
use crate::steering::{Loops, SteerOptions};
use crate::tool_loop::{Bounds, Loop};
use crate::tools::toolbox::Toolbox;
use crate::tools::{
    DynToolset, ToolContext, ToolEntry, ToolError, ToolHandler, ToolPolicy, served_by,
};

/// The metadata word of an instruction that stands alone (RFC §10.1.1
/// rule 7), as the pipeline writes it.
const STANDALONE: &str = "standalone";

/// What [`AiChannel::new`] takes beside its id and its provider: Python's
/// `AIChannel` keywords for the features this implementation has, with
/// Python's defaults.
#[derive(Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Python's keywords, each a switch of its own"
)]
pub struct AiChannelOptions {
    /// What every turn's system prompt opens with.
    pub system_prompt: Option<String>,
    /// How freely the model samples, handed to the provider: Python's
    /// `float`, 0.7 by default. A model that takes no temperature is the
    /// provider's to know (`OpenAiConfig::supports_custom_temperature`).
    pub temperature: f64,
    /// The most tokens a generation writes; `None` leaves it to the
    /// provider's configuration.
    pub max_tokens: Option<u32>,
    /// The window of the default memory: how many of the room's latest
    /// events a turn reads, its own among them (RFC §20.3).
    pub max_context_events: usize,
    /// Where a turn reads its history (RFC §20): Python's `memory`; `None`
    /// is a [`SlidingWindowMemory`] of `max_context_events`.
    pub memory: Option<Arc<dyn MemoryProvider>>,
    /// The handler serving the calls of the tools `tools` defines.
    pub tool_handler: Option<ToolHandler>,
    /// The tools the model may call: definitions the handler serves, and
    /// tools serving themselves.
    pub tools: Vec<ToolEntry>,
    /// The most rounds of calls a turn runs (Appendix A.9).
    pub max_tool_rounds: u32,
    /// How long a turn's tool loop may run, read between its rounds
    /// (Appendix A.9): past it, the loop ends `timeout` before the round's
    /// calls run. `None`, or zero, sets no deadline.
    pub tool_loop_timeout: Option<Duration>,
    /// After how many rounds of calls the loop logs that it is still
    /// running.
    pub tool_loop_warn_after: u32,
    /// How many times a turn tries again a round that ended with no call to
    /// run: an empty answer after a round of calls, or a call the provider
    /// could not hand over (RFC §6.4).
    pub max_empty_retries: u32,
    /// The longest a tool's call may take (RFC §21.6), more than zero;
    /// `None` bounds nothing.
    pub tool_timeout: Option<Duration>,
    /// The skills the channel serves (RFC §24).
    pub skills: Option<SkillRegistry>,
    /// Whether the system prompt lists the skills; without, the host lists
    /// them in its own prompt, and the active skills' instructions are
    /// carried all the same.
    pub skills_in_prompt: bool,
    /// Which tools the model may call (RFC §21.1); `None` allows every
    /// tool.
    pub tool_policy: Option<ToolPolicy>,
    /// The instructions MCP servers published, each under the host's label
    /// for its server, in order (RFC §21.2).
    pub server_instructions: Vec<(String, Option<String>)>,
    /// Whether the model writes with each call one short sentence for the
    /// person, saying what the call does (RFC §6.4).
    pub tool_call_intents: bool,
    /// Whether that sentence is required in the schemas shown to the model.
    pub tool_call_intents_required: bool,
    /// What resolves the room's image references on every turn (RFC §6.4).
    pub image_resolver: Option<Arc<dyn ImageResolver>>,
    /// Whether the channel's turns in a room run one after the other, each
    /// reading the answers before it (RFC §6.4): Python's
    /// `serialize_room_turns`.
    pub serialize_room_turns: bool,
    /// Whether a message for a room where the channel's turn runs is handed
    /// to that turn, which reads it at its next stop, rather than a turn of
    /// its own (RFC §6.4, §21.3); it implies `serialize_room_turns`.
    /// Python's `steer_room_turns`.
    pub steer_room_turns: bool,
    /// The tools that ask a person (RFC §9.3): Python's
    /// `human_input_handler`. The channel declares its tools and serves its
    /// names itself, before the tool handler, under its own timeout rather
    /// than `tool_timeout`; each request fires the kit's
    /// `ON_USER_INPUT_REQUIRED` hooks, and the channel's close settles the
    /// requests still open.
    pub human_input_handler: Option<HumanInputToolHandler>,
}

impl Default for AiChannelOptions {
    fn default() -> Self {
        Self {
            system_prompt: None,
            temperature: 0.7,
            max_tokens: None,
            max_context_events: 50,
            memory: None,
            tool_handler: None,
            tools: Vec::new(),
            max_tool_rounds: 50,
            tool_loop_timeout: Some(Duration::from_secs(300)),
            tool_loop_warn_after: 25,
            max_empty_retries: 1,
            tool_timeout: Some(Duration::from_secs(30)),
            skills: None,
            skills_in_prompt: true,
            tool_policy: None,
            server_instructions: Vec::new(),
            tool_call_intents: false,
            tool_call_intents_required: false,
            image_resolver: None,
            serialize_room_turns: false,
            steer_room_turns: false,
            human_input_handler: None,
        }
    }
}

impl fmt::Debug for AiChannelOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiChannelOptions")
            .field("system_prompt", &self.system_prompt)
            .field("tools", &self.tools)
            .field("tool_policy", &self.tool_policy)
            .finish_non_exhaustive()
    }
}

/// An AI channel (RFC §6.4, Appendix A.9): an intelligence channel whose
/// turn sends the conversation the room hands it to a model through its
/// provider, runs the tools the model calls, and answers with what the
/// model said. Python's `AIChannel`.
///
/// Each turn is one tool loop, the same for every provider (RFC §6.4 "One
/// loop, streamed or not"):
/// - each round declares the tools the [`ToolPolicy`] permits, in the
///   order the options give them;
/// - a round that calls runs its calls concurrently, each through its gate
///   (RFC §21.1: a call cut short, a tool the turn does not carry, the
///   policy, the arguments' schema, then the kit's `BEFORE_TOOL_USE`
///   hooks) and within the bound per call (RFC §21.6, 30 seconds by
///   default), then hands their results to the next round;
/// - the loop ends when the model answers without calling, or at the
///   generation past its round bound (50 by default), whose calls never run;
/// - past its deadline (300 seconds by default), read between rounds, the
///   loop ends `timeout`, the round's calls left unrun;
/// - an empty answer after a round of calls, or a call the provider could
///   not hand over, is tried again with a word to the model, once by
///   default (`max_empty_retries`).
///
/// A call goes to the tool handler first, then to each tool serving itself,
/// the first that serves it winning, as Python's `compose_tool_handlers`.
///
/// Run by a kit, the turn answers with a stream (RFC §6.1
/// `response_stream`), which the kit reads once the event's delivery set
/// is over: nothing of the loop runs but as it is read. The kit stores each
/// row as it ends: each round's text as a MESSAGE, a `TOOL_CALL_START` per
/// call, then the round's `TOOL_CALL_END` rows in call order; the last
/// MESSAGE carries the turn's record. A later turn's history replays each
/// round at its place, from those rows (RFC §6.4). Called outside a kit,
/// the turn returns the same rows at its end, buffered.
///
/// With MCP servers' instructions, each turn's system prompt carries them
/// after the channel's own (RFC §21.2); with skills, it then lists them,
/// and carries the instructions of those active in the room (RFC §24.4).
/// Its close closes its provider ([`AiProvider::close`]); the host closes
/// what serves its tools, as Python's.
///
/// It answers a message or an instruction, never its own events, and takes
/// no inbound message. Cloning it shares its provider, its tools, its
/// skills and its system prompt.
pub struct AiChannel<P> {
    channel_id: ChannelId,
    capabilities: ChannelCapabilities,
    provider: Arc<P>,
    toolbox: Arc<Toolbox>,
    /// The host's handler, which the setter replaces.
    tool_handler: Arc<RwLock<Option<ToolHandler>>>,
    policy: ToolPolicy,
    bounds: Bounds,
    system_prompt: Arc<RwLock<Option<String>>>,
    temperature: f64,
    max_tokens: Option<u32>,
    server_instructions: ServerInstructions,
    memory: Arc<dyn MemoryProvider>,
    image_resolver: Option<Arc<dyn ImageResolver>>,
    skills: Option<Arc<SkillTools>>,
    tool_call_intents: bool,
    tool_call_intents_required: bool,
    /// The turns' running loops, which steering reaches (RFC §21.3).
    loops: Arc<Loops>,
    /// How it takes a room's turns (RFC §6.4).
    room_turns: RoomTurns,
    /// The tools that ask a person, and the token of the channel's
    /// registration on their handler.
    human_input: Option<(HumanInputToolHandler, u64)>,
}

/// What a turn read of the room through its memory (RFC §20.2): its
/// history, the messages the memory built, and its notes and speakers.
struct Recalled {
    history: RoomContext,
    messages: Vec<AiMessage>,
    memory: TurnMemory,
}

/// How an AI channel takes a room's turns (RFC §6.4).
#[derive(Clone, Copy, Debug)]
struct RoomTurns {
    /// One after the other.
    serialized: bool,
    /// A message during a turn handed to it, the turns serialized.
    steered: bool,
}

impl<P> Clone for AiChannel<P> {
    fn clone(&self) -> Self {
        Self {
            channel_id: self.channel_id.clone(),
            capabilities: self.capabilities.clone(),
            provider: Arc::clone(&self.provider),
            toolbox: Arc::clone(&self.toolbox),
            tool_handler: Arc::clone(&self.tool_handler),
            policy: self.policy.clone(),
            bounds: self.bounds.clone(),
            system_prompt: Arc::clone(&self.system_prompt),
            temperature: self.temperature,
            max_tokens: self.max_tokens,
            server_instructions: self.server_instructions.clone(),
            memory: Arc::clone(&self.memory),
            image_resolver: self.image_resolver.clone(),
            skills: self.skills.clone(),
            tool_call_intents: self.tool_call_intents,
            tool_call_intents_required: self.tool_call_intents_required,
            loops: Arc::clone(&self.loops),
            room_turns: self.room_turns,
            human_input: self.human_input.clone(),
        }
    }
}

impl<P: AiProvider> AiChannel<P> {
    /// The channel `channel_id`, answering through `provider`, as `options`
    /// configure it: Python's `AIChannel(channel_id, provider, ...)`.
    ///
    /// # Errors
    ///
    /// Returns, as Python's constructor raises `ValueError`:
    /// [`AiError::DuplicateTool`] for a tool given twice, or one under a
    /// skill tool's name (RFC §21.1); [`AiError::ToolName`] for a tool whose
    /// name no vendor accepts (RFC §6.7); [`AiError::ToolPattern`] for a
    /// policy pattern holding a bracket (RFC §21.1); [`AiError::ServerLabel`]
    /// for a server label that is no identifier and
    /// [`AiError::DuplicateServer`] for one given twice;
    /// [`AiError::ToolTimeout`] for a tool's call bounded by zero (RFC
    /// §21.6).
    pub fn new(
        channel_id: ChannelId,
        provider: P,
        options: AiChannelOptions,
    ) -> Result<Self, AiError> {
        let AiChannelOptions {
            system_prompt,
            temperature,
            max_tokens,
            max_context_events,
            memory,
            tool_handler,
            tools,
            max_tool_rounds,
            tool_loop_timeout,
            tool_loop_warn_after,
            max_empty_retries,
            tool_timeout,
            skills,
            skills_in_prompt,
            tool_policy,
            server_instructions,
            tool_call_intents,
            tool_call_intents_required,
            image_resolver,
            serialize_room_turns,
            steer_room_turns,
            human_input_handler,
        } = options;
        if tool_timeout.is_some_and(|bound| bound.is_zero()) {
            return Err(AiError::ToolTimeout);
        }
        let policy = tool_policy.unwrap_or_default();
        refuse_bracketed(&policy)?;
        let mut instructions = ServerInstructions::default();
        for (label, published) in &server_instructions {
            instructions.add(label, published.as_deref())?;
        }
        let mut toolbox = Toolbox::default();
        let host = tool_handler.is_some();
        // The host's handler is read at each call, so that a handler set
        // later serves the next calls (Python's `tool_handler` setter).
        let tool_handler = Arc::new(RwLock::new(tool_handler));
        let serving = (host || !tools.is_empty()).then(|| host_handler(&tool_handler));
        if let Some(served) = served_by(serving, tools)? {
            toolbox.add_dyn(served.0, served.1)?;
        }
        // A registry given, an empty one too, reserves the skill tools'
        // names, as Python's constructor does.
        if skills.is_some() {
            reserve_skill_names(&toolbox)?;
        }
        let human_input = match human_input_handler {
            Some(human) => Some(serve_human_input(&mut toolbox, human, &channel_id)?),
            None => None,
        };
        let skills = match skills.filter(SkillRegistry::has_entries) {
            Some(skills) => {
                let skill_tools = Arc::new(SkillTools::new(skills, skills_in_prompt));
                let own: Arc<dyn DynToolset> = Arc::<SkillTools>::clone(&skill_tools);
                toolbox.add_own(own, SkillTools::definitions())?;
                Some(skill_tools)
            }
            None => None,
        };
        Ok(Self {
            capabilities: capabilities(&provider),
            channel_id,
            provider: Arc::new(provider),
            toolbox: Arc::new(toolbox),
            tool_handler,
            policy,
            bounds: Bounds {
                max_rounds: max_tool_rounds,
                loop_timeout: tool_loop_timeout.filter(|timeout| !timeout.is_zero()),
                warn_after: tool_loop_warn_after,
                max_empty_retries,
                tool_timeout,
            },
            system_prompt: Arc::new(RwLock::new(
                system_prompt.filter(|prompt| !prompt.is_empty()),
            )),
            temperature,
            max_tokens,
            server_instructions: instructions,
            memory: memory.unwrap_or_else(|| {
                Arc::new(SlidingWindowMemory::new(SlidingWindowMemoryOptions {
                    max_events: max_context_events,
                }))
            }),
            image_resolver,
            skills,
            tool_call_intents,
            tool_call_intents_required,
            loops: Arc::default(),
            room_turns: RoomTurns {
                serialized: serialize_room_turns || steer_room_turns,
                steered: steer_room_turns,
            },
            human_input,
        })
    }

    /// The channel's provider: Python's `provider`.
    pub fn provider(&self) -> &P {
        &self.provider
    }

    /// What every turn's system prompt opens with: Python's
    /// `system_prompt`.
    pub fn system_prompt(&self) -> Option<String> {
        self.system_prompt
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Opens every turn from the next with `prompt`, as Python's
    /// `set_system_prompt`; `None` or an empty text opens none. Clones of
    /// the channel share it.
    pub fn set_system_prompt(&self, prompt: Option<String>) {
        *self
            .system_prompt
            .write()
            .unwrap_or_else(PoisonError::into_inner) = prompt.filter(|prompt| !prompt.is_empty());
    }

    /// The host's handler, serving the tools the options define that
    /// neither the channel nor its own tools serve (RFC §21.1): Python's
    /// `tool_handler`.
    pub fn tool_handler(&self) -> Option<ToolHandler> {
        self.tool_handler
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Replaces the host's handler with `handler`, from the next call on,
    /// `None` taking it away: Python's `tool_handler` setter. The channel's
    /// own tools, and the tools that serve themselves, keep being served;
    /// a call to a tool no turn declares still reaches no handler (RFC
    /// §6.4). Clones of the channel share it.
    pub fn set_tool_handler(&self, handler: Option<ToolHandler>) {
        *self
            .tool_handler
            .write()
            .unwrap_or_else(PoisonError::into_inner) = handler;
    }

    /// The skills active in the room `room_id`, least recently used first:
    /// Python's `active_skill_names`.
    pub fn active_skill_names(&self, room_id: &RoomId) -> Vec<String> {
        self.skills
            .as_ref()
            .map(|skills| skills.active_names(room_id))
            .unwrap_or_default()
    }

    /// Queues `directive` for the running tool loops `options` address
    /// (RFC §21.3): Python's `steer(directive, *, loop_id=None,
    /// room_id=None, turn_id=None)`. Safe to call from anywhere; a `Cancel`
    /// also stops the loop's generation at its next event, and its round's
    /// calls announced but not run.
    ///
    /// One channel serves every room it is bound to, so a host acting for
    /// one room addresses that room: a `Cancel` reaches every loop of the
    /// room (or of the turn `turn_id`), any other directive the most recent
    /// one, never a loop of another room. With no address, the directive
    /// reaches the channel's most recent loop, whatever its room. A loop is
    /// reachable once its turn's stream is read, until it has read its
    /// queue for the last time: at its end, or once the model answered
    /// with no message steered in meanwhile.
    ///
    /// Returns how many loops the directive reached.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::SteerAddress`], Python's `ValueError`, when more
    /// than one of `loop_id`, `room_id` and `turn_id` is given.
    pub fn steer(
        &self,
        directive: impl Into<SteeringDirective>,
        options: SteerOptions,
    ) -> Result<usize, AiError> {
        self.loops.steer(&directive.into(), options)
    }

    /// Whether the channel takes a turn on `event`: a message or an
    /// instruction, never one of its own (RFC §6.4).
    fn answers(&self, event: &RoomEvent) -> bool {
        event.source.channel_id != self.channel_id
            && (event.r#type == EventType::Message || event.r#type == EventType::Instruction)
    }

    /// The system prompt of a turn in `context`'s room: the channel's own,
    /// then the MCP servers' instructions (RFC §21.2), then what its skills
    /// add (RFC §6.4, §24.4).
    fn turn_system_prompt(&self, context: &RoomContext, stands_alone: bool) -> Option<String> {
        let own = self.system_prompt();
        let prompt = self.server_instructions.after(own.as_deref());
        let Some(skills) = &self.skills else {
            return prompt;
        };
        let turn = SkillTurn {
            room: &context.room.id,
            agent: &self.channel_id,
            history: &context.recent_events,
            stands_alone,
        };
        Some(skills.system_prompt(prompt.as_deref(), turn))
    }

    /// Hands `event` to the memory as it reaches the channel, whether the
    /// channel answers it or not (Python's `_ingest`): never one of its
    /// own, a tool call's row, or an instruction, which is the
    /// application's and not the conversation's (RFC §10.1.1). A failure
    /// is logged, and the turn goes on.
    async fn ingest(&self, event: &RoomEvent, context: &RoomContext) {
        let skipped = event.source.channel_id == self.channel_id
            || matches!(
                event.r#type,
                EventType::ToolCallStart | EventType::ToolCallEnd | EventType::Instruction
            );
        if skipped {
            return;
        }
        let options = IngestOptions {
            channel_id: Some(self.channel_id.clone()),
        };
        if let Err(error) = self.memory.ingest(&context.room.id, event, options).await {
            tracing::warn!(target: "roomkit::ai", room_id = %context.room.id, %error, "memory ingestion failed");
        }
    }

    /// What a turn on `event` reads of the room (RFC §20.1): `context` with
    /// its history as the memory retrieved it from the channel's view, the
    /// channel's own tool rows kept beside it for the rounds it replays
    /// (RFC §6.4), and what else the memory answered; nothing of the memory
    /// for an instruction that stands alone, which reads none of the room
    /// (RFC §10.1.1 rule 7). The memory reads the room within the turn's
    /// `footprint` ([`crate::memory::current_turn_footprint`]).
    ///
    /// The memory reads the room's messages and the turn's event, as
    /// Python's turn context holds them: the channel's earlier rounds are
    /// read apart, so that they take none of its window.
    async fn recalled(
        &self,
        event: &RoomEvent,
        context: &RoomContext,
        stands_alone: bool,
        footprint: TurnFootprint,
    ) -> Result<Recalled, ChannelError> {
        let (rows, viewed): (Vec<RoomEvent>, Vec<RoomEvent>) = context
            .recent_events
            .iter()
            .cloned()
            .partition(|past| self.is_own_tool_row(past));
        let MemoryResult {
            messages,
            events,
            notes,
            speakers,
        } = if stands_alone {
            MemoryResult::default()
        } else {
            let viewed = RoomContext {
                recent_events: viewed,
                ..context.clone()
            };
            let options = RetrieveOptions {
                channel_id: Some(self.channel_id.clone()),
            };
            // Called within the footprint's scope: a memory may read it
            // before it returns its future.
            let read = async {
                self.memory
                    .retrieve(&event.room_id, event, &viewed, options)
                    .await
            };
            measured(footprint, read).await?
        };
        let mut recent_events = events;
        recent_events.extend(rows);
        Ok(Recalled {
            history: RoomContext {
                recent_events,
                ..context.clone()
            },
            messages: messages.into_iter().map(runtime_or_cleaned).collect(),
            memory: TurnMemory { notes, speakers },
        })
    }

    /// The channel's earlier rounds of tool calls in `context`, which the
    /// turn's history replays (RFC §6.4); none for an instruction that
    /// stands alone, which reads none of the room (RFC §10.1.1 rule 7).
    fn replayed(&self, context: &RoomContext, stands_alone: bool) -> Vec<Vec<Message>> {
        if stands_alone {
            return Vec::new();
        }
        let rows: Vec<RoomEvent> = context
            .recent_events
            .iter()
            .filter(|past| self.is_own_tool_row(past))
            .cloned()
            .collect();
        replayed_rounds(&rows, &self.channel_id, |name| {
            self.toolbox.in_tool_memory(name)
        })
    }

    /// Whether `event` is one of the channel's tool-call rows.
    fn is_own_tool_row(&self, event: &RoomEvent) -> bool {
        event.source.channel_id == self.channel_id
            && matches!(
                event.r#type,
                EventType::ToolCallStart | EventType::ToolCallEnd
            )
    }

    /// The source of the channel's rows.
    fn source(&self) -> EventSource {
        EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(self.channel_id.clone(), ChannelType::AI)
        }
    }
}

/// What `provider`'s channel takes: text, and media when its model takes
/// images (RFC §6.4).
fn capabilities<P: AiProvider>(provider: &P) -> ChannelCapabilities {
    let mut capabilities = ChannelCapabilities::default();
    if provider.supports_vision() {
        capabilities
            .media_types
            .insert(roomkit::model::ChannelMediaType::Media);
        capabilities.supports_media = true;
    }
    capabilities
}

/// Serves the person's tools `human` defines, and the names it serves that
/// `toolbox` declares already, before their own toolset (RFC §9.3), the
/// channel `channel_id` registered on its handler: the token of the
/// registration.
///
/// # Errors
///
/// [`AiError::DuplicateTool`] for a definition under a name the channel
/// serves already, or given twice (RFC §21.1).
fn serve_human_input(
    toolbox: &mut Toolbox,
    human: HumanInputToolHandler,
    channel_id: &ChannelId,
) -> Result<(HumanInputToolHandler, u64), AiError> {
    let toolset = human.toolset();
    let defined = human.tools();
    let declared_elsewhere: Vec<String> = human
        .tool_names()
        .iter()
        .filter(|name| !defined.iter().any(|tool| &tool.name == *name))
        .cloned()
        .collect();
    toolbox.add_dyn(Arc::clone(&toolset), defined)?;
    toolbox.intercept(toolset, &declared_elsewhere);
    // Only a handler already closed whole refuses a registration: its
    // requests are then refused at each call.
    let registration = human
        .handler()
        .register(channel_id.as_str())
        .unwrap_or_default();
    Ok((human, registration))
}

/// The handler that serves a call with the host's handler `host` holds at
/// the time of the call; with none, it declines the call, which the
/// channel's own tools may serve.
fn host_handler(host: &Arc<RwLock<Option<ToolHandler>>>) -> ToolHandler {
    let host = Arc::clone(host);
    ToolHandler::new(move |name, arguments| {
        let current = host.read().unwrap_or_else(PoisonError::into_inner).clone();
        async move {
            match current {
                Some(handler) => handler.call(name, arguments).await,
                None => Err(ToolError::unserved(format!(
                    "no tool handler serves {name}"
                ))),
            }
        }
    })
}

/// Refuses a tool `toolbox` serves under one of the names a channel given
/// skills keeps for its own.
fn reserve_skill_names(toolbox: &Toolbox) -> Result<(), AiError> {
    match SkillTools::RESERVED_NAMES
        .into_iter()
        .find(|name| toolbox.find(name).is_some())
    {
        Some(name) => Err(AiError::DuplicateTool {
            name: name.to_owned(),
        }),
        None => Ok(()),
    }
}

/// Refuses a pattern of `policy` holding a bracket, which a pattern reads
/// as itself (RFC §21.1): a rule written as a character class would cover
/// none of the names it was written for.
fn refuse_bracketed(policy: &ToolPolicy) -> Result<(), AiError> {
    match policy
        .allow
        .iter()
        .chain(&policy.deny)
        .find(|pattern| pattern.contains(['[', ']']))
    {
        Some(pattern) => Err(AiError::ToolPattern {
            pattern: pattern.clone(),
        }),
        None => Ok(()),
    }
}

impl<P: AiProvider> Channel for AiChannel<P> {
    fn channel_id(&self) -> ChannelId {
        self.channel_id.clone()
    }

    fn channel_type(&self) -> ChannelType {
        ChannelType::AI
    }

    fn category(&self) -> ChannelCategory {
        ChannelCategory::Intelligence
    }

    fn capabilities(&self) -> ChannelCapabilities {
        self.capabilities.clone()
    }

    fn provider_name(&self) -> Option<String> {
        Some(self.provider.name().to_owned())
    }

    /// The events a turn reads: Python's `recent_events_window`, its
    /// memory's.
    fn recent_events_window(&self) -> usize {
        self.memory.recent_events_window()
    }

    /// An AI channel takes no inbound message, as Python's refuses one: a
    /// message sent on its channel is refused before anything enters the
    /// room, so that nothing is stored under its id that it did not write.
    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        future::ready(Err("AI channel does not accept inbound messages".into()))
    }

    /// An intelligence channel is not delivered to: its turn is its
    /// `on_event()`.
    fn deliver(
        &self,
        _event: RoomEvent,
        _binding: ChannelBinding,
        _context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        future::ready(Ok(ChannelOutput::empty()))
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.ingest(&event, &context).await;
        if !self.answers(&event) {
            return Ok(ChannelOutput::default());
        }
        let stands_alone = stands_alone(&event);
        let turn = current_turn_host();
        let system_prompt = self.turn_system_prompt(&context, stands_alone);
        let tools = ToolContext::new(context.room.id.clone(), self.channel_id.clone())
            .within(turn.clone())
            .standing_alone(stands_alone);
        // The tools the room's owner serves this turn join the agent's own
        // (RFC §10.6.5).
        let toolbox = crate::tools::owner::with_owner_tools(&self.toolbox, turn.as_ref())?;
        let mut looped = Loop {
            provider: Arc::clone(&self.provider),
            toolbox,
            policy: self.policy.clone(),
            bounds: self.bounds.clone(),
            system_prompt,
            temperature: self.temperature,
            max_tokens: self.max_tokens,
            turn: turn.clone(),
            context: tools,
            intents: self.tool_call_intents,
            intents_required: self.tool_call_intents_required,
            remembered: Vec::new(),
            loops: Arc::clone(&self.loops),
        };
        // Measured as the first round sends it, before the memory reads the
        // room (RFC §20).
        let footprint = looped.footprint(&self.replayed(&context, stands_alone));
        let recalled = self
            .recalled(&event, &context, stands_alone, footprint)
            .await?;
        looped.remembered = recalled.messages;
        let mut messages = conversation_with_memory(
            &self.channel_id,
            &event,
            &recalled.history,
            usize::MAX,
            |name| self.toolbox.in_tool_memory(name),
            &recalled.memory,
        );
        crate::images::resolve(
            &mut messages,
            self.provider.supports_vision(),
            self.image_resolver.as_deref(),
            &context.room.id,
        )
        .await;
        let items = looped.run(messages);
        match turn {
            // Within the kit, the turn is one stream, read as it is written.
            Some(turn) => Ok(turn.respond_with_stream(rows::response(items))),
            None => Ok(rows::collect(self.source(), &event, items).await?),
        }
    }

    /// Whether its turns in a room run one after the other
    /// (`serialize_room_turns`, or `steer_room_turns`).
    fn serializes_room_turns(&self) -> bool {
        self.room_turns.serialized
    }

    /// Whether a message that comes while its turn runs in the room is read
    /// by that turn (`steer_room_turns`).
    fn steers_room_turns(&self) -> bool {
        self.room_turns.steered
    }

    /// Steers the loops of the turn `turn_id` (RFC §10.6.8), as the kit's
    /// turn executor applies a `steer`.
    fn steer_turn(&self, directive: &SteeringDirective, turn_id: &str) -> Option<usize> {
        let options = SteerOptions {
            turn_id: Some(turn_id.to_owned()),
            ..SteerOptions::default()
        };
        Some(self.loops.steer(directive, options).unwrap_or(0))
    }

    /// Closes the channel's provider ([`AiProvider::close`]), then its
    /// memory, as Python's `close()`, and settles the requests its
    /// person's tools still have open: what serves its other tools is the
    /// host's to close.
    async fn close(&self) {
        self.provider.close().await;
        self.memory.close().await;
        if let Some((human, registration)) = &self.human_input {
            let options = CloseOptions {
                channel_id: Some(self.channel_id.to_string()),
                registration: Some(*registration),
            };
            human.handler().close(options).await;
        }
    }
}

/// Whether `event` is an instruction that stands alone, which reads none
/// of the room's working state (RFC §10.1.1 rule 7).
fn stands_alone(event: &RoomEvent) -> bool {
    event.r#type == EventType::Instruction
        && event.metadata.get(STANDALONE) == Some(&Value::Bool(true))
}

impl<P> fmt::Debug for AiChannel<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AiChannel")
            .field("channel_id", &self.channel_id)
            .field("toolbox", &self.toolbox)
            .field("policy", &self.policy)
            .field("bounds", &self.bounds)
            .field("skills", &self.skills.is_some())
            .field("tool_call_intents", &self.tool_call_intents)
            .finish_non_exhaustive()
    }
}
