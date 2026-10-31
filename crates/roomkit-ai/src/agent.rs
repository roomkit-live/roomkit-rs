//! The AI channel (RFC §6.4, Appendix A.9): an intelligence channel whose
//! turn runs the tool loop over a provider.

use std::fmt;
use std::future::{self, Future};
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventDirection, EventSource, EventType, InboundMessage, RoomContext, RoomEvent,
};
use roomkit::transcript::conversation;
use roomkit::{Channel, ChannelDeclaration, ChannelError, EventDraft, current_turn};

use serde_json::Value;

use crate::error::AiError;
use crate::provider::AiProvider;
use crate::rows;
use crate::server_instructions::ServerInstructions;
use crate::skills::SkillRegistry;
use crate::skills::tools::{SkillTools, SkillTurn};
use crate::tool_loop::{Bounds, Loop};
use crate::tools::toolbox::Toolbox;
use crate::tools::{DynToolset, ToolContext, ToolPolicy, Toolset};

/// How many of the room's latest messages a turn sends by default: as many
/// as a kit hands a turn, 50 messages.
const DEFAULT_HISTORY: usize = 50;

/// The most rounds of calls a turn runs by default (Appendix A.9
/// `max_tool_rounds`).
const DEFAULT_MAX_ROUNDS: u32 = 50;

/// The longest a tool's call may take by default (RFC §21.6).
const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_secs(30);

/// The metadata word of an instruction that stands alone (RFC §10.1.1
/// rule 7), as the pipeline writes it.
const STANDALONE: &str = "standalone";

/// An AI agent (RFC §6.4, Appendix A.9): an intelligence channel whose turn
/// sends the conversation the room hands it to a model through its
/// provider, runs the tools the model calls, and answers with what the
/// model said.
///
/// Each turn is one tool loop, the same for every provider (RFC §6.4 "One
/// loop, streamed or not"):
/// - each round declares the tools the agent's [`ToolPolicy`] permits, in
///   the order its toolsets give them;
/// - a round that calls runs its calls concurrently, each through its gate
///   (RFC §21.1: a call cut short, a tool the turn does not carry, the
///   policy, the arguments' schema, then the kit's `BEFORE_TOOL_USE`
///   hooks) and within the bound per call (RFC §21.6, 30 seconds by
///   default), then hands their results to the next round;
/// - the loop ends when the model answers without calling, or at the
///   generation past its round bound (50 by default), whose calls never run.
///
/// Run by a kit, the turn answers with a stream (RFC §6.1
/// `response_stream`, [`TurnHost::respond_with_stream`](roomkit::TurnHost::respond_with_stream)),
/// which the kit reads once the event's delivery set is over: nothing of
/// the loop runs but as it is read, so a call starts only once the reader
/// read past its start. The kit stores each row as it ends: each round's
/// text as a MESSAGE, a `TOOL_CALL_START` per call, then the round's
/// `TOOL_CALL_END` rows in call order. The last MESSAGE carries the turn's
/// record, `loop_end_reason` and `ai_usage` in its metadata. `ai_last_usage`
/// holds only the final generation's reported counters (null after failure);
/// `ai_usage_complete` is false when any generation lacked counters. Optional
/// `ai_model` and `ai_provider` attribute them without guessing capacity. A provider's
/// error keeps the text it produced and fails the turn (RFC §12.2 step
/// 13s); after a round, the rows stand and the record says `error` (RFC
/// §6.4 "A turn cut short"). A later turn's history replays each round at
/// its place, from those rows (RFC §6.4 "The tools of earlier turns"): its
/// text, then the agent's calls and their results as tool messages, the
/// last 8 calls a tool answered, the 3 most recent with their result whole
/// up to 6,000 characters, the older ones a preview; a call to its skills'
/// tools is left out. Called outside a kit, the turn returns the same rows
/// at its end, buffered.
///
/// With the instructions of MCP servers
/// ([`with_server_instructions`](Self::with_server_instructions)), each
/// turn's system prompt carries them after the agent's own, each in a block
/// it cannot close (RFC §21.2, §6.4). With skills
/// ([`with_skills`](Self::with_skills)), it then lists them, and carries the
/// instructions of those active in the room (RFC §24.4).
///
/// The kit's close closes the agent ([`Channel::close`]), which closes
/// its toolsets: an MCP server's process stops.
///
/// It answers a message or an instruction, never its own events, and takes
/// no inbound message. Cloning it shares its provider, its tools and its
/// skills.
pub struct AiAgent<P> {
    declaration: ChannelDeclaration,
    provider: Arc<P>,
    toolbox: Arc<Toolbox>,
    policy: ToolPolicy,
    bounds: Bounds,
    system_prompt: Option<String>,
    server_instructions: ServerInstructions,
    history: usize,
    image_resolver: Option<Arc<dyn crate::ImageResolver>>,
    skills: Option<Arc<SkillTools>>,
    tool_call_intents: bool,
    tool_call_intents_required: bool,
}

impl<P> Clone for AiAgent<P> {
    fn clone(&self) -> Self {
        Self {
            declaration: self.declaration.clone(),
            provider: Arc::clone(&self.provider),
            toolbox: Arc::clone(&self.toolbox),
            policy: self.policy.clone(),
            bounds: self.bounds.clone(),
            system_prompt: self.system_prompt.clone(),
            server_instructions: self.server_instructions.clone(),
            history: self.history,
            image_resolver: self.image_resolver.clone(),
            skills: self.skills.clone(),
            tool_call_intents: self.tool_call_intents,
            tool_call_intents_required: self.tool_call_intents_required,
        }
    }
}

impl<P: AiProvider> AiAgent<P> {
    /// An agent named `channel_id`, answering through `provider`, with no
    /// system prompt and no tool, the room's history as the kit's context
    /// holds it, 50 rounds and 30 seconds per call.
    pub fn new(channel_id: ChannelId, provider: P) -> Self {
        let mut declaration = ChannelDeclaration::new(
            channel_id,
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        declaration.provider_name = provider.name().map(str::to_owned);
        if provider.supports_vision() {
            declaration
                .capabilities
                .media_types
                .insert(roomkit::model::ChannelMediaType::Media);
            declaration.capabilities.supports_media = true;
        }
        Self {
            declaration,
            provider: Arc::new(provider),
            toolbox: Arc::default(),
            policy: ToolPolicy::default(),
            bounds: Bounds {
                max_rounds: DEFAULT_MAX_ROUNDS,
                tool_timeout: Some(DEFAULT_TOOL_TIMEOUT),
            },
            system_prompt: None,
            server_instructions: ServerInstructions::default(),
            history: DEFAULT_HISTORY,
            image_resolver: None,
            skills: None,
            tool_call_intents: false,
            tool_call_intents_required: false,
        }
    }

    /// Resolve application image references on every turn (RFC §6.4).
    /// The resolver must authorize each reference for the supplied room.
    #[must_use]
    pub fn with_image_resolver(mut self, resolver: Arc<dyn crate::ImageResolver>) -> Self {
        self.image_resolver = Some(resolver);
        self
    }

    /// The channel's id.
    pub fn id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    /// The agent's provider.
    pub fn provider(&self) -> &P {
        &self.provider
    }

    /// The same agent, opening every turn with `prompt`. It stays the same
    /// from turn to turn, so a provider that caches a request's prefix
    /// keeps it (RFC §6.4); the MCP servers' instructions and the skills
    /// follow it.
    #[must_use]
    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into()).filter(|prompt| !prompt.is_empty());
        self
    }

    /// The same agent, its system prompt carrying, after the agent's own
    /// and before the skills, the instructions `instructions` an MCP server
    /// published, under `label`, the host's name for that server (RFC
    /// §21.2): letters, digits, `_`, `.` and `-`, 64 at most, never the
    /// name the server gives itself. They are text from outside (RFC
    /// §6.4): trimmed, cut at 8192 characters with a warning naming the
    /// server, and set apart in a `<server_instructions>` block they
    /// cannot close, under a line naming the label; the blocks sit under a
    /// heading saying that they are their servers' content, which does not
    /// outrank the system's or the user's instructions and grants no
    /// permission. The same on every turn, they stay in the prefix a
    /// provider caches.
    ///
    /// `None` or a blank text adds nothing, so a server that published no
    /// instructions is named the same way:
    ///
    /// ```
    /// use roomkit::model::ChannelId;
    /// use roomkit_ai::AiAgent;
    /// use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
    ///
    /// let model = ScriptedProvider::new([ScriptedRound::answering("Hello.")]);
    /// let agent = AiAgent::new(ChannelId::new("assistant")?, model)
    ///     .with_server_instructions("orders", Some("Call lookup_order first."))?
    ///     .with_server_instructions("calendar", None)?;
    /// # let _ = agent;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`AiError::ServerLabel`] when `label` is no identifier, and
    /// [`AiError::DuplicateServer`] when it was given already.
    pub fn with_server_instructions(
        mut self,
        label: &str,
        instructions: Option<&str>,
    ) -> Result<Self, AiError> {
        self.server_instructions.add(label, instructions)?;
        Ok(self)
    }

    /// The same agent, sending at most the room's latest `messages`
    /// messages before the turn's input. A turn reads only the history the
    /// kit hands it, the room's latest 50 messages and the agent's latest 60
    /// tool-call rows: a value above what it holds sends no more.
    #[must_use]
    pub fn with_history(mut self, messages: usize) -> Self {
        self.history = messages;
        self
    }

    /// The same agent, serving the tools of `toolset` too, declared after
    /// the ones it serves already.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::DuplicateTool`] for a tool under a name the agent
    /// serves already, or one the toolset declares twice (RFC §21.1 "One
    /// tool per name").
    pub fn with_tools(mut self, toolset: impl Toolset) -> Result<Self, AiError> {
        Arc::make_mut(&mut self.toolbox).add(toolset)?;
        Ok(self)
    }

    /// The same agent, serving the skills of `skills` (RFC §24): each
    /// turn's system prompt carries, after the agent's own, the skills'
    /// preamble and manifest, then the instructions of the skills active in
    /// the room (RFC §6.4, §24.4), and the agent serves two tools, which
    /// its policy does not govern (RFC §21.1):
    ///
    /// - `activate_skill` answers a skill's instructions and the names of
    ///   its references, and once the call is served, the skill is active
    ///   in the room: its instructions ride the system prompt from the
    ///   room's next turn, and a repeated activation answers that they are
    ///   in force. A room keeps four active skills at most, the least
    ///   recently used retired; the agent keeps them in memory, and reads
    ///   them again from the room's stored rows the first time it serves
    ///   the room (a restart), from the events its turn is handed. A turn
    ///   that stands alone carries none, and records none.
    /// - `read_skill_reference` answers a file of a skill's `references/`:
    ///   a name of `[A-Za-z0-9._-]` alone, resolved with its links followed
    ///   and inside that directory, at most 256 KiB. What it refuses, the
    ///   model reads why.
    ///
    /// No script runs: the preamble says so. A registry with no skill adds
    /// nothing.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::DuplicateTool`] when the agent serves either
    /// tool already: its skills are given once.
    pub fn with_skills(mut self, skills: SkillRegistry) -> Result<Self, AiError> {
        if skills.is_empty() {
            return Ok(self);
        }
        let tools = Arc::new(SkillTools::new(skills));
        let own: Arc<dyn DynToolset> = Arc::<SkillTools>::clone(&tools);
        Arc::make_mut(&mut self.toolbox).add_own(own, SkillTools::definitions())?;
        self.skills = Some(tools);
        Ok(self)
    }

    /// The same agent, calling only the tools `policy` permits (RFC §21.1):
    /// a tool it denies is never declared, and a call to it is refused.
    #[must_use]
    pub fn with_tool_policy(mut self, policy: ToolPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The same agent, running at most `rounds` rounds of calls per turn
    /// (Appendix A.9 `max_tool_rounds`): the generation past them stores its
    /// text, runs none of its calls, and ends the turn `max_rounds`.
    #[must_use]
    pub fn with_max_rounds(mut self, rounds: u32) -> Self {
        self.bounds.max_rounds = rounds;
        self
    }

    /// The same agent, giving each tool's call `bound` at most (RFC §21.6):
    /// past it, the call is cut and fails as a tool that raised
    /// (`ToolTimeoutError`). `None` bounds nothing.
    #[must_use]
    pub fn with_tool_timeout(mut self, bound: Option<Duration>) -> Self {
        self.bounds.tool_timeout = bound;
        self
    }

    /// The same agent, its model writing with each call one short sentence
    /// for the person, saying what the call does (RFC §6.4): each tool's
    /// schema is shown with the `_roomkit_intent` property, optional by default, which
    /// the loop takes out of the call's arguments before its gate and
    /// carries as the call's `intent`, on its hooks' event and its rows. A
    /// tool whose own schema declares the property keeps it. Off by default.
    #[must_use]
    pub fn with_tool_call_intents(mut self, on: bool) -> Self {
        self.tool_call_intents = on;
        self
    }

    /// Whether enabled tool-call intents are required in the schemas shown to
    /// the model. Optional by default. Enable intents separately with
    /// [`Self::with_tool_call_intents`]; this setting alone changes nothing.
    /// Tool-owned schemas are unchanged, and calls missing an intent still run:
    /// this controls schema presentation, not runtime validation or retries.
    #[must_use]
    pub fn with_tool_call_intents_required(mut self, required: bool) -> Self {
        self.tool_call_intents_required = required;
        self
    }

    /// Whether the agent takes a turn on `event`: a message or an
    /// instruction, never one of its own (RFC §6.4).
    fn answers(&self, event: &RoomEvent) -> bool {
        &event.source.channel_id != self.id()
            && (event.r#type == EventType::Message || event.r#type == EventType::Instruction)
    }

    /// The system prompt of a turn in `context`'s room: the agent's own,
    /// then the MCP servers' instructions (RFC §21.2), then what its skills
    /// add (RFC §6.4, §24.4).
    fn turn_system_prompt(&self, context: &RoomContext, stands_alone: bool) -> Option<String> {
        let prompt = self
            .server_instructions
            .after(self.system_prompt.as_deref());
        let Some(skills) = &self.skills else {
            return prompt;
        };
        let turn = SkillTurn {
            room: &context.room.id,
            agent: self.id(),
            history: &context.recent_events,
            stands_alone,
        };
        Some(skills.system_prompt(prompt.as_deref(), turn))
    }

    /// The source of the agent's rows.
    fn source(&self) -> EventSource {
        EventSource {
            direction: EventDirection::Outbound,
            ..EventSource::new(self.id().clone(), ChannelType::AI)
        }
    }
}

impl<P: AiProvider> Channel for AiAgent<P> {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
    }

    /// An AI agent takes no inbound message, as the reference
    /// implementation's AI channel refuses one: a message sent on its
    /// channel is refused before anything enters the room, so that nothing
    /// is stored under its id that it did not write, which its history
    /// would read as its own words and a restart as its own calls.
    fn handle_inbound(
        &self,
        _message: InboundMessage,
        _context: RoomContext,
        _draft: EventDraft,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        future::ready(Err("AI channel does not accept inbound messages".into()))
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        _binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        if !self.answers(&event) {
            return Ok(ChannelOutput::default());
        }
        let mut messages = conversation(self.id(), &event, &context, self.history, |name| {
            self.toolbox.in_tool_memory(name)
        });
        crate::images::resolve(
            &mut messages,
            self.provider.supports_vision(),
            self.image_resolver.as_deref(),
            &context.room.id,
        )
        .await;
        let turn = current_turn();
        let stands_alone = stands_alone(&event);
        let system_prompt = self.turn_system_prompt(&context, stands_alone);
        let tools = ToolContext::new(context.room.id.clone(), self.id().clone())
            .within(turn.clone())
            .standing_alone(stands_alone);
        let looped = Loop {
            provider: Arc::clone(&self.provider),
            toolbox: Arc::clone(&self.toolbox),
            policy: self.policy.clone(),
            bounds: self.bounds.clone(),
            system_prompt,
            turn: turn.clone(),
            context: tools,
            intents: self.tool_call_intents,
            intents_required: self.tool_call_intents_required,
        };
        let items = looped.run(messages);
        match turn {
            // Within the kit, the turn is one stream, read as it is written.
            Some(turn) => Ok(turn.respond_with_stream(rows::response(items))),
            None => Ok(rows::collect(self.source(), &event, items).await?),
        }
    }

    /// Closes the agent's toolsets, one after the other, in the order it
    /// took them ([`Toolset::close`]): an MCP server's process stops. A
    /// clone of the agent shares them.
    async fn close(&self) {
        self.toolbox.close().await;
    }
}

/// Whether `event` is an instruction that stands alone, which reads none
/// of the room's working state (RFC §10.1.1 rule 7).
fn stands_alone(event: &RoomEvent) -> bool {
    event.r#type == EventType::Instruction
        && event.metadata.get(STANDALONE) == Some(&Value::Bool(true))
}

impl<P> fmt::Debug for AiAgent<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AiAgent")
            .field("channel_id", &self.declaration.channel_id)
            .field("toolbox", &self.toolbox)
            .field("policy", &self.policy)
            .field("bounds", &self.bounds)
            .field("skills", &self.skills.is_some())
            .field("tool_call_intents", &self.tool_call_intents)
            .field(
                "tool_call_intents_required",
                &self.tool_call_intents_required,
            )
            .finish_non_exhaustive()
    }
}
