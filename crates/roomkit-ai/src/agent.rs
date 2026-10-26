//! The AI channel (RFC §6.4, Appendix A.9): an intelligence channel whose
//! turn runs the tool loop over a provider.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventDirection, EventSource, EventType, RoomContext, RoomEvent,
};
use roomkit::transcript::conversation;
use roomkit::{Channel, ChannelDeclaration, ChannelError, current_turn};

use crate::error::AiError;
use crate::provider::AiProvider;
use crate::rows;
use crate::tool_loop::{Bounds, Loop};
use crate::tools::toolbox::Toolbox;
use crate::tools::{ToolContext, ToolPolicy, Toolset};

/// How many of the room's latest messages a turn sends by default: as many
/// as a kit's context can hold, 50 events.
const DEFAULT_HISTORY: usize = 50;

/// The most rounds of calls a turn runs by default (Appendix A.9
/// `max_tool_rounds`).
const DEFAULT_MAX_ROUNDS: u32 = 50;

/// The longest a tool's call may take by default (RFC §21.6).
const DEFAULT_TOOL_TIMEOUT: Duration = Duration::from_secs(30);

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
/// The turn returns its rows at its end (step 4, milestone 5): each round's
/// text as a MESSAGE, a `TOOL_CALL_START` per call, then the round's
/// `TOOL_CALL_END` rows in call order. The last MESSAGE carries the turn's
/// record, `loop_end_reason` and `ai_usage` in its metadata. A provider's
/// error before any round fails the turn; after one, the rows stand and the
/// turn's error is reported (RFC §6.4 "A turn cut short"). A later turn
/// reads each round's text, never its calls.
///
/// It answers a message or an instruction, never its own events. Cloning
/// it shares its provider and its tools.
pub struct AiAgent<P> {
    declaration: ChannelDeclaration,
    provider: Arc<P>,
    toolbox: Arc<Toolbox>,
    policy: ToolPolicy,
    bounds: Bounds,
    system_prompt: Option<String>,
    history: usize,
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
            history: self.history,
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
        // Its turn is one stream of rows, as RFC §6.4 reads every AI turn:
        // a turn another event's delivery asks for draws its reentry pass
        // before it runs (RFC §8.3).
        declaration.answers_as_stream = true;
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
            history: DEFAULT_HISTORY,
        }
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
    /// keeps it (RFC §6.4).
    #[must_use]
    pub fn with_system_prompt(mut self, prompt: impl Into<String>) -> Self {
        self.system_prompt = Some(prompt.into()).filter(|prompt| !prompt.is_empty());
        self
    }

    /// The same agent, sending at most the room's latest `messages`
    /// messages before the turn's input. A turn reads only the context the
    /// kit hands it, the room's latest 50 events: a value above what it
    /// holds sends no more.
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

    /// Whether the agent takes a turn on `event`: a message or an
    /// instruction, never one of its own (RFC §6.4).
    fn answers(&self, event: &RoomEvent) -> bool {
        &event.source.channel_id != self.id()
            && (event.event_type == EventType::Message
                || event.event_type == EventType::Instruction)
    }

    /// The source of the agent's rows.
    fn source(&self) -> EventSource {
        EventSource::new(self.id().clone(), ChannelType::AI, EventDirection::Outbound)
    }
}

impl<P: AiProvider> Channel for AiAgent<P> {
    fn declaration(&self) -> ChannelDeclaration {
        self.declaration.clone()
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
        let messages = conversation(self.id(), &event, &context, self.history);
        let turn = current_turn();
        let tools =
            ToolContext::new(context.room.id.clone(), self.id().clone()).within(turn.clone());
        let looped = Loop {
            provider: &*self.provider,
            toolbox: &self.toolbox,
            policy: &self.policy,
            bounds: &self.bounds,
            system_prompt: self.system_prompt.clone(),
            turn,
            context: tools,
        };
        rows::collect(self.source(), &event, looped.run(messages)).await
    }
}

impl<P> fmt::Debug for AiAgent<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AiAgent")
            .field("channel_id", &self.declaration.channel_id)
            .field("toolbox", &self.toolbox)
            .field("policy", &self.policy)
            .field("bounds", &self.bounds)
            .finish_non_exhaustive()
    }
}
