//! An AI channel with an identity (Python's `roomkit.Agent`): its role,
//! description and scope written into its turns' system prompt as an
//! identity block, its identity declared to the kit.
//!
//! Python's `Agent` is an `AIChannel` subclass; this `Agent` holds its
//! [`AiChannel`] and is a [`Channel`] by handing each call to it, but for
//! its declaration, which carries the identity (decided 2026-11-02).

use std::collections::BTreeMap;
use std::future::Future;

use roomkit::model::{
    ChannelBinding, ChannelCapabilities, ChannelCategory, ChannelDirection, ChannelId,
    ChannelOutput, ChannelType, InboundMessage, Metadata, Room, RoomContext, RoomEvent, RoomId,
    SteeringDirective,
};
use roomkit::{AgentIdentity, Channel, ChannelDeclaration, ChannelError, DeliveryStream};
use serde_json::Value;

use crate::channel::{AiChannel, AiChannelOptions};
use crate::error::AiError;
use crate::provider::AiProvider;
use crate::steering::SteerOptions;
use crate::tools::ToolHandler;

/// The room metadata key Python's conversation state is stored under
/// (`roomkit.orchestration.state`).
const CONVERSATION_STATE: &str = "_conversation_state";

/// What [`Agent::new`] takes beside the channel's id and provider:
/// Python's `Agent(channel_id, *, provider, name=None, role=None,
/// description=None, purpose=None, scope=None, voice=None, greeting=None,
/// language=None, auto_greet=True, identity_in_prompt=True, **kwargs)`,
/// the AI channel's own keywords in `ai`.
#[derive(Clone, Debug)]
pub struct AgentOptions {
    /// Its display name, distinct from its channel id and its role.
    pub name: Option<String>,
    /// What it does: the identity block's `Role:`.
    pub role: Option<String>,
    /// A short label of what it does: the block's `Description:`.
    pub description: Option<String>,
    /// Its own instructions in concise form, for an orchestrator to frame
    /// its tasks with; not in the block.
    pub purpose: Option<String>,
    /// What it answers and what it leaves: the block's `Scope:`.
    pub scope: Option<String>,
    /// The voice it speaks with; not in the block.
    pub voice: Option<String>,
    /// What it says first once a session with it is ready; not in the
    /// block. Said in a room a text session starts in, with `auto_greet`, and
    /// by `RoomKit::send_greeting`; the voice sessions come with the voice
    /// channels.
    pub greeting: Option<String>,
    /// The language it answers in, unless the room's conversation state
    /// sets one: the block's `Language:`.
    pub language: Option<String>,
    /// Whether it greets a session once ready: `true` by default.
    pub auto_greet: bool,
    /// Whether its turns' system prompt ends with its identity block:
    /// `true` by default. `false` keeps the fields readable for a host that
    /// writes the identity in its own prompt.
    pub identity_in_prompt: bool,
    /// The AI channel's options: Python's `**kwargs`.
    pub ai: AiChannelOptions,
}

impl Default for AgentOptions {
    fn default() -> Self {
        Self {
            name: None,
            role: None,
            description: None,
            purpose: None,
            scope: None,
            voice: None,
            greeting: None,
            language: None,
            auto_greet: true,
            identity_in_prompt: true,
            ai: AiChannelOptions::default(),
        }
    }
}

/// What [`Agent::build_identity_block`] takes: Python's
/// `build_identity_block(language=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BuildIdentityBlockOptions {
    /// The language the block names, over the agent's own: Python's
    /// `language=None`, the room's conversation state's in a turn.
    pub language: Option<String>,
}

/// An AI channel with an identity: Python's `roomkit.Agent`.
///
/// ```
/// use roomkit_ai::{Agent, AgentOptions, AiChannelOptions, MockAiProvider, MockAiProviderOptions};
/// use roomkit::model::ChannelId;
///
/// let advisor = Agent::new(
///     ChannelId::new("advisor")?,
///     MockAiProvider::new(MockAiProviderOptions::default()),
///     AgentOptions {
///         role: Some("Advisor".to_owned()),
///         description: Some("Gives financial advice".to_owned()),
///         ai: AiChannelOptions {
///             system_prompt: Some("Be helpful.".to_owned()),
///             ..AiChannelOptions::default()
///         },
///         ..AgentOptions::default()
///     },
/// )?;
/// assert_eq!(
///     advisor.build_identity_block(Default::default()).as_deref(),
///     Some("\n--- Agent Identity ---\nRole: Advisor\nDescription: Gives financial advice"),
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct Agent<P> {
    channel: AiChannel<P>,
    identity: AgentIdentity,
    identity_in_prompt: bool,
}

impl<P: AiProvider> Agent<P> {
    /// The agent `channel_id`, answering with `provider`.
    ///
    /// # Errors
    ///
    /// Returns what [`AiChannel::new`] refuses `options.ai` with.
    pub fn new(channel_id: ChannelId, provider: P, options: AgentOptions) -> Result<Self, AiError> {
        let AgentOptions {
            name,
            role,
            description,
            purpose,
            scope,
            voice,
            greeting,
            language,
            auto_greet,
            identity_in_prompt,
            ai,
        } = options;
        let identity = AgentIdentity {
            name,
            role,
            description,
            scope,
            greeting,
            language,
            purpose,
            voice,
            auto_greet,
        };
        let prompt = identity_in_prompt.then(|| IdentityPrompt::of(&identity));
        let channel = AiChannel::new(channel_id, provider, ai)?.with_identity(prompt);
        Ok(Self {
            channel,
            identity,
            identity_in_prompt,
        })
    }

    /// The AI channel the agent answers through.
    pub fn channel(&self) -> &AiChannel<P> {
        &self.channel
    }

    /// Its identity block, in `options.language` when given, else in its
    /// own: its role, description, scope and language, each on a line
    /// under a header; `None` when it sets none of them, or when
    /// `identity_in_prompt` is off. Python's
    /// `build_identity_block(language=None)`.
    pub fn build_identity_block(&self, options: BuildIdentityBlockOptions) -> Option<String> {
        let BuildIdentityBlockOptions { language } = options;
        if !self.identity_in_prompt {
            return None;
        }
        IdentityPrompt::of(&self.identity).block(language.as_deref())
    }

    /// The provider its turns answer with: Python's `provider`.
    pub fn provider(&self) -> &P {
        self.channel.provider()
    }

    /// Replaces what every turn's system prompt opens with: Python's
    /// `system_prompt` setter, as [`AiChannel::set_system_prompt`].
    pub fn set_system_prompt(&self, prompt: Option<String>) {
        self.channel.set_system_prompt(prompt);
    }

    /// The handler serving its tools' calls: Python's `tool_handler`.
    pub fn tool_handler(&self) -> Option<ToolHandler> {
        self.channel.tool_handler()
    }

    /// The skills active in `room_id`: Python's `active_skill_names`.
    pub fn active_skill_names(&self, room_id: &RoomId) -> Vec<String> {
        self.channel.active_skill_names(room_id)
    }

    /// Steers its running turns: Python's `steer`, as [`AiChannel::steer`].
    ///
    /// # Errors
    ///
    /// Returns what [`AiChannel::steer`] returns.
    pub fn steer(
        &self,
        directive: impl Into<SteeringDirective>,
        options: SteerOptions,
    ) -> Result<usize, AiError> {
        self.channel.steer(directive, options)
    }

    /// Its display name: Python's `name`.
    pub fn name(&self) -> Option<&str> {
        self.identity.name.as_deref()
    }

    /// What it does: Python's `role`.
    pub fn role(&self) -> Option<&str> {
        self.identity.role.as_deref()
    }

    /// A short label of what it does: Python's `description`.
    pub fn description(&self) -> Option<&str> {
        self.identity.description.as_deref()
    }

    /// Its own instructions in concise form: Python's `purpose`.
    pub fn purpose(&self) -> Option<&str> {
        self.identity.purpose.as_deref()
    }

    /// What it answers and what it leaves: Python's `scope`.
    pub fn scope(&self) -> Option<&str> {
        self.identity.scope.as_deref()
    }

    /// The voice it speaks with: Python's `voice`.
    pub fn voice(&self) -> Option<&str> {
        self.identity.voice.as_deref()
    }

    /// What it says first once a session is ready: Python's `greeting`.
    pub fn greeting(&self) -> Option<&str> {
        self.identity.greeting.as_deref()
    }

    /// The language it answers in: Python's `language`.
    pub fn language(&self) -> Option<&str> {
        self.identity.language.as_deref()
    }

    /// Whether it greets a session once ready: Python's `auto_greet`.
    pub fn auto_greet(&self) -> bool {
        self.identity.auto_greet
    }

    /// Whether its turns' prompt ends with its identity block: Python's
    /// `identity_in_prompt`.
    pub fn identity_in_prompt(&self) -> bool {
        self.identity_in_prompt
    }

    /// What every turn's system prompt opens with: Python's
    /// `system_prompt`.
    pub fn system_prompt(&self) -> Option<String> {
        self.channel.system_prompt()
    }
}

impl<P> std::fmt::Debug for Agent<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Agent")
            .field("identity", &self.identity)
            .field("identity_in_prompt", &self.identity_in_prompt)
            .finish_non_exhaustive()
    }
}

/// The fields of an agent's identity its block writes.
#[derive(Clone, Debug)]
pub(crate) struct IdentityPrompt {
    role: Option<String>,
    description: Option<String>,
    scope: Option<String>,
    language: Option<String>,
}

impl IdentityPrompt {
    fn of(identity: &AgentIdentity) -> Self {
        Self {
            role: identity.role.clone(),
            description: identity.description.clone(),
            scope: identity.scope.clone(),
            language: identity.language.clone(),
        }
    }

    /// The block, in `language` when it is set and not empty, else in the
    /// agent's own (Python's `language or self.language`); `None` when no
    /// line is set.
    pub(crate) fn block(&self, language: Option<&str>) -> Option<String> {
        let language = language
            .filter(|language| !language.is_empty())
            .or(self.language.as_deref());
        let lines: Vec<String> = [
            self.role.as_deref().map(|role| format!("Role: {role}")),
            self.description
                .as_deref()
                .map(|description| format!("Description: {description}")),
            self.scope.as_deref().map(|scope| format!("Scope: {scope}")),
            language.map(|language| format!("Language: Always respond in {language}")),
        ]
        .into_iter()
        .flatten()
        .collect();
        if lines.is_empty() {
            return None;
        }
        Some(format!("\n--- Agent Identity ---\n{}", lines.join("\n")))
    }
}

/// The language `room`'s conversation state sets for its agents, if any:
/// its `context`'s `language` (Python's `handler.set_language()`), read
/// from the stored state itself: Python's lax validation takes a state the
/// typed read refuses (a count written as text, a naive instant), and the
/// language comes through it.
pub(crate) fn room_language(room: &Room) -> Option<&str> {
    room.metadata
        .get(CONVERSATION_STATE)?
        .get("context")?
        .get("language")
        .and_then(Value::as_str)
}

impl<P: AiProvider> Channel for Agent<P> {
    fn channel_id(&self) -> ChannelId {
        self.channel.channel_id()
    }

    fn channel_type(&self) -> ChannelType {
        self.channel.channel_type()
    }

    fn category(&self) -> ChannelCategory {
        self.channel.category()
    }

    fn direction(&self) -> ChannelDirection {
        self.channel.direction()
    }

    fn capabilities(&self) -> ChannelCapabilities {
        self.channel.capabilities()
    }

    fn provider_name(&self) -> Option<String> {
        self.channel.provider_name()
    }

    fn info(&self) -> Metadata {
        self.channel.info()
    }

    fn normalize_address(&self, address: &str) -> String {
        self.channel.normalize_address(address)
    }

    fn recipient_key(&self) -> Option<String> {
        self.channel.recipient_key()
    }

    fn recipient_address(&self, binding: &ChannelBinding) -> Option<String> {
        self.channel.recipient_address(binding)
    }

    fn recipient_metadata(&self, address: &str) -> BTreeMap<String, String> {
        self.channel.recipient_metadata(address)
    }

    fn conversation_address(&self, message: &InboundMessage) -> String {
        self.channel.conversation_address(message)
    }

    fn reply_metadata(&self, message: &InboundMessage) -> BTreeMap<String, String> {
        self.channel.reply_metadata(message)
    }

    fn recent_events_window(&self) -> usize {
        self.channel.recent_events_window()
    }

    fn sender_is_participant(&self) -> bool {
        self.channel.sender_is_participant()
    }

    fn supports_streaming_delivery(&self) -> bool {
        self.channel.supports_streaming_delivery()
    }

    fn supports_streaming_delivery_for(&self, room_id: &RoomId) -> bool {
        self.channel.supports_streaming_delivery_for(room_id)
    }

    fn steer_turn(&self, directive: &SteeringDirective, turn_id: &str) -> Option<usize> {
        self.channel.steer_turn(directive, turn_id)
    }

    fn own_tool_names(&self) -> Vec<String> {
        self.channel.own_tool_names()
    }

    fn serializes_room_turns(&self) -> bool {
        Channel::serializes_room_turns(&self.channel)
    }

    fn steers_room_turns(&self) -> bool {
        Channel::steers_room_turns(&self.channel)
    }

    /// The AI channel's declaration, with the agent's identity (Python's
    /// `Agent.declaration()`).
    fn declaration(&self) -> ChannelDeclaration {
        ChannelDeclaration {
            identity: Some(self.identity.clone()),
            ..self.channel.declaration()
        }
    }

    fn extract_text(event: &RoomEvent) -> String
    where
        Self: Sized,
    {
        AiChannel::<P>::extract_text(event)
    }

    fn handle_inbound(
        &self,
        message: InboundMessage,
        context: RoomContext,
    ) -> impl Future<Output = Result<RoomEvent, ChannelError>> + Send {
        self.channel.handle_inbound(message, context)
    }

    fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.channel.on_event(event, binding, context)
    }

    fn on_room_attached(
        &self,
        room_id: RoomId,
        binding: ChannelBinding,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send {
        self.channel.on_room_attached(room_id, binding)
    }

    fn on_room_detached(
        &self,
        room_id: RoomId,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send {
        self.channel.on_room_detached(room_id)
    }

    fn on_room_event(
        &self,
        room_id: RoomId,
        event: RoomEvent,
    ) -> impl Future<Output = Result<(), ChannelError>> + Send {
        self.channel.on_room_event(room_id, event)
    }

    fn deliver(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.channel.deliver(event, binding, context)
    }

    fn deliver_stream(
        &self,
        text_stream: DeliveryStream,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> impl Future<Output = Result<ChannelOutput, ChannelError>> + Send {
        self.channel
            .deliver_stream(text_stream, event, binding, context)
    }

    fn close(&self) -> impl Future<Output = ()> + Send {
        self.channel.close()
    }

    fn human_input_handler(&self) -> Option<roomkit::human_input::HumanInputHandler> {
        self.channel.human_input_handler()
    }
}

#[cfg(test)]
mod tests {
    use roomkit::model::{Room, RoomId};
    use serde_json::json;

    use super::room_language;

    /// Python's `_prompt_identity` reads the room's language from a state
    /// its lax validation takes (a count written as text, a naive instant):
    /// the language comes through whatever the state's other fields hold.
    #[test]
    fn the_room_s_language_reads_through_a_state_python_takes() {
        for other in [
            json!({"handoff_count": "1"}),
            json!({"phase_started_at": "2026-11-02T10:00:00"}),
        ] {
            let mut state = other.as_object().unwrap().clone();
            state.insert("context".to_owned(), json!({"language": "fr"}));
            let mut room = Room::new(RoomId::new("r1").unwrap());
            room.metadata
                .insert("_conversation_state".to_owned(), state.into());
            assert_eq!(room_language(&room), Some("fr"));
        }
    }
}
