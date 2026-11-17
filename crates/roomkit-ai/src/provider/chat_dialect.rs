//! The provider-specific choices in a Chat Completions message (RFC §6.7).

use crate::AiError;

/// The keyword arguments of Python's `ChatDialect()`.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ChatDialectOptions {
    /// The assistant field holding prior reasoning.
    pub thinking_field: Option<String>,
    /// Include the tool's name on each result.
    pub names_tool_results: bool,
    /// Join text parts and drop messages that carry no content.
    pub flattens_text: bool,
    /// Include a reasoning field on every assistant tool round.
    pub round_thinking_required: bool,
    /// Value to write for a required field when the round did not reason.
    pub round_thinking_placeholder: String,
}

/// How a Chat Completions endpoint reads messages and previous tool rounds.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ChatDialect(ChatDialectOptions);

/// OpenAI's default message dialect.
pub const OPENAI_CHAT: ChatDialect = ChatDialect(ChatDialectOptions {
    thinking_field: None,
    names_tool_results: false,
    flattens_text: false,
    round_thinking_required: false,
    round_thinking_placeholder: String::new(),
});

impl ChatDialect {
    /// Construct a dialect from the provider's choices.
    ///
    /// # Errors
    ///
    /// Returns [`AiError::ChatDialect`] if a required field is unnamed or
    /// a placeholder is configured for a field that is not required.
    pub fn new(options: ChatDialectOptions) -> Result<Self, AiError> {
        if options.round_thinking_required && options.thinking_field.is_none() {
            return Err(AiError::ChatDialect {
                reason: "round_thinking_required needs a thinking_field to send",
            });
        }
        if !options.round_thinking_placeholder.is_empty() && !options.round_thinking_required {
            return Err(AiError::ChatDialect {
                reason: "round_thinking_placeholder needs round_thinking_required",
            });
        }
        Ok(Self(options))
    }

    /// The field receiving prior assistant reasoning.
    pub fn thinking_field(&self) -> Option<&str> {
        self.0.thinking_field.as_deref()
    }

    /// Whether a tool message names the tool it answered.
    pub fn names_tool_results(&self) -> bool {
        self.0.names_tool_results
    }

    /// Whether text parts are joined and messages with no content are omitted.
    pub fn flattens_text(&self) -> bool {
        self.0.flattens_text
    }

    /// Whether every assistant round with calls needs a reasoning field.
    pub fn round_thinking_required(&self) -> bool {
        self.0.round_thinking_required
    }

    /// What a required reasoning field carries when the model supplied no thought.
    pub fn round_thinking_placeholder(&self) -> &str {
        &self.0.round_thinking_placeholder
    }
}
