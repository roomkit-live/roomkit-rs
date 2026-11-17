//! What a vendor's Chat Completions server reads of a model's earlier
//! reasoning (RFC §6.4): Python's `ChatDialect`, its reasoning fields, which
//! the vendor providers pick; OpenAI's own rendering sends none, unless its
//! configuration names the field (`OpenAiConfig::thinking_field`).

/// Where a vendor reads a replayed round's reasoning, as Python's
/// `ChatDialect` states it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChatDialect<'a> {
    /// The assistant message field the vendor reads it from; none sends no
    /// reasoning: any other server would read it as words the model said.
    pub(crate) thinking_field: Option<&'a str>,
    /// Every round that called tools carries the field, even one that did
    /// not reason.
    pub(crate) round_thinking_required: bool,
    /// What a required field holds for a round that did not reason.
    pub(crate) round_thinking_placeholder: &'static str,
}

/// OpenAI's own rendering, which every compatible server reads.
pub(crate) const OPENAI_CHAT: ChatDialect<'static> = ChatDialect {
    thinking_field: None,
    round_thinking_required: false,
    round_thinking_placeholder: "",
};

/// DeepSeek's: in thinking mode it refuses a round of the turn in progress
/// without its `reasoning_content`, and takes an empty one (Python's
/// `DEEPSEEK_CHAT`).
pub(crate) const DEEPSEEK_CHAT: ChatDialect<'static> = ChatDialect {
    thinking_field: Some("reasoning_content"),
    round_thinking_required: true,
    round_thinking_placeholder: "",
};

/// MiniMax's: the field on a round or an answer that reasoned, none on one
/// that did not (Python's `MINIMAX_CHAT`).
pub(crate) const MINIMAX_CHAT: ChatDialect<'static> = ChatDialect {
    thinking_field: Some("reasoning_content"),
    round_thinking_required: false,
    round_thinking_placeholder: "",
};

/// Kimi's: the field on every round that called tools, a single space for
/// one that did not reason, since an empty one reads as missing (Python's
/// `KIMI_CHAT`).
pub(crate) const KIMI_CHAT: ChatDialect<'static> = ChatDialect {
    thinking_field: Some("reasoning_content"),
    round_thinking_required: true,
    round_thinking_placeholder: " ",
};

impl<'a> ChatDialect<'a> {
    /// This rendering, reasoning in the field the configuration names when
    /// it names one (Python's `OpenAIAIProvider._dialect`).
    pub(crate) fn with_thinking_field(self, field: Option<&'a str>) -> Self {
        match field {
            Some(_) => Self {
                thinking_field: field,
                ..self
            },
            None => self,
        }
    }

    /// Render the original message parts with the public Chat Completions renderer.
    pub(crate) fn messages(
        &self,
        context: &roomkit_ai::AiContext,
        provider: &str,
    ) -> Result<Vec<serde_json::Value>, roomkit_ai::ProviderError> {
        let options = roomkit_ai::ChatDialectOptions {
            thinking_field: self.thinking_field.map(str::to_owned),
            round_thinking_required: self.round_thinking_required,
            round_thinking_placeholder: self.round_thinking_placeholder.into(),
            ..Default::default()
        };
        let dialect = roomkit_ai::ChatDialect::new(options).map_err(|error| {
            roomkit_ai::ProviderError::new(
                error.to_string(),
                roomkit_ai::ProviderErrorOptions {
                    provider: provider.into(),
                    ..Default::default()
                },
            )
        })?;
        roomkit_ai::chat_messages(
            &context.messages,
            context.system_prompt.as_deref(),
            &dialect,
            provider,
        )
    }

    #[cfg(test)]
    fn field(&self, thinking: &str, calls: bool) -> Option<(&'a str, String)> {
        use roomkit_ai::{
            AiContentPart, AiContext, AiMessage, AiRole, AiThinkingPart, AiToolCallPart,
        };
        let field = self.thinking_field?;
        let mut parts = vec![AiContentPart::Thinking(AiThinkingPart::new(thinking))];
        if calls {
            parts.push(AiContentPart::ToolCall(AiToolCallPart::new("c", "tool")));
        }
        let ctx = AiContext {
            messages: vec![AiMessage::new(AiRole::Assistant, parts)],
            ..AiContext::default()
        };
        self.messages(&ctx, "app").unwrap()[0][field]
            .as_str()
            .map(|value| (field, value.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Python: tests/test_providers/test_chat_request.py::test_a_dialect_with_a_reasoning_field_carries_it_there
    #[test]
    fn a_dialect_with_a_reasoning_field_carries_it_there() {
        for dialect in [DEEPSEEK_CHAT, MINIMAX_CHAT, KIMI_CHAT] {
            assert_eq!(
                dialect.field("why", true),
                Some(("reasoning_content", "why".to_owned()))
            );
            assert_eq!(
                dialect.field("why", false),
                Some(("reasoning_content", "why".to_owned()))
            );
        }
        assert_eq!(OPENAI_CHAT.field("why", true), None);
    }

    // Python: tests/test_providers/test_chat_request.py::test_a_configured_thinking_field_leaves_a_round_that_did_not_reason_without_it
    #[test]
    fn a_named_field_carries_reasoning_and_leaves_a_round_that_did_not_reason_without_it() {
        let dialect = OPENAI_CHAT.with_thinking_field(Some("reasoning"));
        assert_eq!(
            dialect.field("why", true),
            Some(("reasoning", "why".to_owned()))
        );
        assert_eq!(
            dialect.field("why", false),
            Some(("reasoning", "why".to_owned()))
        );
        assert_eq!(dialect.field("", true), None);
        assert_eq!(OPENAI_CHAT.with_thinking_field(None), OPENAI_CHAT);
    }

    // Python: tests/test_providers/test_deepseek.py::test_a_tool_round_that_did_not_reason_carries_the_field_empty
    // and the scenarios reasoning/kimi-s-dialect-... and reasoning/minimax-s-dialect-...
    #[test]
    fn a_round_with_no_reasoning_carries_the_dialect_s_placeholder_where_required() {
        assert_eq!(
            DEEPSEEK_CHAT.field("", true),
            Some(("reasoning_content", String::new()))
        );
        assert_eq!(
            KIMI_CHAT.field("", true),
            Some(("reasoning_content", " ".to_owned()))
        );
        assert_eq!(MINIMAX_CHAT.field("", true), None);
        assert_eq!(KIMI_CHAT.field("", false), None);
    }
}
