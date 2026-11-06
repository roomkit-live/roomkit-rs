//! What a vendor's Chat Completions server reads of a model's earlier
//! reasoning (RFC §6.4): Python's `ChatDialect`, its reasoning fields, which
//! the vendor providers pick; OpenAI's own rendering sends none.

/// Where a vendor reads a replayed round's reasoning, as Python's
/// `ChatDialect` states it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChatDialect {
    /// The assistant message field the vendor reads it from; none sends no
    /// reasoning: any other server would read it as words the model said.
    pub(crate) thinking_field: Option<&'static str>,
    /// Every round that called tools carries the field, even one that did
    /// not reason.
    pub(crate) round_thinking_required: bool,
    /// What a required field holds for a round that did not reason.
    pub(crate) round_thinking_placeholder: &'static str,
}

/// OpenAI's own rendering, which every compatible server reads.
pub(crate) const OPENAI_CHAT: ChatDialect = ChatDialect {
    thinking_field: None,
    round_thinking_required: false,
    round_thinking_placeholder: "",
};

/// DeepSeek's: in thinking mode it refuses a round of the turn in progress
/// without its `reasoning_content`, and takes an empty one (Python's
/// `DEEPSEEK_CHAT`).
pub(crate) const DEEPSEEK_CHAT: ChatDialect = ChatDialect {
    thinking_field: Some("reasoning_content"),
    round_thinking_required: true,
    round_thinking_placeholder: "",
};

/// MiniMax's: the field on a round or an answer that reasoned, none on one
/// that did not (Python's `MINIMAX_CHAT`).
pub(crate) const MINIMAX_CHAT: ChatDialect = ChatDialect {
    thinking_field: Some("reasoning_content"),
    round_thinking_required: false,
    round_thinking_placeholder: "",
};

/// Kimi's: the field on every round that called tools, a single space for
/// one that did not reason, since an empty one reads as missing (Python's
/// `KIMI_CHAT`).
pub(crate) const KIMI_CHAT: ChatDialect = ChatDialect {
    thinking_field: Some("reasoning_content"),
    round_thinking_required: true,
    round_thinking_placeholder: " ",
};

impl ChatDialect {
    /// The reasoning field an assistant's message carries, and its value:
    /// its `thinking` when it has any, the placeholder on a round that
    /// `calls` when the field is required, none otherwise (Python's
    /// `chat_request._with_thinking_field`).
    pub(crate) fn field(&self, thinking: &str, calls: bool) -> Option<(&'static str, String)> {
        let field = self.thinking_field?;
        if !thinking.is_empty() {
            return Some((field, thinking.to_owned()));
        }
        (calls && self.round_thinking_required)
            .then(|| (field, self.round_thinking_placeholder.to_owned()))
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
