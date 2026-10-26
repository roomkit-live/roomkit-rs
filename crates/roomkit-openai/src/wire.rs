//! The Chat Completions wire: the request a turn sends, and what it reads
//! of the answer.

use roomkit::transcript::{Message, Role};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::OpenAiError;

/// The longest provider message an error keeps, in characters.
const MESSAGE_LIMIT: usize = 500;

/// A Chat Completions request: one completion, not streamed, with no tool.
#[derive(Debug, Serialize)]
pub(crate) struct CompletionRequest<'a> {
    model: &'a str,
    messages: Vec<WireMessage<'a>>,
}

/// One message of a request.
#[derive(Debug, Serialize)]
struct WireMessage<'a> {
    role: &'static str,
    content: &'a str,
}

impl<'a> CompletionRequest<'a> {
    /// The request asking `model` to answer `conversation`, after
    /// `system_prompt` when there is one.
    pub(crate) fn new(
        model: &'a str,
        system_prompt: Option<&'a str>,
        conversation: &'a [Message],
    ) -> Self {
        let system = system_prompt
            .filter(|prompt| !prompt.is_empty())
            .map(|prompt| WireMessage {
                role: "system",
                content: prompt,
            });
        let messages = system
            .into_iter()
            .chain(conversation.iter().map(|message| WireMessage {
                role: role(message.role),
                content: &message.text,
            }))
            .collect();
        Self { model, messages }
    }
}

/// The wire's name of `role`.
fn role(role: Role) -> &'static str {
    match role {
        Role::User => "user",
        Role::Assistant => "assistant",
    }
}

/// A chat completion, as far as a turn reads it.
#[derive(Debug, Deserialize)]
struct Completion {
    choices: Vec<Choice>,
}

/// One choice of a completion.
#[derive(Debug, Deserialize)]
struct Choice {
    message: AnswerMessage,
}

/// The message a choice answers with.
#[derive(Debug, Deserialize)]
struct AnswerMessage {
    #[serde(default)]
    content: Option<String>,
}

/// The text a completion's first choice answers with; `None` when it has
/// none (an empty answer, or a refusal the provider gives apart).
///
/// # Errors
///
/// Returns [`OpenAiError::Malformed`] when `body` is no chat completion.
pub(crate) fn answer(body: &[u8]) -> Result<Option<String>, OpenAiError> {
    let completion: Completion =
        serde_json::from_slice(body).map_err(|error| OpenAiError::Malformed {
            reason: error.to_string(),
        })?;
    let choice = completion
        .choices
        .into_iter()
        .next()
        .ok_or_else(|| OpenAiError::Malformed {
            reason: "it has no choice".to_owned(),
        })?;
    Ok(choice
        .message
        .content
        .filter(|text| !text.trim().is_empty()))
}

/// What an error answer says: the provider's message (`error.message`, as
/// OpenAI writes it, a top-level `message`, as vLLM does, or `error` as a
/// string), else the start of the answer, within 500 characters.
pub(crate) fn error_message(body: &[u8]) -> String {
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let message = parsed.as_ref().and_then(|value| {
        value
            .pointer("/error/message")
            .or_else(|| value.get("message"))
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .map(str::to_owned)
    });
    let message = message.unwrap_or_else(|| String::from_utf8_lossy(body).into_owned());
    message.trim().chars().take(MESSAGE_LIMIT).collect()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn an_error_answer_gives_the_provider_s_message() {
        let openai = br#"{"error": {"message": "Incorrect API key provided", "type": "invalid_request_error"}}"#;
        let vllm = br#"{"object": "error", "message": "The model does not exist.", "code": 404}"#;
        assert_eq!(error_message(openai), "Incorrect API key provided");
        assert_eq!(error_message(vllm), "The model does not exist.");
        assert_eq!(error_message(b"Bad Gateway\n"), "Bad Gateway");
    }

    #[test]
    fn a_completion_without_text_answers_nothing() {
        let body = br#"{"choices": [{"message": {"role": "assistant", "content": null}}]}"#;
        assert_eq!(answer(body), Ok(None));
    }
}
