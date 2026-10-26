//! The Chat Completions wire: the request a turn sends, and what it reads
//! of the answer.

use reqwest::StatusCode;
use roomkit::transcript::{Message, Role};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::OpenAiError;

/// The longest provider message an error keeps, in characters.
const MESSAGE_LIMIT: usize = 500;

/// The longest error type or code an error keeps, in characters.
const NAME_LIMIT: usize = 64;

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
        Role::Assistant => "assistant",
        // A role this agent does not know yet reads as a user's: never as
        // the model's own words, nor as the system's.
        Role::User | _ => "user",
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
/// Returns [`OpenAiError::Malformed`] when `body` is no chat completion,
/// saying why without `secret`, the API key.
pub(crate) fn answer(body: &[u8], secret: &str) -> Result<Option<String>, OpenAiError> {
    let malformed = |reason: String| OpenAiError::Malformed {
        reason: cleaned(&reason, secret, MESSAGE_LIMIT),
    };
    let completion: Completion =
        serde_json::from_slice(body).map_err(|error| malformed(error.to_string()))?;
    let choice = completion
        .choices
        .into_iter()
        .next()
        .ok_or_else(|| malformed("it has no choice".to_owned()))?;
    Ok(choice
        .message
        .content
        .filter(|text| !text.trim().is_empty()))
}

/// The error an answer of HTTP `status` holding `body` fails a turn with:
/// the provider's type, code and message, as OpenAI writes them under
/// `error`, or vLLM at the top level, else the status's reason. Each is on
/// one line, bounded, and holds no `secret`, the API key: a provider may
/// echo the request. The rest of the answer is left out.
pub(crate) fn status_error(status: StatusCode, body: &[u8], secret: &str) -> OpenAiError {
    let parsed: Option<Value> = serde_json::from_slice(body).ok();
    let field = |name: &str| {
        let value = parsed.as_ref()?;
        let error = value.get("error").filter(|error| error.is_object());
        let found = error
            .and_then(|error| error.get(name))
            .or_else(|| value.get(name));
        match found? {
            Value::String(text) => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
        }
    };
    let message = field("message")
        .or_else(|| {
            let value = parsed.as_ref()?;
            value.get("error")?.as_str().map(str::to_owned)
        })
        .unwrap_or_else(|| status.canonical_reason().unwrap_or("no message").to_owned());
    let named = |text: String| Some(cleaned(&text, secret, NAME_LIMIT)).filter(|t| !t.is_empty());
    OpenAiError::Status {
        status: status.as_u16(),
        error_type: field("type").and_then(named),
        code: field("code").and_then(named),
        message: cleaned(&message, secret, MESSAGE_LIMIT),
    }
}

/// `text` as an error carries it: `secret` removed wherever it appears,
/// on one line, within `limit` characters.
fn cleaned(text: &str, secret: &str, limit: usize) -> String {
    let removed = if secret.is_empty() {
        text.to_owned()
    } else {
        text.replace(secret, "[removed]")
    };
    removed
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(limit)
        .collect()
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn an_error_answer_gives_the_provider_s_type_code_and_message() {
        let openai = br#"{"error": {"message": "Incorrect API key", "type": "invalid_request_error", "code": "invalid_api_key"}}"#;
        let vllm = br#"{"object": "error", "message": "The model does not exist.", "type": "NotFoundError", "code": 404}"#;
        assert_eq!(
            status_error(StatusCode::UNAUTHORIZED, openai, "sk-1").to_string(),
            "the provider answered HTTP 401 (invalid_request_error, invalid_api_key): Incorrect API key"
        );
        assert_eq!(
            status_error(StatusCode::NOT_FOUND, vllm, "sk-1").to_string(),
            "the provider answered HTTP 404 (NotFoundError, 404): The model does not exist."
        );
    }

    #[test]
    fn an_error_answer_that_is_no_json_gives_the_status_s_reason_only() {
        let error = status_error(StatusCode::BAD_GATEWAY, b"<html>echo: sk-1</html>", "sk-1");
        assert_eq!(
            error.to_string(),
            "the provider answered HTTP 502: Bad Gateway"
        );
    }

    #[test]
    fn a_completion_without_text_answers_nothing() {
        let body = br#"{"choices": [{"message": {"role": "assistant", "content": null}}]}"#;
        assert_eq!(answer(body, "sk-1"), Ok(None));
    }

    #[test]
    fn a_completion_with_no_choice_is_malformed() {
        assert_eq!(
            answer(br#"{"id": "chatcmpl-1", "choices": []}"#, "sk-1"),
            Err(OpenAiError::Malformed {
                reason: "it has no choice".to_owned()
            })
        );
    }

    #[test]
    fn a_malformed_answer_s_reason_holds_no_key() {
        let body = br#"{"choices": "sk-1"}"#;
        let error = answer(body, "sk-1").unwrap_err();
        assert!(!error.to_string().contains("sk-1"), "{error}");
    }
}
