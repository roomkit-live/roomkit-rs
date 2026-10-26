//! The agent: an intelligence channel whose turn is one chat completion.

use std::fmt;

use roomkit::model::{
    ChannelBinding, ChannelCategory, ChannelDirection, ChannelId, ChannelOutput, ChannelType,
    EventContent, EventDirection, EventSource, EventType, RoomContext, RoomEvent,
};
use roomkit::transcript::{Message, conversation};
use roomkit::{Channel, ChannelDeclaration, ChannelError};

use crate::config::OpenAiConfig;
use crate::error::OpenAiError;
use crate::wire::{self, CompletionRequest};

/// The path a completion is asked at, after the base URL.
const COMPLETIONS_PATH: &str = "/chat/completions";

/// The longest answer a turn reads, in bytes: far more than a completion's
/// text takes.
const ANSWER_LIMIT: usize = 4 * 1024 * 1024;

/// An AI agent channel answering through an OpenAI-compatible Chat
/// Completions endpoint (RFC §6.4).
///
/// Each turn sends the configuration's system prompt, then the conversation
/// the room hands the agent ([`roomkit::transcript::conversation`]), and
/// answers with the completion's text. It answers a message or an
/// instruction, never its own events; a completion with no text answers
/// nothing. A turn that fails returns an [`OpenAiError`]; an answer longer
/// than 4 MiB fails it too. Cloning it shares its HTTP client.
#[derive(Clone)]
pub struct OpenAiAgent {
    declaration: ChannelDeclaration,
    config: OpenAiConfig,
    endpoint: reqwest::Url,
    client: reqwest::Client,
}

impl OpenAiAgent {
    /// An agent named `channel_id`, configured by `config`.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiError::BaseUrl`] when the base URL is no `http` or
    /// `https` URL, or holds a query or a fragment, and
    /// [`OpenAiError::Client`] when the HTTP client cannot be built.
    pub fn new(channel_id: ChannelId, config: OpenAiConfig) -> Result<Self, OpenAiError> {
        let endpoint = endpoint(&config.base_url)?;
        // An API endpoint has no use for a redirect, which could carry the
        // key to another scheme of the same host.
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(OpenAiError::client)?;
        let declaration = ChannelDeclaration::new(
            channel_id,
            ChannelType::AI,
            ChannelCategory::Intelligence,
            ChannelDirection::Bidirectional,
        );
        Ok(Self {
            declaration: ChannelDeclaration {
                provider_name: Some("openai".to_owned()),
                ..declaration
            },
            config,
            endpoint,
            client,
        })
    }

    /// The channel's id.
    pub fn id(&self) -> &ChannelId {
        &self.declaration.channel_id
    }

    /// Whether the agent takes a turn on `event`: a message or an
    /// instruction, never one of its own (RFC §6.4).
    fn answers(&self, event: &RoomEvent) -> bool {
        &event.source.channel_id != self.id()
            && (event.event_type == EventType::Message
                || event.event_type == EventType::Instruction)
    }

    /// The text the model answers `conversation` with, if any.
    async fn complete(&self, conversation: &[Message]) -> Result<Option<String>, OpenAiError> {
        let request = CompletionRequest::new(
            &self.config.model,
            self.config.system_prompt.as_deref(),
            conversation,
        );
        let mut call = self.client.post(self.endpoint.clone()).json(&request);
        if !self.config.api_key.is_empty() {
            call = call.bearer_auth(&self.config.api_key);
        }
        let response = call.send().await.map_err(OpenAiError::request)?;
        let status = response.status();
        let body = read_body(response).await?;
        if !status.is_success() {
            return Err(wire::status_error(status, &body, &self.config.api_key));
        }
        wire::answer(&body, &self.config.api_key)
    }

    /// The agent's response to `event`, saying `text`.
    fn response(&self, event: &RoomEvent, text: String) -> RoomEvent {
        let source = EventSource::new(self.id().clone(), ChannelType::AI, EventDirection::Outbound);
        // The pipeline gives the response its own id and depth, and makes it
        // answer the event that asked: the id written here is a placeholder.
        RoomEvent::new(
            event.id.clone(),
            event.room_id.clone(),
            EventType::Message,
            source,
            EventContent::text(text),
            event.created_at,
        )
    }
}

impl Channel for OpenAiAgent {
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
        let conversation = conversation(self.id(), &event, &context, self.config.history);
        let output = match self.complete(&conversation).await? {
            Some(text) => ChannelOutput::respond(vec![self.response(&event, text)]),
            None => ChannelOutput::default(),
        };
        Ok(output)
    }
}

impl fmt::Debug for OpenAiAgent {
    /// The channel's id and its configuration, whose `Debug` hides the API
    /// key and the base URL's credentials.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiAgent")
            .field("channel_id", self.id())
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The body of `response`, refused past [`ANSWER_LIMIT`]: as its head
/// declares it, then as it arrives, so that an endpoint that streams
/// without end fails the turn instead of filling the memory.
async fn read_body(mut response: reqwest::Response) -> Result<Vec<u8>, OpenAiError> {
    let too_long = OpenAiError::AnswerTooLong {
        limit: ANSWER_LIMIT,
    };
    let declared = response.content_length().unwrap_or_default();
    if usize::try_from(declared).map_or(true, |declared| declared > ANSWER_LIMIT) {
        return Err(too_long);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(OpenAiError::request)? {
        if body.len().saturating_add(chunk.len()) > ANSWER_LIMIT {
            return Err(too_long);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// The completions endpoint under `base_url`: `/chat/completions` after
/// its path. A base URL with a query or a fragment is refused, since the
/// path would follow them instead.
fn endpoint(base_url: &str) -> Result<reqwest::Url, OpenAiError> {
    let refused = |reason: &str| OpenAiError::BaseUrl {
        reason: reason.to_owned(),
    };
    let not_http = || refused("it is not an http or https URL");
    let base = reqwest::Url::parse(base_url).map_err(|_| not_http())?;
    if base.query().is_some() || base.fragment().is_some() {
        return Err(refused(
            "it has a query or a fragment, which /chat/completions cannot follow",
        ));
    }
    let joined = format!("{}{COMPLETIONS_PATH}", base_url.trim_end_matches('/'));
    let url = reqwest::Url::parse(&joined).map_err(|_| not_http())?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        _ => Err(not_http()),
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn the_endpoint_is_the_base_url_s_chat_completions() {
        assert_eq!(
            endpoint("https://api.openai.com/v1/").map(String::from),
            Ok("https://api.openai.com/v1/chat/completions".to_owned())
        );
    }

    #[test]
    fn a_base_url_that_is_no_http_url_is_refused() {
        for base_url in ["api.openai.com/v1", "ftp://example.com", ""] {
            assert!(
                matches!(endpoint(base_url), Err(OpenAiError::BaseUrl { .. })),
                "{base_url}"
            );
        }
    }

    #[test]
    fn a_base_url_with_a_query_or_a_fragment_is_refused() {
        for base_url in [
            "https://example.openai.azure.com/openai/deployments/d?api-version=2024-10-21",
            "https://api.openai.com/v1#chat",
        ] {
            let refused = endpoint(base_url);
            assert!(
                matches!(refused, Err(OpenAiError::BaseUrl { .. })),
                "{base_url}: {refused:?}"
            );
        }
    }

    #[test]
    fn a_refused_base_url_is_not_repeated() {
        let error = endpoint("ftp://user:pass@example.com/?token=abc").unwrap_err();
        let printed = format!("{error} {error:?}");
        assert!(
            !printed.contains("pass") && !printed.contains("abc"),
            "{printed}"
        );
    }

    #[test]
    fn the_agent_s_debug_form_hides_the_api_key_and_the_base_url_s_credentials() {
        let config = OpenAiConfig::new(
            "https://user:pass@example.com:8443/v1",
            "sk-secret",
            "gpt-6-luna",
        );
        let agent = OpenAiAgent::new(ChannelId::new("assistant").unwrap(), config).unwrap();
        let printed = format!("{agent:?}");
        for secret in ["sk-secret", "user", "pass", "/v1"] {
            assert!(!printed.contains(secret), "{secret} in {printed}");
        }
    }
}
