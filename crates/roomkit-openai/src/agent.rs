//! The agent: an intelligence channel whose turn is one chat completion.

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

/// An AI agent channel answering through an OpenAI-compatible Chat
/// Completions endpoint (RFC §6.4).
///
/// Each turn sends the configuration's system prompt, then the conversation
/// the room hands the agent ([`roomkit::transcript::conversation`]), and
/// answers with the completion's text. It answers a message or an
/// instruction, never its own events; a completion with no text answers
/// nothing. A turn that fails returns an [`OpenAiError`]. Cloning it shares
/// its HTTP client.
#[derive(Clone, Debug)]
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
    /// `https` URL, and [`OpenAiError::Client`] when the HTTP client cannot
    /// be built.
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
            ChannelType::Ai,
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
        let body = response.bytes().await.map_err(OpenAiError::request)?;
        if !status.is_success() {
            return Err(OpenAiError::Status {
                status: status.as_u16(),
                message: wire::error_message(&body),
            });
        }
        wire::answer(&body)
    }

    /// The agent's response to `event`, saying `text`.
    fn response(&self, event: &RoomEvent, text: String) -> RoomEvent {
        let source = EventSource::new(self.id().clone(), ChannelType::Ai, EventDirection::Outbound);
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

/// The completions endpoint under `base_url`.
fn endpoint(base_url: &str) -> Result<reqwest::Url, OpenAiError> {
    let refused = || OpenAiError::BaseUrl {
        base_url: base_url.to_owned(),
    };
    let joined = format!("{}{COMPLETIONS_PATH}", base_url.trim_end_matches('/'));
    let url = reqwest::Url::parse(&joined).map_err(|_| refused())?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        _ => Err(refused()),
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
            assert_eq!(
                endpoint(base_url),
                Err(OpenAiError::BaseUrl {
                    base_url: base_url.to_owned()
                }),
                "{base_url}"
            );
        }
    }
}
