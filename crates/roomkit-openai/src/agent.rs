//! The agent: the AI channel of `roomkit-ai` over an OpenAI-compatible
//! Chat Completions endpoint.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use roomkit::model::{ChannelBinding, ChannelId, ChannelOutput, RoomContext, RoomEvent};
use roomkit::{Channel, ChannelDeclaration, ChannelError};
use roomkit_ai::{
    AiAgent, AiError, AiProvider, AiRequest, ProviderResponse, ResponseStream, ToolPolicy, Toolset,
};

use crate::config::OpenAiConfig;
use crate::error::OpenAiError;
use crate::wire::{self, CompletionRequest};

/// The path a completion is asked at, after the base URL.
const COMPLETIONS_PATH: &str = "/chat/completions";

/// The longest answer a generation reads, in bytes: far more than a
/// completion's text and calls take.
const ANSWER_LIMIT: usize = 4 * 1024 * 1024;

/// An AI agent channel answering through an OpenAI-compatible Chat
/// Completions endpoint (RFC §6.4): `roomkit-ai`'s [`AiAgent`] over its
/// wire.
///
/// Each turn sends the configuration's system prompt, then the conversation
/// the room hands the agent ([`roomkit::transcript::conversation`]), and
/// declares the tools the agent serves ([`with_tools`](Self::with_tools))
/// that its policy permits. A completion's calls run, their results go back
/// in the next completion, and the turn answers with what the model said,
/// each round's text a message of its own, each call its two rows (see
/// [`AiAgent`]). It answers a message or an instruction, never its own
/// events; a turn with no text answers nothing. A completion that fails
/// returns an [`OpenAiError`]; an answer longer than 4 MiB fails it too.
/// Cloning it shares its HTTP client and its tools.
#[derive(Clone)]
pub struct OpenAiAgent {
    agent: AiAgent<OpenAiProvider>,
}

impl OpenAiAgent {
    /// An agent named `channel_id`, configured by `config`, serving no tool
    /// yet.
    ///
    /// # Errors
    ///
    /// Returns [`OpenAiError::BaseUrl`] when the base URL is no `http` or
    /// `https` URL, or holds a query or a fragment, and
    /// [`OpenAiError::Client`] when the HTTP client cannot be built.
    pub fn new(channel_id: ChannelId, config: OpenAiConfig) -> Result<Self, OpenAiError> {
        let mut agent = AiAgent::new(channel_id, OpenAiProvider::new(config.clone())?)
            .with_history(config.history);
        if let Some(prompt) = config.system_prompt {
            agent = agent.with_system_prompt(prompt);
        }
        Ok(Self { agent })
    }

    /// The channel's id.
    pub fn id(&self) -> &ChannelId {
        self.agent.id()
    }

    /// The same agent, serving the tools of `toolset` too (see
    /// [`AiAgent::with_tools`]).
    ///
    /// # Errors
    ///
    /// Returns [`AiError::DuplicateTool`] for a tool under a name the agent
    /// serves already, or one the toolset declares twice.
    pub fn with_tools(self, toolset: impl Toolset) -> Result<Self, AiError> {
        Ok(Self {
            agent: self.agent.with_tools(toolset)?,
        })
    }

    /// The same agent, calling only the tools `policy` permits (RFC
    /// §21.1).
    #[must_use]
    pub fn with_tool_policy(self, policy: ToolPolicy) -> Self {
        Self {
            agent: self.agent.with_tool_policy(policy),
        }
    }

    /// The same agent, running at most `rounds` rounds of calls per turn
    /// (Appendix A.9, 50 by default).
    #[must_use]
    pub fn with_max_rounds(self, rounds: u32) -> Self {
        Self {
            agent: self.agent.with_max_rounds(rounds),
        }
    }

    /// The same agent, giving each tool's call `bound` at most (RFC §21.6,
    /// 30 seconds by default); `None` bounds nothing.
    #[must_use]
    pub fn with_tool_timeout(self, bound: Option<Duration>) -> Self {
        Self {
            agent: self.agent.with_tool_timeout(bound),
        }
    }
}

impl Channel for OpenAiAgent {
    fn declaration(&self) -> ChannelDeclaration {
        self.agent.declaration()
    }

    async fn on_event(
        &self,
        event: RoomEvent,
        binding: ChannelBinding,
        context: RoomContext,
    ) -> Result<ChannelOutput, ChannelError> {
        self.agent.on_event(event, binding, context).await
    }
}

impl fmt::Debug for OpenAiAgent {
    /// The channel's id, its tools and its configuration, whose `Debug`
    /// hides the API key and the base URL's credentials.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiAgent")
            .field("agent", &self.agent)
            .field("config", &self.agent.provider().config)
            .finish()
    }
}

/// The provider of an OpenAI-compatible endpoint: one generation is one
/// Chat Completions call, not streamed.
pub(crate) struct OpenAiProvider {
    config: Arc<OpenAiConfig>,
    endpoint: reqwest::Url,
    client: reqwest::Client,
}

impl OpenAiProvider {
    /// The provider `config` names.
    fn new(config: OpenAiConfig) -> Result<Self, OpenAiError> {
        let endpoint = endpoint(&config.base_url)?;
        // An API endpoint has no use for a redirect, which could carry the
        // key to another scheme of the same host.
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(OpenAiError::client)?;
        Ok(Self {
            config: Arc::new(config),
            endpoint,
            client,
        })
    }

    /// The completion answering `request`.
    async fn complete(&self, request: &AiRequest) -> Result<ProviderResponse, OpenAiError> {
        let body = CompletionRequest::new(&self.config.model, request);
        let mut call = self.client.post(self.endpoint.clone()).json(&body);
        if !self.config.api_key.is_empty() {
            call = call.bearer_auth(&self.config.api_key);
        }
        let response = call.send().await.map_err(OpenAiError::request)?;
        let status = response.status();
        let body = read_body(response).await?;
        if !status.is_success() {
            return Err(wire::status_error(status, &body, &self.config.api_key));
        }
        wire::response(&body, &self.config.api_key)
    }
}

impl AiProvider for OpenAiProvider {
    fn name(&self) -> Option<&str> {
        Some("openai")
    }

    fn generate<'a>(&'a self, request: &'a AiRequest) -> ResponseStream<'a> {
        ProviderResponse::into_stream(async move { Ok(self.complete(request).await?) })
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
