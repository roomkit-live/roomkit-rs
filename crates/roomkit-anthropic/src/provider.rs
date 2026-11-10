//! The public provider, independent of the channel's tool loop.
use crate::{AnthropicConfig, error, http, models, request, stream};
use roomkit_ai::{
    AiContext, AiProvider, AiResponse, ModelInfo, ProviderError, ProviderStream, TextStream,
    checked_stream, listing, schema_for_generate,
};
use serde_json::Value;
use std::{fmt, sync::Arc};

/// Python's `AnthropicAIProvider`: one shared, cloneable HTTP connection pool.
/// A per-turn key stays on that request and never changes the configured key.
#[derive(Clone)]
pub struct AnthropicAiProvider {
    pub(crate) config: Arc<AnthropicConfig>,
    pub(crate) client: reqwest::Client,
    pub(crate) messages: reqwest::Url,
    pub(crate) models: reqwest::Url,
}

impl AnthropicAiProvider {
    /// Builds the provider described by `config`.
    ///
    /// # Errors
    /// Refuses an invalid base URL or an HTTP client that cannot be built.
    pub fn new(config: AnthropicConfig) -> Result<Self, ProviderError> {
        let config = config.normalized();
        let messages = endpoint(config.base(), "/v1/messages")?;
        let mut models = endpoint(config.base(), "/v1/models")?;
        models.query_pairs_mut().append_pair("limit", "1000");
        let client = reqwest::Client::builder()
            .tls_backend_preconfigured(roomkit_provider_http::tls_config().map_err(error::failed)?)
            .no_gzip()
            .read_timeout(config.timeout)
            .connect_timeout(config.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(error::transport)?;
        Ok(Self {
            config: Arc::new(config),
            client,
            messages,
            models,
        })
    }
}

impl AiProvider for AnthropicAiProvider {
    fn name(&self) -> &'static str {
        "AnthropicAIProvider"
    }
    fn model_name(&self) -> &str {
        &self.config.model
    }
    fn available_models() -> Vec<ModelInfo> {
        models::all().to_vec()
    }
    fn supports_deferred_tools(&self) -> bool {
        self.config.official()
            && self
                .catalog_entry()
                .is_some_and(|model| model.capabilities.iter().any(|tag| tag == "deferred_tools"))
    }

    fn supports_vision(&self) -> bool {
        models::entry(&self.config.model)
            .and_then(|entry| entry.supports_vision)
            .unwrap_or_else(|| self.config.model.starts_with("claude-"))
    }
    fn supports_streaming(&self) -> bool {
        true
    }
    fn supports_structured_streaming(&self) -> bool {
        true
    }
    fn supports_response_schema(&self) -> bool {
        true
    }
    fn supports_response_schema_with_tools(&self) -> bool {
        true
    }

    async fn list_models(&self) -> Result<Vec<ModelInfo>, ProviderError> {
        let response = self.send(None, &self.config.api_key).await?;
        let body: Value = serde_json::from_slice(&http::read_body(response).await?)
            .map_err(|_| error::failed("the model listing is not JSON"))?;
        if body.get("error").is_some() {
            return Err(error::response(
                &body,
                None,
                &self.secrets(&self.config.api_key),
            ));
        }
        let data = body
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| error::failed("the model listing has no data"))?;
        let live = data
            .iter()
            .map(|value| {
                let id = value
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| error::failed("a listed model has no id"))?;
                let mut model = ModelInfo::new(id);
                model.display_name = value
                    .get("display_name")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                Ok(model)
            })
            .collect::<Result<Vec<_>, ProviderError>>()?;
        Ok(listing(live, models::all()))
    }

    async fn generate(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        stream::whole(self.generate_structured_stream(context)).await
    }
    fn generate_stream<'a>(&'a self, context: &'a AiContext) -> TextStream<'a> {
        stream::text(self.generate_structured_stream(context))
    }
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        if let Err(error) = schema_for_generate(context, true, true, "anthropic") {
            return stream::refused(error);
        }
        let body = match request::body(&self.config, context) {
            Ok(body) => body,
            Err(error) => return stream::refused(error),
        };
        let key = context
            .metadata
            .get("api_key")
            .and_then(Value::as_str)
            .filter(|key| !key.is_empty())
            .unwrap_or(&self.config.api_key)
            .to_owned();
        checked_stream(
            stream::answer(self, body, key),
            context,
            "anthropic",
            |done| (done.finish_reason.as_deref() == Some("refusal")).then(|| "refusal".to_owned()),
        )
    }
}

impl fmt::Debug for AnthropicAiProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnthropicAiProvider")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

fn endpoint(base: &str, path: &str) -> Result<reqwest::Url, ProviderError> {
    let base =
        reqwest::Url::parse(base).map_err(|_| error::failed("the base URL is not an HTTP URL"))?;
    if !matches!(base.scheme(), "http" | "https")
        || base.query().is_some()
        || base.fragment().is_some()
    {
        return Err(error::failed(
            "the base URL must be HTTP(S), without a query or fragment",
        ));
    }
    reqwest::Url::parse(&format!("{}{path}", base.as_str().trim_end_matches('/')))
        .map_err(|_| error::failed("the API endpoint is invalid"))
}
