//! The provider and the configuration every vendor of the OpenAI wire has,
//! written once: Python's derivative classes inherit them.

/// A vendor's provider over [`crate::OpenAiAiProvider`]: its rules, its
/// name, its catalogue; every request and answer as OpenAI's wire carries
/// them.
macro_rules! vendor_provider {
    ($(#[$doc:meta])* $provider:ident, $config:ident) => {
        $(#[$doc])*
        #[derive(Clone, Debug)]
        pub struct $provider {
            inner: $crate::OpenAiAiProvider,
        }

        impl $provider {
            /// The provider `config` configures.
            ///
            /// # Errors
            ///
            /// Fails, a [`roomkit_ai::ProviderError`] not retryable, for a
            /// configuration stating `supports_reasoning_effort_with_tools`,
            /// which the vendor's own rule makes moot, or `thinking_field`,
            /// as the provider sends reasoning where its vendor reads it, and as
            /// [`crate::OpenAiAiProvider::new`] does.
            pub fn new(config: $config) -> Result<Self, roomkit_ai::ProviderError> {
                if config.supports_reasoning_effort_with_tools.is_some() {
                    return Err($crate::vendor::refused_tool_turn_setting(stringify!($config)));
                }
                if config.thinking_field.is_some() {
                    return Err($crate::vendor::refused_thinking_field(stringify!($config)));
                }
                let vendor = config.vendor();
                let inner = $crate::OpenAiAiProvider::for_vendor(config.into_openai(), vendor)?;
                Ok(Self { inner })
            }
        }

        impl roomkit_ai::AiProvider for $provider {
            fn name(&self) -> &str {
                self.inner.name()
            }

            fn model_name(&self) -> &str {
                self.inner.model_name()
            }

            fn supports_vision(&self) -> bool {
                self.inner.supports_vision()
            }

            fn available_models() -> Vec<roomkit_ai::ModelInfo> {
                $config::VENDOR.models().to_vec()
            }

            fn catalog_entry(&self) -> Option<roomkit_ai::ModelInfo> {
                self.inner.catalog_entry()
            }

            async fn list_models(
                &self,
            ) -> Result<Vec<roomkit_ai::ModelInfo>, roomkit_ai::ProviderError> {
                self.inner.list_models().await
            }

            fn supports_streaming(&self) -> bool {
                true
            }

            fn supports_structured_streaming(&self) -> bool {
                true
            }

            fn supports_response_schema(&self) -> bool {
                self.inner.supports_response_schema()
            }

            fn supports_response_schema_with_tools(&self) -> bool {
                self.inner.supports_response_schema_with_tools()
            }

            async fn generate(
                &self,
                context: &roomkit_ai::AiContext,
            ) -> Result<roomkit_ai::AiResponse, roomkit_ai::ProviderError> {
                self.inner.generate(context).await
            }

            fn generate_stream<'a>(
                &'a self,
                context: &'a roomkit_ai::AiContext,
            ) -> roomkit_ai::TextStream<'a> {
                self.inner.generate_stream(context)
            }

            fn generate_structured_stream<'a>(
                &'a self,
                context: &'a roomkit_ai::AiContext,
            ) -> roomkit_ai::ProviderStream<'a> {
                self.inner.generate_structured_stream(context)
            }
        }
    };
}

pub(crate) use vendor_provider;

/// The fields every vendor's configuration shares with
/// [`crate::OpenAiConfig`], as Python's derivative configs inherit them,
/// carried into the OpenAI configuration the provider runs on.
macro_rules! into_openai {
    ($config:expr) => {{
        let config = $config;
        $crate::OpenAiConfig {
            base_url: Some(config.base_url),
            max_tokens: config.max_tokens,
            timeout: config.timeout,
            connect_timeout: config.connect_timeout,
            max_retries: config.max_retries,
            include_stream_usage: config.include_stream_usage,
            use_max_completion_tokens: Some(config.use_max_completion_tokens),
            supports_custom_temperature: Some(config.supports_custom_temperature),
            reasoning_effort: config.reasoning_effort,
            default_headers: config.default_headers,
            extra_body: config.extra_body,
            supports_response_schema: config.supports_response_schema,
            supports_response_schema_with_tools: config.supports_response_schema_with_tools,
            ..$crate::OpenAiConfig::new(config.api_key, config.model)
        }
    }};
}

pub(crate) use into_openai;
