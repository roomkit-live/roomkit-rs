//! One schema-constrained provider call, separate from the channel's answer loop.

use std::time::Duration;

use roomkit::ChannelError;
use roomkit_core::rules::transcript::fence;
use serde_json::json;

use super::{
    SpeakingFuture, Thinker, Thought, thinker_input::thinker_input, thinker_words::INSTRUCTIONS,
};
use crate::classifiers::error::invalid;
use crate::{AiContext, AiMessage, AiProvider, AiResponse, AiRole, SharedAiContext};

mod answer;

/// The named options of [`LlmThinker`], with Python's defaults.
#[derive(Clone, Debug, PartialEq)]
pub struct LlmThinkerOptions {
    /// Replaces the default instructions; an empty string uses the default too.
    pub instructions: Option<String>,
    /// The thought's token bound, in the provider context's existing domain.
    pub max_tokens: Option<u32>,
    /// Provider-specific effort, or its configured default.
    pub reasoning_effort: Option<String>,
    /// Seconds before failure. None is unbounded; nonpositive values expire
    /// before entering the provider. A NaN deadline permits an immediate
    /// answer but expires a pending call; positive infinity remains unbounded.
    pub timeout: Option<f64>,
}

impl Default for LlmThinkerOptions {
    fn default() -> Self {
        Self {
            instructions: None,
            max_tokens: Some(600),
            reasoning_effort: None,
            timeout: Some(10.0),
        }
    }
}

/// A model rewriting the agent's thought from its conversation (RFC §6.4).
/// The supplied provider remains its caller's resource; closing this thinker
/// does not close it. No answer loop, retry or tool call is run here.
pub struct LlmThinker<P: AiProvider> {
    provider: P,
    options: LlmThinkerOptions,
}

impl<P: AiProvider> std::fmt::Debug for LlmThinker<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LlmThinker")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl<P: AiProvider> LlmThinker<P> {
    /// Builds a thinker over a provider supporting response schemas.
    ///
    /// # Errors
    /// `ValueError` when the provider does not support a response schema.
    pub fn new(provider: P, options: LlmThinkerOptions) -> Result<Self, ChannelError> {
        if !provider.supports_response_schema() {
            let name = std::any::type_name::<P>()
                .rsplit("::")
                .next()
                .unwrap_or_default();
            return Err(invalid(
                "ValueError",
                format!("{name} does not support a response schema"),
            ));
        }
        Ok(Self { provider, options })
    }

    fn request(&self, previous: &Thought, context: &AiContext) -> AiContext {
        let mut system = self
            .options
            .instructions
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(INSTRUCTIONS)
            .replace("{max}", "3");
        if let Some(agent) = context.system_prompt.as_deref().filter(|s| !s.is_empty()) {
            system.push_str(
                "\n\nThe agent, as it is described (who it is, what it knows, what it can do):\n",
            );
            system.push_str(&fence("agent", agent));
        }
        AiContext {
            system_prompt: Some(system),
            messages: vec![AiMessage::new(AiRole::User, thinker_input(previous, &context.messages))],
            response_schema: json!({"type":"object","properties":{"text":{"type":"string"},"want_to_say":{"type":"array","items":{"type":"string"}},"urgent":{"type":"boolean"}},"required":["text","want_to_say","urgent"],"additionalProperties":false}).as_object().cloned(),
            max_tokens: self.options.max_tokens,
            reasoning_effort: self.options.reasoning_effort.clone(),
            ..AiContext::default()
        }
    }

    async fn generate(&self, request: &AiContext) -> Result<AiResponse, ChannelError> {
        let Some(seconds) = self.options.timeout else {
            return self.provider.generate(request).await.map_err(Into::into);
        };
        if seconds <= 0.0 {
            return Err(invalid("TimeoutError", ""));
        }
        // Tokio polls the inner work first, matching Python's immediate
        // answer under NaN. A real nonpositive bound is handled above so
        // that even an immediately ready provider is never entered.
        let duration = if seconds.is_nan() {
            Some(Duration::ZERO)
        } else {
            Duration::try_from_secs_f64(seconds).ok()
        };
        let deadline = duration.and_then(|bound| tokio::time::Instant::now().checked_add(bound));
        let Some(deadline) = deadline else {
            // No representable clock deadline, including +infinity. Do not
            // panic or turn a huge positive bound into an immediate timeout.
            return self.provider.generate(request).await.map_err(Into::into);
        };
        tokio::time::timeout_at(deadline, self.provider.generate(request))
            .await
            .map_err(|_| invalid("TimeoutError", ""))?
            .map_err(Into::into)
    }
}

impl<P: AiProvider> Thinker for LlmThinker<P> {
    fn think<'a>(
        &'a self,
        previous: &'a Thought,
        context: &'a SharedAiContext,
    ) -> SpeakingFuture<'a, Thought> {
        Box::pin(async move {
            let request = self.request(previous, &context.snapshot());
            let response = self.generate(&request).await?;
            answer::thought(&response.content)
        })
    }
}
