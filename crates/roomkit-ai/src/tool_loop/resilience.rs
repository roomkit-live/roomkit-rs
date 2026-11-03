//! A round's generation recovered from its provider's refusal (RFC §6.4):
//! a context too long for the model compacted and replayed once, a
//! refusal worth another try tried again under the channel's retry
//! policy, then the fallback provider asked. Python's
//! `_generate_stream_with_retry` and `_fallback_stream`.
//!
//! One rule holds for every recovery: a generation that handed anything is
//! never read again, by a replay, a retry or the fallback, since the room
//! and the turn already hold what it handed.

use std::sync::Arc;
use std::time::Duration;

use roomkit::model::RetryPolicy;
use roomkit_core::rules::transcript::Message;

use super::generation::{Failed, Generation};
use super::{Loop, TurnItem};
use crate::error::AiError;
use crate::provider::{AiContext, AiProvider, DynAiProvider, ProviderError};
use crate::steering::Steering;
use crate::stream::Yielder;
use crate::tools::AiTool;

/// What a round's generation is read with: the loop's steering, its
/// reader, the text a round replays of the round before.
pub(super) type Reading<'a> = (
    &'a Steering,
    &'a Yielder<Result<TurnItem, ProviderError>>,
    &'a str,
);

/// How a channel's generations recover from their provider's refusals:
/// Python's `retry_policy` and `fallback_provider`.
#[derive(Clone, Default)]
pub(crate) struct Resilience {
    /// How a refusal worth another try is tried again; `None` tries
    /// nothing again.
    pub(crate) retry_policy: Option<RetryPolicy>,
    /// The provider asked once the retries are spent.
    pub(crate) fallback_provider: Option<Arc<dyn DynAiProvider>>,
}

impl Resilience {
    /// The channel's `retry_policy` and `fallback_provider`.
    ///
    /// # Errors
    ///
    /// Refuses a policy whose delay or multiplier is not greater than 0,
    /// as Python's `RetryPolicy` does (`gt=0`).
    pub(crate) fn checked(
        retry_policy: Option<RetryPolicy>,
        fallback_provider: Option<Arc<dyn DynAiProvider>>,
    ) -> Result<Self, AiError> {
        if let Some(policy) = &retry_policy {
            policy.check().map_err(|refused| AiError::RetryPolicy {
                reason: refused.to_string(),
            })?;
        }
        Ok(Self {
            retry_policy,
            fallback_provider,
        })
    }
}

impl std::fmt::Debug for Resilience {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Resilience")
            .field("retry_policy", &self.retry_policy)
            .field(
                "fallback_provider",
                &self.fallback_provider.as_ref().map(|p| p.dyn_name()),
            )
            .finish()
    }
}

impl<P: AiProvider> Loop<P> {
    /// Reads the round's generation, asked on `messages` with the `asked`
    /// tools and system prompt, `reading` as [`Generation::read`] takes it.
    /// A refusal before the provider handed anything is recovered from, in
    /// Python's order:
    /// - a context too long for the model is compacted and replayed, once
    ///   per call, before any retry: replaying the same context would be
    ///   refused again;
    /// - a refusal not worth another try fails the call;
    /// - one worth another try is tried again, at most the policy's
    ///   `max_retries` times, each after its delay;
    /// - once the retries are spent, the fallback provider is asked once.
    ///
    /// # Errors
    ///
    /// The provider's error, whether the generation handed anything before
    /// it, and what the room was handed; a fallback that failed before it
    /// handed anything leaves the primary's error, its own the source.
    pub(super) async fn generate(
        &mut self,
        messages: &[Message],
        (tools, system_prompt): (Vec<AiTool>, Option<String>),
        reading: Reading<'_>,
    ) -> Result<Generation, Failed> {
        let (steering, out, prefix) = reading;
        let mut context = AiContext {
            tools,
            ..self.round_context(messages, system_prompt)
        };
        let policy = self.resilience.retry_policy.clone().unwrap_or(RetryPolicy {
            max_retries: 0,
            ..RetryPolicy::default()
        });
        let mut compacted = false;
        let mut attempt = 0;
        let last = loop {
            let stream = self.provider.generate_structured_stream(&context);
            let failure = match Generation::read(stream, steering, out, prefix).await {
                Ok(generation) => return Ok(generation),
                Err(failure) => failure,
            };
            if failure.streamed {
                return Err(failure);
            }
            if !compacted && failure.error.is_context_overflow() {
                // One compacted replay, then the ordinary retries: an error
                // that only read as an overflow keeps its retries.
                compacted = true;
                self.compact(&mut context, messages, &failure.error)?;
                continue;
            }
            if !failure.error.retryable {
                return Err(failure);
            }
            if attempt >= policy.max_retries {
                break failure;
            }
            let delay = retry_delay(&policy, attempt);
            tracing::warn!(
                target: "roomkit::ai",
                error = %failure.error,
                attempt = attempt + 1,
                of = policy.max_retries,
                ?delay,
                "a generation failed: trying it again"
            );
            tokio::time::sleep(delay).await;
            attempt += 1;
        };
        match self.resilience.fallback_provider.clone() {
            Some(fallback) => fall_back(&*fallback, &context, last, reading).await,
            None => Err(last),
        }
    }
}

/// The fallback provider's generation on `context`, once the primary's
/// retries are spent on `primary` and nothing was handed: Python's
/// `_fallback_stream`. It is a second chance, not the turn's provider: a
/// failure before it handed anything leaves the primary's error, its own
/// riding as the source; one after is the turn's.
async fn fall_back(
    fallback: &dyn DynAiProvider,
    context: &AiContext,
    primary: Failed,
    (steering, out, prefix): Reading<'_>,
) -> Result<Generation, Failed> {
    tracing::warn!(target: "roomkit::ai", provider = fallback.dyn_name(), "trying the fallback provider");
    let stream = fallback.dyn_generate_structured_stream(context);
    match Generation::read(stream, steering, out, prefix).await {
        Err(failure) if !failure.streamed => {
            tracing::error!(target: "roomkit::ai", error = %failure.error, "the fallback provider failed too");
            Err(Failed {
                error: primary.error.with_source(failure.error),
                ..primary
            })
        }
        Ok(generation) => Ok(Generation {
            by_fallback: true,
            ..generation
        }),
        Err(failure) => Err(failure),
    }
}

/// How long the loop waits before trying a generation again after
/// `attempt` tries failed: Python's `min(base_delay_seconds *
/// exponential_base ** attempt, max_delay_seconds)`. A delay no `Duration`
/// holds (an infinite cap) waits as long as one can.
fn retry_delay(policy: &RetryPolicy, attempt: u32) -> Duration {
    let exponent = i32::try_from(attempt).unwrap_or(i32::MAX);
    let delay = policy.base_delay_seconds * policy.exponential_base.powi(exponent);
    Duration::try_from_secs_f64(delay.min(policy.max_delay_seconds)).unwrap_or(Duration::MAX)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    // Python: tests/test_channels/test_ai_retry.py::TestRetry::test_backoff_delays_are_exponential
    #[test]
    fn backoff_delays_are_exponential() {
        let policy = RetryPolicy {
            max_retries: 3,
            base_delay_seconds: 1.0,
            max_delay_seconds: 10.0,
            exponential_base: 2.0,
            ..RetryPolicy::default()
        };
        let delays: Vec<Duration> = (0..5)
            .map(|attempt| retry_delay(&policy, attempt))
            .collect();
        let seconds = |s: u64| Duration::from_secs(s);
        assert_eq!(
            delays,
            [seconds(1), seconds(2), seconds(4), seconds(8), seconds(10)]
        );
    }
}
