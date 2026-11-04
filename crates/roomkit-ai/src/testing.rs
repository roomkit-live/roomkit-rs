//! A scripted provider, for tests and examples: a model that answers from a
//! script and keeps what it was asked.

use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit::model::Metadata;

use crate::provider::{
    AiContext, AiProvider, AiResponse, ProviderError, ProviderErrorOptions, ProviderStream,
    StreamDone, StreamEvent, StreamTextDelta, StreamThinkingDelta, read_tool_calls,
};
use crate::stream::{Yielder, items};

/// A provider answering each generation with the next round of its script,
/// and keeping each request it was asked. A generation past the script
/// fails. Cloning it shares its script and its requests, so a test keeps a
/// clone to read them once the agent holds the provider.
///
/// ```
/// use roomkit::model::ChannelId;
/// use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
/// use roomkit_ai::{AiChannel, AiChannelOptions};
///
/// let model = ScriptedProvider::new([
///     ScriptedRound::calling("Let me check.", [("call_1", "lookup_order", r#"{"order": "42"}"#)]),
///     ScriptedRound::answering("Order 42 shipped on Monday."),
/// ]);
/// let options = AiChannelOptions::default();
/// let agent = AiChannel::new(ChannelId::new("assistant")?, model.clone(), options)?;
/// # let _ = agent;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug, Default)]
pub struct ScriptedProvider {
    shared: Arc<Mutex<Script>>,
}

#[derive(Debug, Default)]
struct Script {
    rounds: VecDeque<ScriptedRound>,
    requests: Vec<AiContext>,
}

/// One generation's answer: its text, its calls, why it stopped and what it
/// used; or its text, then a failure.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ScriptedRound {
    /// What the model says.
    pub text: String,
    /// The pieces it writes its text in, one text event each; none writes
    /// the text in one.
    pub deltas: Vec<String>,
    /// The pieces of its reasoning, each an event before its text.
    pub thinking: Vec<String>,
    /// The calls it makes, each an id, a tool's name and its argument
    /// text, which the provider reads as a provider reads its wire.
    calls: Vec<(String, String, String)>,
    /// How the generation ends.
    pub done: StreamDone,
    /// The failure the generation ends on, once its text is written, before
    /// its calls and its done event: the provider's stream fails.
    pub fails: Option<String>,
    /// The refusal the generation ends on, once its text is written,
    /// before its calls and its done event: the provider refuses the
    /// request with this HTTP status and message, an error not worth
    /// another try unless it is [`retrying`](Self::retrying).
    pub refuses: Option<(u16, String)>,
    /// Whether the provider says its refusal is worth another try
    /// (`ProviderError::retryable`).
    retryable: bool,
    /// How the provider classes its refusal: a context too long for the
    /// model, not one, or unclassed (`ProviderError::context_overflow`).
    context_overflow: Option<bool>,
    /// The provider that refuses, by its name; none when empty
    /// (`ProviderError::provider`).
    provider: String,
    /// Whether its stream ends after its calls without its done event.
    pub without_done: bool,
    /// Where its failure or refusal comes.
    failure_at: FailureAt,
}

/// Where a round's failure or refusal comes in its stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum FailureAt {
    /// Once its text is written.
    #[default]
    Text,
    /// Once its calls are.
    Calls,
    /// Once its done event is.
    Done,
}

impl ScriptedRound {
    /// A round answering `text`, calling nothing, ended `stop`.
    pub fn answering(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            deltas: Vec::new(),
            thinking: Vec::new(),
            calls: Vec::new(),
            done: StreamDone {
                finish_reason: Some("stop".to_owned()),
                ..StreamDone::default()
            },
            fails: None,
            refuses: None,
            retryable: false,
            context_overflow: None,
            provider: String::new(),
            without_done: false,
            failure_at: FailureAt::Text,
        }
    }

    /// A round answering the text `deltas` join to, written in those
    /// pieces, calling nothing, ended `stop`.
    pub fn writing<'a>(deltas: impl IntoIterator<Item = &'a str>) -> Self {
        let deltas: Vec<String> = deltas.into_iter().map(str::to_owned).collect();
        Self {
            deltas: deltas.clone(),
            ..Self::answering(deltas.concat())
        }
    }

    /// A round saying `text` and making `calls`, each an id, a tool's name
    /// and its argument text, ended `tool_calls`.
    pub fn calling<'a>(
        text: impl Into<String>,
        calls: impl IntoIterator<Item = (&'a str, &'a str, &'a str)>,
    ) -> Self {
        Self {
            text: text.into(),
            deltas: Vec::new(),
            thinking: Vec::new(),
            calls: calls
                .into_iter()
                .map(|(id, name, arguments)| (id.to_owned(), name.to_owned(), arguments.to_owned()))
                .collect(),
            done: StreamDone {
                finish_reason: Some("tool_calls".to_owned()),
                ..StreamDone::default()
            },
            fails: None,
            refuses: None,
            retryable: false,
            context_overflow: None,
            provider: String::new(),
            without_done: false,
            failure_at: FailureAt::Text,
        }
    }

    /// The same round, written in the pieces `deltas`, which join to its
    /// text.
    #[must_use]
    pub fn in_pieces<'a>(mut self, deltas: impl IntoIterator<Item = &'a str>) -> Self {
        self.deltas = deltas.into_iter().map(str::to_owned).collect();
        self.text = self.deltas.concat();
        self
    }

    /// The same round, its reasoning streamed in the pieces `thinking`
    /// before its text.
    #[must_use]
    pub fn reasoning<'a>(mut self, thinking: impl IntoIterator<Item = &'a str>) -> Self {
        self.thinking = thinking.into_iter().map(str::to_owned).collect();
        self
    }

    /// The same round, its provider refusing the request with the HTTP
    /// `status` and `message` once its text is written, an error not worth
    /// another try unless the round is [`retrying`](Self::retrying).
    #[must_use]
    pub fn refusing(mut self, status: u16, message: impl Into<String>) -> Self {
        self.refuses = Some((status, message.into()));
        self
    }

    /// The same round, its provider saying its refusal is worth another
    /// try (`ProviderError::retryable`): a rate limit, an overload.
    #[must_use]
    pub fn retrying(mut self) -> Self {
        self.retryable = true;
        self
    }

    /// The same round, its provider's refusal classed as a context too
    /// long for the model when `overflow`, as not one otherwise
    /// (`ProviderError::context_overflow`); a refusal is unclassed without
    /// it.
    #[must_use]
    pub fn with_context_overflow(mut self, overflow: bool) -> Self {
        self.context_overflow = Some(overflow);
        self
    }

    /// The same round, its refusal made by the provider named `provider`
    /// (`ProviderError::provider`); a refusal names none without it.
    #[must_use]
    pub fn refused_by(mut self, provider: impl Into<String>) -> Self {
        self.provider = provider.into();
        self
    }

    /// The error the round ends on, if any: its failure, else its refusal.
    fn failure(&self) -> Option<ProviderError> {
        if let Some(failure) = &self.fails {
            return Some(failure.clone().into());
        }
        self.refuses.as_ref().map(|(status, message)| {
            let options = ProviderErrorOptions {
                retryable: self.retryable,
                status_code: Some(*status),
                context_overflow: self.context_overflow,
                provider: self.provider.clone(),
            };
            ProviderError::new(message.clone(), options)
        })
    }

    /// The same round, failing with `message` once its text is written:
    /// the provider's stream fails mid-answer.
    #[must_use]
    pub fn failing(mut self, message: impl Into<String>) -> Self {
        self.fails = Some(message.into());
        self
    }

    /// The same round, its failure or its refusal coming once its calls
    /// are written, never its done event: the provider began its answer.
    #[must_use]
    pub fn failing_after_calls(mut self) -> Self {
        self.failure_at = FailureAt::Calls;
        self
    }

    /// The same round, its failure or its refusal coming once its done
    /// event is written: the provider's stream fails past its end.
    #[must_use]
    pub fn failing_after_done(mut self) -> Self {
        self.failure_at = FailureAt::Done;
        self
    }

    /// The same round, its stream ending after its calls without its done
    /// event, as a provider's that drops it.
    #[must_use]
    pub fn without_done(mut self) -> Self {
        self.without_done = true;
        self
    }

    /// The same round, ending for `finish_reason`.
    #[must_use]
    pub fn with_finish_reason(mut self, finish_reason: impl Into<String>) -> Self {
        self.done.finish_reason = Some(finish_reason.into());
        self
    }

    /// The same round, having used `usage`.
    #[must_use]
    pub fn with_usage(mut self, usage: Metadata) -> Self {
        self.done.usage = usage;
        self
    }
}

impl ScriptedProvider {
    /// A provider answering with `rounds`, in order.
    pub fn new(rounds: impl IntoIterator<Item = ScriptedRound>) -> Self {
        Self {
            shared: Arc::new(Mutex::new(Script {
                rounds: rounds.into_iter().collect(),
                requests: Vec::new(),
            })),
        }
    }

    /// The contexts it was asked, in order.
    pub fn requests(&self) -> Vec<AiContext> {
        self.lock().requests.clone()
    }

    fn lock(&self) -> MutexGuard<'_, Script> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl AiProvider for ScriptedProvider {
    fn name(&self) -> &'static str {
        "scripted"
    }

    fn model_name(&self) -> &'static str {
        "scripted"
    }

    fn supports_structured_streaming(&self) -> bool {
        true
    }

    /// The next round, whole: its text, its calls, how it ended.
    fn generate<'a>(
        &'a self,
        context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        std::future::ready(self.whole(context))
    }
    /// The next round, as a stream: its reasoning, its text in its pieces,
    /// then its calls and its end (none for a round [`ScriptedRound::without_done`]), or
    /// its failure once its text is written, once its calls are for a
    /// round [`ScriptedRound::failing_after_calls`], or once its end is for
    /// a round [`ScriptedRound::failing_after_done`].
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        let round = self.next_round(context);
        Box::pin(items(
            move |out: Yielder<Result<StreamEvent, ProviderError>>| async move {
                let round = match round {
                    Ok(round) => round,
                    Err(error) => return out.give(Err(error)).await,
                };
                for thinking in &round.thinking {
                    let delta = StreamThinkingDelta::new(thinking.as_str());
                    out.give(Ok(StreamEvent::ThinkingDelta(delta))).await;
                }
                for text in round.pieces() {
                    out.give(Ok(StreamEvent::TextDelta(StreamTextDelta { text })))
                        .await;
                }
                let mut failure = round.failure();
                let mut failing_at = |at: FailureAt| failure.take_if(|_| round.failure_at == at);
                if let Some(failure) = failing_at(FailureAt::Text) {
                    return out.give(Err(failure)).await;
                }
                let finish_reason = round.done.finish_reason.as_deref();
                for call in read_tool_calls(round.calls, finish_reason) {
                    out.give(Ok(StreamEvent::ToolCall(call.into()))).await;
                }
                if let Some(failure) = failing_at(FailureAt::Calls) {
                    return out.give(Err(failure)).await;
                }
                if !round.without_done {
                    out.give(Ok(StreamEvent::Done(round.done))).await;
                }
                if let Some(failure) = failing_at(FailureAt::Done) {
                    out.give(Err(failure)).await;
                }
            },
        ))
    }
}

impl ScriptedProvider {
    /// The next round, whole: its text, its calls, how it ended.
    fn whole(&self, context: &AiContext) -> Result<AiResponse, ProviderError> {
        let round = self.next_round(context)?;
        if let Some(failure) = round.failure() {
            return Err(failure);
        }
        let calls = read_tool_calls(round.calls, round.done.finish_reason.as_deref());
        Ok(AiResponse {
            finish_reason: round.done.finish_reason,
            usage: round.done.usage,
            metadata: round.done.metadata,
            tool_calls: calls,
            thinking: Some(round.thinking.concat()).filter(|thinking| !thinking.is_empty()),
            ..AiResponse::new(round.text)
        })
    }

    /// Keeps `context` and takes the script's next round.
    fn next_round(&self, context: &AiContext) -> Result<ScriptedRound, ProviderError> {
        let mut script = self.lock();
        script.requests.push(context.clone());
        script
            .rounds
            .pop_front()
            .ok_or_else(|| "the scripted model has no round left".into())
    }
}

impl ScriptedRound {
    /// The calls the round makes, read as its provider reads them for the
    /// round's ending. For a test's own provider.
    #[doc(hidden)]
    pub fn tool_calls(&self) -> Vec<crate::AiToolCall> {
        read_tool_calls(self.calls.clone(), self.done.finish_reason.as_deref())
    }

    /// The pieces the round writes its text in.
    fn pieces(&self) -> Vec<String> {
        if !self.deltas.is_empty() {
            return self.deltas.clone();
        }
        if self.text.is_empty() {
            return Vec::new();
        }
        vec![self.text.clone()]
    }
}
