//! Python's `roomkit.providers.ai.MockAIProvider`: a provider answering
//! from a list, round-robin, for tests and examples.

use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit::model::Metadata;
use serde_json::Value;

use crate::provider::{
    AiContext, AiProvider, AiResponse, ProviderError, ProviderStream, StreamEvent, TextStream,
    response_events,
};
use crate::stream::{Yielder, items};

/// What [`MockAiProvider::new`] takes: Python's `responses=None, *,
/// vision=False, ai_responses=None, streaming=False`.
#[derive(Clone, Debug, Default)]
pub struct MockAiProviderOptions {
    /// The texts it answers, round-robin; empty answers `Hello from AI`.
    pub responses: Vec<String>,
    /// Whether its model takes images.
    pub vision: bool,
    /// The whole responses it answers, round-robin, in place of
    /// `responses`.
    pub ai_responses: Option<Vec<AiResponse>>,
    /// Whether it says it streams (`supports_streaming`,
    /// `supports_structured_streaming`); either way its structured stream
    /// sends each response as its events.
    pub streaming: bool,
}

/// A provider answering each generation with the next of its responses,
/// round-robin, and keeping each context it was asked: Python's
/// `MockAIProvider`. Its model is `mock`. Cloning it shares its responses
/// and the contexts it kept, so a test keeps a clone to read them once the
/// channel holds the provider.
///
/// ```
/// use roomkit::model::ChannelId;
/// use roomkit_ai::{AiChannel, AiChannelOptions, MockAiProvider, MockAiProviderOptions};
///
/// let model = MockAiProvider::new(MockAiProviderOptions {
///     responses: vec!["Order 42 shipped on Monday.".into()],
///     ..MockAiProviderOptions::default()
/// });
/// let agent = AiChannel::new(
///     ChannelId::new("assistant")?,
///     model.clone(),
///     AiChannelOptions::default(),
/// )?;
/// # let _ = agent;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug)]
pub struct MockAiProvider {
    responses: Arc<Vec<AiResponse>>,
    vision: bool,
    streaming: bool,
    shared: Arc<Mutex<Asked>>,
}

/// What the provider was asked: each context, and the next response.
#[derive(Debug, Default)]
struct Asked {
    calls: Vec<AiContext>,
    next: usize,
}

impl MockAiProvider {
    /// The provider answering as `options` says.
    pub fn new(options: MockAiProviderOptions) -> Self {
        let MockAiProviderOptions {
            responses,
            vision,
            ai_responses,
            streaming,
        } = options;
        let responses = match ai_responses.filter(|whole| !whole.is_empty()) {
            Some(whole) => whole,
            None if responses.is_empty() => vec![text_response("Hello from AI")],
            None => responses
                .iter()
                .map(String::as_str)
                .map(text_response)
                .collect(),
        };
        Self {
            responses: Arc::new(responses),
            vision,
            streaming,
            shared: Arc::default(),
        }
    }

    /// The contexts it was asked, in order: Python's `calls`.
    pub fn calls(&self) -> Vec<AiContext> {
        self.lock().calls.clone()
    }

    /// Keeps `context` and takes the next response, round-robin.
    fn answer(&self, context: &AiContext) -> AiResponse {
        let mut asked = self.lock();
        asked.calls.push(context.clone());
        let at = asked.next % self.responses.len().max(1);
        asked.next = asked.next.wrapping_add(1);
        self.responses
            .get(at)
            .cloned()
            .unwrap_or_else(|| text_response("Hello from AI"))
    }

    fn lock(&self) -> MutexGuard<'_, Asked> {
        self.shared.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl AiProvider for MockAiProvider {
    /// Python's class name.
    fn name(&self) -> &'static str {
        "MockAIProvider"
    }

    fn model_name(&self) -> &'static str {
        "mock"
    }

    fn supports_vision(&self) -> bool {
        self.vision
    }

    fn supports_streaming(&self) -> bool {
        self.streaming
    }

    fn supports_structured_streaming(&self) -> bool {
        self.streaming
    }

    fn generate<'a>(
        &'a self,
        context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        std::future::ready(Ok(self.answer(context)))
    }

    /// Each response as its events, text first, the way a streaming
    /// provider sends them and Python's mock wraps its `generate` for one
    /// that does not.
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        Box::pin(items(
            move |out: Yielder<Result<StreamEvent, ProviderError>>| async move {
                for event in response_events(self.answer(context)) {
                    out.give(Ok(event)).await;
                }
            },
        ))
    }

    /// The text of each response, as one piece.
    fn generate_stream<'a>(&'a self, context: &'a AiContext) -> TextStream<'a> {
        Box::pin(items(
            move |out: Yielder<Result<String, ProviderError>>| async move {
                let response = self.answer(context);
                if !response.content.is_empty() {
                    out.give(Ok(response.content)).await;
                }
            },
        ))
    }
}

/// A response saying `text`, ending `stop`, with Python's mock usage.
fn text_response(text: &str) -> AiResponse {
    let mut usage = Metadata::new();
    usage.insert("prompt_tokens".to_owned(), Value::from(10));
    usage.insert("completion_tokens".to_owned(), Value::from(5));
    AiResponse {
        finish_reason: Some("stop".to_owned()),
        usage,
        ..AiResponse::new(text)
    }
}
