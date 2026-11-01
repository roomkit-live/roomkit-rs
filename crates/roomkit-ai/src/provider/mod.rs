//! The provider a model is reached through (RFC §6.7): Python's
//! `roomkit.providers.ai.AIProvider`, what one generation is asked and what
//! it answers.

mod context;
mod error;
mod response;

use std::future::Future;
use std::pin::Pin;

use futures_core::Stream;

pub(crate) use self::context::ai_messages;
pub use self::context::{
    AiContentPart, AiContext, AiImagePart, AiMessage, AiMessageContent, AiRole, AiTextPart,
    AiToolCallPart, AiToolResultPart,
};
pub use self::error::{ProviderError, ProviderErrorOptions};
pub use self::response::{
    AiResponse, AiToolCall, StreamDone, StreamEvent, StreamTextDelta, StreamToolCall,
    read_tool_calls, tool_arguments,
};
pub(crate) use self::response::{ran_out_of_room, response_events};
use crate::stream::{Yielder, items, next};

/// The events of one generation, as a structured stream reads them: text
/// as it comes, the calls, then the end.
pub type ProviderStream<'a> =
    Pin<Box<dyn Stream<Item = Result<StreamEvent, ProviderError>> + Send + 'a>>;

/// The text of one generation, as it comes.
pub type TextStream<'a> = Pin<Box<dyn Stream<Item = Result<String, ProviderError>> + Send + 'a>>;

/// A model provider (RFC §6.7): Python's `AIProvider`. `model_name` and
/// `generate` are required; everything else is provided as Python
/// provides it.
///
/// The agent's tool loop is the same for every provider (RFC §6.4 "One
/// loop, streamed or not", Appendix A.9): it reads
/// [`generate_structured_stream`](Self::generate_structured_stream), which
/// wraps [`generate`](Self::generate) for a provider that does not stream.
pub trait AiProvider: Send + Sync + 'static {
    /// The provider's name, which the agent's channel declares: Python's
    /// `name`, the provider's class name by default.
    fn name(&self) -> &str {
        short_type_name::<Self>()
    }

    /// The model the provider reaches, as configured: Python's
    /// `model_name`.
    fn model_name(&self) -> &str;

    /// Whether the configured model takes images (RFC §6.4).
    fn supports_vision(&self) -> bool {
        false
    }

    /// Whether [`generate_stream`](Self::generate_stream) streams.
    fn supports_streaming(&self) -> bool {
        false
    }

    /// Whether [`generate_structured_stream`](Self::generate_structured_stream)
    /// streams, rather than wrap a whole response.
    fn supports_structured_streaming(&self) -> bool {
        false
    }

    /// One whole generation answering `context`.
    fn generate<'a>(
        &'a self,
        context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a;

    /// The text of one generation answering `context`, as it comes. A
    /// provider that does not stream fails it, as Python's raises
    /// `NotImplementedError`.
    fn generate_stream<'a>(&'a self, _context: &'a AiContext) -> TextStream<'a> {
        let failure = format!("{} does not support streaming generation", self.name());
        Box::pin(items(
            move |out: Yielder<Result<String, ProviderError>>| async move {
                out.give(Err(ProviderError::from(failure))).await;
            },
        ))
    }

    /// The events of one generation answering `context`. By default, as
    /// Python's: a turn without tools streams through
    /// [`generate_stream`](Self::generate_stream) when the provider streams
    /// text, ending `stop`; any other wraps [`generate`](Self::generate),
    /// everything it returned kept.
    fn generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        if self.supports_streaming() && context.tools.is_empty() {
            let mut text = self.generate_stream(context);
            return Box::pin(items(
                move |out: Yielder<Result<StreamEvent, ProviderError>>| async move {
                    while let Some(delta) = next(&mut text).await {
                        match delta {
                            Ok(text) => {
                                let delta = StreamTextDelta { text };
                                out.give(Ok(StreamEvent::TextDelta(delta))).await;
                            }
                            Err(error) => return out.give(Err(error)).await,
                        }
                    }
                    // A text stream reports no usage and no stop reason.
                    let done = StreamDone {
                        finish_reason: Some("stop".to_owned()),
                        ..StreamDone::default()
                    };
                    out.give(Ok(StreamEvent::Done(done))).await;
                },
            ));
        }
        let response = self.generate(context);
        Box::pin(items(
            move |out: Yielder<Result<StreamEvent, ProviderError>>| async move {
                match response.await {
                    Ok(response) => {
                        for event in response_events(response) {
                            out.give(Ok(event)).await;
                        }
                    }
                    Err(error) => out.give(Err(error)).await,
                }
            },
        ))
    }

    /// Releases what the provider holds: Python's `close()`, which the AI
    /// channel's close calls. Nothing by default.
    fn close(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}

/// The name of the type `T`, without its path nor its parameters.
fn short_type_name<T: ?Sized>() -> &'static str {
    let full = std::any::type_name::<T>();
    let base = full.split('<').next().unwrap_or(full);
    base.rsplit("::").next().unwrap_or(base)
}
