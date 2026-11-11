//! A provider held as an object (RFC §6.7): Python takes any `AIProvider`
//! where it takes a provider beside the channel's own, which a Rust trait
//! with `async` methods cannot be.

use std::future::Future;
use std::pin::Pin;

use super::{AiContext, AiProvider, ModelInfo, ProviderStream};

/// An [`AiProvider`] as an object, which every provider is: what an
/// [`AiRouter`](crate::AiRouter) names and a
/// [`FallbackRouter`](crate::FallbackRouter) holds, a provider of any type,
/// as Python's `AIProvider`. A caller never implements it nor calls it:
/// `Arc::new(provider)` is one.
///
/// Its methods are [`AiProvider`]'s under names of their own, so that a
/// provider with both traits in scope reads each method as one.
pub trait DynAiProvider: Send + Sync + 'static {
    /// [`AiProvider::name`].
    fn dyn_name(&self) -> &str;

    /// [`AiProvider::model_name`].
    fn dyn_model_name(&self) -> &str;

    /// [`AiProvider::catalog_entry`].
    fn dyn_catalog_entry(&self) -> Option<ModelInfo>;

    /// [`AiProvider::supports_vision`].
    fn dyn_supports_vision(&self) -> bool;

    /// [`AiProvider::context_window`].
    fn dyn_context_window(&self) -> Option<u64>;

    /// [`AiProvider::supports_deferred_tools`].
    fn dyn_supports_deferred_tools(&self) -> bool;

    /// [`AiProvider::generate_structured_stream`].
    fn dyn_generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a>;

    /// [`AiProvider::close`].
    fn dyn_close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// The provider's type name, without its path nor its parameters: what
    /// a turn's `llm.generate` span names (Python's `type(provider).__name__`).
    #[doc(hidden)]
    fn dyn_type_name(&self) -> &'static str;
}

impl<P: AiProvider> DynAiProvider for P {
    fn dyn_name(&self) -> &str {
        self.name()
    }

    fn dyn_model_name(&self) -> &str {
        self.model_name()
    }

    fn dyn_catalog_entry(&self) -> Option<ModelInfo> {
        self.catalog_entry()
    }

    fn dyn_supports_vision(&self) -> bool {
        self.supports_vision()
    }

    fn dyn_context_window(&self) -> Option<u64> {
        self.context_window()
    }

    fn dyn_supports_deferred_tools(&self) -> bool {
        self.supports_deferred_tools()
    }

    fn dyn_generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        self.generate_structured_stream(context)
    }

    fn dyn_close(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(self.close())
    }

    fn dyn_type_name(&self) -> &'static str {
        super::short_type_name::<P>()
    }
}
