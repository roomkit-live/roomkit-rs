//! A provider held as an object (RFC §6.7): Python takes any `AIProvider`
//! where it takes a provider beside the channel's own, which a Rust trait
//! with `async` methods cannot be.

use super::{AiContext, AiProvider, ModelInfo, ProviderStream};

/// An [`AiProvider`] as an object, which every provider is: what
/// [`AiChannelOptions::fallback_provider`](crate::AiChannelOptions::fallback_provider)
/// holds, a provider of any type, as Python's `AIProvider`. A caller never
/// names it nor calls it: `Some(Arc::new(provider))` is one.
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

    /// [`AiProvider::supports_deferred_tools`].
    fn dyn_supports_deferred_tools(&self) -> bool;

    /// [`AiProvider::generate_structured_stream`].
    fn dyn_generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a>;
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

    fn dyn_supports_deferred_tools(&self) -> bool {
        self.supports_deferred_tools()
    }

    fn dyn_generate_structured_stream<'a>(&'a self, context: &'a AiContext) -> ProviderStream<'a> {
        self.generate_structured_stream(context)
    }
}
