//! An image provider's offline catalogue (RFC §25.6): Python's
//! `ImageModelInfo` and `image_model_entry`.
//!
//! Python's catalogue is a `list[ModelInfo]` whose entries are a
//! `ModelInfo`, for a model whose capabilities nobody verified, or its
//! subclass `ImageModelInfo`, which states them; a Rust list cannot hold a
//! type and its subclass, so an entry is one or the other (HITL 4071eaf1).

use serde::{Deserialize, Serialize};

use super::ImageCapabilities;
use crate::{ModelInfo, ModelPricing};

/// A catalogue entry with the capabilities of the model on its image
/// endpoint: Python's `ImageModelInfo`, a `ModelInfo` with `image` and
/// `aliases`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ImageModelInfo {
    /// The model's exact identifier, as the provider's API takes it.
    pub id: String,
    /// A name for people.
    #[serde(default)]
    pub display_name: Option<String>,
    /// The input context window, in tokens.
    #[serde(default)]
    pub context_window: Option<u64>,
    /// Whether the model takes images as input.
    #[serde(default)]
    pub supports_vision: Option<bool>,
    /// Whether the vendor retired it.
    #[serde(default)]
    pub deprecated: bool,
    /// Its capability tags, [`IMAGE_GEN_CAPABILITY`](super::IMAGE_GEN_CAPABILITY)
    /// among them.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Its rates, for a vendor that meters it per token.
    #[serde(default)]
    pub pricing: Option<ModelPricing>,
    /// What it takes on its image endpoint.
    pub image: ImageCapabilities,
    /// The snapshot ids verified to be the same model.
    #[serde(default)]
    pub aliases: Vec<String>,
}

impl ImageModelInfo {
    /// The model `id`, of the capabilities `image`, nothing else known of
    /// it.
    pub fn new(id: impl Into<String>, image: ImageCapabilities) -> Self {
        Self {
            id: id.into(),
            display_name: None,
            context_window: None,
            supports_vision: None,
            deprecated: false,
            capabilities: Vec::new(),
            pricing: None,
            image,
            aliases: Vec::new(),
        }
    }
}

impl From<ImageModelInfo> for ModelInfo {
    /// The entry as the `ModelInfo` it is, its image capabilities left out.
    fn from(entry: ImageModelInfo) -> Self {
        Self {
            display_name: entry.display_name,
            context_window: entry.context_window,
            supports_vision: entry.supports_vision,
            deprecated: entry.deprecated,
            capabilities: entry.capabilities,
            pricing: entry.pricing,
            ..Self::new(entry.id)
        }
    }
}

/// One entry of an image provider's catalogue: a model whose image
/// capabilities nobody verified, or one that states them. Python's
/// `ModelInfo | ImageModelInfo` (HITL 4071eaf1).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
#[expect(
    clippy::large_enum_variant,
    reason = "Python's ModelInfo | ImageModelInfo, each entry as a catalogue writes it"
)]
pub enum ImageCatalogEntry {
    /// An entry with its image capabilities: Python's `ImageModelInfo`.
    Image(ImageModelInfo),
    /// An entry without: Python's `ModelInfo`, whose capabilities stay
    /// unknown (RFC §25.6).
    Model(ModelInfo),
}

impl ImageCatalogEntry {
    /// The model's exact identifier.
    #[must_use]
    pub fn id(&self) -> &str {
        match self {
            Self::Image(entry) => &entry.id,
            Self::Model(entry) => &entry.id,
        }
    }

    /// Its image capabilities, `None` when nobody verified them.
    #[must_use]
    pub fn image(&self) -> Option<&ImageCapabilities> {
        match self {
            Self::Image(entry) => Some(&entry.image),
            Self::Model(_) => None,
        }
    }
}

impl From<ModelInfo> for ImageCatalogEntry {
    fn from(entry: ModelInfo) -> Self {
        Self::Model(entry)
    }
}

impl From<ImageModelInfo> for ImageCatalogEntry {
    fn from(entry: ImageModelInfo) -> Self {
        Self::Image(entry)
    }
}

/// The entry of `models` for `model`: its exact id, else a snapshot alias
/// an entry with image capabilities verified. Python's
/// `image_model_entry(models, model)`.
pub(crate) fn image_model_entry(
    models: Vec<ImageCatalogEntry>,
    model: &str,
) -> Option<ImageCatalogEntry> {
    models.into_iter().find(|entry| {
        entry.id() == model
            || matches!(entry, ImageCatalogEntry::Image(image) if image.aliases.iter().any(|alias| alias == model))
    })
}
