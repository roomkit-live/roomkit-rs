//! The controls one image request takes: Python's `ImageOptions` and the
//! `Literal`s its fields are typed by (`roomkit.providers.image.options`).

use serde::{Deserialize, Serialize};

use super::ImageRequestError;

/// How much rendering effort: Python's `Literal` of `ImageOptions.quality`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageQuality {
    /// The vendor's choice.
    Auto,
    /// Low.
    Low,
    /// Medium.
    Medium,
    /// High.
    High,
    /// Above high.
    Xhigh,
    /// The most the model gives.
    Max,
}

/// What lies behind the subject: Python's `Literal` of
/// `ImageOptions.background`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageBackground {
    /// The vendor's choice.
    Auto,
    /// Opaque.
    Opaque,
    /// Transparent, which a JPEG cannot carry.
    Transparent,
}

/// The encoding of the image: Python's `Literal` of
/// `ImageOptions.output_format`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageOutputFormat {
    /// PNG.
    Png,
    /// JPEG.
    Jpeg,
    /// WebP.
    Webp,
}

/// How strictly the vendor filters: Python's `Literal` of
/// `ImageOptions.moderation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageModeration {
    /// The vendor's default.
    Auto,
    /// Less strict.
    Low,
}

/// How closely an edit keeps its references: Python's `Literal` of
/// `ImageOptions.input_fidelity`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageInputFidelity {
    /// Low.
    Low,
    /// High.
    High,
}

/// A resolution tier, native geometry with no promise of exact pixels:
/// Python's `Literal["512", "1K", "2K", "4K"]` of `ImageOptions.image_size`,
/// values no Rust name can spell, written as Python writes them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ImageSize {
    /// `"512"`.
    #[serde(rename = "512")]
    Tier512,
    /// `"1K"`.
    #[serde(rename = "1K")]
    Tier1K,
    /// `"2K"`.
    #[serde(rename = "2K")]
    Tier2K,
    /// `"4K"`.
    #[serde(rename = "4K")]
    Tier4K,
}

/// How much the model thinks before it draws: Python's `Literal` of
/// `ImageOptions.thinking_level`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageThinkingLevel {
    /// Minimal.
    Minimal,
    /// Medium.
    Medium,
    /// High.
    High,
}

/// A search that grounds the image: Python's `Literal` of the items of
/// `ImageOptions.search_types`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImageSearchType {
    /// A web search.
    WebSearch,
    /// An image search.
    ImageSearch,
}

/// The controls of one request: Python's `ImageOptions`. A control left
/// out takes the provider's configured default; one given overrides it.
/// `size` stays on the generation for portable pixel geometry;
/// `aspect_ratio` and `image_size` ask native geometry, with no promise of
/// exact pixels; `partial_images` asks previews, which need a progress
/// callback.
///
/// Python refuses an out-of-range value when the options are built; a Rust
/// struct literal runs no check, so
/// [`ImageCapabilities::validate_request`](super::ImageCapabilities::validate_request)
/// refuses it, before any request.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ImageOptions {
    /// The rendering effort.
    pub quality: Option<ImageQuality>,
    /// The background.
    pub background: Option<ImageBackground>,
    /// The encoding.
    pub output_format: Option<ImageOutputFormat>,
    /// The compression of a JPEG or a WebP, 0 to 100.
    pub output_compression: Option<u8>,
    /// The moderation.
    pub moderation: Option<ImageModeration>,
    /// How closely an edit keeps its references.
    pub input_fidelity: Option<ImageInputFidelity>,
    /// A native aspect ratio (`16:9`).
    pub aspect_ratio: Option<String>,
    /// A native resolution tier.
    pub image_size: Option<ImageSize>,
    /// How much the model thinks.
    pub thinking_level: Option<ImageThinkingLevel>,
    /// The interaction this one continues, never empty.
    pub previous_interaction_id: Option<String>,
    /// Whether the vendor keeps the interaction; left out, its default.
    pub store: Option<bool>,
    /// The searches that ground the image.
    pub search_types: Option<Vec<ImageSearchType>>,
    /// How many previews to stream, 0 to 3.
    pub partial_images: Option<u8>,
}

impl ImageOptions {
    /// The names of the controls given, in Python's field order: Python's
    /// `model_dump(exclude_none=True)` keys.
    pub(crate) fn supplied(&self) -> Vec<&'static str> {
        let given = [
            ("quality", self.quality.is_some()),
            ("background", self.background.is_some()),
            ("output_format", self.output_format.is_some()),
            ("output_compression", self.output_compression.is_some()),
            ("moderation", self.moderation.is_some()),
            ("input_fidelity", self.input_fidelity.is_some()),
            ("aspect_ratio", self.aspect_ratio.is_some()),
            ("image_size", self.image_size.is_some()),
            ("thinking_level", self.thinking_level.is_some()),
            (
                "previous_interaction_id",
                self.previous_interaction_id.is_some(),
            ),
            ("store", self.store.is_some()),
            ("search_types", self.search_types.is_some()),
            ("partial_images", self.partial_images.is_some()),
        ];
        given
            .into_iter()
            .filter_map(|(name, given)| given.then_some(name))
            .collect()
    }

    /// The values of the controls whose value a model's capabilities list,
    /// in the order Python checks them, as Python writes them.
    pub(crate) fn listed_values(&self) -> [(&'static str, Option<String>); 6] {
        [
            ("quality", self.quality.as_ref().map(written)),
            ("output_format", self.output_format.as_ref().map(written)),
            ("background", self.background.as_ref().map(written)),
            ("aspect_ratio", self.aspect_ratio.clone()),
            ("image_size", self.image_size.as_ref().map(written)),
            ("thinking_level", self.thinking_level.as_ref().map(written)),
        ]
    }

    /// The refusal Python's model raises when it is built, for a value out
    /// of its field's bounds.
    pub(crate) fn out_of_bounds(&self) -> Result<(), ImageRequestError> {
        if self.output_compression.is_some_and(|value| value > 100) {
            return Err(ImageRequestError::new(
                "output_compression must be between 0 and 100",
            ));
        }
        if self.partial_images.is_some_and(|value| value > 3) {
            return Err(ImageRequestError::new(
                "partial_images must be between 0 and 3",
            ));
        }
        if self.previous_interaction_id.as_deref() == Some("") {
            return Err(ImageRequestError::new(
                "previous_interaction_id must not be empty",
            ));
        }
        Ok(())
    }
}

/// A `Literal`'s value, as Python and the wire write it.
pub(crate) fn written<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(text)) => text,
        _ => String::new(),
    }
}
