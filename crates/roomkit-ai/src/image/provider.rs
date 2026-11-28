//! The image generation contract (RFC §25.2): Python's `ImageProvider`, what
//! one generation is asked and what it fails with.

use std::fmt;
use std::future::Future;

use super::catalog::image_model_entry;
use super::{
    ImageCatalogEntry, ImageGenerationError, ImageOptions, ImageProgressCallback,
    ImageRequestError, ImageResult, ImageResultError,
};
use crate::provider::short_type_name;
use crate::{AiImagePart, ProviderError};

/// What a generation fails with: Python's two exceptions, which a Rust
/// result holds as two variants (HITL 0e571af4).
#[derive(Clone, Debug, thiserror::Error)]
pub enum ImageError {
    /// A request the provider cannot express, refused before anything is
    /// sent or billed: Python's `ValueError`.
    #[error(transparent)]
    Value(ImageRequestError),
    /// The vendor's call failed: Python's `ProviderError`, an
    /// [`ImageGenerationError`] its source once a request started.
    #[error(transparent)]
    Provider(ProviderError),
}

impl From<ImageRequestError> for ImageError {
    fn from(refused: ImageRequestError) -> Self {
        Self::Value(refused)
    }
}

impl From<ProviderError> for ImageError {
    fn from(failed: ProviderError) -> Self {
        Self::Provider(failed)
    }
}

impl From<ImageGenerationError> for ImageError {
    fn from(failed: ImageGenerationError) -> Self {
        Self::Provider(failed.into())
    }
}

impl From<ImageResultError> for ImageError {
    /// A result whose image is no `data:` URI: Python's `ValidationError`
    /// of `ImageResult`, a `ValueError`.
    fn from(refused: ImageResultError) -> Self {
        Self::Value(ImageRequestError::new(refused.to_string()))
    }
}

/// What [`ImageProvider::generate`] takes beside the prompt: Python's
/// keywords, with their defaults.
#[derive(Clone, Debug, PartialEq)]
pub struct GenerateOptions {
    /// The geometry, `WIDTHxHEIGHT`, `None` for the model's default. A
    /// provider whose API speaks aspect ratios translates; a size the model
    /// cannot draw is refused, never replaced.
    pub size: Option<String>,
    /// How many images, at least 1: 1 unless given. A provider whose API
    /// has no batch issues concurrent calls.
    pub n: u32,
    /// The images to edit or draw from: an edit when there is one, which a
    /// provider that cannot edit refuses.
    pub reference_images: Vec<AiImagePart>,
}

impl Default for GenerateOptions {
    fn default() -> Self {
        Self {
            size: None,
            n: 1,
            reference_images: Vec::new(),
        }
    }
}

/// What [`ImageProvider::generate_with_options`] takes beside the prompt:
/// Python's keywords, with their defaults.
#[derive(Clone)]
pub struct GenerateWithOptionsOptions {
    /// As [`GenerateOptions::size`].
    pub size: Option<String>,
    /// As [`GenerateOptions::n`]: 1 unless given.
    pub n: u32,
    /// As [`GenerateOptions::reference_images`].
    pub reference_images: Vec<AiImagePart>,
    /// The controls of the request.
    pub options: Option<ImageOptions>,
    /// The region of the reference to repaint.
    pub mask: Option<AiImagePart>,
    /// What is told as each attempt moves.
    pub on_progress: Option<ImageProgressCallback>,
}

impl Default for GenerateWithOptionsOptions {
    fn default() -> Self {
        Self {
            size: None,
            n: 1,
            reference_images: Vec::new(),
            options: None,
            mask: None,
            on_progress: None,
        }
    }
}

impl fmt::Debug for GenerateWithOptionsOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenerateWithOptionsOptions")
            .field("size", &self.size)
            .field("n", &self.n)
            .field("reference_images", &self.reference_images)
            .field("options", &self.options)
            .field("mask", &self.mask)
            .field("on_progress", &self.on_progress.as_ref().map(|_| ".."))
            .finish()
    }
}

/// An image generator (RFC §25.2), decoupled from the conversation: Python's
/// `ImageProvider`. `model_name` and `generate` are required; everything
/// else is provided as Python provides it.
pub trait ImageProvider: Send + Sync + 'static {
    /// The provider's name: Python's `name`, the provider's type name by
    /// default.
    fn name(&self) -> &str {
        short_type_name::<Self>()
    }

    /// The model the provider draws with, as configured: Python's
    /// `model_name`.
    fn model_name(&self) -> &str;

    /// Whether reference images are honoured: false by default, which
    /// refuses them.
    fn supports_editing(&self) -> bool {
        false
    }

    /// The provider's offline catalogue of image models (Python's
    /// classmethod `available_models()`), disjoint from the conversational
    /// one (RFC §25.6). None by default.
    fn available_models() -> Vec<ImageCatalogEntry> {
        Vec::new()
    }

    /// The catalogue's entry for the configured model, by its id or a
    /// verified snapshot alias (Python's `catalog_entry()`).
    fn catalog_entry(&self) -> Option<ImageCatalogEntry> {
        image_model_entry(Self::available_models(), self.model_name())
    }

    /// Exactly `n` images drawn from `prompt`, or a failure, never fewer.
    fn generate<'a>(
        &'a self,
        prompt: &'a str,
        options: GenerateOptions,
    ) -> impl Future<Output = Result<Vec<ImageResult>, ImageError>> + Send + 'a;

    /// As [`generate`](Self::generate), with the controls of the request, a
    /// mask and a progress callback: Python's `generate_with_options`. A
    /// provider that takes them overrides it; by default, controls, a mask
    /// or a callback are refused rather than dropped.
    fn generate_with_options<'a>(
        &'a self,
        prompt: &'a str,
        options: GenerateWithOptionsOptions,
    ) -> impl Future<Output = Result<Vec<ImageResult>, ImageError>> + Send + 'a {
        async move {
            let GenerateWithOptionsOptions {
                size,
                n,
                reference_images,
                options,
                mask,
                on_progress,
            } = options;
            let controls = options.is_some_and(|options| !options.supplied().is_empty());
            if mask.is_some() || controls {
                let refused = format!("{} does not support advanced image options", self.name());
                return Err(ImageRequestError::new(refused).into());
            }
            if on_progress.is_some() {
                let refused = format!("{} does not support image progress callbacks", self.name());
                return Err(ImageRequestError::new(refused).into());
            }
            let options = GenerateOptions {
                size,
                n,
                reference_images,
            };
            self.generate(prompt, options).await
        }
    }

    /// Releases what the provider holds.
    fn close(&self) -> impl Future<Output = ()> + Send {
        async {}
    }
}
