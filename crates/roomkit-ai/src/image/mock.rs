//! An image provider for tests, with no vendor nor key: Python's
//! `MockImageProvider`.

use std::sync::Mutex;

use serde_json::Value;

use super::{
    GenerateOptions, ImageError, ImageProvider, ImageRequestError, ImageResult, parse_size,
    to_data_uri,
};
use crate::AiImagePart;

/// A real 1x1 transparent PNG, so that a consumer's test runs the whole
/// path, decoding, writing and measuring: Python's `_PNG_1X1`.
const PNG_1X1: &[u8] = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89\x00\x00\x00\rIDATx\xdacd`\xf8_\x0f\x00\x02\x87\x01\x80\xebG\xba\x92\x00\x00\x00\x00IEND\xaeB`\x82";

/// One call a [`MockImageProvider`] was asked: its prompt, size, count and
/// reference images.
type ImageCall = (String, Option<String>, u32, Vec<AiImagePart>);

/// What [`MockImageProvider::new`] takes: Python's keywords, with their
/// defaults.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MockImageProviderOptions {
    /// The images it returns, in turn: a 1x1 PNG when none.
    pub images: Vec<Vec<u8>>,
    /// Its model: `mock-image` unless given.
    pub model: String,
    /// The media type of its images: `image/png` unless given.
    pub mime_type: String,
    /// Whether it edits: true unless given.
    pub supports_editing: bool,
    /// The revised prompt each result reports.
    pub revised_prompt: Option<String>,
}

impl Default for MockImageProviderOptions {
    fn default() -> Self {
        Self {
            images: Vec::new(),
            model: "mock-image".to_owned(),
            mime_type: "image/png".to_owned(),
            supports_editing: true,
            revised_prompt: None,
        }
    }
}

/// An image provider that returns canned images, in turn, and records what
/// it was asked: Python's `MockImageProvider`.
#[derive(Debug)]
pub struct MockImageProvider {
    images: Vec<Vec<u8>>,
    model: String,
    mime_type: String,
    supports_editing: bool,
    revised_prompt: Option<String>,
    drawn: Mutex<Drawn>,
}

/// What the mock was asked, and the next image it returns.
#[derive(Debug, Default)]
struct Drawn {
    index: usize,
    calls: Vec<ImageCall>,
}

impl MockImageProvider {
    /// The mock `options` describe.
    #[must_use]
    pub fn new(options: MockImageProviderOptions) -> Self {
        let MockImageProviderOptions {
            images,
            model,
            mime_type,
            supports_editing,
            revised_prompt,
        } = options;
        let images = if images.is_empty() {
            vec![PNG_1X1.to_vec()]
        } else {
            images
        };
        Self {
            images,
            model,
            mime_type,
            supports_editing,
            revised_prompt,
            drawn: Mutex::new(Drawn::default()),
        }
    }

    /// Every call it was asked, in order: Python's attribute `calls`.
    #[must_use]
    pub fn calls(&self) -> Vec<ImageCall> {
        self.drawn().calls.clone()
    }

    fn drawn(&self) -> std::sync::MutexGuard<'_, Drawn> {
        self.drawn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The result of the image `data`, for `prompt` and `references`
    /// reference images.
    fn result(
        &self,
        data: &[u8],
        prompt: &str,
        references: usize,
    ) -> Result<ImageResult, ImageError> {
        let uri = to_data_uri(data, &self.mime_type);
        let mut result = ImageResult::new(uri, self.mime_type.clone())?;
        result.revised_prompt.clone_from(&self.revised_prompt);
        let words = prompt
            .split(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
            .filter(|word| !word.is_empty())
            .count();
        let counters = [
            ("input_tokens", words),
            ("input_image_tokens", references * 100),
            ("output_tokens", 0),
            ("output_image_tokens", 1024),
        ];
        result.usage = counters
            .into_iter()
            .map(|(name, count)| (name.to_owned(), Value::from(count)))
            .collect();
        Ok(result)
    }
}

impl MockImageProvider {
    /// The images `prompt` and `options` ask, cycling through the mock's,
    /// the call recorded.
    fn generated(
        &self,
        prompt: &str,
        options: GenerateOptions,
    ) -> Result<Vec<ImageResult>, ImageError> {
        let GenerateOptions {
            size,
            n,
            reference_images,
        } = options;
        if n < 1 {
            return Err(ImageRequestError::new(format!("n must be at least 1, got {n}")).into());
        }
        if let Some(size) = &size {
            parse_size(size)?;
        }
        if !reference_images.is_empty() && !self.supports_editing {
            let refused = format!("{} does not support editing", self.name());
            return Err(ImageRequestError::new(refused).into());
        }
        let references = reference_images.len();
        let picked: Vec<Vec<u8>> = {
            let mut drawn = self.drawn();
            drawn
                .calls
                .push((prompt.to_owned(), size, n, reference_images));
            (0..n)
                .map(|_| {
                    let at = drawn.index % self.images.len();
                    drawn.index += 1;
                    self.images.get(at).cloned().unwrap_or_default()
                })
                .collect()
        };
        picked
            .iter()
            .map(|data| self.result(data, prompt, references))
            .collect()
    }
}

impl Default for MockImageProvider {
    fn default() -> Self {
        Self::new(MockImageProviderOptions::default())
    }
}

impl ImageProvider for MockImageProvider {
    fn model_name(&self) -> &str {
        &self.model
    }

    fn supports_editing(&self) -> bool {
        self.supports_editing
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "as Python's, the call runs when it is awaited, not when it is made"
    )]
    async fn generate(
        &self,
        prompt: &str,
        options: GenerateOptions,
    ) -> Result<Vec<ImageResult>, ImageError> {
        self.generated(prompt, options)
    }
}
