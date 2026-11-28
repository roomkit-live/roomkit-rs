//! What one model takes on its image endpoint, and the refusal of a request
//! it cannot serve before anything is billed: Python's `ImageCapabilities`
//! (RFC §25.2, §25.6).

use serde::{Deserialize, Serialize};

use super::options::written;
use super::{ImageBackground, ImageOptions, ImageOutputFormat, ImageRequestError};
use crate::Date;

/// The controls verified for one model on its image endpoint: Python's
/// `ImageCapabilities`. An absent entry is unknown, never a neighbouring
/// model's; a list names the values taken, an empty one a control not
/// taken.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Python's ImageCapabilities field for field"
)]
pub struct ImageCapabilities {
    /// The controls the model takes, by their `ImageOptions` names.
    #[serde(default)]
    pub options: Vec<String>,
    /// The qualities it takes.
    #[serde(default)]
    pub qualities: Vec<String>,
    /// The encodings it writes.
    #[serde(default)]
    pub formats: Vec<String>,
    /// The backgrounds it draws.
    #[serde(default)]
    pub backgrounds: Vec<String>,
    /// The native aspect ratios it takes.
    #[serde(default)]
    pub aspect_ratios: Vec<String>,
    /// The resolution tiers it takes.
    #[serde(default)]
    pub image_sizes: Vec<String>,
    /// The thinking levels it takes.
    #[serde(default)]
    pub thinking_levels: Vec<String>,
    /// The sizes it draws, as `WIDTHxHEIGHT`.
    #[serde(default)]
    pub sizes: Vec<String>,
    /// Whether it draws other sizes, within its geometry's bounds.
    #[serde(default)]
    pub flexible_size: bool,
    /// The most images one request asks, `None` for no bound: 10 unless
    /// given.
    #[serde(default = "ten")]
    pub max_images: Option<u32>,
    /// The most reference images it takes.
    #[serde(default)]
    pub max_references: usize,
    /// Whether it takes a mask.
    #[serde(default)]
    pub mask: bool,
    /// Whether it continues a previous interaction.
    #[serde(default)]
    pub continuity: bool,
    /// The searches it grounds an image with.
    #[serde(default)]
    pub search_types: Vec<String>,
    /// Whether it streams previews.
    #[serde(default)]
    pub streaming: bool,
    /// The day the vendor retires it, when announced.
    #[serde(default)]
    pub retirement_date: Option<Date>,
    /// The day these capabilities were verified.
    pub verified: Date,
}

/// Python's default of `max_images`, which serde fills an absent field
/// with.
#[expect(clippy::unnecessary_wraps, reason = "the default of an optional field")]
fn ten() -> Option<u32> {
    Some(10)
}

impl ImageCapabilities {
    /// Capabilities verified on `verified`, the model taking no control,
    /// up to 10 images and no reference.
    #[must_use]
    pub fn new(verified: Date) -> Self {
        Self {
            options: Vec::new(),
            qualities: Vec::new(),
            formats: Vec::new(),
            backgrounds: Vec::new(),
            aspect_ratios: Vec::new(),
            image_sizes: Vec::new(),
            thinking_levels: Vec::new(),
            sizes: Vec::new(),
            flexible_size: false,
            max_images: ten(),
            max_references: 0,
            mask: false,
            continuity: false,
            search_types: Vec::new(),
            streaming: false,
            retirement_date: None,
            verified,
        }
    }

    /// Refuses a request of `n` images, `references` reference images and
    /// a mask or none, at `size` with `options`, that the model cannot
    /// serve: before any billable call, in Python's order, with its
    /// messages. A value out of its field's bounds, which Python refuses
    /// when its options are built, is refused first.
    ///
    /// # Errors
    ///
    /// [`ImageRequestError`] for the first thing the model cannot serve.
    pub fn validate_request(
        &self,
        options: &ImageOptions,
        size: Option<&str>,
        n: u32,
        references: usize,
        mask: bool,
    ) -> Result<(), ImageRequestError> {
        options.out_of_bounds()?;
        self.refuse_counts(n, references, mask)?;
        self.refuse_controls(options)?;
        self.refuse_features(options)?;
        refuse_combinations(options, size)?;
        match size.filter(|size| !size.is_empty()) {
            Some(size) => self.validate_size(size),
            None => Ok(()),
        }
    }

    /// Refuses `size`, as `WIDTHxHEIGHT`, the model does not draw: one of
    /// its sizes, or, for a flexible one, edges multiples of 16, at most
    /// 3840 each and 3:1, between 655360 and 8294400 pixels.
    ///
    /// # Errors
    ///
    /// [`ImageRequestError`] for a size the model does not draw.
    pub fn validate_size(&self, size: &str) -> Result<(), ImageRequestError> {
        if self.sizes.iter().any(|known| known == size) {
            return Ok(());
        }
        if !self.flexible_size {
            let sizes = self.sizes.join(", ");
            return Err(ImageRequestError::new(format!(
                "size must be one of {sizes}"
            )));
        }
        let (width, height) = edges(size)
            .ok_or_else(|| ImageRequestError::new("size must be WIDTHxHEIGHT or auto"))?;
        let (short, long) = (width.min(height), width.max(height));
        let pixels = width.saturating_mul(height);
        if short < 16
            || width % 16 != 0
            || height % 16 != 0
            || long > 3840
            || long > short.saturating_mul(3)
            || !(655_360..=8_294_400).contains(&pixels)
        {
            return Err(ImageRequestError::new(
                "Image dimensions must be multiples of 16, at most 3840 per edge, \
                 at most 3:1, and between 655360 and 8294400 pixels",
            ));
        }
        Ok(())
    }

    /// Refuses a count of images, of references or a mask out of bounds.
    fn refuse_counts(
        &self,
        n: u32,
        references: usize,
        mask: bool,
    ) -> Result<(), ImageRequestError> {
        if n < 1 || self.max_images.is_some_and(|most| n > most) {
            let limit = self
                .max_images
                .map_or_else(|| "None".to_owned(), |most| most.to_string());
            return Err(ImageRequestError::new(format!(
                "n must be at least 1; model limit: {limit}"
            )));
        }
        if references > self.max_references {
            return Err(ImageRequestError::new(format!(
                "This model accepts at most {} references",
                self.max_references
            )));
        }
        if mask && (!self.mask || references == 0) {
            return Err(ImageRequestError::new(
                "A mask requires a reference image and a model supporting masks",
            ));
        }
        Ok(())
    }

    /// Refuses a control the model does not take, then a value of one it
    /// does not list.
    fn refuse_controls(&self, options: &ImageOptions) -> Result<(), ImageRequestError> {
        let mut unsupported: Vec<&str> = options
            .supplied()
            .into_iter()
            .filter(|name| !self.options.iter().any(|taken| taken == name))
            .collect();
        if !unsupported.is_empty() {
            unsupported.sort_unstable();
            return Err(ImageRequestError::new(format!(
                "Unsupported image options: {}",
                unsupported.join(", ")
            )));
        }
        for (field, value) in options.listed_values() {
            let allowed = self.allowed(field);
            if value.is_some_and(|value| !allowed.contains(&value)) {
                return Err(ImageRequestError::new(format!(
                    "{field} must be one of {}",
                    allowed.join(", ")
                )));
            }
        }
        Ok(())
    }

    /// The values the model lists for `field`.
    fn allowed(&self, field: &str) -> &[String] {
        match field {
            "quality" => &self.qualities,
            "output_format" => &self.formats,
            "background" => &self.backgrounds,
            "aspect_ratio" => &self.aspect_ratios,
            "image_size" => &self.image_sizes,
            _ => &self.thinking_levels,
        }
    }

    /// Refuses a search or a continuity the model does not have.
    fn refuse_features(&self, options: &ImageOptions) -> Result<(), ImageRequestError> {
        let mut searches = options.search_types.iter().flatten().map(written);
        if searches.any(|search| !self.search_types.contains(&search)) {
            return Err(ImageRequestError::new(
                "This model does not support the requested search types",
            ));
        }
        let continues = options
            .previous_interaction_id
            .as_deref()
            .is_some_and(|id| !id.is_empty());
        if continues && !self.continuity {
            return Err(ImageRequestError::new(
                "This model does not support interaction continuity",
            ));
        }
        Ok(())
    }
}

/// Refuses controls that cannot go together, whatever the model.
fn refuse_combinations(
    options: &ImageOptions,
    size: Option<&str>,
) -> Result<(), ImageRequestError> {
    let transparent = options.background == Some(ImageBackground::Transparent);
    if transparent && options.output_format == Some(ImageOutputFormat::Jpeg) {
        return Err(ImageRequestError::new(
            "Transparent output requires PNG or WebP",
        ));
    }
    let compressed = matches!(
        options.output_format,
        Some(ImageOutputFormat::Jpeg | ImageOutputFormat::Webp)
    );
    if options.output_compression.is_some() && !compressed {
        return Err(ImageRequestError::new(
            "output_compression requires an explicit JPEG or WebP format",
        ));
    }
    let sized = size.is_some_and(|size| !size.is_empty());
    let native = options
        .aspect_ratio
        .as_deref()
        .is_some_and(|ratio| !ratio.is_empty())
        || options.image_size.is_some();
    if sized && native {
        return Err(ImageRequestError::new(
            "Use size or aspect_ratio/image_size, not both",
        ));
    }
    Ok(())
}

/// The two edges `size` names as Python's `(\d+)[xX](\d+)` reads them,
/// an edge too long for a `u64` read as the largest, which no bound takes.
fn edges(size: &str) -> Option<(u64, u64)> {
    let (width, height) = size.split_once(['x', 'X'])?;
    let edge = |text: &str| {
        let digits = !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit());
        digits.then(|| text.parse().unwrap_or(u64::MAX))
    };
    Some((edge(width)?, edge(height)?))
}
