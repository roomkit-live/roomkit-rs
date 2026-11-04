//! What a provider knows of its models before any request (RFC §6.7):
//! Python's `ModelInfo` and `ModelPricing`, the entries of a provider's
//! curated catalogue ([`AiProvider::available_models`](super::AiProvider::available_models)).

use std::collections::HashMap;

use roomkit::model::Metadata;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::date::Date;
use super::model_tags::with_speech_tags;
use crate::AiError;

/// The counters a response's usage bills, disjoint, a token counted under
/// one of them only (RFC §6.7): Python's `BILLED_USAGE_COUNTERS`, which
/// [`ModelPricing::cost_for`] prices and a turn's token budget sums (RFC
/// §6.4). `reasoning_tokens` is the thinking share of `output_tokens`.
pub(crate) const BILLED_USAGE_COUNTERS: [&str; 6] = [
    "input_tokens",
    "output_tokens",
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "input_image_tokens",
    "output_image_tokens",
];

/// Metadata describing one model a provider offers: Python's
/// `ModelInfo(*, id, display_name=None, context_window=None,
/// supports_vision=None, deprecated=False, capabilities=[], pricing=None)`.
///
/// Only `id` is certain; the rest is what the source states, `None` where
/// it states nothing. A curated catalogue's `capabilities` may hold a
/// provider's own routing tags (`chat_tools_refused`,
/// `tools_reasoning_none`); empty means unknown, not none.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ModelInfo {
    /// The model's exact identifier, as the provider's API takes it.
    pub id: String,
    /// A name for people (`GPT-6 Luna`).
    #[serde(default)]
    pub display_name: Option<String>,
    /// The model's input context window, in tokens.
    #[serde(default)]
    pub context_window: Option<u64>,
    /// Whether the model takes images.
    #[serde(default)]
    pub supports_vision: Option<bool>,
    /// Whether the provider marks the model deprecated.
    #[serde(default)]
    pub deprecated: bool,
    /// The model's capability tags.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// The vendor's list price, where it publishes one.
    #[serde(default)]
    pub pricing: Option<ModelPricing>,
}

impl ModelInfo {
    /// The entry for model `id`, every other field Python's default.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            display_name: None,
            context_window: None,
            supports_vision: None,
            deprecated: false,
            capabilities: Vec::new(),
            pricing: None,
        }
    }
}

/// `live` as every [`AiProvider::list_models`](super::AiProvider::list_models)
/// returns it: backfilled from the provider's `curated` catalogue, its speech
/// models tagged. Python's `AIProvider._listing`, for a provider of another
/// crate.
#[doc(hidden)]
pub fn listing(live: Vec<ModelInfo>, curated: &[ModelInfo]) -> Vec<ModelInfo> {
    with_speech_tags(merge_curated(live, curated))
}

/// What a live endpoint left out filled from the `curated` catalogue, for
/// each model of `live` it carries, what the endpoint did report kept: a
/// display name, a context window, whether it takes images, its price. A
/// catalogue's capabilities stay out: most carry internal routing flags, not
/// a public tag set. Python's `AIProvider._merge_curated`.
fn merge_curated(live: Vec<ModelInfo>, curated: &[ModelInfo]) -> Vec<ModelInfo> {
    let index: HashMap<&str, &ModelInfo> = curated
        .iter()
        .map(|model| (model.id.as_str(), model))
        .collect();
    live.into_iter()
        .map(|model| match index.get(model.id.as_str()) {
            Some(known) => ModelInfo {
                // Python's `or`: an empty name and a window of 0 are none.
                display_name: model
                    .display_name
                    .filter(|name| !name.is_empty())
                    .or_else(|| known.display_name.clone()),
                context_window: model
                    .context_window
                    .filter(|window| *window != 0)
                    .or(known.context_window),
                supports_vision: model.supports_vision.or(known.supports_vision),
                pricing: model.pricing.or_else(|| known.pricing.clone()),
                ..model
            },
            None => model,
        })
        .collect()
}

/// The list price of one model, per million tokens, as its vendor
/// published it on `verified`: Python's `ModelPricing(*, input_per_million,
/// output_per_million, cache_read_per_million=None,
/// cache_write_per_million=None, image_input_per_million=None,
/// image_output_per_million=None, long_context_threshold_tokens=None,
/// long_context_input_multiplier=1.0, long_context_output_multiplier=1.0,
/// currency='USD', verified)`.
///
/// Its rates mirror the counters a response's `usage` reports. A rate left
/// `None` is a counter the catalogue does not bill apart. Above
/// `long_context_threshold_tokens` input tokens, the multipliers apply.
///
/// Python's pydantic checks the fields as it builds the price:
/// [`ModelPricing::new`] checks the two rates it takes, and
/// [`cost_for`](Self::cost_for) and a read from JSON check them all.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "PricingFields")]
pub struct ModelPricing {
    /// The price of a million uncached input tokens.
    pub input_per_million: f64,
    /// The price of a million output tokens.
    pub output_per_million: f64,
    /// The price of a million tokens read again from the prompt cache.
    pub cache_read_per_million: Option<f64>,
    /// The price of a million tokens written to the prompt cache.
    pub cache_write_per_million: Option<f64>,
    /// The price of a million image input tokens, where the vendor bills
    /// them apart from text.
    pub image_input_per_million: Option<f64>,
    /// The price of a million generated-image tokens.
    pub image_output_per_million: Option<f64>,
    /// The input tokens above which the long-context multipliers apply;
    /// `None` for a flat price.
    pub long_context_threshold_tokens: Option<u64>,
    /// The multiplier of every input and cache charge above the threshold.
    pub long_context_input_multiplier: f64,
    /// The multiplier of the output charges above the threshold.
    pub long_context_output_multiplier: f64,
    /// The ISO 4217 code of the rates' currency.
    pub currency: String,
    /// The day the rates were read from the vendor's own price list.
    pub verified: Date,
}

impl ModelPricing {
    /// The price of `input_per_million` and `output_per_million`, read on
    /// `verified`, every other field Python's default.
    ///
    /// # Errors
    ///
    /// [`AiError::Pricing`] for a rate below 0 or not finite, as Python's
    /// pydantic refuses it.
    pub fn new(
        input_per_million: f64,
        output_per_million: f64,
        verified: Date,
    ) -> Result<Self, AiError> {
        let pricing = Self {
            input_per_million,
            output_per_million,
            cache_read_per_million: None,
            cache_write_per_million: None,
            image_input_per_million: None,
            image_output_per_million: None,
            long_context_threshold_tokens: None,
            long_context_input_multiplier: 1.0,
            long_context_output_multiplier: 1.0,
            currency: "USD".to_owned(),
            verified,
        };
        pricing.check()?;
        Ok(pricing)
    }

    /// The price of one response's `usage`, in [`currency`](Self::currency):
    /// Python's `cost_for(usage)`.
    ///
    /// It reads the billed counters (`input_tokens`, `output_tokens`,
    /// `cache_read_input_tokens`, `cache_creation_input_tokens`,
    /// `input_image_tokens`, `output_image_tokens`), each charged once, a
    /// missing one zero, any other key ignored. A counter with no rate is
    /// not charged. Past the long-context threshold, the multipliers apply.
    ///
    /// # Errors
    ///
    /// [`AiError::UsageCounter`] for a billed counter that is no
    /// non-negative integer, and [`AiError::Pricing`] for a field of the
    /// price Python's pydantic would have refused.
    pub fn cost_for(&self, usage: &Metadata) -> Result<f64, AiError> {
        self.check()?;
        // BILLED_USAGE_COUNTERS, read in their order.
        let input_tokens = usage_counter(usage, "input_tokens")?;
        let output_tokens = usage_counter(usage, "output_tokens")?;
        let cache_read = usage_counter(usage, "cache_read_input_tokens")?;
        let cache_creation = usage_counter(usage, "cache_creation_input_tokens")?;
        let input_image = usage_counter(usage, "input_image_tokens")?;
        let output_image = usage_counter(usage, "output_image_tokens")?;
        let mut input = tokens(input_tokens) * self.input_per_million;
        for (count, rate) in [
            (cache_read, self.cache_read_per_million),
            (cache_creation, self.cache_write_per_million),
            (input_image, self.image_input_per_million),
        ] {
            if let Some(rate) = rate {
                input += tokens(count) * rate;
            }
        }
        let mut output = tokens(output_tokens) * self.output_per_million;
        if let Some(rate) = self.image_output_per_million {
            output += tokens(output_image) * rate;
        }
        let total_input = [cache_read, cache_creation, input_image]
            .into_iter()
            .fold(input_tokens, u64::saturating_add);
        if self
            .long_context_threshold_tokens
            .is_some_and(|threshold| total_input > threshold)
        {
            input *= self.long_context_input_multiplier;
            output *= self.long_context_output_multiplier;
        }
        Ok((input + output) / 1_000_000.0)
    }

    /// Refuses a field Python's pydantic refuses, naming the first.
    fn check(&self) -> Result<(), AiError> {
        rate("input_per_million", self.input_per_million)?;
        rate("output_per_million", self.output_per_million)?;
        for (field, value) in [
            ("cache_read_per_million", self.cache_read_per_million),
            ("cache_write_per_million", self.cache_write_per_million),
            ("image_input_per_million", self.image_input_per_million),
            ("image_output_per_million", self.image_output_per_million),
        ] {
            value.map_or(Ok(()), |value| rate(field, value))?;
        }
        if self.long_context_threshold_tokens == Some(0) {
            return Err(refused(
                "long_context_threshold_tokens",
                "Input should be greater than 0",
            ));
        }
        multiplier(
            "long_context_input_multiplier",
            self.long_context_input_multiplier,
        )?;
        multiplier(
            "long_context_output_multiplier",
            self.long_context_output_multiplier,
        )?;
        if self.currency.is_empty() {
            return Err(refused(
                "currency",
                "String should have at least 1 character",
            ));
        }
        Ok(())
    }
}

/// A count of tokens as a price multiplies it, as Python's `int * float`.
#[expect(
    clippy::cast_precision_loss,
    reason = "a count of tokens is far below 2^53"
)]
fn tokens(count: u64) -> f64 {
    count as f64
}

/// The billed counter `name` of `usage`, zero when missing.
fn usage_counter(usage: &Metadata, name: &str) -> Result<u64, AiError> {
    match usage.get(name) {
        None => Ok(0),
        Some(Value::Number(number)) => number.as_u64().ok_or_else(|| counter_refused(name)),
        Some(_) => Err(counter_refused(name)),
    }
}

fn counter_refused(name: &str) -> AiError {
    AiError::UsageCounter {
        name: name.to_owned(),
    }
}

/// A rate: finite, and not below 0 (`ge=0, allow_inf_nan=False`).
fn rate(field: &'static str, value: f64) -> Result<(), AiError> {
    if !value.is_finite() {
        return Err(refused(field, "Input should be a finite number"));
    }
    if value < 0.0 {
        return Err(refused(field, "Input should be greater than or equal to 0"));
    }
    Ok(())
}

/// A multiplier: finite, and above 0 (`gt=0, allow_inf_nan=False`).
fn multiplier(field: &'static str, value: f64) -> Result<(), AiError> {
    if !value.is_finite() {
        return Err(refused(field, "Input should be a finite number"));
    }
    if value <= 0.0 {
        return Err(refused(field, "Input should be greater than 0"));
    }
    Ok(())
}

fn refused(field: &'static str, reason: &str) -> AiError {
    AiError::Pricing {
        field,
        reason: reason.to_owned(),
    }
}

/// [`ModelPricing`]'s fields as JSON states them, Python's defaults for
/// the ones it leaves out, checked before they become a price.
#[derive(Deserialize)]
struct PricingFields {
    input_per_million: f64,
    output_per_million: f64,
    #[serde(default)]
    cache_read_per_million: Option<f64>,
    #[serde(default)]
    cache_write_per_million: Option<f64>,
    #[serde(default)]
    image_input_per_million: Option<f64>,
    #[serde(default)]
    image_output_per_million: Option<f64>,
    #[serde(default)]
    long_context_threshold_tokens: Option<u64>,
    #[serde(default = "one")]
    long_context_input_multiplier: f64,
    #[serde(default = "one")]
    long_context_output_multiplier: f64,
    #[serde(default = "usd")]
    currency: String,
    verified: Date,
}

fn one() -> f64 {
    1.0
}

fn usd() -> String {
    "USD".to_owned()
}

impl TryFrom<PricingFields> for ModelPricing {
    type Error = AiError;

    fn try_from(fields: PricingFields) -> Result<Self, Self::Error> {
        let pricing = Self {
            input_per_million: fields.input_per_million,
            output_per_million: fields.output_per_million,
            cache_read_per_million: fields.cache_read_per_million,
            cache_write_per_million: fields.cache_write_per_million,
            image_input_per_million: fields.image_input_per_million,
            image_output_per_million: fields.image_output_per_million,
            long_context_threshold_tokens: fields.long_context_threshold_tokens,
            long_context_input_multiplier: fields.long_context_input_multiplier,
            long_context_output_multiplier: fields.long_context_output_multiplier,
            currency: fields.currency,
            verified: fields.verified,
        };
        pricing.check()?;
        Ok(pricing)
    }
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
