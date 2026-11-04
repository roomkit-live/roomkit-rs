//! Translated from Python's `tests/test_providers/test_ai_models.py`: a
//! model's price and the base provider's catalogue.

use pretty_assertions::assert_eq;
use serde_json::{Value, json};

use super::*;
use crate::{
    AiContext, AiProvider, AiResponse, MockAiProvider, MockAiProviderOptions, ProviderError,
};

fn day() -> Date {
    Date::new(2026, 8, 5).unwrap()
}

fn usage(value: Value) -> Metadata {
    serde_json::from_value(value).unwrap()
}

fn approx(actual: f64, expected: f64) {
    assert!(
        (actual - expected).abs() <= expected.abs() * 1e-6 + 1e-12,
        "{actual} is not {expected}"
    );
}

// Python: tests/test_providers/test_ai_models.py::test_cost_for_prices_input_and_output
#[test]
fn cost_for_prices_input_and_output() {
    let pricing = ModelPricing::new(1.5, 7.5, day()).unwrap();
    let cost = pricing
        .cost_for(&usage(json!({"input_tokens": 12_693, "output_tokens": 20})))
        .unwrap();
    approx(cost, 12_693.0 * 1.5e-6 + 20.0 * 7.5e-6);
}

// Python: tests/test_providers/test_ai_models.py::test_cost_for_uses_the_cache_rates_when_the_model_has_them
#[test]
fn cost_for_uses_the_cache_rates_when_the_model_has_them() {
    let pricing = ModelPricing {
        cache_read_per_million: Some(0.5),
        cache_write_per_million: Some(6.25),
        ..ModelPricing::new(5.0, 25.0, day()).unwrap()
    };
    let cost = pricing
        .cost_for(&usage(json!({
            "input_tokens": 1_000,
            "output_tokens": 100,
            "cache_read_input_tokens": 100_000,
            "cache_creation_input_tokens": 10_000,
        })))
        .unwrap();
    approx(cost, 0.005 + 0.0025 + 0.05 + 0.0625);
}

// Python: tests/test_providers/test_ai_models.py::test_cost_for_omits_cache_counter_without_a_per_token_rate
#[test]
fn cost_for_omits_cache_counter_without_a_per_token_rate() {
    let pricing = ModelPricing::new(2.0, 6.0, day()).unwrap();
    let cost = pricing
        .cost_for(&usage(json!({"cache_read_input_tokens": 1_000_000})))
        .unwrap();
    approx(cost, 0.0);
}

// Python: tests/test_providers/test_ai_models.py::test_cost_for_applies_long_context_multipliers_to_all_token_classes
#[test]
fn cost_for_applies_long_context_multipliers_to_all_token_classes() {
    let pricing = ModelPricing {
        cache_read_per_million: Some(0.2),
        cache_write_per_million: Some(2.5),
        long_context_threshold_tokens: Some(200_000),
        long_context_input_multiplier: 2.0,
        long_context_output_multiplier: 1.5,
        ..ModelPricing::new(2.0, 10.0, day()).unwrap()
    };
    let cost = pricing
        .cost_for(&usage(json!({
            "input_tokens": 100_000,
            "cache_read_input_tokens": 100_000,
            "cache_creation_input_tokens": 1,
            "output_tokens": 10_000,
        })))
        .unwrap();
    approx(cost, ((0.2 + 0.02 + 0.000_002_5) * 2.0) + 0.15);
}

// Python: tests/test_providers/test_ai_models.py::test_cost_for_ignores_unknown_counters_and_missing_keys
#[test]
fn cost_for_ignores_unknown_counters_and_missing_keys() {
    let pricing = ModelPricing::new(1.0, 1.0, day()).unwrap();
    approx(pricing.cost_for(&Metadata::new()).unwrap(), 0.0);
    let cost = pricing
        .cost_for(&usage(
            json!({"reasoning_tokens": 5_000, "input_tokens": 1_000}),
        ))
        .unwrap();
    approx(cost, 0.001);
}

// Python: tests/test_providers/test_ai_models.py::test_pricing_rejects_invalid_financial_fields
#[test]
fn pricing_rejects_invalid_financial_fields() {
    let valid = || ModelPricing::new(1.0, 1.0, day()).unwrap();
    let refused: [(&str, ModelPricing); 8] = [
        (
            "input_per_million",
            ModelPricing {
                input_per_million: -1.0,
                ..valid()
            },
        ),
        (
            "output_per_million",
            ModelPricing {
                output_per_million: f64::INFINITY,
                ..valid()
            },
        ),
        (
            "cache_read_per_million",
            ModelPricing {
                cache_read_per_million: Some(-1.0),
                ..valid()
            },
        ),
        (
            "cache_write_per_million",
            ModelPricing {
                cache_write_per_million: Some(f64::NAN),
                ..valid()
            },
        ),
        (
            "long_context_threshold_tokens",
            ModelPricing {
                long_context_threshold_tokens: Some(0),
                ..valid()
            },
        ),
        (
            "long_context_input_multiplier",
            ModelPricing {
                long_context_input_multiplier: 0.0,
                ..valid()
            },
        ),
        (
            "long_context_output_multiplier",
            ModelPricing {
                long_context_output_multiplier: -1.0,
                ..valid()
            },
        ),
        (
            "currency",
            ModelPricing {
                currency: String::new(),
                ..valid()
            },
        ),
    ];
    for (field, pricing) in refused {
        let error = pricing.cost_for(&Metadata::new()).unwrap_err();
        assert!(
            matches!(&error, AiError::Pricing { field: named, .. } if *named == field),
            "{field}: {error}"
        );
        // A price read from JSON is checked as Python's pydantic builds it;
        // JSON writes no infinity nor NaN, which serde writes `null`.
        let written = serde_json::to_value(&pricing).unwrap();
        if !written[field].is_null() {
            let read = serde_json::from_value::<ModelPricing>(written);
            assert!(read.is_err(), "{field} read from JSON");
        }
    }
    assert!(ModelPricing::new(-1.0, 1.0, day()).is_err());
    assert!(ModelPricing::new(1.0, f64::NAN, day()).is_err());
}

// Python: tests/test_providers/test_ai_models.py::test_cost_for_rejects_invalid_usage_counters
#[test]
fn cost_for_rejects_invalid_usage_counters() {
    let pricing = ModelPricing::new(1.0, 1.0, day()).unwrap();
    for value in [json!(-1), json!(true), json!(1.5), json!("100")] {
        let error = pricing
            .cost_for(&usage(json!({"input_tokens": value})))
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "usage counter 'input_tokens' must be a non-negative integer"
        );
    }
}

/// A provider no catalogue carries, as Python's `_Bare`.
struct Bare;

impl AiProvider for Bare {
    fn model_name(&self) -> &'static str {
        "some-custom-local-model"
    }

    fn generate<'a>(
        &'a self,
        _context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        std::future::ready(Ok(AiResponse::new("")))
    }
}

// Python: tests/test_providers/test_ai_models.py::test_base_available_models_is_empty
#[test]
fn base_available_models_is_empty() {
    assert_eq!(Bare::available_models(), Vec::new());
}

// Python: tests/test_providers/test_ai_models.py::test_context_window_resolves_from_catalog
#[test]
fn context_window_resolves_from_catalog() {
    assert_eq!(
        MockAiProvider::new(MockAiProviderOptions::default()).context_window(),
        Some(8192)
    );
}

// Python: tests/test_providers/test_ai_models.py::test_context_window_none_when_model_absent
#[test]
fn context_window_none_when_model_absent() {
    assert_eq!(Bare.context_window(), None);
}

// Python: tests/test_providers/test_ai_models.py::test_catalog_entry_returns_the_active_models_metadata
#[test]
fn catalog_entry_returns_the_active_models_metadata() {
    let entry = MockAiProvider::new(MockAiProviderOptions::default())
        .catalog_entry()
        .unwrap();
    assert_eq!(entry.id, "mock");
    assert_eq!(entry.context_window, Some(8192));
}

// Python: tests/test_providers/test_ai_models.py::test_catalog_entry_is_none_for_an_unknown_model
#[test]
fn catalog_entry_is_none_for_an_unknown_model() {
    assert_eq!(Bare.catalog_entry(), None);
}

// Python: tests/test_providers/test_ai_models.py::test_base_list_models_falls_back_to_curated
#[tokio::test]
async fn base_list_models_falls_back_to_curated() {
    let provider = MockAiProvider::new(MockAiProviderOptions::default());
    assert_eq!(
        provider.list_models().await.unwrap(),
        MockAiProvider::available_models()
    );
}

// Python: tests/test_providers/test_ai_models.py::test_merge_curated_backfills_pricing
#[test]
fn merge_curated_backfills_pricing() {
    let pricing = ModelPricing::new(1.0, 2.0, day()).unwrap();
    let curated = [ModelInfo {
        pricing: Some(pricing.clone()),
        ..ModelInfo::new("a")
    }];
    let merged = merge_curated(vec![ModelInfo::new("a"), ModelInfo::new("b")], &curated);
    assert_eq!(
        merged
            .iter()
            .map(|m| m.pricing.as_ref())
            .collect::<Vec<_>>(),
        [Some(&pricing), None]
    );
}

// Python: tests/test_providers/test_ai_models.py::test_merge_curated_backfills_missing_metadata
#[test]
fn merge_curated_backfills_missing_metadata() {
    let known = ModelInfo {
        display_name: Some("A".to_owned()),
        context_window: Some(100),
        supports_vision: Some(true),
        ..ModelInfo::new("a")
    };
    let merged = merge_curated(
        vec![ModelInfo::new("a"), ModelInfo::new("b")],
        std::slice::from_ref(&known),
    );
    assert_eq!(merged, [known, ModelInfo::new("b")]);
}

// Python: tests/test_providers/test_ai_models.py::test_merge_curated_prefers_live_values
#[test]
fn merge_curated_prefers_live_values() {
    let curated = [ModelInfo {
        display_name: Some("Curated".to_owned()),
        context_window: Some(100),
        supports_vision: Some(true),
        ..ModelInfo::new("a")
    }];
    let live = ModelInfo {
        display_name: Some("Live".to_owned()),
        context_window: Some(200),
        supports_vision: Some(false),
        ..ModelInfo::new("a")
    };
    assert_eq!(merge_curated(vec![live.clone()], &curated), [live]);
}

/// Python's `or`: an empty name and a window of 0 read as what the endpoint
/// left out.
#[test]
fn merge_curated_reads_an_empty_live_value_as_none() {
    let known = ModelInfo {
        display_name: Some("A".to_owned()),
        context_window: Some(100),
        ..ModelInfo::new("a")
    };
    let live = ModelInfo {
        display_name: Some(String::new()),
        context_window: Some(0),
        ..ModelInfo::new("a")
    };
    assert_eq!(
        merge_curated(vec![live], std::slice::from_ref(&known)),
        [known]
    );
}

// Python: tests/test_providers/test_ai_models.py::test_a_listing_ends_backfilled_and_speech_tagged
#[test]
fn a_listing_ends_backfilled_and_speech_tagged() {
    let curated = [ModelInfo {
        display_name: Some("A".to_owned()),
        capabilities: vec!["chat_tools_refused".to_owned()],
        ..ModelInfo::new("a")
    }];
    let listed = listing(vec![ModelInfo::new("a"), ModelInfo::new("tts-1")], &curated);
    let read: Vec<(Option<&str>, &[String])> = listed
        .iter()
        .map(|model| (model.display_name.as_deref(), &model.capabilities[..]))
        .collect();
    assert_eq!(
        read,
        [(Some("A"), &[][..]), (None, &["speech".to_owned()][..])]
    );
}

// Python: tests/test_providers/test_ai_models.py::test_merge_curated_keeps_a_catalogs_flags_out_of_a_listing
#[test]
fn merge_curated_keeps_a_catalogs_flags_out_of_a_listing() {
    let curated = [ModelInfo {
        capabilities: vec!["chat_tools_refused".to_owned()],
        ..ModelInfo::new("a")
    }];
    let reported = ModelInfo {
        capabilities: vec!["tools".to_owned()],
        ..ModelInfo::new("b")
    };
    let merged = merge_curated(vec![ModelInfo::new("a"), reported.clone()], &curated);
    assert_eq!(merged, [ModelInfo::new("a"), reported]);
}

// Python: tests/test_providers/test_ai_models.py::test_mock_catalog
#[test]
fn mock_catalog() {
    let ids: Vec<String> = MockAiProvider::available_models()
        .into_iter()
        .map(|entry| entry.id)
        .collect();
    assert_eq!(ids, ["mock", "mock-vision"]);
}

/// An entry reads Python's `model_dump(mode="json")` and writes it back,
/// Python's defaults for what it leaves out.
#[test]
fn an_entry_reads_and_writes_python_s_json() {
    let written = json!({
        "id": "m",
        "display_name": null,
        "context_window": 400_000,
        "supports_vision": true,
        "deprecated": false,
        "capabilities": ["tools_reasoning_none"],
        "pricing": {
            "input_per_million": 1.0,
            "output_per_million": 2.0,
            "cache_read_per_million": null,
            "cache_write_per_million": null,
            "image_input_per_million": null,
            "image_output_per_million": null,
            "long_context_threshold_tokens": null,
            "long_context_input_multiplier": 1.0,
            "long_context_output_multiplier": 1.0,
            "currency": "USD",
            "verified": "2026-08-05",
        },
    });
    let entry: ModelInfo = serde_json::from_value(written.clone()).unwrap();
    assert_eq!(serde_json::to_value(&entry).unwrap(), written);
    let sparse: ModelInfo = serde_json::from_value(json!({"id": "m"})).unwrap();
    assert_eq!(sparse, ModelInfo::new("m"));
}
