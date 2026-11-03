//! Translated from Python's `tests/test_providers/test_ai_models.py`, its
//! OpenAI cases: the catalogue the provider embeds.

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use pretty_assertions::{assert_eq, assert_ne};
use roomkit_ai::{AiProvider, Date};

use super::*;
use crate::{OpenAiAiProvider, OpenAiConfig};

/// The copy reads whole and writes back as Python wrote it: nothing of
/// Python's entries is lost or added in the reading.
#[test]
fn the_catalogue_reads_whole() {
    let models = read().unwrap();
    let python: serde_json::Value = serde_json::from_str(CATALOG).unwrap();
    assert_eq!(serde_json::to_value(&models).unwrap(), python);
    assert_eq!(models.len(), self::models().len());
}

// Python: tests/test_providers/test_ai_models.py::test_curated_catalog_is_nonempty_and_unique
#[test]
fn curated_catalog_is_nonempty_and_unique() {
    let models = OpenAiAiProvider::available_models();
    assert_ne!(models, Vec::new());
    let ids: BTreeSet<&str> = models.iter().map(|model| model.id.as_str()).collect();
    assert_eq!(ids.len(), models.len(), "duplicate model ids");
}

// Python: tests/test_providers/test_ai_models.py::test_every_model_in_a_priced_catalog_carries_a_price
#[test]
fn every_model_in_a_priced_catalog_carries_a_price() {
    for model in models().iter().filter(|model| !model.deprecated) {
        assert!(model.pricing.is_some(), "{} has no price", model.id);
    }
}

// Python: tests/test_providers/test_ai_models.py::test_catalog_prices_are_well_formed
#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "the test compares the catalogue's dates with today"
)]
fn catalog_prices_are_well_formed() {
    let days = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs()
        / 86_400;
    let today = civil_from_days(days);
    for model in models() {
        let Some(pricing) = &model.pricing else {
            continue;
        };
        assert!(pricing.input_per_million > 0.0, "{} input rate", model.id);
        assert!(pricing.output_per_million > 0.0, "{} output rate", model.id);
        assert!(!pricing.currency.is_empty(), "{} currency", model.id);
        assert!(
            pricing.verified <= today,
            "{} verified in the future",
            model.id
        );
        for rate in [
            pricing.cache_read_per_million,
            pricing.cache_write_per_million,
        ] {
            assert!(
                rate.is_none_or(|rate| rate >= 0.0),
                "{} cache rate",
                model.id
            );
        }
    }
}

/// The day `days` after the Unix epoch (Howard Hinnant's `civil_from_days`).
fn civil_from_days(days: u64) -> Date {
    let z = i64::try_from(days).unwrap() + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    Date::new(
        u16::try_from(year).unwrap(),
        u8::try_from(month).unwrap(),
        u8::try_from(day).unwrap(),
    )
    .unwrap()
}

// Python: tests/test_providers/test_ai_models.py::test_openai_gpt_5_6_luna_has_its_smaller_untiered_context
#[test]
fn openai_gpt_5_6_luna_has_its_smaller_untiered_context() {
    let luna = entry("gpt-5.6-luna").unwrap();
    assert_eq!(luna.context_window, Some(400_000));
    let pricing = luna.pricing.as_ref().unwrap();
    assert_eq!(pricing.long_context_threshold_tokens, None);
}

/// The provider reads its configured model's entry, and none for a model
/// the catalogue does not carry (Python's `catalog_entry()` and
/// `context_window`).
#[test]
fn the_provider_reads_its_model_s_entry() {
    let provider = OpenAiAiProvider::new(OpenAiConfig::new("sk-test", "gpt-6-luna")).unwrap();
    let entry = provider.catalog_entry().unwrap();
    assert_eq!(entry.id, "gpt-6-luna");
    assert_eq!(provider.context_window(), entry.context_window);
    let local = OpenAiAiProvider::new(OpenAiConfig::new("", "Qwen/Qwen3-8B")).unwrap();
    assert_eq!(local.catalog_entry(), None);
    assert_eq!(local.context_window(), None);
}
