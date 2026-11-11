//! What a turn may spend (RFC §6.4), as the reference implementation's
//! `channels/_turn_budget.py` counts it.
//!
//! A token budget over every token the provider bills for the turn, a cost
//! budget at the model's catalogue price, or both. The tool loop counts
//! each generation's usage as it comes and stops at the first round
//! boundary where the turn has reached either.

use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use roomkit::model::Metadata;
use serde_json::Value;

use crate::error::AiError;
use crate::memory::repr::float_repr;
use crate::provider::{BILLED_USAGE_COUNTERS, DynAiProvider, ModelPricing};

/// A turn's spending cap: billed tokens, a cost in the price's currency,
/// or both (Python's `TurnBudget`).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct TurnBudget {
    /// The most tokens the turn's generations bill.
    pub(crate) tokens: Option<u64>,
    /// The most the turn's generations cost.
    pub(crate) usd: Option<f64>,
    /// The model's catalogue price, which prices each generation.
    pricing: Option<ModelPricing>,
}

impl TurnBudget {
    /// The tokens one generation's `usage` bills: its billed counters
    /// summed, one that is no count read as none (where Python's
    /// `int(...)` would coerce a text of digits or a float: only a
    /// provider's own malformed usage holds one).
    pub(crate) fn tokens_of(usage: &Metadata) -> u64 {
        BILLED_USAGE_COUNTERS
            .iter()
            .filter_map(|counter| usage.get(*counter).and_then(Value::as_u64))
            .fold(0, u64::saturating_add)
    }

    /// What one generation's `usage` costs, priced as one response; 0
    /// without a price.
    ///
    /// # Errors
    ///
    /// [`ModelPricing::cost_for`]'s, for a billed counter that is no
    /// non-negative integer.
    pub(crate) fn cost_of(&self, usage: &Metadata) -> Result<f64, AiError> {
        self.pricing
            .as_ref()
            .map_or(Ok(0.0), |pricing| pricing.cost_for(usage))
    }

    /// Whether a turn that billed `tokens` and cost `cost` so far has
    /// reached it.
    pub(crate) fn reached(&self, tokens: u64, cost: f64) -> bool {
        if self.tokens.is_some_and(|budget| tokens >= budget) {
            return true;
        }
        self.usd.is_some_and(|budget| cost >= budget)
    }
}

/// The budget `tokens` and `usd` set for a turn on `provider`, or `None`
/// when neither is set (Python's `turn_budget`).
///
/// Every generation of the turn is priced at `provider`'s rate, one a router
/// hands to another provider included (RFC §6.4). Without a `provider` (a
/// channel whose router picks it per turn), only the numbers are checked.
///
/// # Errors
///
/// [`AiError::UnpricedModel`] for `usd` set on a model with no known
/// price, whose cost cannot be counted.
pub(crate) fn turn_budget(
    tokens: Option<u64>,
    usd: Option<f64>,
    provider: Option<&dyn DynAiProvider>,
) -> Result<Option<TurnBudget>, AiError> {
    if tokens.is_none() && usd.is_none() {
        return Ok(None);
    }
    let Some(provider) = provider else {
        return Ok(Some(TurnBudget {
            tokens,
            usd,
            pricing: None,
        }));
    };
    let pricing = pricing(provider);
    if usd.is_some() && pricing.is_none() {
        return Err(AiError::UnpricedModel {
            model: provider.dyn_model_name().to_owned(),
        });
    }
    Ok(Some(TurnBudget {
        tokens,
        usd,
        pricing,
    }))
}

/// Refuses a token budget of 0, then a cost budget not above 0 (`NaN`
/// too, infinity not), as Python's `turn_budget` checks each budget
/// positive, a turn's configuration's included.
///
/// # Errors
///
/// [`AiError::TurnBudgetTokens`], then [`AiError::TurnBudgetUsd`] naming
/// the budget.
pub(crate) fn refuse_unpositive(tokens: Option<u64>, usd: Option<f64>) -> Result<(), AiError> {
    if tokens == Some(0) {
        return Err(AiError::TurnBudgetTokens);
    }
    let refused = usd.filter(|budget| budget.partial_cmp(&0.0) != Some(Ordering::Greater));
    if let Some(budget) = refused {
        return Err(AiError::TurnBudgetUsd {
            shown: float_repr(budget),
        });
    }
    Ok(())
}

/// Refuses the budgets a channel is built with as Python's constructor
/// does: [`refuse_unpositive`]'s, then one on a model with no price, when
/// the channel has a provider of its own; with a router, the price is
/// checked per turn, on the model it picks.
///
/// # Errors
///
/// [`refuse_unpositive`]'s, then [`turn_budget`]'s.
pub(crate) fn check_budgets(
    tokens: Option<u64>,
    usd: Option<f64>,
    provider: Option<&dyn DynAiProvider>,
) -> Result<(), AiError> {
    refuse_unpositive(tokens, usd)?;
    turn_budget(tokens, usd, provider).map(|_| ())
}

/// `provider`'s catalogue price, if it has one.
fn pricing(provider: &dyn DynAiProvider) -> Option<ModelPricing> {
    provider.dyn_catalog_entry().and_then(|entry| entry.pricing)
}

/// Logs once per model that a cost `budget`, priced at the rate of the
/// model its turn started on, prices `other`'s generations at that rate when
/// `other` is priced otherwise; whether it logged (Python's
/// `warn_if_priced_otherwise`).
pub(crate) fn warn_if_priced_otherwise(
    budget: Option<&TurnBudget>,
    other: &dyn DynAiProvider,
) -> bool {
    static WARNED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let Some(budget) = budget.filter(|budget| budget.usd.is_some()) else {
        return false;
    };
    if pricing(other) == budget.pricing {
        return false;
    }
    let model = other.dyn_model_name().to_owned();
    let first = WARNED
        .get_or_init(Mutex::default)
        .lock()
        .is_ok_and(|mut warned| warned.insert(model.clone()));
    if first {
        tracing::warn!(
            target: "roomkit::ai",
            model = %model,
            "turn_budget_usd prices every generation of a turn at the rate of the model it \
             started on; the model the router handed a generation is priced otherwise"
        );
    }
    first
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;
    use crate::provider::{AiContext, AiProvider, AiResponse, Date, ModelInfo, ProviderError};

    /// A model named `model`, with `pricing` as its catalogue price.
    struct Priced {
        model: &'static str,
        pricing: Option<ModelPricing>,
    }

    impl AiProvider for Priced {
        fn model_name(&self) -> &str {
            self.model
        }

        fn catalog_entry(&self) -> Option<ModelInfo> {
            Some(ModelInfo {
                pricing: self.pricing.clone(),
                ..ModelInfo::new(self.model)
            })
        }

        fn generate(
            &self,
            _context: &AiContext,
        ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send {
            std::future::ready(Ok(AiResponse::new("")))
        }
    }

    fn price(input_per_million: f64) -> ModelPricing {
        ModelPricing::new(input_per_million, 50.0, Date::new(2026, 9, 30).unwrap()).unwrap()
    }

    fn usage(value: &Value) -> Metadata {
        value.as_object().cloned().unwrap_or_default()
    }

    // Python: tests/test_channels/test_turn_budget.py::test_a_generation_handed_to_a_model_priced_otherwise_is_logged_once
    #[test]
    fn a_generation_handed_to_a_model_priced_otherwise_is_logged_once() {
        let primary = Priced {
            model: "priced-primary",
            pricing: Some(price(10.0)),
        };
        let budget = turn_budget(None, Some(1.0), Some(&primary)).unwrap();
        let otherwise = Priced {
            model: "handed-priced-otherwise",
            pricing: Some(price(20.0)),
        };
        let alike = Priced {
            model: "handed-priced-alike",
            pricing: Some(price(10.0)),
        };
        assert!(warn_if_priced_otherwise(budget.as_ref(), &otherwise));
        assert!(!warn_if_priced_otherwise(budget.as_ref(), &otherwise));
        assert!(!warn_if_priced_otherwise(budget.as_ref(), &alike));
    }

    // Python: channels/_turn_budget.py TurnBudget.reached, either cap.
    #[test]
    fn a_budget_is_reached_at_either_cap() {
        let budget = TurnBudget {
            tokens: Some(1200),
            usd: Some(0.02),
            pricing: Some(price(10.0)),
        };
        assert_eq!(
            (
                budget.reached(1199, 0.0199),
                budget.reached(1200, 0.0),
                budget.reached(0, 0.02)
            ),
            (false, true, true)
        );
        let cost = budget
            .cost_of(&usage(&json!({"input_tokens": 600})))
            .unwrap();
        assert!((cost - 0.006).abs() < 1e-12, "{cost}");
    }

    #[test]
    fn a_token_budget_sums_every_billed_counter() {
        let used = usage(&json!({
            "input_tokens": 1, "output_tokens": 2, "cache_read_input_tokens": 4,
            "cache_creation_input_tokens": 8, "input_image_tokens": 16,
            "output_image_tokens": 32, "reasoning_tokens": 64,
        }));
        assert_eq!(TurnBudget::tokens_of(&used), 63);
    }
}
