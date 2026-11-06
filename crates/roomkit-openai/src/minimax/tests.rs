//! MiniMax's provider: its catalogue.
//! Python: `tests/test_providers/test_minimax_kimi.py`.

use pretty_assertions::assert_eq;
use roomkit_ai::AiProvider;

use super::*;

// Python: tests/test_providers/test_ai_models.py, the curated catalogues
// and test_minimax_kimi.py::test_minimax_m3_bills_twice_its_rates_past_512k_input_tokens
#[test]
fn the_catalogue_is_python_s() {
    let models = MiniMaxAiProvider::available_models();
    let ids: Vec<&str> = models.iter().map(|model| model.id.as_str()).collect();
    assert_eq!(ids, ["MiniMax-M3.1-Flash", "MiniMax-M3"]);
    let m3 = models[1].pricing.as_ref().unwrap();
    assert_eq!(m3.long_context_threshold_tokens, Some(512_000));
}
