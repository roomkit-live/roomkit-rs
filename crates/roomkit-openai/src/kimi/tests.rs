//! Kimi's provider: its catalogue.
//! Python: `tests/test_providers/test_minimax_kimi.py`.

use pretty_assertions::assert_eq;
use roomkit_ai::AiProvider;

use super::*;

// Python: tests/test_providers/test_ai_models.py, the curated catalogues
#[test]
fn the_catalogue_is_python_s() {
    let models = KimiAiProvider::available_models();
    let ids: Vec<&str> = models.iter().map(|model| model.id.as_str()).collect();
    assert_eq!(ids, ["kimi-k3.1-pro", "kimi-k3.1-pro-highspeed", "kimi-k3"]);
    assert!(models.iter().all(|model| model.pricing.is_some()));
}
