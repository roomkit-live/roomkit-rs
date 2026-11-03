//! OpenAI's model catalogue (RFC §6.7): the Python implementation's
//! `OpenAIAIProvider.available_models()`, copied as JSON into
//! `catalog/models.json` by `make models-sync` from one roomkit commit
//! (`catalog/ROOMKIT_COMMIT`), never edited here; `make models-check` fails
//! when the copy is not that commit's catalogue.

use std::sync::LazyLock;

use roomkit_ai::ModelInfo;

/// The catalogue as Python writes it.
const CATALOG: &str = include_str!("../catalog/models.json");

/// The catalogue, read once. The copy is checked to read whole
/// (`the_catalogue_reads_whole`): a copy that did not would read empty.
static MODELS: LazyLock<Vec<ModelInfo>> = LazyLock::new(|| read().unwrap_or_default());

/// Reads the catalogue's copy.
pub(crate) fn read() -> Result<Vec<ModelInfo>, serde_json::Error> {
    serde_json::from_str(CATALOG)
}

/// Every entry of the catalogue, in Python's order.
pub(crate) fn models() -> &'static [ModelInfo] {
    &MODELS
}

/// The catalogue's entry for `model`, if it carries one.
pub(crate) fn entry(model: &str) -> Option<&'static ModelInfo> {
    MODELS.iter().find(|entry| entry.id == model)
}

#[cfg(test)]
mod tests;
