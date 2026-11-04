//! OpenAI's model catalogue (RFC §6.7): the Python implementation's
//! `OpenAIAIProvider.available_models()`, copied as JSON into
//! `catalog/models.json` by `make models-sync` from one roomkit commit
//! (`catalog/ROOMKIT_COMMIT`), never edited here; `make models-check` fails
//! when the copy is not that commit's catalogue.

use std::sync::LazyLock;

use roomkit_ai::ModelInfo;
use serde::Deserialize;

use crate::error::OpenAiError;

/// The catalogue as Python writes it.
const CATALOG: &str = include_str!("../catalog/models.json");

/// The catalogue, read once. An empty one would quietly lift the floors,
/// the tool-turn tags and the model refusals: a copy that does not read
/// is a defect of this crate, which `the_catalogue_reads_whole` and
/// `make models-check` keep from being built.
static MODELS: LazyLock<Vec<ModelInfo>> = LazyLock::new(|| {
    #[expect(
        clippy::expect_used,
        reason = "the embedded copy is checked to read whole by the_catalogue_reads_whole"
    )]
    read().expect("the embedded OpenAI catalogue reads")
});

/// Reads the catalogue's copy.
pub(crate) fn read() -> Result<Vec<ModelInfo>, serde_json::Error> {
    serde_json::from_str(CATALOG)
}

/// What a `GET /models` answer holds: a page of models, each read by its id
/// alone, as Python's `list_models` reads it.
#[derive(Deserialize)]
struct Listing {
    data: Vec<Listed>,
}

/// One model of a [`Listing`].
#[derive(Deserialize)]
struct Listed {
    id: String,
}

/// The models the `body` of a `GET /models` answer lists, nothing known of
/// them but their ids. A body that is no such page is no listing: an error,
/// not an empty one. The error does not repeat it.
pub(crate) fn listed(body: &[u8]) -> Result<Vec<ModelInfo>, OpenAiError> {
    let listing: Listing = serde_json::from_slice(body).map_err(|_| OpenAiError::Malformed {
        reason: "the model listing is no page of models with their ids".to_owned(),
    })?;
    Ok(listing
        .data
        .into_iter()
        .map(|model| ModelInfo::new(model.id))
        .collect())
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
