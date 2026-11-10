//! Python's pinned, offline Anthropic catalogue.
use roomkit_ai::ModelInfo;
use std::sync::LazyLock;

static MODELS: LazyLock<Vec<ModelInfo>> = LazyLock::new(|| {
    #[expect(
        clippy::expect_used,
        reason = "the embedded catalogue is checked by tests and models-check"
    )]
    serde_json::from_str(include_str!("../catalog/models.json"))
        .expect("the pinned catalogue reads")
});

pub(crate) fn all() -> &'static [ModelInfo] {
    &MODELS
}
pub(crate) fn entry(model: &str) -> Option<&'static ModelInfo> {
    MODELS.iter().find(|entry| entry.id == model)
}
