//! The host policy for continuing an answer that did not act (RFC §6.4).

use std::sync::Arc;

use crate::ProviderError;

/// Python's `ContinuationPolicy`: the exact final text of a natural stop in,
/// an instruction to continue or `None` out. An empty instruction still asks
/// for another try. Clones share the callback and its state; returned text is
/// owned. Errors preserve their source and fail the turn without provider retry.
pub type ContinuationPolicy =
    Arc<dyn Fn(&str) -> Result<Option<String>, ProviderError> + Send + Sync>;
