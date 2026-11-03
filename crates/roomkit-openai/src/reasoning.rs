//! The reasoning effort a request carries, against what OpenAI's catalogue
//! says of its model (RFC §6.7): Python's `providers/ai/reasoning.py`
//! (`reasoning_floor`, `floored_effort`) and the OpenAI provider's
//! `_check_model_serves`.

use roomkit_ai::ModelInfo;

use crate::error::OpenAiError;

/// The catalogue tag naming the least `reasoning_effort` a model takes
/// (`reasoning_floor_none`, `reasoning_floor_minimal`, `reasoning_floor_low`).
const FLOOR_TAG: &str = "reasoning_floor_";

/// A model that takes function tools on Chat Completions only with an
/// effort of `none`.
pub(crate) const TOOLS_REASONING_NONE: &str = "tools_reasoning_none";

/// A model that takes the turn's effort beside function tools on Chat
/// Completions.
pub(crate) const TOOLS_REASONING_EFFORT: &str = "tools_reasoning_effort";

/// Whether the catalogue's `entry` carries `tag`.
pub(crate) fn tagged(entry: Option<&ModelInfo>, tag: &str) -> bool {
    entry.is_some_and(|entry| entry.capabilities.iter().any(|carried| carried == tag))
}

/// The least `reasoning_effort` the model takes, as its catalogue entry
/// declares it: `none` where reasoning can be switched off, else its lowest
/// level; `None` where the entry declares none, or there is no entry
/// (Python's `reasoning_floor`).
pub(crate) fn reasoning_floor(entry: Option<&ModelInfo>) -> Option<&str> {
    entry?
        .capabilities
        .iter()
        .find_map(|tag| tag.strip_prefix(FLOOR_TAG))
}

/// The effort a turn sends a model whose least effort is `floor`: the
/// turn's over the configured one, and `floor` where the turn states off
/// (an effort of `none`), the model's off value or, on one that cannot stop
/// reasoning, its lowest level. Without a floor the effort goes as stated
/// (Python's `floored_effort`).
pub(crate) fn floored_effort(
    turn: Option<&str>,
    configured: Option<&str>,
    floor: Option<&str>,
) -> Option<String> {
    let effort = turn.or(configured);
    match floor {
        Some(floor) if effort == Some("none") => Some(floor.to_owned()),
        _ => effort.map(str::to_owned),
    }
}

/// Refuses, before the request, a Chat Completions turn OpenAI's endpoint
/// refuses `model`, as its catalogue `entry` states it: a model Chat
/// Completions does not serve, or function tools for one that takes none
/// there (Python's `_check_model_serves`). A server behind a base URL
/// decides, which the caller leaves unchecked.
pub(crate) fn check_model_serves(
    model: &str,
    entry: Option<&ModelInfo>,
    tools: bool,
) -> Result<(), OpenAiError> {
    let reason = if tagged(entry, "responses_only") {
        format!("{model} is served by OpenAI's Responses API only, not Chat Completions")
    } else if tools && tagged(entry, "chat_tools_refused") {
        format!("{model} takes no function tools on Chat Completions")
    } else {
        return Ok(());
    };
    Err(OpenAiError::ModelRefused { reason })
}

#[cfg(test)]
mod tests;
