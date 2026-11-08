//! Tool Search (RFC §6.4, §21.1): past a threshold, an agent declares a few
//! tools and hides the rest of its catalogue behind `find_tools` and
//! `list_tools`, which it serves itself: a tool `find_tools` names enters the
//! declaration of the next round, and of the room's later turns.
//!
//! - this module: whether a turn hides its catalogue, and what it keeps
//!   declared, Python's `should_activate_tool_search` and the rest of
//!   `_collapse_behind_tool_search`;
//! - `catalogue`: the search and its answers (`_tool_search.py`);
//! - `turn`: the turn's reveal window, and the two tools it serves
//!   (`_ai_tools.py`, `_ai_policy.py`);
//! - `texts`: its words (`_tool_search_constants.py`).
//!
//! A `RealtimeVoiceChannel`'s Tool Search comes with voice.

mod catalogue;
mod texts;
mod turn;

use std::collections::HashSet;
use std::sync::Arc;

use roomkit::model::RoomId;

pub use self::texts::{TOOL_FIND_TOOLS, TOOL_LIST_TOOLS, TOOL_SEARCH_INFRA_TOOL_NAMES};
pub(crate) use self::texts::{TOOL_SEARCH_PREAMBLE, search_tool_defs};
pub(crate) use self::turn::TurnSearch;
use crate::error::AiError;
use crate::memory::estimate_tool_tokens;
use crate::tool_usage::ToolUsageMemory;
use crate::tools::AiTool;
use crate::tools::toolbox::Toolbox;

/// The tool count past which a catalogue is hidden when the model's window is
/// unknown, and the most matches a search returns. Python's
/// `DEFAULT_TOOL_SEARCH_THRESHOLD`.
pub(crate) const DEFAULT_TOOL_SEARCH_THRESHOLD: usize = 20;

/// The share of the model's window, in percent, past which a catalogue is
/// hidden. Python's `DEFAULT_TOOL_SEARCH_THRESHOLD_PCT`.
pub(crate) const DEFAULT_TOOL_SEARCH_THRESHOLD_PCT: f64 = 10.0;

/// The schema tokens past which a catalogue is hidden whatever the window: a
/// large catalogue re-sent at every round costs more than a round of discovery.
/// Python's `DEFAULT_TOOL_SEARCH_THRESHOLD_TOKENS`.
pub(crate) const DEFAULT_TOOL_SEARCH_THRESHOLD_TOKENS: u64 = 8000;

/// What decides a turn's Tool Search: the channel's `tool_search*` options.
#[derive(Clone, Debug)]
pub(crate) struct ToolSearch {
    mode: Option<bool>,
    pinned: HashSet<String>,
    threshold: usize,
    threshold_pct: f64,
    miss_hint: Option<String>,
    threshold_tokens: Option<u64>,
}

impl ToolSearch {
    /// The channel's `tool_search*` options as it holds them, its thresholds
    /// the tool count, the share of the window and the token cap.
    ///
    /// # Errors
    ///
    /// [`AiError::ToolSearchThresholdTokens`] for a token cap of zero, as
    /// Python's `checked_threshold_tokens` raises `ValueError`.
    pub(crate) fn checked(
        mode: Option<bool>,
        pinned: Vec<String>,
        (threshold, threshold_pct, threshold_tokens): (usize, f64, Option<u64>),
        miss_hint: Option<String>,
    ) -> Result<Self, AiError> {
        if threshold_tokens == Some(0) {
            return Err(AiError::ToolSearchThresholdTokens);
        }
        Ok(Self {
            mode,
            pinned: pinned.into_iter().collect(),
            threshold,
            threshold_pct,
            miss_hint,
            threshold_tokens,
        })
    }

    /// Whether the channel may serve the discovery tools, whose names no host
    /// tool may then take (RFC §21.1): unless `tool_search` is `Some(false)`.
    pub(crate) fn reserves_names(&self) -> bool {
        self.mode != Some(false)
    }

    /// The Tool Search of a turn on `toolbox`, on a model of `window` tokens,
    /// in `room`, active when it hides the catalogue. A turn that stands alone
    /// reads none of the room's memory of the tools it used (RFC §10.1.1).
    pub(crate) fn open(
        self: &Arc<Self>,
        toolbox: &Toolbox,
        window: Option<u64>,
        room: &RoomId,
        (memory, stands_alone): (&Arc<ToolUsageMemory>, bool),
    ) -> Arc<TurnSearch> {
        let active = self.hides(&toolbox.deferrable(), window);
        let mut never = toolbox.never_hidden();
        never.extend(TOOL_SEARCH_INFRA_TOOL_NAMES.map(str::to_owned));
        // The re-read of stored results, the agent's own: listed once a round
        // declares it, never named by `find_tools` (Python's
        // `ToolTraits(deferrable=False)` of `read_stored_result`).
        never.extend(toolbox.reread().map(|tool| tool.name.clone()));
        let mut sticky = HashSet::new();
        if active {
            let names = toolbox.names();
            let held = names
                .iter()
                .filter(|name| never.contains(*name) || self.pinned.contains(*name));
            tracing::info!(
                target: "roomkit::ai",
                deferred = names.len(),
                pinned = held.count(),
                window = window
                    .filter(|window| *window != 0)
                    .map_or_else(|| "unknown".to_owned(), |window| window.to_string()),
                "Tool Search active: tools deferred behind find_tools/list_tools"
            );
            if !stands_alone {
                sticky.extend(
                    memory
                        .tool_names(room)
                        .into_iter()
                        .filter(|name| names.contains(name)),
                );
            }
        }
        Arc::new(TurnSearch::new(
            Arc::clone(self),
            (room.clone(), Arc::clone(memory)),
            active,
            (never, sticky),
        ))
    }

    /// Whether a catalogue of `catalogue`, what Tool Search may hide, is hidden
    /// on a model of `window` tokens: forced by `tool_search`, else past the
    /// token cap, else past its share of the window, else, the window unknown
    /// or nothing, past the tool count. A pinned tool counts toward the tool
    /// count alone. Python's `should_activate_tool_search()`.
    #[expect(
        clippy::cast_precision_loss,
        reason = "Python compares an int with a float; token counts stay far under 2^52"
    )]
    fn hides(&self, catalogue: &[&AiTool], window: Option<u64>) -> bool {
        if let Some(forced) = self.mode {
            return forced;
        }
        let tokens: usize = catalogue
            .iter()
            .filter(|tool| !self.pinned.contains(&tool.name))
            .map(|tool| estimate_tool_tokens(tool))
            .sum();
        let tokens = tokens as u64;
        if self.threshold_tokens.is_some_and(|cap| tokens > cap) {
            return true;
        }
        match window.filter(|window| *window != 0) {
            Some(window) => tokens as f64 > window as f64 * self.threshold_pct / 100.0,
            None => catalogue.len() > self.threshold,
        }
    }
}

#[cfg(test)]
mod tests;
