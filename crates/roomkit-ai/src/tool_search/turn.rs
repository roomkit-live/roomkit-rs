//! A turn's Tool Search (RFC §6.4): what it keeps declared from round to
//! round, and the two tools it serves. Python's reveal window, on its
//! `_ToolLoopContext`, and `_handle_find_tools`, `_handle_list_tools`,
//! `_reveal`, `_settle_served_call`, `_retain_called_tool`,
//! `_declaration_origin`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit::model::{RoomId, ToolCall, ToolCallOutcome};
use serde_json::json;

use super::ToolSearch;
use super::catalogue::{
    normalize_max_results, related_family_tools, render_find_payload, render_list_payload,
    search_catalogue, strip,
};
use super::texts::{TOOL_FIND_TOOLS, TOOL_SEARCH_INFRA_TOOL_NAMES};
use crate::loop_events::ToolDeclarationOrigin;
use crate::skills::dumps;
use crate::tool_usage::ToolUsageMemory;
use crate::tools::{AiTool, BoxFuture, DynToolset, ToolContext, ToolError, ToolResult};

/// A turn's Tool Search, shared by its loop and the tools it serves. A turn that
/// hides nothing has one too: an activation's hint reveals into the room's
/// memory whatever the turn's mode (RFC §24.4).
pub(crate) struct TurnSearch {
    config: Arc<ToolSearch>,
    room: RoomId,
    memory: Arc<ToolUsageMemory>,
    /// Whether the turn hides its catalogue.
    active: bool,
    window: Mutex<RevealWindow>,
}

/// What the turn keeps declared and what it holds until a call settles.
#[derive(Default)]
struct RevealWindow {
    /// What the turn may reach, as the search and the listing read it.
    catalogue: Vec<AiTool>,
    /// What Tool Search never hides: the channel's own tools, a person's, what
    /// orchestration injected, what a hook added.
    never_hidden: HashSet<String>,
    /// What the last search revealed.
    revealed: HashSet<String>,
    /// The tools the room used or found, and those this turn called: they stay
    /// declared whatever a search reveals.
    sticky: HashSet<String>,
    /// The round whose search last swapped the window: searches of one round
    /// add up, a later round's swaps it.
    revealed_in_round: Option<u32>,
    /// The rounds of calls that ended.
    round: u32,
    /// What a `find_tools` call matched or an activation's hint named, until
    /// the call is served.
    pending_reveals: HashMap<String, Vec<String>>,
}

impl TurnSearch {
    /// The turn's search under `config` in `room`, `active` or not, what is
    /// `never` hidden and what stays `sticky` from the room's earlier turns as
    /// it starts.
    pub(super) fn new(
        config: Arc<ToolSearch>,
        (room, memory): (RoomId, Arc<ToolUsageMemory>),
        active: bool,
        (never, sticky): (HashSet<String>, HashSet<String>),
    ) -> Self {
        Self {
            config,
            room,
            memory,
            active,
            window: Mutex::new(RevealWindow {
                never_hidden: never,
                sticky,
                ..RevealWindow::default()
            }),
        }
    }

    fn window(&self) -> MutexGuard<'_, RevealWindow> {
        self.window.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether the turn hides its catalogue.
    pub(crate) fn active(&self) -> bool {
        self.active
    }

    /// Whether `name` is declared this round, were it reachable.
    fn keeps(&self, window: &RevealWindow, name: &str) -> bool {
        self.never_deferred(window, name)
            || window.revealed.contains(name)
            || window.sticky.contains(name)
    }

    /// Whether Tool Search never hides `name`: the keep-set's floor, and what
    /// `find_tools` never names. Python's `_never_deferred`.
    fn never_deferred(&self, window: &RevealWindow, name: &str) -> bool {
        self.config.pinned.contains(name) || window.never_hidden.contains(name)
    }

    /// `reachable`, what the policy lets the turn reach and nothing withdrew,
    /// as this round declares it, which the search reads from now on. Python's
    /// `_apply_tool_filters`.
    pub(crate) fn round_declaration(&self, reachable: Vec<AiTool>) -> Vec<AiTool> {
        if !self.active {
            return reachable;
        }
        self.window().catalogue.clone_from(&reachable);
        self.collapsed(reachable)
    }

    /// `tools` as a round would declare them, nothing read from them.
    pub(crate) fn collapsed(&self, tools: Vec<AiTool>) -> Vec<AiTool> {
        if !self.active {
            return tools;
        }
        let window = self.window();
        tools
            .into_iter()
            .filter(|tool| self.keeps(&window, &tool.name))
            .collect()
    }

    /// Keeps `names`, the tools a generation hook added, declared at every
    /// round, never named by `find_tools` (RFC §6.4).
    pub(crate) fn never_hide(&self, names: impl IntoIterator<Item = String>) {
        self.window().never_hidden.extend(names);
    }

    /// Why `name` is in the declaration, the earliest reason winning (RFC
    /// §6.4). Python's `_declaration_origin`.
    pub(crate) fn origin(&self, name: &str) -> ToolDeclarationOrigin {
        if !self.active {
            return ToolDeclarationOrigin::Always;
        }
        let window = self.window();
        if self.config.pinned.contains(name) {
            ToolDeclarationOrigin::Pinned
        } else if window.never_hidden.contains(name) {
            ToolDeclarationOrigin::Always
        } else if window.sticky.contains(name) {
            ToolDeclarationOrigin::Sticky
        } else if window.revealed.contains(name) {
            ToolDeclarationOrigin::Revealed
        } else {
            ToolDeclarationOrigin::Always
        }
    }

    /// Says a call names a tool the round hid, which the policy admits: a
    /// search applied at call time, a tool a small model saw and called without
    /// searching. Python's `_recover_deferred_tool`: what it holds for later is
    /// `settle`'s here, a tool that answered staying declared either way.
    pub(crate) fn note_recovery(&self, name: &str) {
        if self.active && !self.keeps(&self.window(), name) {
            tracing::info!(target: "roomkit::ai", tool = %name, "recovered a hidden catalogue tool at call time");
        }
    }

    /// Keeps `name` declared for the turn's next rounds, without recording a
    /// call: the tool is reachable, only the model's arguments were refused,
    /// and its next attempt needs the schema (RFC §6.4). Python's
    /// `_retain_called_tool`.
    pub(crate) fn retain(&self, name: &str) {
        let mut window = self.window();
        if self.active && !self.never_deferred(&window, name) {
            window.sticky.insert(name.to_owned());
        }
    }

    /// Holds what the call `call_id` asks to reveal until it is served: an
    /// activation's hint's tools (RFC §24.4). Python's `_defer_reveal`.
    pub(crate) fn reveal_once_served(&self, call_id: &str, names: Vec<String>) {
        self.window()
            .pending_reveals
            .insert(call_id.to_owned(), names);
    }

    /// The call `call_id` to `name`, which ran, ended `outcome`: a served
    /// search reveals its matches, whatever a hook replaced its result with;
    /// a tool that answered, served, failed or withheld, stays declared for
    /// the rest of the turn (RFC §6.4). A call a gate refused, or that nothing
    /// served, leaves everything as it was. Python's `_settle_served_call`,
    /// `_settle_recovery`, `_retain_called_tool`.
    pub(crate) fn settle(&self, call_id: &str, name: &str, outcome: ToolCallOutcome) {
        let mut window = self.window();
        let found = window.pending_reveals.remove(call_id);
        if let Some(found) = found.filter(|_| outcome == ToolCallOutcome::Served) {
            self.reveal(&mut window, found);
        }
        let answered = matches!(
            outcome,
            ToolCallOutcome::Served | ToolCallOutcome::Failed | ToolCallOutcome::Blocked
        );
        if answered && self.active && !self.never_deferred(&window, name) {
            window.sticky.insert(name.to_owned());
        }
    }

    /// Reveals `names` as `find_tools` does: the window swapped, what is never
    /// hidden left out, and for the room's later turns. A tool a served call
    /// used keeps its place, being sticky. Two searches of one round each told
    /// the model its matches come next round, so the second adds to what the
    /// first swapped. Python's `_reveal`.
    fn reveal(&self, window: &mut RevealWindow, names: Vec<String>) {
        let revealed: Vec<String> = names
            .into_iter()
            .filter(|name| !self.never_deferred(window, name))
            .collect();
        if revealed.is_empty() {
            return;
        }
        if window.revealed_in_round != Some(window.round) {
            window.revealed.clear();
            window.revealed_in_round = Some(window.round);
        }
        window.revealed.extend(revealed.iter().cloned());
        self.memory.record_revealed(&self.room, &revealed);
    }

    /// A round of calls ended.
    pub(crate) fn next_round(&self) {
        self.window().round += 1;
    }

    /// What `find_tools` answers. Python's `_handle_find_tools`.
    fn find(&self, call: &ToolCall) -> String {
        let query = call.arguments.get("query").and_then(|query| query.as_str());
        let query = strip(query.unwrap_or_default());
        if query.is_empty() {
            return dumps(&json!({
                "error": "query is required",
                "hint": "Pass a short natural-language description.",
            }));
        }
        let max_results =
            normalize_max_results(call.arguments.get("max_results"), self.config.threshold);
        let mut window = self.window();
        let mut exclude = window.never_hidden.clone();
        exclude.extend(self.config.pinned.iter().cloned());
        let matches = search_catalogue(&window.catalogue, query, max_results, &exclude);
        let related = related_family_tools(&window.catalogue, &matches, &exclude);
        let answer = render_find_payload(&matches, self.config.miss_hint.as_deref(), &related);
        let names: Vec<String> = matches.iter().map(|tool| tool.name.clone()).collect();
        if !names.is_empty() {
            window.pending_reveals.insert(call.id.clone(), names);
        }
        answer
    }

    /// What `list_tools` answers: it reveals nothing. Python's
    /// `_handle_list_tools`.
    fn list(&self, call: &ToolCall) -> String {
        let category = strip(
            call.arguments
                .get("category")
                .and_then(|category| category.as_str())
                .unwrap_or(""),
        );
        let exclude: HashSet<String> = TOOL_SEARCH_INFRA_TOOL_NAMES.map(str::to_owned).into();
        render_list_payload(&self.window().catalogue, category, &exclude)
    }
}

impl std::fmt::Debug for TurnSearch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnSearch")
            .field("room", &self.room)
            .field("active", &self.active)
            .finish_non_exhaustive()
    }
}

impl DynToolset for TurnSearch {
    fn call(
        &self,
        call: ToolCall,
        _context: ToolContext,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        let answer = if call.name == TOOL_FIND_TOOLS {
            self.find(&call)
        } else {
            self.list(&call)
        };
        Box::pin(std::future::ready(Ok(ToolResult::Text(answer))))
    }
}
