//! Large tool results stored for re-reading (RFC §21.5): Python's
//! `channels/_tool_eviction.py`.
//!
//! An outcome of a call past the agent's threshold reaches the model as a
//! placeholder previewing its first and last lines; its whole text is
//! stored in the room, which `read_stored_result` pages through or
//! searches. The store lives on the channel, shared by every room it
//! serves, and is scoped per room: one conversation's tool output never
//! reads in another.
//!
//! - this module: the store, its bounds and its ids;
//! - `preview`: the placeholder's preview;
//! - `stored_read`: a page of a stored result, or a search in it;
//! - `text`: lengths and lines as Python's `str` counts them.

mod preview;
mod stored_read;
mod text;

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use roomkit::model::{Metadata, ToolCall};
use serde_json::{Value, json};

use self::preview::preview;
use self::stored_read::{REREAD_DESCRIPTION, page, reread_parameters, search};
pub(crate) use self::text::char_len;
use crate::dumps;
use crate::memory::estimate_tokens;
use crate::provider::AiTextPart;
use crate::skills::ACTIVATE_SKILL;
use crate::tools::{
    AiTool, BoxFuture, DynToolset, ToolContext, ToolError, ToolResult, ToolResultPart,
};

/// The estimated tokens past which an outcome of a call is stored and
/// previewed: Python's `evict_threshold_tokens` default.
pub(crate) const DEFAULT_EVICT_THRESHOLD_TOKENS: u64 = 5000;

/// The tool that pages a stored result back.
pub(crate) const REREAD_TOOL: &str = "read_stored_result";

/// How every placeholder starts: one is never stored again.
const PLACEHOLDER_PREFIX: &str = "Result too large (";

/// The stored results a room keeps, its least recently read going first.
const MAX_EVICTED: usize = 50;
/// The results the whole store keeps, which take from the room holding the
/// most first: a room holding few keeps them while others evict.
const MAX_EVICTED_TOTAL: usize = 200;
/// The characters the whole store keeps, taken as its results are.
const MAX_EVICTED_CHARS: usize = 64 * 1024 * 1024;
/// The ids the store handed out that it remembers, well past what any
/// context can still name: a released id is not given to another result
/// while it is among the last this many issued.
const ISSUED_IDS_REMEMBERED: usize = 10_000;
/// The characters reserved in a page for its JSON envelope and a margin, so
/// that worst-case escaping still leaves the page under the threshold.
const PAGE_ENVELOPE_CHARS: usize = 512;
/// The ceiling of a placeholder's preview, in characters.
const PREVIEW_MAX_CHARS: usize = 8000;
/// The lines a page reads when the call names no limit.
const DEFAULT_PAGE_LINES: usize = 800;

/// Whether the model reads `tool_name`'s result whole, never as a preview:
/// a skill's instructions are binding rules the model must hold whole (RFC
/// §24.4). Every other result is data. Python's `kept_whole`.
pub(crate) fn kept_whole(tool_name: &str) -> bool {
    tool_name == ACTIVATE_SKILL
}

/// Whether `text` is the placeholder of a stored result: Python's
/// `is_eviction_placeholder`.
pub(crate) fn is_placeholder(text: &str) -> bool {
    text.starts_with(PLACEHOLDER_PREFIX)
}

/// A stored result's key: its room, then its id.
type Key = (String, String);

/// The stored results of every room the channel serves, and the ids handed
/// out: Python's `ToolEviction`.
#[derive(Debug)]
pub(crate) struct ToolEviction {
    threshold_tokens: usize,
    store: Mutex<Store>,
    /// Numbers the ids of results stored for a call with no id.
    anonymous: AtomicU64,
}

#[derive(Debug, Default)]
struct Store {
    /// The stored results, the least recently stored or read first, each
    /// with its characters.
    results: Vec<(Key, Arc<str>, usize)>,
    /// Their characters.
    chars: usize,
    /// The ids handed out, the oldest first.
    issued: VecDeque<Key>,
    issued_set: HashSet<Key>,
}

impl ToolEviction {
    /// The store of a channel whose results past `threshold_tokens`
    /// estimated tokens are stored.
    pub(crate) fn new(threshold_tokens: u64) -> Self {
        Self {
            threshold_tokens: usize::try_from(threshold_tokens).unwrap_or(usize::MAX),
            store: Mutex::default(),
            anonymous: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether `room` stores a result: Python's `has_evicted`.
    pub(crate) fn has_evicted(&self, room: &str) -> bool {
        self.lock().results.iter().any(|((at, _), _, _)| at == room)
    }

    /// Discards this room's results on detach or attach (RFC §10.6.2).
    /// Issued ids stay taken: an old placeholder must not read a new result.
    pub(crate) fn forget(&self, room: &str) {
        let mut store = self.lock();
        store.results.retain(|((at, _), _, _)| at != room);
        store.chars = store.results.iter().map(|(_, _, chars)| chars).sum();
    }

    /// `result`, the model's copy of an outcome of a call in `room`,
    /// bounded: a text or the text of its parts stored and previewed when
    /// past the threshold, its images kept (Python's
    /// `_maybe_truncate_result`).
    pub(crate) fn bounded(&self, room: &str, result: ToolResult, call_id: &str) -> ToolResult {
        match result {
            ToolResult::Text(text) => ToolResult::Text(self.maybe_evict(room, text, call_id)),
            ToolResult::Parts(parts) => {
                ToolResult::Parts(self.maybe_evict_parts(room, parts, call_id))
            }
            ToolResult::Json(_) => result,
        }
    }

    /// `result` as it is when within the threshold or already a
    /// placeholder, else stored and previewed: Python's `maybe_evict`. A
    /// placeholder evicted again would overwrite the text it points to.
    fn maybe_evict(&self, room: &str, result: String, call_id: &str) -> String {
        if estimate_tokens(&result) <= self.threshold_tokens || is_placeholder(&result) {
            return result;
        }
        self.evict(room, &result, call_id, None)
    }

    /// Stores `result` in `room` for re-reading and returns the
    /// placeholder previewing it in `preview_chars` (the store's own budget
    /// when `None` or zero), whatever its size: Python's `evict`.
    pub(crate) fn evict(
        &self,
        room: &str,
        result: &str,
        call_id: &str,
        preview_chars: Option<usize>,
    ) -> String {
        let base = if call_id.is_empty() {
            let number = self.anonymous.fetch_add(1, Ordering::Relaxed);
            format!("evicted_{number}")
        } else {
            format!("evicted_{call_id}")
        };
        let (stored, chars): (Arc<str>, usize) = (Arc::from(result), char_len(result));
        let result_id = {
            let mut store = self.lock();
            let result_id = store.free_id(room, &base);
            store.chars += chars;
            let key = (room.to_owned(), result_id.clone());
            store.results.push((key, stored, chars));
            store.bound(room);
            result_id
        };
        let budget = preview_chars
            .filter(|chars| *chars > 0)
            .unwrap_or_else(|| self.preview_budget());
        format!(
            "{PLACEHOLDER_PREFIX}{} tokens). Full output saved as '{result_id}'. Use \
             {REREAD_TOOL} to read it with pagination.\n\nPreview:\n{}",
            estimate_tokens(result),
            preview(result, budget)
        )
    }

    /// `parts` as they are when their text, joined, is within the threshold
    /// or holds a placeholder, else with their text stored: Python's
    /// `maybe_evict_parts`. The text parts are measured joined, so that
    /// many medium parts cannot add up past the threshold unseen.
    fn maybe_evict_parts(
        &self,
        room: &str,
        parts: Vec<ToolResultPart>,
        call_id: &str,
    ) -> Vec<ToolResultPart> {
        let texts = texts_of(&parts);
        if estimate_tokens(&texts.join("\n")) <= self.threshold_tokens
            || texts.iter().any(|text| is_placeholder(text))
        {
            return parts;
        }
        self.evict_parts(room, parts, call_id, None)
    }

    /// `parts` with their text stored as one result and replaced by its
    /// placeholder where the first text part was, their images kept:
    /// Python's `evict_parts`.
    pub(crate) fn evict_parts(
        &self,
        room: &str,
        parts: Vec<ToolResultPart>,
        call_id: &str,
        preview_chars: Option<usize>,
    ) -> Vec<ToolResultPart> {
        let texts = texts_of(&parts);
        if texts.is_empty() {
            return parts;
        }
        let stored = self.evict(room, &texts.join("\n"), call_id, preview_chars);
        let mut placeholder = Some(ToolResultPart::Text(AiTextPart::new(stored)));
        parts
            .into_iter()
            .filter_map(|part| match part {
                ToolResultPart::Text(_) => placeholder.take(),
                ToolResultPart::Image(_) => Some(part),
            })
            .collect()
    }

    /// The characters a preview may use: the ceiling, or half of what is
    /// stored (the estimator reads four characters a token) when the
    /// threshold is small.
    fn preview_budget(&self) -> usize {
        PREVIEW_MAX_CHARS.min(self.threshold_tokens.saturating_mul(2))
    }

    /// The characters a page, or a search's matches, may carry: under the
    /// threshold however its content is escaped, so that a page is never
    /// stored again and the model never chases stored pages.
    fn page_budget(&self) -> usize {
        self.threshold_tokens
            .saturating_mul(2)
            .saturating_sub(PAGE_ENVELOPE_CHARS)
            .max(1)
    }

    /// `read_stored_result` in `room` with `arguments`: a page of the
    /// stored result, or its lines holding the call's `query`; a result the
    /// room does not store is refused, its words naming the ids it holds.
    /// Python's `handle_read`.
    fn read(&self, room: &str, arguments: &Metadata) -> Result<String, String> {
        let result_id = arguments
            .get("result_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let offset = count(arguments.get("offset")).unwrap_or(0);
        let limit = count(arguments.get("limit")).unwrap_or(DEFAULT_PAGE_LINES);
        let stored = {
            let mut store = self.lock();
            let Some(at) = store.position(room, result_id) else {
                let available: Vec<&str> = store
                    .results
                    .iter()
                    .filter(|((at, _), _, _)| at == room)
                    .map(|((_, id), _, _)| id.as_str())
                    .collect();
                let error = format!("Result '{result_id}' not found");
                return Err(dumps(&json!({"error": error, "available": available})));
            };
            // Read back, it is in use: the room's least recently read go first.
            let entry = store.results.remove(at);
            let stored = Arc::clone(&entry.1);
            store.results.push(entry);
            stored
        };
        // A blank query, an optional field a model filled in, reads a page.
        let query = match arguments.get("query") {
            Some(Value::String(query)) => query.clone(),
            None | Some(Value::Null | Value::Bool(false)) => String::new(),
            Some(other) => other.to_string(),
        };
        if query.trim().is_empty() {
            return Ok(page(&stored, offset, limit, self.page_budget()));
        }
        Ok(search(&stored, &query, offset, self.page_budget()))
    }

    /// The re-read tool's definition: Python's `tool_definition`.
    pub(crate) fn tool_definition() -> AiTool {
        AiTool::own(REREAD_TOOL, REREAD_DESCRIPTION, reread_parameters())
    }
}

/// A count a call's arguments give, as a page reads it.
fn count(value: Option<&Value>) -> Option<usize> {
    value
        .and_then(Value::as_u64)
        .map(|count| usize::try_from(count).unwrap_or(usize::MAX))
}

/// The texts of `parts`, in order.
pub(crate) fn texts_of(parts: &[ToolResultPart]) -> Vec<&str> {
    parts
        .iter()
        .filter_map(|part| match part {
            ToolResultPart::Text(text) => Some(text.text.as_str()),
            ToolResultPart::Image(_) => None,
        })
        .collect()
}

impl Store {
    fn position(&self, room: &str, result_id: &str) -> Option<usize> {
        self.results
            .iter()
            .position(|((at, id), _, _)| at == room && id == result_id)
    }

    /// `base`, numbered when the room was already given it: an id is not
    /// given again while it is among the last ids issued, even once its
    /// result left the store, so that a placeholder still in a context
    /// never reads another call's data. A call id reused in a later turn
    /// gets `base_2`. Python's `_free_id`.
    fn free_id(&mut self, room: &str, base: &str) -> String {
        let mut result_id = base.to_owned();
        let mut number = 1;
        loop {
            let key = (room.to_owned(), result_id.clone());
            if !self.issued_set.contains(&key) && self.position(room, &result_id).is_none() {
                break;
            }
            number += 1;
            result_id = format!("{base}_{number}");
        }
        let key = (room.to_owned(), result_id.clone());
        self.issued_set.insert(key.clone());
        self.issued.push_back(key);
        while self.issued.len() > ISSUED_IDS_REMEMBERED {
            if let Some(oldest) = self.issued.pop_front() {
                self.issued_set.remove(&oldest);
            }
        }
        result_id
    }

    /// Keeps [`MAX_EVICTED`] results in `room`, then the store's bounds:
    /// over one, the room holding the most gives up its least recently read
    /// result first, so that a room holding few keeps them. The newest
    /// result always stays, however large. Python's `_bound`.
    fn bound(&mut self, room: &str) {
        let in_room = self
            .results
            .iter()
            .filter(|((at, _), _, _)| at == room)
            .count();
        for _ in MAX_EVICTED..in_room {
            if let Some(at) = self.results.iter().position(|((at, _), _, _)| at == room) {
                self.drop_at(at);
            }
        }
        while self.results.len() > 1
            && (self.results.len() > MAX_EVICTED_TOTAL || self.chars > MAX_EVICTED_CHARS)
        {
            let at = self.oldest_of_fullest_room();
            self.drop_at(at);
        }
    }

    /// Where the least recently read result of the room holding the most
    /// is, the first of them in store order on a tie.
    fn oldest_of_fullest_room(&self) -> usize {
        let mut counts: Vec<(&str, usize)> = Vec::new();
        for ((room, _), _, _) in &self.results {
            match counts.iter_mut().find(|(known, _)| known == room) {
                Some((_, count)) => *count += 1,
                None => counts.push((room, 1)),
            }
        }
        // Python's `max` keeps the first of the largest counts.
        let fullest = counts
            .iter()
            .fold(None::<(&str, usize)>, |best, &(room, count)| match best {
                Some((_, most)) if most >= count => best,
                _ => Some((room, count)),
            })
            .map(|(room, _)| room.to_owned())
            .unwrap_or_default();
        self.results
            .iter()
            .position(|((room, _), _, _)| *room == fullest)
            .unwrap_or(0)
    }

    fn drop_at(&mut self, at: usize) {
        let (_, _, chars) = self.results.remove(at);
        self.chars -= chars;
    }
}

/// The re-read tool as the channel serves it: Python's
/// `_handle_read_tool_result`.
pub(crate) struct Reread(pub(crate) Arc<ToolEviction>);

impl DynToolset for Reread {
    fn call(
        &self,
        call: ToolCall,
        context: ToolContext,
    ) -> BoxFuture<'_, Result<ToolResult, ToolError>> {
        // A refusal, not a result: the room's tool memory never keeps it as
        // the answer to the read.
        let read = self.0.read(context.room_id.as_str(), &call.arguments);
        Box::pin(async move { read.map(ToolResult::Text).map_err(ToolError::Refused) })
    }
}

#[cfg(test)]
mod tests;
