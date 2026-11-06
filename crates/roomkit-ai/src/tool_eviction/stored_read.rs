//! Reading a stored tool result back: a page of it, or a search in it (RFC
//! §21.5). Python's `channels/_stored_read.py`.
//!
//! A result too large for the model's context is stored and previewed
//! ([`ToolEviction`](super::ToolEviction)); what the model reads of it
//! afterwards comes from here, always bounded to a budget of characters so
//! that the answer is never stored again. Every length and index counts
//! characters, as Python's `str` does.

use roomkit_core::rules::casefold;
use serde_json::{Map, Value, json};

use super::text::{char_len, char_slice, splitlines};
use crate::dumps;
use crate::skills::dumps_unescaped;

/// The lines shown around each line a search finds.
const SEARCH_CONTEXT_LINES: usize = 2;

/// The JSON page of `full_result` from line `offset`, at most `limit` lines
/// and bounded to `budget` characters: Python's `page`.
pub(super) fn page(full_result: &str, offset: usize, limit: usize, budget: usize) -> String {
    let lines = paginable_lines(full_result, budget);
    let total_lines = lines.len();
    let mut shown: Vec<&str> = Vec::new();
    let mut used = 0;
    for line in lines.iter().skip(offset).take(limit) {
        let length = char_len(line);
        if !shown.is_empty() && used + length > budget {
            break;
        }
        shown.push(line);
        used += length + 1;
    }
    let next = offset.saturating_add(shown.len());
    let has_more = next < total_lines;
    let mut envelope = Map::new();
    envelope.insert("content".to_owned(), Value::from(shown.join("\n")));
    envelope.insert("offset".to_owned(), Value::from(offset));
    envelope.insert("lines_returned".to_owned(), Value::from(shown.len()));
    envelope.insert("total_lines".to_owned(), Value::from(total_lines));
    envelope.insert("has_more".to_owned(), Value::Bool(has_more));
    envelope.insert(
        "next_offset".to_owned(),
        if has_more {
            Value::from(next)
        } else {
            Value::Null
        },
    );
    if has_more {
        // Small models read `content` and skip the pagination fields, then
        // assert "X is not in the result" off one page: the partiality is
        // made impossible to miss.
        envelope.insert(
            "warning".to_owned(),
            Value::from(format!(
                "PARTIAL CONTENT \u{2014} lines {}-{next} of {total_lines}. Never conclude that \
                 something is absent or that this is the complete result until you have read \
                 EVERY page; continue with offset={next}.",
                offset.saturating_add(1),
            )),
        );
    }
    // Text as it is: an escaped \uXXXX is six characters, and a page of
    // non-ASCII text would come back past the threshold and be stored again.
    dumps_unescaped(&Value::Object(envelope))
}

/// The JSON matches of `query` in `full_result`, case aside, from its
/// `offset`-th match on, bounded as a page is: Python's `search`.
///
/// `query` is one line, matched as written and never as a pattern, within
/// each line of the result as stored (RFC §21.5): a search that finds
/// nothing then says the text is absent from the whole result.
pub(super) fn search(full_result: &str, query: &str, offset: usize, budget: usize) -> String {
    if query.contains(['\n', '\r']) {
        return dumps(
            &json!({"error": "query must be one line: a search finds text within a line"}),
        );
    }
    let lines = splitlines(full_result);
    let needle = casefold(query);
    let hits: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter(|(_, line)| casefold(line).contains(&needle))
        .map(|(at, _)| at)
        .collect();
    let room = to_signed(budget) - to_signed(char_len(query));
    let unread = hits.get(offset..).unwrap_or_default();
    let (shown, content) = numbered_windows(&lines, unread, &needle, room);
    let next = offset.saturating_add(shown);
    let has_more = next < hits.len();
    let mut envelope = Map::new();
    envelope.insert("query".to_owned(), Value::from(query));
    envelope.insert("content".to_owned(), Value::from(content));
    envelope.insert("total_matches".to_owned(), Value::from(hits.len()));
    envelope.insert("matches_returned".to_owned(), Value::from(shown));
    envelope.insert("has_more".to_owned(), Value::Bool(has_more));
    envelope.insert(
        "next_offset".to_owned(),
        if has_more {
            Value::from(next)
        } else {
            Value::Null
        },
    );
    if let Some(note) = search_note(hits.len(), offset, shown) {
        let key = if has_more { "warning" } else { "note" };
        envelope.insert(key.to_owned(), Value::from(note));
    }
    dumps_unescaped(&Value::Object(envelope))
}

/// What the model must not miss about a search's answer, if anything:
/// Python's `_search_note`.
fn search_note(total: usize, offset: usize, shown: usize) -> Option<String> {
    if total == 0 {
        return Some(
            "No line contains it: the search covered the whole result, so it is absent.".to_owned(),
        );
    }
    if offset >= total {
        return Some(format!(
            "offset {offset} is past the last match: a search's offset counts matches \
             (total_matches={total}), not lines."
        ));
    }
    let next = offset + shown;
    (next < total).then(|| {
        format!(
            "PARTIAL CONTENT \u{2014} matches {}-{next} of {total}. Continue with \
             offset={next} to see the others.",
            offset + 1
        )
    })
}

/// The lines of `text`, those longer than `budget` split into chunks of
/// `budget` characters so that every line fits within one page: Python's
/// `paginable_lines`.
fn paginable_lines(text: &str, budget: usize) -> Vec<String> {
    let budget = budget.max(1);
    let mut lines = Vec::new();
    for line in splitlines(text) {
        let chars: Vec<char> = line.chars().collect();
        if chars.len() <= budget {
            lines.push(line.to_owned());
        } else {
            lines.extend(chars.chunks(budget).map(|chunk| chunk.iter().collect()));
        }
    }
    lines
}

/// How many of `hits` fit in `budget` characters, and their lines with
/// [`SEARCH_CONTEXT_LINES`] around each, numbered from 1: windows that
/// overlap are merged, a gap between two is marked `...`, and a line
/// longer than its share of the budget is cut around `needle`. Python's
/// `_numbered_windows`, whose budget may be below zero.
fn numbered_windows(lines: &[&str], hits: &[usize], needle: &str, budget: i64) -> (usize, String) {
    let window_lines = to_signed(2 * SEARCH_CONTEXT_LINES + 1);
    let share = usize::try_from((budget.div_euclid(window_lines) - 16).max(80)).unwrap_or(80);
    let (mut shown, mut used) = (0, 0_i64);
    // The last line shown, plus one: where a window not merged starts.
    let mut next_unshown = 0;
    let mut out: Vec<String> = Vec::new();
    for &hit in hits {
        let start = hit.saturating_sub(SEARCH_CONTEXT_LINES).max(next_unshown);
        let stop = (hit + SEARCH_CONTEXT_LINES + 1).min(lines.len());
        let mut block: Vec<String> = (start..stop)
            .filter_map(|at| {
                let line = lines.get(at)?;
                Some(format!("{}: {}", at + 1, excerpt(line, needle, share)))
            })
            .collect();
        if !out.is_empty() && start < stop && start > next_unshown {
            block.insert(0, "...".to_owned());
        }
        let size: i64 = block.iter().map(|line| to_signed(char_len(line)) + 1).sum();
        if used + size > budget {
            break;
        }
        out.extend(block);
        used += size;
        shown += 1;
        next_unshown = next_unshown.max(stop);
    }
    (shown, out.join("\n"))
}

/// `line`, or `share` characters of it around `needle` (its head when it
/// holds none), a cut end marked `[…]`: Python's `_excerpt`, which finds
/// the needle in the folded line and cuts the line as written there.
fn excerpt(line: &str, needle: &str, share: usize) -> String {
    let length = char_len(line);
    if length <= share {
        return line.to_owned();
    }
    let folded = casefold(line);
    let start = folded.find(needle).map_or(0, |at| {
        let at = to_signed(folded.get(..at).map_or(0, char_len));
        let start = (at - to_signed(share / 2)).min(to_signed(length - share));
        usize::try_from(start.max(0)).unwrap_or(0)
    });
    let head = if start > 0 { "[\u{2026}]" } else { "" };
    let tail = if start + share < length {
        "[\u{2026}]"
    } else {
        ""
    };
    format!("{head}{}{tail}", char_slice(line, start, start + share))
}

/// `count` as a signed number, for the arithmetic Python does below zero.
fn to_signed(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

/// The read-back tool's description, as the model reads it (RFC §21.5).
pub(super) const REREAD_DESCRIPTION: &str = "Read a previously evicted large tool result. \
Supports line-based pagination via offset and limit; pages are size-bounded, so follow \
next_offset until has_more is false to read everything. To find something in it, pass query \
(one line of text): you get the lines that contain it with their neighbours and line numbers, \
searched across the whole result, so no match means it is not there.";

/// The read-back tool's parameters, as the model reads them.
pub(super) fn reread_parameters() -> Value {
    json!({
        "type": "object",
        "properties": {
            "result_id": {
                "type": "string",
                "description": "The evicted result ID shown in the preview.",
            },
            "query": {
                "type": "string",
                "description": "One line of text to find, case aside (matched as written, not a \
    pattern). Returns only the lines that contain it, with the lines around them.",
            },
            "offset": {
                "type": "integer",
                "minimum": 0,
                "default": 0,
                "description": "Line number to start reading from; with query, the match to \
    start from (next_offset).",
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "default": 800,
                "description": "Maximum number of lines to return (pages only).",
            },
        },
        "required": ["result_id"],
    })
}
