//! Searching and listing a catalogue (RFC §6.4): the scoring and the result
//! payloads, pure functions of the tools they read. Python's
//! `_tool_search.py`.

use std::collections::{BTreeSet, HashSet};

use serde_json::{Map, Value, json};

use crate::skills::dumps;
use crate::tools::AiTool;

/// How many matches a search returns unless it asks otherwise. Python's
/// `DEFAULT_FIND_TOOLS_LIMIT`.
const DEFAULT_FIND_TOOLS_LIMIT: i64 = 5;

/// A word in a tool's name counts this many times one in its description.
/// Python's `_NAME_WEIGHT`.
const NAME_WEIGHT: f64 = 3.0;

/// A word in a tool's tags, for a word its name does not carry. Below the name:
/// a tag belongs to the source, which puts the same words on every tool it
/// serves. Well above the description: tags are curated keywords, the bridge
/// to a tool named in another language. Python's `_TAGS_WEIGHT`.
const TAGS_WEIGHT: f64 = 2.0;

/// Only matches within this fraction of the best are kept. Python's
/// `_RELATIVE_SCORE_CUTOFF`.
const RELATIVE_SCORE_CUTOFF: f64 = 0.5;

/// The characters of a description a match carries. Python's `_MATCH_DESC_LIMIT`.
const MATCH_DESC_LIMIT: usize = 200;

/// The characters of a description a listing carries: one line, or a listing
/// would send the catalogue again. Python's `_LIST_DESC_LIMIT`.
const LIST_DESC_LIMIT: usize = 64;

/// The most entries a listing shows. Python's `render_list_payload(limit=60)`.
const LIST_LIMIT: usize = 60;

/// The most related names a result carries. Python's `_RELATED_MAX_NAMES`.
const RELATED_MAX_NAMES: usize = 10;

/// `text`'s words, lowered: split at anything but a letter or a digit, and
/// where a camel-case or an acronym boundary falls, so that `SpotifySearch` is
/// two words. Python's `tokenize()`.
fn tokenize(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    let mut words = Vec::new();
    let mut word = String::new();
    for (at, &c) in chars.iter().enumerate() {
        if !roomkit_core::rules::is_alnum(c) || starts_a_word(&chars, at) {
            if !word.is_empty() {
                words.push(std::mem::take(&mut word));
            }
            if !roomkit_core::rules::is_alnum(c) {
                continue;
            }
        }
        word.push(c);
    }
    if !word.is_empty() {
        words.push(word);
    }
    words.iter().map(|word| word.to_lowercase()).collect()
}

/// Whether the character at `at` opens a word though nothing separates it from
/// the one before: Python's `_CAMEL_BOUNDARY` and `_ACRONYM_BOUNDARY`.
fn starts_a_word(chars: &[char], at: usize) -> bool {
    let before = at.checked_sub(1).and_then(|before| chars.get(before));
    let Some(previous) = before.filter(|_| chars.get(at).is_some_and(char::is_ascii_uppercase))
    else {
        return false;
    };
    if previous.is_ascii_lowercase() || previous.is_ascii_digit() {
        return true;
    }
    previous.is_ascii_uppercase() && chars.get(at + 1).is_some_and(char::is_ascii_lowercase)
}

/// `token`'s number folded, so that a query and a tool meet whatever each
/// spelled. Blunt, and safe because both sides are folded alike. Python's
/// `fold()`.
fn fold(token: &str) -> String {
    if token.chars().count() < 4 || token.ends_with("ss") {
        return token.to_owned();
    }
    if let Some(stem) = token.strip_suffix("ies") {
        return format!("{stem}y");
    }
    let suffix = if ["ches", "shes", "xes", "zes", "ses"]
        .iter()
        .any(|ending| token.ends_with(ending))
    {
        "es"
    } else {
        "s"
    };
    token.strip_suffix(suffix).unwrap_or(token).to_owned()
}

/// The folded form of each of `text`'s words.
fn folded(text: &str) -> HashSet<String> {
    tokenize(text).iter().map(|word| fold(word)).collect()
}

/// `raw`, a model's `max_results`, as a count between 1 and `threshold`: the
/// call's schema admits a number or nothing, nothing asking for the default.
/// Python's `normalize_max_results()`.
pub(crate) fn normalize_max_results(raw: Option<&Value>, threshold: usize) -> usize {
    let value = raw
        .and_then(Value::as_number)
        .and_then(|number| number.as_i64().or_else(|| number.as_f64().map(truncated)))
        .unwrap_or(DEFAULT_FIND_TOOLS_LIMIT);
    usize::try_from(value).map_or(1, |value| value.min(threshold).max(1))
}

/// `value` as Python's `int()` reads a float: toward zero.
#[expect(
    clippy::cast_possible_truncation,
    reason = "toward zero, saturating, as a count bounded just after"
)]
fn truncated(value: f64) -> i64 {
    value as i64
}

/// A catalogue tool and the folded words of each field it is searched on.
/// Python's `_Candidate`.
struct Candidate<'a> {
    tool: &'a AiTool,
    name_words: HashSet<String>,
    description_words: HashSet<String>,
    tag_words: HashSet<String>,
}

impl<'a> Candidate<'a> {
    fn of(tool: &'a AiTool) -> Self {
        Self {
            tool,
            name_words: folded(&tool.name),
            description_words: folded(&tool.description),
            tag_words: folded(&tool.tags.join(" ")),
        }
    }

    /// Whether the word is in any of its fields.
    fn carries(&self, word: &str) -> bool {
        self.name_words.contains(word)
            || self.description_words.contains(word)
            || self.tag_words.contains(word)
    }

    /// Its relevance: each query word once, from the strongest of name and
    /// tags that holds it, its description adding on top. Added up term by term
    /// as Python's `_score_tool()`: a tool at exactly half of the best is kept
    /// or cut on the last bit of the sum.
    fn score(&self, weights: &[(&str, f64)]) -> f64 {
        let mut score = 0.0;
        for (word, weight) in weights {
            if self.name_words.contains(*word) {
                score += NAME_WEIGHT * weight;
            } else if self.tag_words.contains(*word) {
                score += TAGS_WEIGHT * weight;
            }
            if self.description_words.contains(*word) {
                score += weight;
            }
        }
        score
    }
}

/// How rare each query word is among `candidates`, in the words' order: a word
/// in nearly every tool weighs little, one no tool carries nothing. Python's
/// `_word_weights()`.
#[expect(
    clippy::cast_precision_loss,
    reason = "a catalogue's size, far under f64's exact integers"
)]
fn word_weights<'q>(
    candidates: &[Candidate<'_>],
    query: &'q BTreeSet<String>,
) -> Vec<(&'q str, f64)> {
    let n = candidates.len() as f64;
    query
        .iter()
        .map(|word| {
            let holding = candidates.iter().filter(|c| c.carries(word)).count();
            let weight = if holding == 0 {
                0.0
            } else {
                ((n + 1.0) / (holding as f64 + 1.0)).ln() + 1.0
            };
            (word.as_str(), weight)
        })
        .collect()
}

/// The `max_results` best tools of `catalogue` for `query`, bar those of
/// `exclude`, which the model already sees: a tool named by the query first,
/// whatever the others score, then each by the words it shares with the query,
/// within half of the best. Python's `search_catalogue()`.
pub(crate) fn search_catalogue<'a>(
    catalogue: &'a [AiTool],
    query: &str,
    max_results: usize,
    exclude: &HashSet<String>,
) -> Vec<&'a AiTool> {
    let words: BTreeSet<String> = folded(query).into_iter().collect();
    if words.is_empty() {
        return Vec::new();
    }
    let candidates: Vec<Candidate<'a>> = catalogue
        .iter()
        .filter(|tool| !exclude.contains(&tool.name))
        .map(Candidate::of)
        .collect();
    let weights = word_weights(&candidates, &words);
    let mut scored: Vec<(f64, &'a AiTool)> = candidates
        .iter()
        .map(|candidate| (candidate.score(&weights), candidate.tool))
        .filter(|(score, _)| *score > 0.0)
        .collect();
    // Stable: equal scores keep the catalogue's order, as Python's `sort`.
    scored.sort_by(|a, b| b.0.total_cmp(&a.0));
    let Some(best) = scored.first().map(|(score, _)| *score) else {
        return Vec::new();
    };
    let cutoff = best * RELATIVE_SCORE_CUTOFF;
    let exact = exact_name_match(&candidates, query);
    let kept = scored
        .into_iter()
        .filter(|(score, tool)| {
            *score >= cutoff && exact.is_none_or(|exact| exact.name != tool.name)
        })
        .map(|(_, tool)| tool);
    exact.into_iter().chain(kept).take(max_results).collect()
}

/// The tool whose name is the query, compared without case or surrounding
/// space. Scored among the tools sharing its words, a model asking for a tool
/// by name could read it as not existing. Python's `_exact_name_match()`.
fn exact_name_match<'a>(candidates: &[Candidate<'a>], query: &str) -> Option<&'a AiTool> {
    let wanted = casefolded(strip(query));
    candidates
        .iter()
        .find(|candidate| casefolded(&candidate.tool.name) == wanted)
        .map(|candidate| candidate.tool)
}

/// `text` without the surrounding characters Python's `str.strip()` takes
/// away: the whitespace Rust knows, and the four separators it does not.
pub(crate) fn strip(text: &str) -> &str {
    text.trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
}

/// `text` as Python's `str.casefold()` writes it where it can meet a tool's
/// name, which is ASCII (RFC §6.7): lowered, each character that folds to ASCII
/// letters but does not lower to them written as those.
fn casefolded(text: &str) -> String {
    text.to_lowercase()
        .replace(['\u{df}', '\u{1e9e}'], "ss")
        .replace('\u{17f}', "s")
        .replace('\u{fb00}', "ff")
        .replace('\u{fb01}', "fi")
        .replace('\u{fb02}', "fl")
        .replace('\u{fb03}', "ffi")
        .replace('\u{fb04}', "ffl")
        .replace(['\u{fb05}', '\u{fb06}'], "st")
}

/// What is before a name's first `_`, the family it belongs to (the MCP
/// gateways' convention); a name without one has none.
fn family(name: &str) -> Option<&str> {
    name.split_once('_').map(|(family, _)| family)
}

/// The names of the catalogue's tools that share a family with a match and
/// were not matched, bar `exclude`, what the search never returns (RFC §21.1):
/// the wider action space of the source it found. Python's
/// `related_family_tools()`.
pub(crate) fn related_family_tools(
    catalogue: &[AiTool],
    matches: &[&AiTool],
    exclude: &HashSet<String>,
) -> Vec<String> {
    let families: HashSet<&str> = matches.iter().filter_map(|m| family(&m.name)).collect();
    catalogue
        .iter()
        .map(|tool| tool.name.as_str())
        .filter(|name| !exclude.contains(*name) && !matches.iter().any(|m| m.name == *name))
        .filter(|name| family(name).is_some_and(|family| families.contains(family)))
        .take(RELATED_MAX_NAMES)
        .map(str::to_owned)
        .collect()
}

/// `text`'s first `limit` characters.
fn first_chars(text: &str, limit: usize) -> String {
    text.chars().take(limit).collect()
}

/// A catalogue entry as a result shows it: its name and a description cut at
/// `limit`.
fn gist(tool: &AiTool, limit: usize) -> Value {
    json!({ "name": tool.name, "description": first_chars(&tool.description, limit) })
}

/// What a search's result says of its matches.
const FOUND_NOTE: &str = "These tools are now invocable — their full schemas are in your tool \
list. Call the right one directly. Do not call find_tools again unless none of these fit.";

/// What it adds when it names their family.
const RELATED_NOTE: &str = "related_tools_same_source lists MORE tools from the same source (NOT \
yet invocable) — run find_tools for one of those when the user's next ask falls in this domain, \
instead of assuming you cannot do it.";

/// What a search that found nothing says, ending with the host's hint.
fn miss_note(hint: Option<&str>) -> String {
    let note = "No tools matched. Try a different query, or call list_tools to see all available \
tools.";
    match hint.filter(|hint| !hint.is_empty()) {
        Some(hint) => format!("{note} {hint}"),
        None => note.to_owned(),
    }
}

/// What `find_tools` answers: each match's name and a short description, no
/// schema, which the next round's declaration delivers. Python's
/// `render_find_payload()`.
pub(crate) fn render_find_payload(
    matches: &[&AiTool],
    miss_hint: Option<&str>,
    related: &[String],
) -> String {
    let mut payload = Map::new();
    let listed: Vec<Value> = matches.iter().map(|m| gist(m, MATCH_DESC_LIMIT)).collect();
    payload.insert("matches".to_owned(), Value::Array(listed));
    let note = if matches.is_empty() {
        miss_note(miss_hint)
    } else if related.is_empty() {
        FOUND_NOTE.to_owned()
    } else {
        format!("{FOUND_NOTE} {RELATED_NOTE}")
    };
    payload.insert("_note".to_owned(), Value::String(note));
    if !matches.is_empty() && !related.is_empty() {
        payload.insert("related_tools_same_source".to_owned(), json!(related));
    }
    dumps(&Value::Object(payload))
}

/// What `list_tools` answers: each tool's name and one line, those `category`
/// prefixes, bar `exclude`, at most [`LIST_LIMIT`] spread over the whole list:
/// the first ones would cap the caller's ordering rather than the size.
/// Python's `render_list_payload()`.
pub(crate) fn render_list_payload(
    catalogue: &[AiTool],
    category: &str,
    exclude: &HashSet<String>,
) -> String {
    let items: Vec<&AiTool> = catalogue
        .iter()
        .filter(|tool| !exclude.contains(&tool.name) && tool.name.starts_with(category))
        .collect();
    let total = items.len();
    let shown = spread(&items, LIST_LIMIT);
    let listed: Vec<Value> = shown.iter().map(|t| gist(t, LIST_DESC_LIMIT)).collect();
    let mut payload = Map::new();
    let count = listed.len();
    payload.insert("tools".to_owned(), Value::Array(listed));
    payload.insert("count".to_owned(), json!(count));
    if total > LIST_LIMIT {
        payload.insert("truncated".to_owned(), json!(true));
        payload.insert("total".to_owned(), json!(total));
        payload.insert(
            "_note".to_owned(),
            json!(format!(
                "Showing {LIST_LIMIT} of {total} tools, sampled across the catalogue. Use \
category= to filter, or prefer find_tools(query=...) for action."
            )),
        );
    }
    dumps(&Value::Object(payload))
}

/// `limit` of `items` at a regular stride, in order, or all of them when they
/// are no more. Python's `_spread()`.
#[expect(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "Python's `int(i * step)`, on a list's indexes"
)]
fn spread<'a>(items: &[&'a AiTool], limit: usize) -> Vec<&'a AiTool> {
    if items.len() <= limit {
        return items.to_vec();
    }
    let step = items.len() as f64 / limit as f64;
    (0..limit)
        .filter_map(|at| items.get((at as f64 * step) as usize).copied())
        .collect()
}

#[cfg(test)]
#[path = "catalogue_tests.rs"]
mod tests;
