//! The anti-loop guard (RFC §6.4): a turn's calls repeated with identical
//! arguments, and a tool's answers repeated for any. Python's
//! `_repeated_call_guard` and `_repeated_result_note`.
//!
//! The observed failure: a small model re-runs one call round after round and
//! burns the whole turn without answering. The third identical call is
//! refused (the second for a pure tool, whose repeat cannot say anything new);
//! a model that re-issues it three more times is force-stopped.
//!
//! A model that varies its arguments never trips that, and is told nothing
//! changes by a note on the third identical answer. The note never blocks: six
//! deletions each answering `{"success": true}` are six correct operations.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

use roomkit::model::Metadata;
use serde_json::{Map, Value};

use crate::texts;

/// The call that repeats at this count is refused: two identical executions
/// can be legitimate, a retry after a transient failure.
const REPEAT_CALL_LIMIT: u32 = 3;
/// The same, for a pure tool: an identical repeat never says anything new.
const PURE_CALL_LIMIT: u32 = 2;
/// How many times past its limit a refused call is re-issued before the loop
/// is force-stopped: the advisory clearly is not landing.
const REPEAT_FORCE_STOP_AT: u32 = 3;
/// How many identical answers of one tool before the model is told: a second
/// is ordinary (a retry, a poll, two rows deleted), a third a pattern.
const REPEAT_RESULT_LIMIT: u32 = 3;
/// Marks the guard's own words, so that a repeated advisory is not noted as a
/// repeated result: it already says what is wrong.
const ADVISORY_MARKER: &str = "these EXACT arguments";

/// What the next generation reads once the guard force-stopped the turn: it
/// is the last, none of its calls runs. Python's `_FORCE_STOP_NUDGE`.
pub(super) const FORCE_STOP_NUDGE: &str = "You have repeated the same tool call with identical \
arguments several times; it cannot produce anything new and further tool calls are disabled. Stop \
now and reply to the user in plain text with a summary of what you found and what remains, using \
the results already above. A rejected tool call did not execute. Report its failure and the \
correction needed; never claim an action succeeded without a successful tool result.";

/// What one turn's guard counted. A round's calls run concurrently, each
/// through it.
#[derive(Debug, Default)]
pub(crate) struct RepeatGuard {
    calls: Mutex<HashMap<(String, String), u32>>,
    results: Mutex<HashMap<(String, String), u32>>,
    force_stop: AtomicBool,
}

impl RepeatGuard {
    /// Counts a call to `name` with `arguments`, `pure` when the tool reads
    /// what cannot change within the turn: what the model reads in its place
    /// once it repeats past the limit, `None` while it may run.
    pub(super) fn refusal(&self, name: &str, arguments: &Metadata, pure: bool) -> Option<String> {
        let key = (name.to_owned(), sorted_json(arguments));
        let count = {
            let mut counts = lock(&self.calls);
            let count = counts.entry(key).or_insert(0);
            *count += 1;
            *count
        };
        let limit = if pure {
            PURE_CALL_LIMIT
        } else {
            REPEAT_CALL_LIMIT
        };
        if count < limit {
            return None;
        }
        if count >= limit + REPEAT_FORCE_STOP_AT {
            self.force_stop.store(true, Ordering::SeqCst);
        }
        Some(texts::repeated_call(name, count - 1))
    }

    /// Whether the guard pulled the ripcord: the next generation is the
    /// turn's last.
    pub(super) fn force_stopped(&self) -> bool {
        self.force_stop.load(Ordering::SeqCst)
    }

    /// `result`, the model's copy of what `name` answered, with a note once
    /// it gave that answer a third time this turn, counted on `answer`, what
    /// the tool gave, when the model's copy differs from it: a stored
    /// result's placeholder carries an id of its own per call (RFC §21.5).
    /// The guard's own words are left as they are, uncounted. Python's
    /// `_repeated_result_note`.
    pub(super) fn noted(&self, name: &str, result: String, answer: Option<&str>) -> String {
        if result.contains(ADVISORY_MARKER) {
            return result;
        }
        let key = (name.to_owned(), answer.unwrap_or(&result).to_owned());
        let count = {
            let mut counts = lock(&self.results);
            let count = counts.entry(key).or_insert(0);
            *count += 1;
            *count
        };
        if count < REPEAT_RESULT_LIMIT {
            return result;
        }
        // The only witness: the note rides on what the model reads, past the
        // hooks an audit listens on.
        tracing::warn!(
            target: "roomkit::ai",
            tool = %name,
            count,
            "a tool returned an identical result again this turn"
        );
        format!(
            "{result}\n\n[identical result: '{name}' has now returned exactly this {count} times \
this turn, for different arguments. Varying the arguments is not finding anything new \u{2014} \
this answer is settled. Use it and move on, or answer with what you have.]"
        )
    }
}

/// `arguments` as one text whatever the order of their keys, at any depth:
/// Python's `json.dumps(arguments, sort_keys=True)`.
fn sorted_json(arguments: &Metadata) -> String {
    let sorted: Map<String, Value> = sorted_entries(arguments);
    Value::Object(sorted).to_string()
}

fn sorted_entries(object: &Map<String, Value>) -> Map<String, Value> {
    let mut entries: Vec<(&String, &Value)> = object.iter().collect();
    entries.sort_by_key(|(key, _)| *key);
    entries
        .into_iter()
        .map(|(key, value)| (key.clone(), sorted_value(value)))
        .collect()
}

fn sorted_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(sorted_entries(object)),
        Value::Array(items) => Value::Array(items.iter().map(sorted_value).collect()),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => value.clone(),
    }
}

/// The lock on a count, which no panic leaves inconsistent.
fn lock<T>(counts: &Mutex<T>) -> MutexGuard<'_, T> {
    counts.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;
    use serde_json::json;

    use super::*;

    fn arguments(value: &Value) -> Metadata {
        value.as_object().cloned().unwrap_or_default()
    }

    const NOTE: &str = "identical result";

    // Python: tests/test_repeated_result_note.py::test_the_first_two_identical_answers_pass_unremarked
    #[test]
    fn the_first_two_identical_answers_pass_unremarked() {
        let guard = RepeatGuard::default();
        assert_eq!(guard.noted("boards", "same".to_owned(), None), "same");
        assert_eq!(guard.noted("boards", "same".to_owned(), None), "same");
        let third = guard.noted("boards", "same".to_owned(), None);
        assert!(third.contains(NOTE) && third.starts_with("same"), "{third}");
    }

    // Python: tests/test_repeated_result_note.py::test_a_different_result_never_accumulates
    #[test]
    fn a_different_result_never_accumulates() {
        let guard = RepeatGuard::default();
        for i in 0..10 {
            let read = guard.noted("boards", format!("row-{i}"), None);
            assert!(!read.contains(NOTE), "{read}");
        }
    }

    // Python: tests/test_repeated_result_note.py::test_two_tools_returning_the_same_string_are_counted_apart
    #[test]
    fn two_tools_returning_the_same_string_are_counted_apart() {
        let guard = RepeatGuard::default();
        for _ in 0..2 {
            guard.noted("todos", "ok".to_owned(), None);
            guard.noted("boards", "ok".to_owned(), None);
        }
        assert_eq!(guard.noted("notes", "ok".to_owned(), None), "ok");
    }

    // Python: tests/test_repeated_result_note.py::test_the_advisory_error_is_not_itself_annotated
    #[test]
    fn the_advisory_error_is_not_itself_annotated() {
        let guard = RepeatGuard::default();
        for _ in 0..5 {
            let read = guard.noted("boards", texts::repeated_call("boards", 2), None);
            assert_eq!(read, texts::repeated_call("boards", 2));
        }
    }

    // Python: tests/test_repeated_result_note.py::test_the_note_counts_what_it_saw
    #[test]
    fn the_note_counts_what_it_saw() {
        for n in [3, 8, 23] {
            let guard = RepeatGuard::default();
            let read = (0..n).fold(String::new(), |_, _| {
                guard.noted("boards", "same".to_owned(), None)
            });
            assert!(read.contains(&format!("{n} times this turn")), "{read}");
        }
    }

    // Python's key: `json.dumps(arguments, sort_keys=True)`; this crate's maps
    // keep their order, a nested one's too.
    #[test]
    fn the_order_of_the_keys_makes_no_new_call() {
        let guard = RepeatGuard::default();
        let one = arguments(&json!({"a": 1, "b": {"x": 1, "y": [{"p": 1, "q": 2}]}}));
        let two = arguments(&json!({"b": {"y": [{"q": 2, "p": 1}], "x": 1}, "a": 1}));
        assert_eq!(guard.refusal("t", &one, false), None);
        assert_eq!(guard.refusal("t", &two, false), None);
        assert_eq!(
            guard.refusal("t", &one, false),
            Some(texts::repeated_call("t", 2))
        );
    }

    #[test]
    fn a_refused_call_force_stops_past_its_limit() {
        let guard = RepeatGuard::default();
        let same = Metadata::new();
        for _ in 0..5 {
            guard.refusal("t", &same, false);
            assert!(!guard.force_stopped());
        }
        guard.refusal("t", &same, false);
        assert!(guard.force_stopped());
    }
}
