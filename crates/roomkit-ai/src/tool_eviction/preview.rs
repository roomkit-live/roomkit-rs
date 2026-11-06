//! The preview that stands in for a stored result: its first and last
//! lines within a budget of characters (RFC §21.5). Python's `_preview`
//! and its helpers in `channels/_tool_eviction.py`.

use super::text::{char_len, char_slice, splitlines};

/// The most head lines a preview shows.
const HEAD_LINES: usize = 5;
/// The most tail lines a preview shows.
const TAIL_LINES: usize = 5;

/// The marker that counts the lines a preview leaves out.
fn omission_marker(count: usize) -> String {
    format!("[... {count} lines omitted ...]")
}

/// The marker that ends a line cut to fit.
fn truncation_marker(count: usize) -> String {
    format!(" [... {count} chars truncated ...]")
}

/// `line` cut to fit `room` characters, the cut stated in a marker; `None`
/// when the marker leaves no room for any of the line.
fn clip_line(line: &str, room: i64) -> Option<String> {
    let length = char_len(line);
    // Sized for the widest count, so that the real marker is never longer.
    let keep = room - signed(char_len(&truncation_marker(length)));
    let keep = usize::try_from(keep).ok().filter(|keep| *keep >= 1)?;
    Some(format!(
        "{}{}",
        char_slice(line, 0, keep),
        truncation_marker(length.saturating_sub(keep))
    ))
}

/// The leading `lines` that fit in `budget` characters once joined by
/// newlines. The first line that does not fit is clipped, if its marker
/// leaves room for some of it, and ends the run.
fn fit_lines(lines: &[&str], budget: usize) -> Vec<String> {
    let mut kept: Vec<String> = Vec::new();
    let mut used = 0;
    for line in lines {
        let separator = i64::from(!kept.is_empty());
        // Below zero once a line filled the budget: even an empty line
        // then ends the run, as Python's arithmetic has it.
        let room = signed(budget) - used - separator;
        let length = signed(char_len(line));
        if length > room {
            kept.extend(clip_line(line, room));
            break;
        }
        kept.push((*line).to_owned());
        used += separator + length;
    }
    kept
}

/// `count` as a signed number, for the arithmetic Python does below zero.
fn signed(count: usize) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

/// A head and tail preview of `result` within `budget` characters.
///
/// A bound on lines alone would let one giant line (minified HTML, a JSON
/// blob) reach the provider whole. The head and the tail split the lines,
/// so that even a short result shows its last line: the head takes at most
/// half the budget, or what the whole tail leaves; the tail takes the rest,
/// filled from the last line. A line that does not fit is clipped with a
/// marker, and every line not shown is counted in the omission marker.
pub(super) fn preview(result: &str, budget: usize) -> String {
    let lines = splitlines(result);
    // Reserved at its widest, which also covers the newline joining head
    // and tail when nothing is omitted.
    let reserve = char_len(&omission_marker(lines.len())) + 4;
    if budget < reserve {
        return String::new();
    }
    let head_count = HEAD_LINES.min(lines.len().div_ceil(2));
    let head_source = lines.get(..head_count).unwrap_or_default();
    let tail_start = head_count.max(lines.len().saturating_sub(TAIL_LINES));
    let tail_source = lines.get(tail_start..).unwrap_or_default();
    let lines_budget = budget - reserve;
    let whole_tail = char_len(&tail_source.join("\n"));
    let head = fit_lines(
        head_source,
        (lines_budget / 2).max(lines_budget.saturating_sub(whole_tail)),
    );
    let head_text = head.join("\n");
    let reversed: Vec<&str> = tail_source.iter().rev().copied().collect();
    let mut tail = fit_lines(&reversed, lines_budget.saturating_sub(char_len(&head_text)));
    tail.reverse();
    let tail_text = tail.join("\n");
    let omitted = lines.len() - head.len() - tail.len();
    if omitted == 0 {
        return [head, tail].concat().join("\n");
    }
    [head_text, omission_marker(omitted), tail_text]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}
