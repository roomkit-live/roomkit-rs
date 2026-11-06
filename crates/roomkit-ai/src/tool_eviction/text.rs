//! Text as Python's `str` counts and cuts it: by characters, its lines split
//! at every break `str.splitlines` knows.

/// The characters in `text`: Python's `len`.
pub(crate) fn char_len(text: &str) -> usize {
    text.chars().count()
}

/// The characters of `text` from `start` up to `end`, both clamped to its
/// length: Python's `text[start:end]`.
pub(super) fn char_slice(text: &str, start: usize, end: usize) -> &str {
    let at = |index: usize| {
        text.char_indices()
            .nth(index)
            .map_or(text.len(), |(byte, _)| byte)
    };
    let (from, to) = (at(start), at(end.max(start)));
    text.get(from..to).unwrap_or_default()
}

/// `text`'s lines, split at every line break Python's `str.splitlines`
/// knows, a final break ending the last line rather than opening one.
pub(super) fn splitlines(text: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut rest = text;
    while let Some((at, c)) = rest.char_indices().find(|(_, c)| is_line_break(*c)) {
        let (line, tail) = rest.split_at(at);
        lines.push(line);
        let after = tail.get(c.len_utf8()..).unwrap_or_default();
        rest = if c == '\r' {
            after.strip_prefix('\n').unwrap_or(after)
        } else {
            after
        };
    }
    if !rest.is_empty() {
        lines.push(rest);
    }
    lines
}

/// Whether Python's `str.splitlines` ends a line at `c`.
fn is_line_break(c: char) -> bool {
    matches!(
        c,
        '\n' | '\r'
            | '\u{0b}'
            | '\u{0c}'
            | '\u{1c}'
            | '\u{1d}'
            | '\u{1e}'
            | '\u{85}'
            | '\u{2028}'
            | '\u{2029}'
    )
}
