//! The text a delegation hands a model or the status bus (RFC §6.4 "Text
//! from outside in a model's context"), as Python's `roomkit._text` writes
//! it: a text bounded at a word, on one line, quoted so it cannot close its
//! quote, and an identifier kept to an identifier's characters.

/// `text` within `limit` characters, cut at a word, the cut ending in `…`:
/// Python's `bounded_text`.
pub(crate) fn bounded_text(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_owned();
    }
    let cut: Vec<char> = text.chars().take(limit.saturating_sub(1)).collect();
    let kept: String = match cut.iter().rposition(|c| *c == ' ') {
        Some(space) if space > limit / 2 => cut.iter().take(space).collect(),
        _ => cut.iter().collect(),
    };
    format!("{}…", kept.trim_end())
}

/// `text` on one line, each run of whitespace one space: Python's
/// `one_line`.
pub(crate) fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Every double quote mark, made a single one inside a quoted text, and
/// the marks that reorder text, dropped: Python's `_DOUBLE_QUOTES`.
const DOUBLE_QUOTES: &[char] = &[
    '"',
    '\u{201c}',
    '\u{201d}',
    '\u{201e}',
    '\u{201f}',
    '\u{ab}',
    '\u{bb}',
    '\u{ff02}',
    '\u{301d}',
    '\u{301e}',
    '\u{301f}',
    '\u{2e42}',
    '\u{275d}',
    '\u{275e}',
    '\u{2760}',
    '\u{1f676}',
    '\u{1f677}',
    '\u{1f678}',
    '\u{2033}',
    '\u{2036}',
    '\u{2ba}',
    '\u{2dd}',
    '\u{2ee}',
    '\u{5f4}',
    '\u{3003}',
    '\u{2034}',
    '\u{2057}',
    '\u{e0022}',
    '\u{2f6}',
    '\u{5f2}',
    '\u{1cd3}',
];

/// The bidirectional embeddings, overrides and isolates a quote drops.
const BIDI_CONTROLS: &[char] = &[
    '\u{202a}', '\u{202b}', '\u{202c}', '\u{202d}', '\u{202e}', '\u{2066}', '\u{2067}', '\u{2068}',
    '\u{2069}',
];

/// The tags of the blocks a cut can reach: Python's `FENCED_TAGS`.
const FENCED_TAGS: &[&str] = &[
    "tool_result",
    "worker_output",
    "knowledge",
    "conversation_summary",
    "context",
    "task",
    "vision",
];

/// External `text` quoted inline (RFC §6.4): on one line, within `limit`
/// characters, between `“` and `”` it cannot close; cut short, it names
/// the blocks it holds rather than leave part of one open. Python's
/// `quoted`.
pub(crate) fn quoted(text: &str, limit: usize) -> String {
    let flat: String = one_line(text)
        .chars()
        .filter(|c| !BIDI_CONTROLS.contains(c))
        .map(|c| if DOUBLE_QUOTES.contains(&c) { '\'' } else { c })
        .collect();
    let flat = if flat.chars().count() > limit {
        named_blocks(&flat)
    } else {
        flat
    };
    format!("“{}”", bounded_text(&flat, limit))
}

/// `text` with each fenced block, from its opening tag through its closing
/// one or the text's end, replaced by its tag in brackets: Python's
/// `named_blocks`, for the tags as written.
fn named_blocks(text: &str) -> String {
    FENCED_TAGS
        .iter()
        .fold(text.to_owned(), |text, tag| named(&text, tag))
}

fn named(text: &str, tag: &str) -> String {
    let (opening, closing) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut kept = String::with_capacity(text.len());
    let mut rest = text;
    while let Some((before, after)) = rest.split_once(opening.as_str()) {
        kept.push_str(before);
        kept.push('[');
        kept.push_str(tag);
        kept.push(']');
        rest = after
            .split_once(closing.as_str())
            .map_or("", |(_, tail)| tail);
    }
    kept.push_str(rest);
    kept
}

/// `value` as the runtime gives an identifier outside a quote: letters
/// with their marks, digits and `_ . @ + -`, any other character a `-`,
/// within 64 characters, composed first; `fallback` when nothing is left.
/// Python's `identifier`, the transcript's.
pub(crate) fn identifier(value: &str, fallback: &str) -> String {
    roomkit_core::rules::transcript::identifier(value, fallback)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn a_bounded_text_is_cut_at_a_word() {
        assert_eq!(
            [
                bounded_text("short", 10),
                bounded_text("one two three four", 10),
            ],
            ["short".to_owned(), "one two…".to_owned()]
        );
    }

    #[test]
    fn a_quoted_text_cannot_close_its_quote() {
        assert_eq!(quoted("Say \"hi\"\nnow", 200), "“Say 'hi' now”");
    }

    // Python: tests/test_external_text.py::test_a_name_keeps_its_letters_and_their_marks
    #[test]
    fn an_identifier_keeps_letters_and_their_marks() {
        assert_eq!(
            [identifier("सुनील", "x"), identifier("Cafe\u{301}", "x")],
            ["सुनील", "Café"]
        );
    }

    #[test]
    fn an_identifier_keeps_its_characters() {
        assert_eq!(
            [
                identifier("worker", "x"),
                identifier("a b::c", "x"),
                identifier("", "worker")
            ],
            ["worker", "a-b-c", "worker"]
        );
    }
}
