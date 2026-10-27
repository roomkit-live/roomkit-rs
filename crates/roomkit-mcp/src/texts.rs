//! The bounds on what a server's words become: a tool's description, a
//! call's result, a message the host reads.

/// The longest message of the server's that an error or a log line keeps,
/// in characters.
const MESSAGE_LIMIT: usize = 1000;

/// `text` within `limit` characters, cut at a word when one ends in the
/// second half, the cut ending in `…`, as the reference implementation's
/// `bounded_text` cuts a text: never in the middle of a word or a number.
pub(crate) fn bounded_text(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_owned();
    }
    let cut: Vec<char> = text.chars().take(limit.saturating_sub(1)).collect();
    let end = cut
        .iter()
        .rposition(|&c| c == ' ')
        .filter(|&space| space > limit / 2)
        .unwrap_or(cut.len());
    let kept: String = cut.iter().take(end).collect();
    format!("{}…", kept.trim_end())
}

/// `text` cut to `limit` characters with a note saying so, as the
/// reference implementation's `bounded_result` cuts a tool's result for a
/// model that reads it whole: the note counts within the bound.
pub(crate) fn bounded_result(text: &str, limit: usize) -> String {
    let chars = text.chars().count();
    if chars <= limit {
        return text.to_owned();
    }
    let notice = format!("\n... [truncated: the result was {chars} characters]");
    let room = limit.saturating_sub(notice.chars().count());
    let kept: String = text.chars().take(room).collect();
    if room == 0 {
        return text.chars().take(limit).collect();
    }
    format!("{kept}{notice}")
}

/// `text`, a message the server's words make up, within the bound an error
/// or a log line keeps.
pub(crate) fn bounded_message(text: &str) -> String {
    bounded_text(text, MESSAGE_LIMIT)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn a_result_past_its_bound_ends_with_the_note_within_it() {
        let cut = bounded_result(&"x".repeat(200), 100);
        assert_eq!(
            (
                cut.chars().count(),
                cut.ends_with("[truncated: the result was 200 characters]")
            ),
            (100, true)
        );
    }
}
