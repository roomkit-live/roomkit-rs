//! The forms of a field's value the line reader reads whole, as YAML reads
//! them: a comment's end, quoted texts with YAML's escapes, block scalars
//! and their final line breaks, flow lists and list items.

/// The plain texts YAML reads as no value.
pub(super) const NULLS: [&str; 4] = ["~", "null", "Null", "NULL"];

/// A block scalar's style and how it keeps its final line breaks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BlockHeader {
    folded: bool,
    chomp: Chomp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Chomp {
    /// One final line break (YAML's default).
    Clip,
    /// None (`-`).
    Strip,
    /// Every final line break (`+`).
    Keep,
}

/// A list's item as the reader reads it.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Item {
    /// A text.
    Text(String),
    /// Nothing: an empty item, or a null.
    Nothing,
    /// More than a text, which a line-by-line reading does not read.
    Unread,
}

/// `text` up to its YAML comment: a `#` first or after a space.
pub(super) fn without_comment(text: &str) -> &str {
    let mut previous_space = true;
    for (at, character) in text.char_indices() {
        if character == '#' && previous_space {
            return text.get(..at).unwrap_or_default();
        }
        previous_space = character.is_whitespace();
    }
    text
}

/// Whether a value starts as YAML reads more than a plain text there: a
/// collection, an indicator, or a nested item, key or entry.
pub(super) fn not_a_plain_text(value: &str) -> bool {
    let mut characters = value.chars();
    let first = characters.next();
    let second = characters.next();
    match first {
        Some('{' | '}' | ']' | '%' | '@' | '`' | ',') => true,
        Some('-' | '?' | ':') => second.is_none_or(char::is_whitespace),
        _ => false,
    }
}

/// A block scalar's header (`|`, `>`, and `-` or `+`), with nothing but a
/// comment after it; an indentation indicator is not read.
pub(super) fn block_header(value: &str) -> Option<BlockHeader> {
    let mut characters = value.chars();
    let folded = characters.next()? == '>';
    let rest = characters.as_str();
    let (chomp, rest) = match rest.chars().next() {
        Some('-') => (Chomp::Strip, rest.get(1..)?),
        Some('+') => (Chomp::Keep, rest.get(1..)?),
        _ => (Chomp::Clip, rest),
    };
    let spaced = rest.is_empty() || rest.starts_with(char::is_whitespace);
    (spaced && without_comment(rest).trim().is_empty()).then_some(BlockHeader { folded, chomp })
}

/// A block scalar's text from its `lines` as written, as YAML reads it:
/// its indentation the first line's, a literal one's lines kept, a folded
/// one's joined, its final line breaks kept as its header says (`at_end`
/// when the frontmatter ended on its last line, with no break after it).
/// `None` for a line indented less than the first, or a folded one's line
/// indented more.
pub(super) fn block_text(
    header: BlockHeader,
    mut lines: Vec<String>,
    at_end: bool,
) -> Option<String> {
    let mut trailing = 0;
    while lines.last().is_some_and(|line| line.trim().is_empty()) {
        lines.pop();
        trailing += 1;
    }
    let indent = lines
        .iter()
        .find(|line| !line.trim().is_empty())
        .map_or(1, |line| line.len() - line.trim_start_matches(' ').len());
    if indent == 0 {
        return None;
    }
    let mut content = Vec::with_capacity(lines.len());
    for line in &lines {
        if line.trim().is_empty() {
            content.push("");
            continue;
        }
        let indented = line
            .get(..indent)
            .is_some_and(|head| head.bytes().all(|b| b == b' '));
        let rest = line.get(indent..).filter(|_| indented)?;
        if header.folded && rest.starts_with(char::is_whitespace) {
            return None;
        }
        content.push(rest);
    }
    let mut text = if header.folded {
        folded(&content)
    } else {
        content.join("\n")
    };
    let breaks = usize::from(!at_end || trailing > 0) + trailing;
    match header.chomp {
        Chomp::Clip if !text.is_empty() && breaks > 0 => text.push('\n'),
        Chomp::Keep => text.push_str(&"\n".repeat(breaks)),
        Chomp::Clip | Chomp::Strip => {}
    }
    Some(text)
}

/// A folded block's lines joined: one space between two lines, a line feed
/// for each blank line between them, or before the first.
fn folded(lines: &[&str]) -> String {
    let mut text = String::new();
    let mut blanks = 0;
    for line in lines {
        if line.is_empty() {
            blanks += 1;
            continue;
        }
        if blanks == 0 && !text.is_empty() {
            text.push(' ');
        } else {
            text.push_str(&"\n".repeat(blanks));
        }
        text.push_str(line);
        blanks = 0;
    }
    text
}

/// A quoted value whole on its line, with at most a comment after it.
pub(super) fn quoted_whole(value: &str) -> Option<String> {
    let (text, after) = quoted(value)?;
    without_comment(after).trim().is_empty().then_some(text)
}

/// The quoted text `value` opens, and what follows its closing quote:
/// a single-quoted one with `''` for a quote, a double-quoted one with
/// YAML's escapes. `None` when it does not close on its line, or holds an
/// escape YAML does not know.
fn quoted(value: &str) -> Option<(String, &str)> {
    let mut characters = value.char_indices();
    let (_, quote) = characters.next()?;
    let mut text = String::new();
    while let Some((at, character)) = characters.next() {
        if character == quote {
            if quote == '\'' && value.get(at + 1..)?.starts_with('\'') {
                characters.next();
                text.push('\'');
                continue;
            }
            return Some((text, value.get(at + 1..)?));
        }
        if quote == '"' && character == '\\' {
            text.push(escape(&mut characters)?);
            continue;
        }
        text.push(character);
    }
    None
}

/// The character a double-quoted text's escape stands for (YAML 1.2
/// §5.7), read after its backslash.
fn escape(characters: &mut std::str::CharIndices<'_>) -> Option<char> {
    let (_, code) = characters.next()?;
    let digits = match code {
        '0' => return Some('\0'),
        'a' => return Some('\u{7}'),
        'b' => return Some('\u{8}'),
        't' | '\t' => return Some('\t'),
        'n' => return Some('\n'),
        'v' => return Some('\u{b}'),
        'f' => return Some('\u{c}'),
        'r' => return Some('\r'),
        'e' => return Some('\u{1b}'),
        ' ' | '"' | '/' | '\\' => return Some(code),
        'N' => return Some('\u{85}'),
        '_' => return Some('\u{a0}'),
        'L' => return Some('\u{2028}'),
        'P' => return Some('\u{2029}'),
        'x' => 2,
        'u' => 4,
        'U' => 8,
        _ => return None,
    };
    let mut hex = String::with_capacity(digits);
    for _ in 0..digits {
        hex.push(characters.next()?.1);
    }
    char::from_u32(u32::from_str_radix(&hex, 16).ok()?)
}

/// The items of a flow list whole on its line (`[a, 'b', "c"]`), its
/// empty items and nulls left out; `None` for anything else.
pub(super) fn flow_list(value: &str) -> Option<Vec<String>> {
    let inner = value.strip_prefix('[')?.strip_suffix(']')?;
    if inner.contains(['[', ']', '{', '}']) {
        return None;
    }
    let mut items = Vec::new();
    for part in inner.split(',') {
        match list_item(part.trim()) {
            Item::Text(item) => items.push(item),
            Item::Nothing => {}
            Item::Unread => return None,
        }
    }
    Some(items)
}

/// A list's item, its quotes and comment taken off.
pub(super) fn list_item(text: &str) -> Item {
    if text.starts_with(['"', '\'']) {
        return quoted_whole(text).map_or(Item::Unread, Item::Text);
    }
    if text.starts_with(['[', '|', '>', '&', '!']) || not_a_plain_text(text) {
        return Item::Unread;
    }
    let plain = without_comment(text).trim();
    if plain.is_empty() || NULLS.contains(&plain) {
        Item::Nothing
    } else {
        Item::Text(plain.to_owned())
    }
}
