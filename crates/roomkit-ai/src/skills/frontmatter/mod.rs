//! A `SKILL.md` frontmatter, read line by line (RFC §24.1, step 4 decision
//! 11: no YAML reader), within §24.1's bounds.
//!
//! Each `key: value` line at the top level is a field; a line indented
//! under a key belongs to that key's value. The reader reads whole the
//! fields the registry uses (`name`, `description`, `license`,
//! `compatibility`) or refuses the skill: a plain text, folded over the
//! lines indented under it; a quoted text on its line; a block scalar
//! (`|`, `>`, with `-` or `+`); a list in either YAML form (`[a, b]` on the
//! key's line, or `- item` lines), its items joined by commas. It does not
//! read the other keys' values, and refuses:
//!
//! - a frontmatter past 65,536 characters, or holding more than 64
//!   brackets and braces open at once (§24.1);
//! - `allowed_tools` and `requires`: this implementation gates no tool,
//!   and a skill read without what it gates would leave its tools open;
//! - a top-level line that is no key, no item under a key, no comment,
//!   since YAML may read there a key this reading cannot name (an explicit
//!   key, a tag, an anchor, a merge, a flow mapping);
//! - an anchor or a tag on a value, so that no alias names anything;
//! - a line break YAML reads that a line-by-line reading does not
//!   (`\r` alone, U+0085, U+2028, U+2029), and any other control
//!   character but the tab.

mod scalars;

use self::scalars::{
    BlockHeader, Item, NULLS, block_header, block_text, flow_list, list_item, not_a_plain_text,
    quoted_whole, without_comment,
};

/// The most characters a frontmatter takes (RFC §24.1).
pub(crate) const FRONTMATTER_MAX: usize = 64 * 1024;

/// The most brackets and braces a frontmatter holds open at once, quoted
/// ones counted too (RFC §24.1).
const FLOW_DEPTH_MAX: usize = 64;

/// The keys that gate or require tools (RFC §24.2), which no skill of this
/// implementation may declare.
const GATING_KEYS: [&str; 3] = ["allowed_tools", "allowed-tools", "requires"];

/// The fields the registry reads, in the order of [`Fields`].
const FIELDS: [&str; 4] = ["name", "description", "license", "compatibility"];

/// The fields of a frontmatter the registry uses, each absent when the
/// frontmatter does not give it, or gives it no value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Fields {
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) license: Option<String>,
    pub(crate) compatibility: Option<String>,
}

impl Fields {
    /// The slot of the field `FIELDS[field]`.
    fn slot(&mut self, field: usize) -> Option<&mut Option<String>> {
        match field {
            0 => Some(&mut self.name),
            1 => Some(&mut self.description),
            2 => Some(&mut self.license),
            3 => Some(&mut self.compatibility),
            _ => None,
        }
    }
}

/// `content`, a `SKILL.md`, as its frontmatter's text and its body, both
/// trimmed: what lies between a first line of `---` and the next line
/// starting `---`, and what follows it.
///
/// # Errors
///
/// Says why when `content` opens no frontmatter or never closes it.
pub(crate) fn split(content: &str) -> Result<(&str, &str), String> {
    let content = content.trim_start_matches('\u{feff}');
    let Some(after) = content.strip_prefix("---") else {
        return Err("SKILL.md must start with '---' frontmatter delimiter".to_owned());
    };
    let Some(end) = after.find("\n---") else {
        return Err("SKILL.md missing closing '---' frontmatter delimiter".to_owned());
    };
    let frontmatter = after.get(..end).unwrap_or_default();
    let body = after.get(end + "\n---".len()..).unwrap_or_default();
    Ok((frontmatter.trim(), body.trim()))
}

/// The fields `text`, a frontmatter, gives.
///
/// # Errors
///
/// Says why the frontmatter cannot be read whole (see the module).
pub(crate) fn read(text: &str) -> Result<Fields, String> {
    if text.chars().count() > FRONTMATTER_MAX {
        return Err(format!(
            "the frontmatter is past {FRONTMATTER_MAX} characters"
        ));
    }
    if flow_depth(text) > FLOW_DEPTH_MAX {
        return Err("the frontmatter nests too deep".to_owned());
    }
    if let Some(unread) = unread_character(text) {
        return Err(format!(
            "the frontmatter holds a character it cannot read line by line (U+{:04X})",
            u32::from(unread)
        ));
    }
    let mut reader = Reader::default();
    for (number, line) in text.lines().enumerate() {
        reader.line(number + 1, line)?;
    }
    reader.finish(true)?;
    Ok(reader.fields)
}

/// The most brackets and braces `text` holds open at once, quotes not
/// read: an upper bound on its YAML flow nesting.
fn flow_depth(text: &str) -> usize {
    let (mut depth, mut deepest) = (0_usize, 0_usize);
    for character in text.chars() {
        match character {
            '[' | '{' => {
                depth += 1;
                deepest = deepest.max(depth);
            }
            ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    deepest
}

/// The first character of `text` a line-by-line reading cannot read as
/// YAML does: a control character but the tab and a line feed (and a
/// carriage return before one), or a line break YAML reads (U+0085,
/// U+2028, U+2029).
fn unread_character(text: &str) -> Option<char> {
    let mut characters = text.chars().peekable();
    while let Some(character) = characters.next() {
        let unread = match character {
            '\t' | '\n' => false,
            '\r' => characters.peek() != Some(&'\n'),
            '\u{85}' | '\u{2028}' | '\u{2029}' => true,
            other => other.is_control(),
        };
        if unread {
            return Some(character);
        }
    }
    None
}

/// What the key the reader is in holds so far.
#[derive(Debug, Default)]
enum Open {
    /// No key, or a key whose value ended on its line.
    #[default]
    Closed,
    /// A key this reader does not read: the lines indented under it are
    /// its value's.
    Skipped,
    /// A field whose value ended on its line.
    Ended { field: usize, text: Option<String> },
    /// A field alone on its line: `- item` lines may follow.
    Items {
        field: usize,
        items: Vec<String>,
        indent: Option<usize>,
    },
    /// A field's plain text, which indented lines continue.
    Plain {
        field: usize,
        text: String,
        blanks: usize,
        ended: bool,
    },
    /// A field's block scalar: its header, then its lines as written.
    Block {
        field: usize,
        header: BlockHeader,
        lines: Vec<String>,
    },
}

#[derive(Debug, Default)]
struct Reader {
    fields: Fields,
    open: Open,
}

impl Reader {
    /// Reads `line`, the line `number` of the frontmatter.
    fn line(&mut self, number: usize, line: &str) -> Result<(), String> {
        if line.trim().is_empty() {
            self.blank();
            return Ok(());
        }
        if line.starts_with(char::is_whitespace) {
            return self.indented(line);
        }
        if line.starts_with('#') {
            return self.comment();
        }
        if is_item(line) {
            return self.item(line);
        }
        self.finish(false)?;
        self.key_line(number, line)
    }

    fn blank(&mut self) {
        match &mut self.open {
            Open::Plain { blanks, ended, .. } if !*ended => *blanks += 1,
            Open::Block { lines, .. } => lines.push(String::new()),
            Open::Closed
            | Open::Skipped
            | Open::Ended { .. }
            | Open::Items { .. }
            | Open::Plain { .. } => {}
        }
    }

    /// A comment at the top level ends a text, a list going on after it.
    fn comment(&mut self) -> Result<(), String> {
        match &mut self.open {
            Open::Plain { ended, .. } => *ended = true,
            Open::Block { .. } => self.finish(false)?,
            Open::Closed | Open::Skipped | Open::Ended { .. } | Open::Items { .. } => {}
        }
        Ok(())
    }

    /// A line indented under the key the reader is in.
    fn indented(&mut self, line: &str) -> Result<(), String> {
        let read = line.trim_start();
        let comment = read.starts_with('#');
        match &mut self.open {
            // Not a key: YAML refuses it, and it gives no field.
            Open::Closed | Open::Skipped => Ok(()),
            Open::Block { lines, .. } => {
                lines.push(line.to_owned());
                Ok(())
            }
            Open::Ended { .. } | Open::Items { .. } if comment => Ok(()),
            Open::Items { .. } if is_item(read) => self.item(line),
            Open::Ended { field, .. } | Open::Items { field, .. } => Err(unread(*field)),
            Open::Plain {
                field,
                text,
                blanks,
                ended,
            } => continue_plain(*field, text, blanks, ended, read),
        }
    }

    /// A `- item` line, under a key alone on its line.
    fn item(&mut self, line: &str) -> Result<(), String> {
        let indent = line.len() - line.trim_start().len();
        match &mut self.open {
            Open::Skipped => Ok(()),
            Open::Items {
                field,
                items,
                indent: first,
            } => {
                // YAML folds an item indented otherwise into another value.
                if first.is_some_and(|first| first != indent) {
                    return Err(unread(*field));
                }
                *first = Some(indent);
                let text = line.trim_start().get(1..).unwrap_or_default().trim();
                match list_item(text) {
                    Item::Text(item) => items.push(item),
                    Item::Nothing => {}
                    Item::Unread => return Err(unread(*field)),
                }
                Ok(())
            }
            Open::Closed | Open::Ended { .. } | Open::Plain { .. } | Open::Block { .. } => {
                Err("the frontmatter holds an item under no key alone on its line".to_owned())
            }
        }
    }

    /// A top-level `key: value` line.
    fn key_line(&mut self, number: usize, line: &str) -> Result<(), String> {
        let Some((key, value)) = key_and_value(line) else {
            return Err(format!(
                "the frontmatter's line {number} is no key a line-by-line reading can name"
            ));
        };
        if GATING_KEYS.contains(&key) {
            return Err(format!(
                "the frontmatter declares {key}, which this implementation does not read: \
                 a skill that gates or requires tools is refused"
            ));
        }
        let value = value.trim();
        if matches!(
            key,
            "disable-model-invocation" | "user-invocable" | "context" | "agent"
        ) {
            return Err(format!(
                "the frontmatter declares unsupported invocation policy: {key}"
            ));
        }
        if value.starts_with(['&', '!']) {
            return Err(format!(
                "the frontmatter's {key} holds an anchor or a tag, which a line-by-line \
                 reading does not read"
            ));
        }
        let Some(field) = FIELDS.iter().position(|field| *field == key) else {
            self.open = Open::Skipped;
            return Ok(());
        };
        self.open = open_value(field, value)?;
        Ok(())
    }

    /// Ends the value the reader is in, recording its field; `at_end` when
    /// the frontmatter ends there, with no line break after its last line.
    fn finish(&mut self, at_end: bool) -> Result<(), String> {
        let (field, value) = match std::mem::take(&mut self.open) {
            Open::Closed | Open::Skipped => return Ok(()),
            Open::Ended { field, text } => (field, text),
            Open::Items { field, items, .. } => {
                (field, (!items.is_empty()).then(|| items.join(", ")))
            }
            Open::Plain { field, text, .. } => {
                (field, (!NULLS.contains(&text.as_str())).then_some(text))
            }
            Open::Block {
                field,
                header,
                lines,
            } => {
                let text = block_text(header, lines, at_end).ok_or_else(|| unread(field))?;
                (field, Some(text))
            }
        };
        if let Some(slot) = self.fields.slot(field) {
            *slot = value;
        }
        Ok(())
    }
}

/// The refusal of a field this reading cannot read whole.
fn unread(field: usize) -> String {
    let name = FIELDS.get(field).copied().unwrap_or_default();
    format!("the frontmatter's {name} cannot be read line by line")
}

/// Whether `line` is a YAML block sequence's item: `-` alone or before a
/// space.
fn is_item(line: &str) -> bool {
    line == "-" || line.starts_with("- ") || line.starts_with("-\t")
}

/// The key a top-level `line` names and the text after its colon: a key
/// of word characters and hyphens, bare or in quotes, as YAML names one
/// without an escape. `None` for any other line.
fn key_and_value(line: &str) -> Option<(&str, &str)> {
    let quote = line.chars().next().filter(|c| *c == '"' || *c == '\'');
    let (key, after) = if let Some(quote) = quote {
        line.get(1..)?.split_once(quote)?
    } else {
        let end = line
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '-'))
            .unwrap_or(line.len());
        (line.get(..end)?, line.get(end..)?)
    };
    let starts_as_a_word = key
        .chars()
        .next()
        .is_some_and(|c| c.is_alphanumeric() || c == '_');
    let a_word = key
        .chars()
        .all(|c| c.is_alphanumeric() || c == '_' || c == '-');
    let value = after.trim_start().strip_prefix(':')?;
    (starts_as_a_word && a_word).then_some((key, value))
}

/// The value a field's key line opens, from what follows its colon.
fn open_value(field: usize, value: &str) -> Result<Open, String> {
    let refused = || unread(field);
    let plain = without_comment(value).trim();
    if plain.is_empty() {
        return Ok(Open::Items {
            field,
            items: Vec::new(),
            indent: None,
        });
    }
    if value.starts_with(['|', '>']) {
        let header = block_header(value).ok_or_else(refused)?;
        return Ok(Open::Block {
            field,
            header,
            lines: Vec::new(),
        });
    }
    if value.starts_with(['"', '\'']) {
        return quoted_whole(value)
            .map(|text| text_value(field, Some(text)))
            .ok_or_else(refused);
    }
    if value.starts_with('[') {
        let items = flow_list(plain).ok_or_else(refused)?;
        let text = (!items.is_empty()).then(|| items.join(", "));
        return Ok(text_value(field, text));
    }
    if not_a_plain_text(value) {
        return Err(refused());
    }
    Ok(Open::Plain {
        field,
        text: plain.to_owned(),
        blanks: 0,
        // A comment ends the text: a line after it is YAML's error.
        ended: without_comment(value).len() < value.len(),
    })
}

/// A field whose value ended on its line.
fn text_value(field: usize, text: Option<String>) -> Open {
    Open::Ended { field, text }
}

/// A plain text's line `next`, indented under it, folded into `text` as
/// YAML folds a plain scalar: one space for a line break, a line feed for
/// each blank line.
fn continue_plain(
    field: usize,
    text: &mut String,
    blanks: &mut usize,
    ended: &mut bool,
    next: &str,
) -> Result<(), String> {
    if *ended {
        return Err(unread(field));
    }
    let line = without_comment(next);
    let read = line.trim();
    if read.is_empty() {
        // A comment line ends the text.
        *ended = true;
        return Ok(());
    }
    if read.contains(": ") || read.ends_with(':') {
        return Err(unread(field));
    }
    if *blanks == 0 {
        text.push(' ');
    } else {
        text.push_str(&"\n".repeat(*blanks));
    }
    text.push_str(read);
    *blanks = 0;
    // A comment ends the text: a line after it is YAML's error.
    *ended = line.len() < next.len();
    Ok(())
}

#[cfg(test)]
mod tests;
