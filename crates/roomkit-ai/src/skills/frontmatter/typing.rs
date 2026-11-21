//! A plain scalar as the registry reads it, as Python's does: typed by
//! YAML 1.1 (`SafeLoader`), then written by `str()`, so `true` is kept as
//! `True` and `0x10` as `16` (RFC §24.1, RMK-1579); untyped in a
//! frontmatter Python reads line by line, which YAML does not read.

use super::scalars::{Item, NULLS, list_item};
use crate::skills::served::{PythonText, python_text};

/// Whether plain scalars are typed, and whether one YAML constructs no
/// value from was met.
#[derive(Debug, Default)]
pub(super) struct Typing {
    /// Plain scalars are read as Python's YAML reading writes them.
    pub(super) typed: bool,
    /// A plain scalar YAML constructs no value from was met: Python reads
    /// this frontmatter line by line, each value as written.
    pub(super) not_yaml: bool,
}

impl Typing {
    /// The value of the plain text `text`, whole: `None` for a null.
    pub(super) fn plain(&mut self, text: String) -> Option<String> {
        if !self.typed {
            return (!NULLS.contains(&text.as_str())).then_some(text);
        }
        match python_text(&text) {
            PythonText::Null => None,
            PythonText::Text(typed) => Some(typed),
            PythonText::NotYaml => {
                self.not_yaml = true;
                Some(text)
            }
        }
    }

    /// A list's item (a `- item` line's, or a flow list's entry), its
    /// quotes and comment taken off, typed when it is plain.
    pub(super) fn item(&mut self, text: &str) -> Item {
        match list_item(text) {
            Item::Text(item) if !text.starts_with(['"', '\'']) => {
                self.plain(item).map_or(Item::Nothing, Item::Text)
            }
            read @ (Item::Text(_) | Item::Nothing | Item::Unread) => read,
        }
    }
}
