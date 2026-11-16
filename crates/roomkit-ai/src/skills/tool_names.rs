//! The two values of `SkillMetadata.allowed_tools` (RFC §24.2).

use serde_json::Value;

/// Parse skill tool names or patterns, preserving order and duplicates (RFC §24.2).
///
/// Text is comma-separated, optionally wrapped in brackets. Array entries are
/// converted to Python-style text before trimming; other values yield no names.
/// Python tuples use arrays here. Convert non-JSON Python objects to their
/// Python text before constructing an array (HITL 63fe8e98).
pub fn parse_tool_names(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => text_names(text),
        Value::Array(items) => names(items.iter().map(|item| {
            item.as_str()
                .map_or_else(|| crate::memory::repr::repr(item), str::to_owned)
        })),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Object(_) => Vec::new(),
    }
}

fn strip(text: &str) -> &str {
    text.trim_matches(|c: char| c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c))
}

fn text_names(text: &str) -> Vec<String> {
    let text = strip(text);
    let inner = text
        .strip_prefix('[')
        .and_then(|text| text.strip_suffix(']'));
    let text = inner
        .filter(|inner| !inner.contains(['[', ']']))
        .unwrap_or(text);
    names(text.split(','))
}

fn names(entries: impl IntoIterator<Item = impl AsRef<str>>) -> Vec<String> {
    entries
        .into_iter()
        .filter_map(|entry| {
            let name = strip(strip(entry.as_ref()).trim_matches(['\'', '"']));
            (!name.is_empty()).then(|| name.to_owned())
        })
        .collect()
}

/// Python's text-or-list value, retained without changing its public shape.
/// `None` on [`super::SkillMetadata::allowed_tools`] means no value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkillToolNames {
    /// A comma-separated text, optionally enclosed in one pair of brackets.
    Text(String),
    /// Individual names or glob patterns, in their original order.
    List(Vec<String>),
}

impl SkillToolNames {
    pub(crate) fn names(&self) -> Vec<String> {
        match self {
            Self::List(entries) => names(entries),
            Self::Text(text) => text_names(text),
        }
    }

    /// Repeated keys cannot erase an earlier gate (RFC §24.1).
    pub(crate) fn merge(&mut self, next: &Self) {
        let mut names = self.names();
        for name in next.names() {
            if !names.contains(&name) {
                names.push(name);
            }
        }
        *self = Self::List(names);
    }
}
