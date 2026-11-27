//! A gating value is kept whole and typed, or refused (RFC §24.1).

use super::scalars::{
    Item, NULLS, flow_list_with, list_item, not_a_plain_text, quoted_whole, without_comment,
};
use crate::skills::SkillToolNames;
use crate::skills::served::typed_by_yaml;

#[derive(Debug)]
pub(super) struct Gates {
    pub(super) key: &'static str,
    pub(super) value: Option<SkillToolNames>,
    open: bool,
    indent: Option<usize>,
}

impl Gates {
    pub(super) fn read(key: &'static str, value: &str) -> Result<Self, String> {
        let plain = without_comment(value).trim();
        let open = plain.is_empty();
        let value = if open || NULLS.contains(&plain) {
            None
        } else if value.starts_with(['"', '\'']) {
            Some(SkillToolNames::Text(
                quoted_whole(value).ok_or_else(|| unread(key))?,
            ))
        } else if value.starts_with('[') {
            Some(SkillToolNames::List(
                flow_list_with(plain, flow_gate_item).ok_or_else(|| unread(key))?,
            ))
        } else if not_a_plain_text(value) || value.starts_with(['|', '>', '&', '!']) {
            return Err(unread(key));
        } else {
            match gate_item(value) {
                Item::Text(text) => Some(SkillToolNames::Text(text)),
                Item::Nothing => None,
                Item::Unread => return Err(unread(key)),
            }
        };
        Ok(Self {
            key,
            value,
            open,
            indent: None,
        })
    }

    pub(super) fn line(&mut self, line: &str) -> Result<(), String> {
        let text = line.trim_start();
        if text.starts_with('#') {
            return Ok(());
        }
        let indent = line.len() - text.len();
        if !self.open || !super::is_item(text) || self.indent.is_some_and(|held| held != indent) {
            return Err(unread(self.key));
        }
        self.indent = Some(indent);
        let item = gate_item(text.get(1..).unwrap_or_default().trim());
        let value = self
            .value
            .get_or_insert_with(|| SkillToolNames::List(Vec::new()));
        let SkillToolNames::List(items) = value else {
            return Err(unread(self.key));
        };
        match item {
            Item::Text(item) => items.push(item),
            Item::Nothing => {}
            Item::Unread => return Err(unread(self.key)),
        }
        Ok(())
    }

    pub(super) fn finish(self, held: &mut Option<SkillToolNames>) {
        if let Some(value) = self.value {
            match held {
                Some(earlier) => earlier.merge(&value),
                None => *held = Some(value),
            }
        }
    }
}

/// Never mistake a YAML scalar's spelling for the name it gates. Quoted
/// values stay text; an unquoted one YAML types (a boolean, a number, a
/// date) is refused, as Python's readers refuse it, and so is a text YAML
/// reads as a key (RFC §24.1). A plain text YAML keeps as written (`yEs`,
/// `08`, `1e5`, `0:30`) gates that name.
fn gate_item(text: &str) -> Item {
    if !text.starts_with(['"', '\'']) {
        let plain = without_comment(text).trim();
        if typed_by_yaml(plain) || holds_a_key(plain) {
            return Item::Unread;
        }
    }
    list_item(text)
}

/// An entry of a `[a, b]` list, read as [`gate_item`] reads an item. One
/// starting with `?` is refused: YAML reads it there as an explicit key
/// (`[lookup, ?admin]` holds a mapping), where a block item `- ?admin` is
/// the text it is.
fn flow_gate_item(text: &str) -> Item {
    if text.starts_with('?') {
        return Item::Unread;
    }
    gate_item(text)
}

/// Whether YAML reads a key in `plain`, an unquoted text: a colon before a
/// space, a tab or its end. Read as one name, `issue_refund: money` would
/// gate a name no tool has and leave `issue_refund` open.
fn holds_a_key(plain: &str) -> bool {
    plain.ends_with(':') || plain.contains(": ") || plain.contains(":\t")
}

fn unread(key: &str) -> String {
    format!("the frontmatter's {key} cannot be read line by line")
}
