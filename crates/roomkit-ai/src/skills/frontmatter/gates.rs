//! A gating value is kept whole and typed, or refused (RFC §24.1).

use super::scalars::{
    Item, NULLS, flow_list_with, list_item, not_a_plain_text, quoted_whole, without_comment,
};
use crate::skills::SkillToolNames;

#[derive(Debug)]
pub(super) struct Gates {
    pub(super) key: &'static str,
    value: Option<SkillToolNames>,
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
                flow_list_with(plain, gate_item).ok_or_else(|| unread(key))?,
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
/// values stay text; ambiguous unquoted numbers, dates and booleans are
/// refused instead of approximating YAML's scalar conversion (RFC §24.1).
fn gate_item(text: &str) -> Item {
    if !text.starts_with(['"', '\'']) {
        let plain = without_comment(text).trim();
        let lower = plain.to_ascii_lowercase();
        let scalar = matches!(
            lower.as_str(),
            "yes" | "no" | "true" | "false" | "on" | "off"
        );
        if scalar || numeric_scalar(plain) {
            return Item::Unread;
        }
    }
    list_item(text)
}

/// Conservative around YAML's numeric/date forms, without treating tool
/// names such as `3d_render` or `123abc*` as numbers.
fn numeric_scalar(text: &str) -> bool {
    let text = text.trim_start_matches(['+', '-']).replace('_', "");
    if matches!(text.to_ascii_lowercase().as_str(), ".inf" | ".nan") {
        return true;
    }
    if !text.starts_with(|c: char| c.is_ascii_digit() || c == '.') {
        return false;
    }
    if text.parse::<f64>().is_ok() {
        return true;
    }
    for (prefix, radix) in [("0x", 16), ("0b", 2)] {
        if let Some(digits) = text.strip_prefix(prefix) {
            return !digits.is_empty() && digits.chars().all(|c| c.is_digit(radix));
        }
    }
    // Sexagesimal numbers and ISO dates/timestamps use only this alphabet.
    text.contains([':', '-'])
        && text
            .chars()
            .all(|c| c.is_ascii_digit() || ":-.+ tTzZ".contains(c))
}

fn unread(key: &str) -> String {
    format!("the frontmatter's {key} cannot be read line by line")
}
