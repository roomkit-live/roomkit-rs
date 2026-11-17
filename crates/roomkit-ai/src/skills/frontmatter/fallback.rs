//! Python's line fallback for the two non-YAML plain-scalar forms the local
//! grammar accepts: an unquoted colon before whitespace, or a leading glob.

use std::collections::HashSet;

use roomkit::model::Metadata;
use serde_json::Value;

use super::gates::Gates;
use super::scalars::{Item, list_item, without_comment};
use super::{SkillToolNames, is_item, key_and_value};
use crate::skills::parse_tool_names;

pub(super) fn needed(text: &str) -> bool {
    let mut block = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let value = if !line.starts_with(char::is_whitespace) && !is_item(line) {
            let Some((_, value)) = key_and_value(line) else {
                continue;
            };
            let value = value.trim();
            block = value.starts_with(['|', '>']);
            value
        } else if block {
            continue;
        } else {
            trimmed.strip_prefix("- ").unwrap_or(trimmed).trim()
        };
        let value = without_comment(value).trim();
        if let Some(items) = value.strip_prefix('[').and_then(|v| v.strip_suffix(']')) {
            if items.split(',').map(str::trim).any(plain_fallback) {
                return true;
            }
        } else if plain_fallback(value) {
            return true;
        }
    }
    false
}

fn plain_fallback(value: &str) -> bool {
    !value.starts_with(['\'', '"', '|', '>'])
        && (value.starts_with('*')
            || value.ends_with(':')
            || value.contains(": ")
            || value.contains(":\t"))
}

/// The local reader has already checked all keys, their nesting and each
/// gating value. Re-read their values with Python's fallback projection.
pub(super) fn read(text: &str) -> Result<Metadata, String> {
    let mut fields = Metadata::new();
    let mut open = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        if is_item(trimmed)
            && let Some(key) = open
        {
            append_item(
                &mut fields,
                key,
                trimmed.strip_prefix('-').unwrap_or_default().trim(),
            );
            continue;
        }
        open = None;
        if !line.starts_with(char::is_whitespace)
            && let Some((key, raw)) = key_and_value(line)
        {
            let value = raw.trim();
            match key {
                "allowed_tools" | "requires" => {
                    let key = if key == "requires" {
                        "requires"
                    } else {
                        "allowed_tools"
                    };
                    if without_comment(value).trim().is_empty() {
                        open = Some(key);
                    } else {
                        add_gate(&mut fields, Gates::read(key, value)?);
                    }
                }
                _ if value.is_empty() => {
                    fields.shift_remove(key);
                    open = Some(key);
                }
                _ => {
                    fields.insert(key.to_owned(), Value::from(value.trim_matches(['\'', '"'])));
                }
            }
        }
    }
    Ok(fields)
}

fn append_item(fields: &mut Metadata, key: &str, text: &str) {
    let held = fields
        .entry(key)
        .or_insert_with(|| Value::Array(Vec::new()));
    if !held.is_array() {
        *held = Value::from(parse_tool_names(held));
    }
    if let (Some(items), Item::Text(item)) = (held.as_array_mut(), list_item(text))
        && !item.is_empty()
    {
        items.push(Value::String(item));
    }
}

fn add_gate(fields: &mut Metadata, gate: Gates) {
    let mut value = match gate.value {
        Some(SkillToolNames::Text(text)) => Value::String(text),
        Some(SkillToolNames::List(mut items)) => {
            items.retain(|item| !item.is_empty());
            Value::from(items)
        }
        None => Value::Array(Vec::new()),
    };
    if let Some(earlier) = fields.get(gate.key) {
        let mut seen = HashSet::new();
        let mut names = parse_tool_names(earlier);
        names.extend(parse_tool_names(&value));
        names.retain(|name| seen.insert(name.clone()));
        value = Value::from(names);
    }
    fields.insert(gate.key.to_owned(), value);
}
