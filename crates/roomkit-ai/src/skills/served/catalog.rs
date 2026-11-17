//! Shared server entries and retirement counters; registry projections commit separately.

use super::{ServedOrigin, SkillEntry, parse_entry};
use crate::skills::registry::RegistryState;
use crate::{Skill, SkillMetadata};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub(crate) struct Catalog {
    pub(crate) origins: HashMap<String, Arc<ServedOrigin>>,
    pub(crate) entries: HashMap<String, SkillEntry>,
    pub(crate) labels: Vec<(String, String)>,
    pub(crate) retirements: HashMap<String, u64>,
}

#[derive(Clone, Debug)]
pub(crate) struct Listed {
    pub(crate) name: String,
    pub(crate) entry: Option<SkillEntry>,
    pub(crate) metadata: Option<SkillMetadata>,
    pub(crate) reason: Option<String>,
}

impl Catalog {
    pub(crate) fn has_servers(&self) -> bool {
        !self.origins.is_empty()
    }
    pub(crate) fn label_of<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        self.labels
            .iter()
            .find(|(held, _)| held == name)
            .map(|(_, label)| label.as_str())
            .or_else(|| {
                name.split_once(':')
                    .map(|(label, _)| label)
                    .filter(|label| self.origins.contains_key(*label))
            })
    }
    pub(crate) fn names_of(&self, label: &str) -> Vec<String> {
        self.labels
            .iter()
            .filter(|(_, held)| held == label)
            .map(|(name, _)| name.clone())
            .collect()
    }
    pub(crate) fn record(&mut self, origin: Arc<ServedOrigin>, listed: &[Listed]) {
        for name in self.names_of(&origin.label) {
            self.entries.remove(&name);
        }
        self.labels.retain(|(_, label)| label != &origin.label);
        for item in listed {
            self.record_unlisted(item, &origin.label);
        }
        self.origins.insert(origin.label.clone(), origin);
    }
    pub(crate) fn record_unlisted(&mut self, item: &Listed, label: &str) {
        if !self.labels.iter().any(|(name, _)| name == &item.name) {
            self.labels.push((item.name.clone(), label.to_owned()));
        }
        if let Some(entry) = &item.entry {
            self.entries.insert(item.name.clone(), entry.clone());
        }
    }
    pub(crate) fn refusal(&self, name: &str) -> Option<String> {
        let entry = self.entries.get(name)?;
        let origin = self.origins.get(self.label_of(name)?)?;
        unavailable(entry, origin)
    }
}

impl RegistryState {
    pub(crate) fn record_served(&mut self, item: &Listed) {
        if let Some(metadata) = &item.metadata {
            let placeholder = Skill {
                metadata: metadata.clone(),
                instructions: String::new(),
                path: std::path::PathBuf::from("/dev/null"),
            };
            if let Some(held) = self
                .skills
                .iter_mut()
                .find(|skill| skill.name() == item.name)
            {
                *held = placeholder;
            } else {
                self.skills.push(placeholder);
            }
            self.marks
                .unavailable
                .retain(|(name, _)| name != &item.name);
        } else {
            self.skills.retain(|skill| skill.name() != item.name);
            self.loaded.remove(&item.name);
            self.marks
                .set_unavailable(&item.name, item.reason.as_deref().unwrap_or("unavailable"));
        }
    }
    pub(crate) fn remove_served(&mut self, names: &[String], offered: &[String]) {
        self.skills
            .retain(|skill| !names.iter().any(|name| name == skill.name()));
        self.marks
            .unavailable
            .retain(|(name, _)| !names.contains(name));
        self.marks.unlisted.retain(|name| !names.contains(name));
        self.loaded
            .retain(|name, _| !names.contains(name) || offered.contains(name));
    }
}

pub(crate) fn listed(origin: &ServedOrigin, index: usize, raw: &Value) -> Listed {
    let entry = match parse_entry(raw) {
        Ok(entry) => entry,
        Err(error) => {
            tracing::warn!(target:"roomkit::skills",%error,label=%origin.label,"malformed served entry");
            return Listed {
                name: format!("{}:#{index}", origin.label),
                entry: None,
                metadata: None,
                reason: Some("its server's entry is malformed".to_owned()),
            };
        }
    };
    let Some(name) = served_name(&origin.label, &entry) else {
        return Listed {
            name: format!("{}:#{index}", origin.label),
            entry: None,
            metadata: None,
            reason: Some("its path holds characters a name cannot".to_owned()),
        };
    };
    from_entry(origin, name, entry)
}

pub(crate) fn from_entry(origin: &ServedOrigin, name: String, entry: SkillEntry) -> Listed {
    let reason = unavailable(&entry, origin);
    let metadata = reason.is_none().then(|| metadata(&name, &entry));
    Listed {
        name,
        entry: Some(entry),
        metadata,
        reason,
    }
}

pub(crate) fn served_name(label: &str, entry: &SkillEntry) -> Option<String> {
    valid_path(entry.path()).then(|| format!("{label}:{}", entry.path()))
}
pub(crate) fn valid_path(path: &str) -> bool {
    let path = if let Some((scheme, path)) = path.split_once("://") {
        if !scheme.starts_with(|c: char| c.is_ascii_lowercase())
            || !scheme
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || "+.-".contains(c))
        {
            return false;
        }
        path
    } else {
        path
    };
    path.split('/').all(|segment| {
        !segment.is_empty()
            && segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "._~@+-".contains(c))
    })
}
pub(crate) fn valid_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 64
        && label
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "_.-".contains(c))
}
fn unavailable(entry: &SkillEntry, origin: &ServedOrigin) -> Option<String> {
    if entry.dynamic() && !origin.allow_dynamic {
        return Some("its server generates its content, so nothing of it can be verified, and this host does not take such skills".to_owned());
    }
    entry
        .limit_exceeded()
        .map(|reason| format!("it is past the limits of a skill served over MCP: {reason}"))
}
fn metadata(name: &str, entry: &SkillEntry) -> SkillMetadata {
    let mut metadata = SkillMetadata::new(name, entry.description().unwrap_or_default());
    for (key, value) in entry.frontmatter() {
        let text = frontmatter_text(value);
        match key.as_str() {
            "name" | "description" | "allowed_tools" => {}
            "license" => metadata.license = (!text.is_empty()).then_some(text),
            "compatibility" => metadata.compatibility = (!text.is_empty()).then_some(text),
            _ => {
                metadata
                    .extra_metadata
                    .insert(key.clone(), Value::String(text));
            }
        }
    }
    metadata
}
fn frontmatter_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Array(values) => values
            .iter()
            .map(super::json_tree::text_of)
            .collect::<Vec<_>>()
            .join(", "),
        Value::Bool(_) | Value::Number(_) | Value::String(_) | Value::Object(_) => {
            super::json_tree::text_of(value)
        }
    }
}
pub(crate) fn quoted(text: &str) -> String {
    roomkit_core::rules::transcript::quoted(text, 1100)
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
