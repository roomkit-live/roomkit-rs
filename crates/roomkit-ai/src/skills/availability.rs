//! Registry visibility and unavailable skills' retained gates (RFC §24.3).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::model::{Skill, SkillMetadata};
use super::registry::SkillRegistry;
use super::texts::escaped;

/// Marks preserve insertion order in the model-facing manifest.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Marks {
    unavailable: Vec<(String, String)>,
    closed: HashMap<String, Vec<String>>,
    unlisted: HashSet<String>,
}

impl Marks {
    pub(super) fn clear(&mut self, name: &str) {
        self.unavailable.retain(|(held, _)| held != name);
        self.closed.remove(name);
        self.unlisted.remove(name);
    }

    pub(super) fn subset(&self, names: Option<&[String]>, skills: &[Skill]) -> Self {
        let wanted = |name: &str| names.is_none_or(|names| names.iter().any(|held| held == name));
        Self {
            unavailable: self
                .unavailable
                .iter()
                .filter(|(name, _)| wanted(name))
                .cloned()
                .collect(),
            closed: self
                .closed
                .iter()
                .filter(|(name, _)| wanted(name))
                .map(|(name, gates)| (name.clone(), gates.clone()))
                .collect(),
            unlisted: self
                .unlisted
                .iter()
                .filter(|name| skills.iter().any(|skill| skill.name() == name.as_str()))
                .cloned()
                .collect(),
        }
    }

    pub(super) fn has_entries(&self) -> bool {
        !self.unavailable.is_empty()
    }

    pub(super) fn manifest(&self, skills: &[Skill]) -> String {
        let listed: Vec<_> = skills
            .iter()
            .filter(|skill| !self.unlisted.contains(skill.name()))
            .collect();
        let mut lines = Vec::new();
        if !listed.is_empty() {
            lines.push("<available_skills>".to_owned());
            for skill in listed {
                available_lines(&mut lines, skill);
            }
            lines.push("</available_skills>".to_owned());
        }
        if !self.unavailable.is_empty() {
            lines.push("<unavailable_skills>".to_owned());
            for (name, reason) in &self.unavailable {
                lines.push(format!("  <skill name=\"{}\">", escaped(name)));
                lines.push(format!("    <reason>{}</reason>", escaped(reason)));
                lines.push("  </skill>".to_owned());
            }
            lines.push("</unavailable_skills>".to_owned());
        }
        lines.join("\n")
    }
}

fn available_lines(lines: &mut Vec<String>, skill: &Skill) {
    let metadata = &skill.metadata;
    lines.push(format!("  <skill name=\"{}\">", escaped(&metadata.name)));
    let fields = [
        ("description", Some(metadata.description.as_str())),
        ("license", metadata.license.as_deref()),
        ("compatibility", metadata.compatibility.as_deref()),
    ];
    for (tag, value) in fields {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            lines.push(format!("    <{tag}>{}</{tag}>", escaped(value)));
        }
    }
    lines.push("  </skill>".to_owned());
}

impl SkillRegistry {
    /// Closed patterns are opened only by the identical pattern of an active
    /// available skill, never merely by an overlapping glob (RFC §24.2).
    pub(crate) fn closed_patterns(&self, activated: &HashSet<String>) -> HashSet<String> {
        let opened: HashSet<String> = self
            .all_metadata()
            .into_iter()
            .filter(|metadata| activated.contains(&metadata.name))
            .flat_map(SkillMetadata::gated_tool_names)
            .collect();
        self.marks
            .closed
            .values()
            .flatten()
            .filter(|pattern| !opened.contains(*pattern))
            .cloned()
            .collect()
    }

    pub(crate) fn gated_patterns(&self, activated: &HashSet<String>) -> HashSet<String> {
        let mut patterns = self.closed_patterns(activated);
        patterns.extend(
            self.all_metadata()
                .into_iter()
                .filter(|metadata| !activated.contains(&metadata.name))
                .flat_map(SkillMetadata::gated_tool_names),
        );
        patterns
    }

    pub(crate) fn unopenable_patterns(&self) -> HashSet<String> {
        self.closed_patterns(&self.skill_names().into_iter().map(str::to_owned).collect())
    }

    /// Remove a skill from available reads while retaining its reason and
    /// gated patterns (RFC §24.3). Repeating the mark keeps those patterns.
    pub fn mark_unavailable(&mut self, name: &str, reason: &str) {
        if let Some(metadata) = self.get_metadata(name) {
            let gates = metadata.gated_tool_names();
            if !gates.is_empty() {
                self.marks.closed.insert(name.to_owned(), gates);
            }
        }
        Arc::make_mut(&mut self.skills).retain(|skill| skill.name() != name);
        if let Some((_, held)) = self
            .marks
            .unavailable
            .iter_mut()
            .find(|(held, _)| held == name)
        {
            reason.clone_into(held);
        } else {
            self.marks
                .unavailable
                .push((name.to_owned(), reason.to_owned()));
        }
    }

    /// Hide a registered skill from the manifest, preserving activation
    /// and all ordinary skill reads; ignore unknown names (RFC §24.3).
    pub fn mark_unlisted(&mut self, name: &str) {
        if self.get_metadata(name).is_some() {
            self.marks.unlisted.insert(name.to_owned());
        }
    }

    /// Available skills advertised in the manifest, in registration order.
    pub fn listed_names(&self) -> Vec<&str> {
        self.skill_names()
            .into_iter()
            .filter(|name| !self.marks.unlisted.contains(*name))
            .collect()
    }

    /// The reason a skill is unavailable here (RFC §24.3).
    pub fn get_unavailable_reason(&self, name: &str) -> Option<&str> {
        self.marks
            .unavailable
            .iter()
            .find(|(held, _)| held == name)
            .map(|(_, reason)| reason.as_str())
    }

    /// A detached name-to-reason snapshot of unavailable skills (RFC §24.3).
    pub fn unavailable_skills(&self) -> HashMap<String, String> {
        self.marks.unavailable.iter().cloned().collect()
    }
}
