//! Registry visibility and unavailable skills' retained gates (RFC §24.3).

use super::registry::lock;
use std::collections::{HashMap, HashSet};

use super::error::SkillError;
use super::model::Skill;
use super::registry::{MANIFEST_MAX, SkillRegistry};
use super::texts::escaped;

/// Marks preserve insertion order in the model-facing manifest.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Marks {
    pub(super) unavailable: Vec<(String, String)>,
    pub(super) closed: HashMap<String, Vec<String>>,
    pub(super) unlisted: HashSet<String>,
}

impl Marks {
    pub(super) fn set_unavailable(&mut self, name: &str, reason: &str) {
        if let Some((_, held)) = self.unavailable.iter_mut().find(|(held, _)| held == name) {
            reason.clone_into(held);
        } else {
            self.unavailable.push((name.to_owned(), reason.to_owned()));
        }
    }

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

    pub(super) fn check_manifest(&self, skills: &[Skill]) -> Result<(), SkillError> {
        let characters = self.manifest(skills).chars().count();
        if characters > MANIFEST_MAX {
            return Err(SkillError::ManifestTooLarge {
                characters,
                limit: MANIFEST_MAX,
            });
        }
        Ok(())
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
    let origin = metadata.name.split_once(':').map(|(label, _)| label);
    lines.push(match origin {
        Some(label) => format!(
            "  <skill name=\"{}\" origin=\"{}\">",
            escaped(&metadata.name),
            escaped(label)
        ),
        None => format!("  <skill name=\"{}\">", escaped(&metadata.name)),
    });
    let fields = [
        ("description", Some(metadata.description.as_str())),
        ("license", metadata.license.as_deref()),
        ("compatibility", metadata.compatibility.as_deref()),
    ];
    for (tag, value) in fields {
        if let Some(value) = value.filter(|value| !value.is_empty()) {
            let text = if origin.is_some() {
                crate::skills::served::catalog::quoted(value)
            } else {
                escaped(value)
            };
            lines.push(format!("    <{tag}>{text}</{tag}>"));
        }
    }
    lines.push("  </skill>".to_owned());
}

impl SkillRegistry {
    /// Closed patterns are opened only by the identical pattern of an active
    /// available skill, never merely by an overlapping glob (RFC §24.2).
    pub fn closed_tool_names(&self, activated: &HashSet<String>) -> HashSet<String> {
        let opened: HashSet<String> = self
            .all_metadata()
            .into_iter()
            .filter(|metadata| activated.contains(&metadata.name))
            .flat_map(|metadata| metadata.gated_tool_names())
            .collect();
        lock(&self.state)
            .marks
            .closed
            .values()
            .flatten()
            .filter(|pattern| !opened.contains(*pattern))
            .cloned()
            .collect()
    }

    /// Patterns retained by inactive or unavailable skills (RFC §24.2).
    /// Pass an empty set for Python's default activated=().
    pub fn gated_tool_names(&self, activated: &HashSet<String>) -> HashSet<String> {
        let mut patterns = self.closed_tool_names(activated);
        patterns.extend(
            self.all_metadata()
                .into_iter()
                .filter(|metadata| !activated.contains(&metadata.name))
                .flat_map(|metadata| metadata.gated_tool_names()),
        );
        patterns
    }

    /// Closed patterns no available skill could open (RFC §24.2).
    pub fn unopenable_tool_names(&self) -> HashSet<String> {
        self.closed_tool_names(&self.skill_names().into_iter().collect())
    }

    /// Remove a skill from available reads while retaining its reason and
    /// gated patterns (RFC §24.3). Repeating the mark keeps those patterns.
    ///
    /// # Errors
    /// Returns [`SkillError::ManifestTooLarge`] if the final escaped manifest
    /// would exceed 65,536 characters. The registry stays unchanged.
    pub fn mark_unavailable(&self, name: &str, reason: &str) -> Result<(), SkillError> {
        let mut held = lock(&self.state);
        let mut next = held.clone();
        if let Some(skill) = next.skills.iter().find(|skill| skill.name() == name) {
            let gates = skill.metadata.gated_tool_names();
            if !gates.is_empty() {
                next.marks.closed.insert(name.to_owned(), gates);
            }
        }
        next.skills.retain(|skill| skill.name() != name);
        next.loaded.remove(name);
        next.marks.set_unavailable(name, reason);
        next.marks.check_manifest(&next.skills)?;
        *held = next;
        Ok(())
    }

    /// Hide a registered skill from the manifest, preserving activation
    /// and all ordinary skill reads; ignore unknown names (RFC §24.3).
    pub fn mark_unlisted(&self, name: &str) {
        if self.get_metadata(name).is_some() {
            lock(&self.state).marks.unlisted.insert(name.to_owned());
        }
    }

    /// Available skills advertised in the manifest, in registration order.
    pub fn listed_names(&self) -> Vec<String> {
        let state = lock(&self.state);
        state
            .skills
            .iter()
            .filter(|skill| !state.marks.unlisted.contains(skill.name()))
            .map(|skill| skill.name().to_owned())
            .collect()
    }

    /// The reason a skill is unavailable here (RFC §24.3).
    pub fn get_unavailable_reason(&self, name: &str) -> Option<String> {
        lock(&self.state)
            .marks
            .unavailable
            .iter()
            .find(|(held, _)| held == name)
            .map(|(_, reason)| reason.clone())
    }

    /// A detached name-to-reason snapshot of unavailable skills (RFC §24.3).
    pub fn unavailable_skills(&self) -> HashMap<String, String> {
        lock(&self.state)
            .marks
            .unavailable
            .iter()
            .cloned()
            .collect()
    }
}
