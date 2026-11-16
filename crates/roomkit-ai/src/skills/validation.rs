//! Metadata admission for directory and in-memory skills.

use super::frontmatter::Fields;
use super::model::SkillMetadata;
use super::texts::quoted_name;

/// The most characters a directory skill's name takes (RFC §24.1).
const NAME_MAX: usize = 64;
/// The most characters a directory skill's description takes.
const DESCRIPTION_MAX: usize = 1024;

/// What is wrong with `fields`, the frontmatter of the skill in the
/// directory `label`, in the reference implementation's words and order.
pub(super) fn invalid(fields: &Fields, label: &str) -> Vec<String> {
    let mut reasons = pattern_errors(fields);
    let name = fields.name.as_deref().unwrap_or_default();
    let characters = name.chars().count();
    if name.is_empty() {
        reasons.push("Missing required field: name".to_owned());
    } else if !is_kebab_case(name) {
        reasons.push(format!(
            "Invalid name {}: must be kebab-case (lowercase alphanumeric + hyphens)",
            quoted_name(name)
        ));
    } else if characters > NAME_MAX {
        reasons.push(format!(
            "Name too long: {characters} chars (max {NAME_MAX})"
        ));
    }
    if !name.is_empty() && name != label {
        reasons.push(format!(
            "Name {} does not match directory name {}",
            quoted_name(name),
            quoted_name(label)
        ));
    }
    let description = fields.description.as_deref().unwrap_or_default();
    let characters = description.chars().count();
    if description.is_empty() {
        reasons.push("Missing required field: description".to_owned());
    } else if characters > DESCRIPTION_MAX {
        reasons.push(format!(
            "Description too long: {characters} chars (max {DESCRIPTION_MAX})"
        ));
    }
    reasons
}

/// Bracketed patterns would not match as character classes.
fn pattern_errors(fields: &Fields) -> Vec<String> {
    bracket_errors([
        (
            "allowed_tools",
            fields
                .allowed_tools
                .as_ref()
                .map(super::SkillToolNames::names)
                .unwrap_or_default(),
        ),
        (
            "requires",
            fields
                .requires
                .as_ref()
                .map(super::SkillToolNames::names)
                .unwrap_or_default(),
        ),
    ])
}

/// In-memory metadata has no directory naming or description-length constraint.
pub(super) fn memory_errors(metadata: &SkillMetadata) -> Vec<String> {
    if metadata.name.contains(':') {
        return vec![format!(
            "Skill name {} holds ':', which names a served skill",
            quoted_name(&metadata.name)
        )];
    }
    bracket_errors([
        ("allowed_tools", metadata.gated_tool_names()),
        ("requires", metadata.required_tool_names()),
    ])
}

fn bracket_errors(fields: [(&str, Vec<String>); 2]) -> Vec<String> {
    let mut reasons = Vec::new();
    for (key, names) in fields {
        for pattern in names.into_iter().filter(|name| name.contains(['[', ']'])) {
            reasons.push(format!(
                "{key}: tool pattern {} holds a bracket: a pattern reads * and ? alone",
                quoted_name(&pattern),
            ));
        }
    }
    reasons
}

/// Whether `name` is lowercase ASCII letters and digits in runs a hyphen
/// separates.
pub(super) fn is_kebab_case(name: &str) -> bool {
    name.split('-').all(|run| {
        !run.is_empty()
            && run
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    })
}
