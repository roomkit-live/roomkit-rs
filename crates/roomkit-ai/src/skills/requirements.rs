//! Prerequisites read the admitted catalogue, before ordinary skill gates.

use std::collections::HashSet;

use serde_json::json;

use super::{RequiresMatch, SkillMetadata, SkillRegistry, texts};
use crate::tools::{ToolContext, ToolError, glob_matches};

/// Return unsatisfied skill prerequisites in declaration order (RFC §24.3).
///
/// Only admitted names matching no closed pattern are offered to the matcher.
/// Duplicate prerequisites remain repeated. Pass `&[]` and `None` for Python's
/// defaults; `Some(matcher)` uses the host's existing `RequiresMatch`.
///
/// # Errors
///
/// Propagates the first matcher error unchanged without checking further names.
pub fn missing_required_tools(
    metadata: &SkillMetadata,
    admitted: impl IntoIterator<Item = impl AsRef<str>>,
    closed: &[String],
    r#match: Option<&RequiresMatch>,
) -> Result<Vec<String>, ToolError> {
    let offered: HashSet<String> = admitted
        .into_iter()
        .map(|name| name.as_ref().to_owned())
        .filter(|name| !closed.iter().any(|pattern| glob_matches(pattern, name)))
        .collect();
    let mut missing = Vec::new();
    for name in metadata.required_tool_names() {
        let served = match r#match {
            Some(matcher) => matcher.call(&name, &offered)?,
            None => offered.contains(&name),
        };
        if !served {
            missing.push(name);
        }
    }
    Ok(missing)
}

pub(crate) fn check(
    registry: &SkillRegistry,
    metadata: &SkillMetadata,
    context: &ToolContext,
    room_active: Vec<String>,
) -> Result<(), ToolError> {
    let Some(admitted) = &context.skill_admitted else {
        return Ok(());
    };
    let mut activated = context.skill_activated.clone();
    activated.extend(room_active);
    activated.insert(metadata.name.clone());
    let closed: Vec<String> = registry.closed_tool_names(&activated).into_iter().collect();
    let missing =
        missing_required_tools(metadata, admitted, &closed, Some(&registry.requires_match))?;
    if missing.is_empty() {
        return Ok(());
    }
    Err(ToolError::Refused(texts::object(&[(
        "error",
        json!(format!(
            "Required tools not available: {}",
            missing.join(", ")
        )),
    )])))
}
