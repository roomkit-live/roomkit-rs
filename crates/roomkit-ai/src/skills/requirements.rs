//! Prerequisites read the admitted catalogue, before ordinary skill gates.

use std::collections::HashSet;

use serde_json::json;

use super::{SkillMetadata, SkillRegistry, texts};
use crate::tools::{ToolContext, ToolError, glob_matches};

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
    let closed = registry.closed_patterns(&activated);
    let offered: HashSet<&String> = admitted
        .iter()
        .filter(|name| !closed.iter().any(|pattern| glob_matches(pattern, name)))
        .collect();
    let missing: Vec<String> = metadata
        .required_tool_names()
        .into_iter()
        .filter(|name| !offered.contains(name))
        .collect();
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
