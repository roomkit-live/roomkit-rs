//! A skill and its metadata (RFC §24.1, §24.2): Python's `Skill` and
//! `SkillMetadata`.

use std::path::PathBuf;

use serde_json::Value;

use super::SkillToolNames;
use roomkit::model::Metadata;

/// What a skill's `SKILL.md` frontmatter declares (RFC §24.2): Python's
/// `SkillMetadata(name, description, license=None, compatibility=None,
/// allowed_tools=None, extra_metadata={})`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillMetadata {
    /// Its name. Directory registration requires kebab-case, at most 64
    /// characters, matching its directory; in-memory addition reserves `:` only.
    pub name: String,
    /// What it does. Directory registration limits this to 1,024 characters.
    pub description: String,
    /// Its license, when its frontmatter names one.
    pub license: Option<String>,
    /// What it needs to run, as free text, when its frontmatter says.
    pub compatibility: Option<String>,
    /// Tool-name patterns kept closed until this skill is activated (RFC §24.2).
    pub allowed_tools: Option<SkillToolNames>,
    /// The frontmatter's other keys, each with its value as text, a list's
    /// items joined by commas; a key with no value, or one the line-by-line
    /// reading cannot read whole, is left out.
    pub extra_metadata: Metadata,
}

impl SkillMetadata {
    /// The metadata of the skill `name`, which does what `description`
    /// says, with no license, no compatibility and no other key.
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            license: None,
            compatibility: None,
            allowed_tools: None,
            extra_metadata: Metadata::new(),
        }
    }

    /// The patterns this skill gates, with empty entries and surrounding
    /// spaces/quotes removed, as Python's `gated_tool_names` (RFC §24.2).
    pub fn gated_tool_names(&self) -> Vec<String> {
        self.allowed_tools
            .as_ref()
            .map(SkillToolNames::names)
            .unwrap_or_default()
    }

    /// Exact prerequisite names from optional `requires` metadata (RFC §24.2).
    /// These describe dependencies, not permission grants.
    pub fn required_tool_names(&self) -> Vec<String> {
        let value = match self.extra_metadata.get("requires") {
            Some(Value::String(text)) => SkillToolNames::Text(text.clone()),
            Some(Value::Array(items)) => SkillToolNames::List(
                items
                    .iter()
                    .map(|item| {
                        item.as_str()
                            .map_or_else(|| crate::memory::repr::repr(item), str::to_owned)
                    })
                    .collect(),
            ),
            _ => return Vec::new(),
        };
        value.names()
    }

    /// Whether a pattern gates `tool_name`, using the policy's `*`/`?` matcher.
    pub fn gates(&self, tool_name: &str) -> bool {
        self.gated_tool_names()
            .iter()
            .any(|pattern| crate::tools::glob_matches(pattern, tool_name))
    }
}

/// A complete skill (RFC §24.1): metadata, instructions and its bundled-file
/// directory. Directory registration resolves the path and reads SKILL.md;
/// in-memory addition preserves the supplied values. Python's
/// `Skill(metadata, instructions, path)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Skill {
    /// What its frontmatter declares.
    pub metadata: SkillMetadata,
    /// Its instructions, loaded from SKILL.md or supplied by the host.
    pub instructions: String,
    /// Its directory, resolved by directory registration or supplied by the host.
    pub path: PathBuf,
}

impl Skill {
    /// Its name, its metadata's.
    pub fn name(&self) -> &str {
        &self.metadata.name
    }

    /// What it does, its metadata's description.
    pub fn description(&self) -> &str {
        &self.metadata.description
    }
}

/// What [`discover`](super::SkillRegistry::discover) takes: Python's
/// `strict=True`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiscoverOptions {
    /// The first directory that cannot be scanned, or skill that cannot be
    /// loaded, fails the call; without, each is logged and skipped, which
    /// suits skills from a source the host does not control.
    pub strict: bool,
}

impl Default for DiscoverOptions {
    fn default() -> Self {
        Self { strict: true }
    }
}
