//! Transport-independent entries and byte verification (RFC §24.7).

mod entry;
mod json_tree;
mod parse;
mod resource;

pub use entry::{SkillEntry, SkillEntryKeyError, SkillResources};
pub use parse::parse_entry;
pub use resource::{SkillResource, SkillResourceSize, verify_skill_resource};

/// The MCP Skills extension's identifier (RFC §24.7).
pub const SKILLS_EXTENSION: &str = "io.modelcontextprotocol/skills";
/// The maximum listed files, including the skill's own SKILL.md (RFC §24.7).
pub const MAX_RESOURCES_PER_SKILL: usize = 512;
/// The maximum sum of a skill's listed byte lengths (RFC §24.7).
pub const MAX_TOTAL_SIZE: u64 = 16 * 1024 * 1024;
