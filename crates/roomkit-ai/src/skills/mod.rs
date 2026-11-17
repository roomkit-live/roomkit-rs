//! Skills from the host's own directories (RFC §24, step 4 decision 13):
//! packaged instructions an agent lists in its system prompt and loads
//! when the model activates one.
//!
//! - [`SkillRegistry`]: discovery and registration, strict and atomic,
//!   each `SKILL.md`'s frontmatter read line by line within RFC §24.1's
//!   bounds (`frontmatter`), each file read within a bound (`files`);
//! - the agent's own toolset (`tools`): `activate_skill`, which hands the
//!   model a skill's instructions, then rides them on the system prompt of
//!   the room's next turns, and `read_skill_reference`, which reads a file
//!   of the skill's `references/`, confined there once links are followed;
//! - which skills are active in each room (`activation`), in memory, read
//!   again from the room's stored rows after a restart.
//!
//! Prerequisites (`requires`) use the admitted catalogue and the host's matcher,
//! exact by default. Script execution uses the host's [`ScriptExecutor`],
//! and transport-independent served-skill entries can be parsed and verified
//! (RFC §24.7). Registering, loading and activating served skills remain separate.

mod activation;
mod availability;
mod error;
mod executor;
mod file_error;
mod files;
mod frontmatter;
mod local_files;
mod matcher;
mod model;
mod paths;
mod registry;
mod requirements;
mod script_tool;
mod served;
mod texts;
mod tool_names;
pub(crate) mod tools;
mod validation;

pub use self::error::SkillError;
pub use self::executor::{ScriptExecutor, ScriptResult};
pub use self::file_error::{SkillFileError, SkillPathError};
pub use self::frontmatter::parse_frontmatter;
pub use self::matcher::{CopyOptions, RequiresMatch, SkillRegistryOptions, serves_exactly};
pub use self::model::{DiscoverOptions, Skill, SkillMetadata};
pub use self::paths::safe_join_filename;
pub use self::registry::SkillRegistry;
pub use self::requirements::missing_required_tools;
pub use self::script_tool::RunSkillScriptTool;
pub use self::served::{
    AddMcpServerOptions, ApproveExecution, LoadedSkill, MAX_RESOURCES_PER_SKILL, MAX_TOTAL_SIZE,
    McpSkill, McpSkillOptions, McpSkillServer, McpSkillServerError, McpSkillServerFuture,
    SKILLS_EXTENSION, ServedOrigin, SkillEntry, SkillEntryKeyError, SkillExecutionRequest,
    SkillResource, SkillResourceSize, SkillResources, parse_entry, verify_frontmatter,
    verify_skill_resource,
};
#[doc(hidden)]
pub use self::texts::dumps;
pub(crate) use self::texts::dumps_unescaped;
pub(crate) use self::texts::{ACTIVATE_SKILL, quoted_name};
pub use self::tool_names::{SkillToolNames, parse_tool_names};
