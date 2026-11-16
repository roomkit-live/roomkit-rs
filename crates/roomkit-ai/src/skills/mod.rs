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
//! exact by default. No skill script runs,
//! and skills an MCP server serves (RFC §24.7) remain unsupported.

mod activation;
mod availability;
mod error;
mod file_error;
mod files;
mod frontmatter;
mod local_files;
mod matcher;
mod model;
mod paths;
mod registry;
mod requirements;
mod texts;
mod tool_names;
pub(crate) mod tools;
mod validation;

pub use self::error::SkillError;
pub use self::file_error::{SkillFileError, SkillPathError};
pub use self::matcher::{CopyOptions, RequiresMatch, SkillRegistryOptions};
pub use self::model::{DiscoverOptions, Skill, SkillMetadata};
pub use self::registry::SkillRegistry;
#[doc(hidden)]
pub use self::texts::dumps;
pub(crate) use self::texts::dumps_unescaped;
pub(crate) use self::texts::{ACTIVATE_SKILL, quoted_name};
pub use self::tool_names::SkillToolNames;
