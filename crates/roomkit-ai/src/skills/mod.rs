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
//! A skill that gates or requires tools (`allowed_tools`, `requires`), and
//! a skill's scripts, are not supported: the first is refused, and no
//! script runs. Skills an MCP server serves (RFC §24.7) are not either.

mod activation;
mod error;
mod files;
mod frontmatter;
mod model;
mod registry;
mod texts;
pub(crate) mod tools;

pub use self::error::SkillError;
pub use self::model::{DiscoverOptions, Skill, SkillMetadata};
pub use self::registry::SkillRegistry;
#[doc(hidden)]
pub use self::texts::dumps;
