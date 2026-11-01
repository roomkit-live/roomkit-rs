//! A skill and its metadata (RFC §24.1, §24.2): Python's `Skill` and
//! `SkillMetadata`.

use std::path::PathBuf;

use roomkit::model::Metadata;

/// What a skill's `SKILL.md` frontmatter declares (RFC §24.2): Python's
/// `SkillMetadata(name, description, license=None, compatibility=None,
/// extra_metadata={})`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillMetadata {
    /// Its name: lowercase letters and digits in hyphen-separated runs, at
    /// most 64 characters, its directory's name.
    pub name: String,
    /// What it does, at most 1,024 characters.
    pub description: String,
    /// Its license, when its frontmatter names one.
    pub license: Option<String>,
    /// What it needs to run, as free text, when its frontmatter says.
    pub compatibility: Option<String>,
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
            extra_metadata: Metadata::new(),
        }
    }
}

/// A skill whole (RFC §24.1): its metadata, its instructions, the body of
/// its `SKILL.md`, and its directory, resolved, where its references are
/// read. Python's `Skill(metadata, instructions, path)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Skill {
    /// What its frontmatter declares.
    pub metadata: SkillMetadata,
    /// Its instructions: its `SKILL.md`'s body.
    pub instructions: String,
    /// Its directory, resolved.
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
