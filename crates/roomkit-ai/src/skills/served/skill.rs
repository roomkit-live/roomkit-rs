//! Owned served-skill snapshots; no served helper reads a local path (RFC §24.7).

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use super::{SkillEntry, SkillResources};
use crate::skills::texts::quoted_name;
use crate::{Skill, SkillFileError, SkillMetadata, SkillPathError};

/// Optional fields of Python's `MCPSkill`, retaining their original defaults.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct McpSkillOptions {
    /// The host's server label, empty until registered.
    pub origin: String,
    /// The entry from which this snapshot was loaded.
    pub entry: Option<SkillEntry>,
    /// Verified bytes keyed by their resource URI.
    pub files: BTreeMap<String, Vec<u8>>,
    /// Total bytes read, including SKILL.md.
    pub read_total: usize,
    /// The shared catalog's retirement counter at loading.
    pub retirements: u64,
}

/// Python's `MCPSkill`: owned content and verified files from one MCP origin.
/// Returned values are detached snapshots; later registry reads see cache updates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpSkill {
    /// The registry metadata, whose name includes the host's origin label.
    pub metadata: SkillMetadata,
    /// Verified instructions, with frontmatter removed.
    pub instructions: String,
    /// A compatibility field, never used by served helpers for local access.
    pub path: PathBuf,
    /// The host's label, never the server's self-reported name.
    pub origin: String,
    /// The entry held for all reads during this activation.
    pub entry: Option<SkillEntry>,
    /// Verified bytes keyed by resource URI.
    pub files: BTreeMap<String, Vec<u8>>,
    /// The bytes read so far, bounding dynamic skills.
    pub read_total: usize,
    /// The catalog's retirement counter at loading.
    pub retirements: u64,
}

impl McpSkill {
    /// Construct a snapshot without validating or reading its content.
    pub fn new(
        metadata: SkillMetadata,
        instructions: String,
        path: PathBuf,
        options: McpSkillOptions,
    ) -> Self {
        Self {
            metadata,
            instructions,
            path,
            origin: options.origin,
            entry: options.entry,
            files: options.files,
            read_total: options.read_total,
            retirements: options.retirements,
        }
    }
    /// Its registry name.
    pub fn name(&self) -> &str {
        &self.metadata.name
    }
    /// Its declared description.
    pub fn description(&self) -> &str {
        &self.metadata.description
    }
    /// The entry whose integrity checks apply to this snapshot.
    ///
    /// # Errors
    /// Returns a path error when constructed without an entry.
    pub fn held(&self) -> Result<&SkillEntry, SkillPathError> {
        self.entry.as_ref().ok_or_else(|| {
            SkillPathError(format!("Skill {} holds no entry", quoted_name(self.name())))
        })
    }
    /// False: files from a server are never local executable scripts.
    pub fn has_scripts(&self) -> bool {
        false
    }
    /// Whether the held listing contains any supplementary file.
    ///
    /// # Errors
    /// Returns a path error when no entry is held.
    pub fn has_references(&self) -> Result<bool, SkillPathError> {
        Ok(!self.list_references()?.is_empty())
    }
    /// Empty: the server's script files can only be read as verified references.
    pub fn list_scripts(&self) -> Vec<OsString> {
        Vec::new()
    }
    /// Supplementary relative paths, sorted as Python strings (RFC §24.7).
    ///
    /// # Errors
    /// Returns a path error when no entry is held.
    pub fn list_references(&self) -> Result<Vec<OsString>, SkillPathError> {
        let entry = self.held()?;
        let SkillResources::Listed(resources) = entry.resources() else {
            return Ok(Vec::new());
        };
        let mut names: Vec<_> = resources
            .iter()
            .filter(|resource| resource.uri() != entry.uri())
            .map(|resource| OsString::from(entry.relative(resource.uri())))
            .collect();
        names.sort();
        Ok(names)
    }
    /// Refuse local reads: use `SkillRegistry::read_file` to verify the resource.
    ///
    /// # Errors
    /// Always returns a path error; never touches the filesystem.
    pub fn read_reference(&self, _filename: &str) -> Result<String, SkillPathError> {
        Err(SkillPathError(format!(
            "Skill {} is read through SkillRegistry.read_file",
            quoted_name(self.name())
        )))
    }
    /// Refuse local execution of server-supplied content.
    ///
    /// # Errors
    /// Always returns a path error; never resolves the compatibility path.
    pub fn resolve_script(&self, _script_name: &str) -> Result<PathBuf, SkillPathError> {
        Err(SkillPathError(format!(
            "Skill {} is served by an MCP server: it runs nothing",
            quoted_name(self.name())
        )))
    }
}

/// A detached result of `SkillRegistry::get_skill` or `load_skill` (RFC §24.7).
/// File operations explicitly dispatch; there is no dereference to a local skill.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LoadedSkill {
    /// A host-owned local skill.
    Local(Skill),
    /// A verified snapshot from one MCP server.
    Served(McpSkill),
}

impl LoadedSkill {
    /// The owned snapshot's metadata.
    pub fn metadata(&self) -> &SkillMetadata {
        match self {
            Self::Local(s) => &s.metadata,
            Self::Served(s) => &s.metadata,
        }
    }
    /// The owned snapshot's instructions.
    pub fn instructions(&self) -> &str {
        match self {
            Self::Local(s) => &s.instructions,
            Self::Served(s) => &s.instructions,
        }
    }
    /// The local directory or unused served compatibility path.
    pub fn path(&self) -> &Path {
        match self {
            Self::Local(s) => &s.path,
            Self::Served(s) => &s.path,
        }
    }
    /// The registry name.
    pub fn name(&self) -> &str {
        &self.metadata().name
    }
    /// The declared description.
    pub fn description(&self) -> &str {
        &self.metadata().description
    }
    /// Whether a local skill contains scripts; false for a served skill.
    ///
    /// # Errors
    /// Preserves local filesystem and containment errors.
    pub fn has_scripts(&self) -> Result<bool, SkillFileError> {
        match self {
            Self::Local(s) => s.has_scripts(),
            Self::Served(s) => Ok(s.has_scripts()),
        }
    }
    /// Whether the skill has supplementary references.
    ///
    /// # Errors
    /// Preserves local file errors or an absent served entry.
    pub fn has_references(&self) -> Result<bool, SkillFileError> {
        match self {
            Self::Local(s) => s.has_references(),
            Self::Served(s) => Ok(s.has_references()?),
        }
    }
    /// Sorted local scripts, or no scripts for served content.
    ///
    /// # Errors
    /// Preserves local filesystem and containment errors.
    pub fn list_scripts(&self) -> Result<Vec<OsString>, SkillFileError> {
        match self {
            Self::Local(s) => s.list_scripts(),
            Self::Served(s) => Ok(s.list_scripts()),
        }
    }
    /// Sorted references from the local directory or held served entry.
    ///
    /// # Errors
    /// Preserves local file errors or an absent served entry.
    pub fn list_references(&self) -> Result<Vec<OsString>, SkillFileError> {
        match self {
            Self::Local(s) => s.list_references(),
            Self::Served(s) => Ok(s.list_references()?),
        }
    }
    /// Read a local reference; served content requires the registry's verified reader.
    ///
    /// # Errors
    /// Returns local file failures or a served-path refusal.
    pub fn read_reference(&self, filename: &str) -> Result<String, SkillFileError> {
        match self {
            Self::Local(s) => s.read_reference(filename),
            Self::Served(s) => Ok(s.read_reference(filename)?),
        }
    }
    /// Resolve a local script; served content is never executable this way.
    ///
    /// # Errors
    /// Returns local file failures or a served-path refusal.
    pub fn resolve_script(&self, filename: &str) -> Result<PathBuf, SkillFileError> {
        match self {
            Self::Local(s) => s.resolve_script(filename),
            Self::Served(s) => Ok(s.resolve_script(filename)?),
        }
    }
}
