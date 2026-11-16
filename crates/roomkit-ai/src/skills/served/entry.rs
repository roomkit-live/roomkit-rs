//! Frozen outer entry fields, with owned mutable frontmatter (RFC §24.7).

use std::fmt;

use roomkit::model::Metadata;

use super::json_tree::{copy_map, drop_map, maps_equal, text_of};
use super::resource::digest;
use super::{MAX_RESOURCES_PER_SKILL, MAX_TOTAL_SIZE, SkillResource};
use crate::SkillPathError;
use crate::skills::texts::quoted_name;

/// The entry's explicit file listing, or Python's literal "dynamic" (RFC §24.7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SkillResources {
    /// Listed files in server order.
    Listed(Vec<SkillResource>),
    /// Content has no stable digests.
    Dynamic,
}

/// A missing name or description in a directly constructed entry: Python's `KeyError`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("'{key}'")]
pub struct SkillEntryKeyError {
    /// The missing frontmatter key.
    pub key: &'static str,
}

/// An MCP skill manifest (RFC §24.7), independent of its transport and registry.
///
/// Construction is non-validating. Outer fields are frozen; frontmatter is an
/// owned map a host can mutate through an exclusive borrow.
pub struct SkillEntry {
    uri: String,
    frontmatter: Metadata,
    resources: SkillResources,
}

impl SkillEntry {
    /// Construct an entry without validating its URI, fields or file list.
    pub fn new(uri: impl Into<String>, frontmatter: Metadata, resources: SkillResources) -> Self {
        Self {
            uri: uri.into(),
            frontmatter,
            resources,
        }
    }

    /// The URI of its SKILL.md.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Its owned frontmatter.
    pub fn frontmatter(&self) -> &Metadata {
        &self.frontmatter
    }

    /// Mutate the map inside the frozen entry.
    pub fn frontmatter_mut(&mut self) -> &mut Metadata {
        &mut self.frontmatter
    }

    /// Its immutable file listing or dynamic declaration.
    pub fn resources(&self) -> &SkillResources {
        &self.resources
    }

    /// The name as Python's str renders it.
    ///
    /// # Errors
    /// Returns [`SkillEntryKeyError`] if frontmatter has no name.
    pub fn name(&self) -> Result<String, SkillEntryKeyError> {
        self.text("name")
    }

    /// The description as Python's str renders it.
    ///
    /// # Errors
    /// Returns [`SkillEntryKeyError`] if frontmatter has no description.
    pub fn description(&self) -> Result<String, SkillEntryKeyError> {
        self.text("description")
    }

    fn text(&self, key: &'static str) -> Result<String, SkillEntryKeyError> {
        self.frontmatter
            .get(key)
            .map(text_of)
            .ok_or(SkillEntryKeyError { key })
    }

    /// Whether the entry carries no stable file digests.
    pub fn dynamic(&self) -> bool {
        matches!(self.resources, SkillResources::Dynamic)
    }

    /// The URI with its last nine characters removed, without validation.
    pub fn root(&self) -> &str {
        let end = self
            .uri
            .char_indices()
            .rev()
            .nth(8)
            .map_or(0, |(index, _)| index);
        self.uri.get(..end).unwrap_or_default()
    }

    /// The root without its literal "skill://" prefix, if present.
    pub fn path(&self) -> &str {
        self.root().strip_prefix("skill://").unwrap_or(self.root())
    }

    /// The exact sum of listed sizes, or zero for a dynamic entry.
    pub fn total_size(&self) -> i128 {
        match &self.resources {
            SkillResources::Listed(files) => files.iter().map(SkillResource::size).sum(),
            SkillResources::Dynamic => 0,
        }
    }

    /// SHA-256 of sorted URI/digest lines, with no trailing newline (RFC §24.7).
    pub fn fingerprint(&self) -> Option<String> {
        let SkillResources::Listed(files) = &self.resources else {
            return None;
        };
        let mut lines: Vec<_> = files
            .iter()
            .map(|file| format!("{} {}", file.uri(), file.digest()))
            .collect();
        lines.sort();
        Some(digest(lines.join("\n").as_bytes()))
    }

    /// A listed file by URI; direct duplicates retain the last record.
    pub fn resource(&self, uri: &str) -> Option<&SkillResource> {
        let SkillResources::Listed(files) = &self.resources else {
            return None;
        };
        files.iter().rev().find(|file| file.uri() == uri)
    }

    /// Resolve a plain relative path, preserving its lexical characters.
    ///
    /// # Errors
    /// Returns [`SkillPathError`] for an empty or absolute path, a backslash,
    /// or any empty, "." or ".." segment.
    pub fn resolve(&self, relative: &str) -> Result<String, SkillPathError> {
        if relative.contains('\\') || !segments_ok(relative) {
            return Err(SkillPathError(format!(
                "Not a path inside the skill: {}",
                quoted_name(relative)
            )));
        }
        Ok(format!("{}/{relative}", self.root()))
    }

    /// Remove root-length plus one characters, without validating the supplied URI.
    pub fn relative<'a>(&self, uri: &'a str) -> &'a str {
        let start = uri
            .char_indices()
            .nth(self.root().chars().count() + 1)
            .map_or(uri.len(), |(index, _)| index);
        uri.get(start..).unwrap_or_default()
    }

    /// Why the listing exceeds its file-count or total-size bound, in that order.
    pub fn limit_exceeded(&self) -> Option<String> {
        let SkillResources::Listed(files) = &self.resources else {
            return None;
        };
        if files.len() > MAX_RESOURCES_PER_SKILL {
            return Some(format!(
                "it lists {} files, past {MAX_RESOURCES_PER_SKILL}",
                files.len()
            ));
        }
        let total = self.total_size();
        (total > i128::from(MAX_TOTAL_SIZE))
            .then(|| format!("its files hold {total} bytes, past {MAX_TOTAL_SIZE}"))
    }
}

pub(super) fn segments_ok(path: &str) -> bool {
    path.split('/')
        .all(|segment| !matches!(segment, "" | "." | ".."))
}

impl Clone for SkillEntry {
    fn clone(&self) -> Self {
        Self::new(
            self.uri.clone(),
            copy_map(&self.frontmatter),
            self.resources.clone(),
        )
    }
}

impl PartialEq for SkillEntry {
    fn eq(&self, other: &Self) -> bool {
        self.uri == other.uri
            && self.resources == other.resources
            && maps_equal(&self.frontmatter, &other.frontmatter)
    }
}
impl Eq for SkillEntry {}

impl Drop for SkillEntry {
    fn drop(&mut self) {
        drop_map(std::mem::take(&mut self.frontmatter));
    }
}

impl fmt::Debug for SkillEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SkillEntry")
            .field("uri", &self.uri)
            .field(
                "frontmatter",
                &super::json_tree::MapDebug(&self.frontmatter),
            )
            .field("resources", &self.resources)
            .finish()
    }
}
