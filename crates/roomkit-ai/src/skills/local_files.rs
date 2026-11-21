//! Direct helpers deliberately differ from the bounded model-facing reader.

use std::ffi::{OsStr, OsString};
use std::io::{self, ErrorKind, Read as _};
use std::path::{Path, PathBuf};

use super::files::{self, OpenFailure};
use super::paths::{contained_directory, safe_join_filename};
use super::texts::quoted_name;
use super::{Skill, SkillFileError, SkillPathError};

impl Skill {
    /// Whether `scripts/` contains any entry, including a subdirectory (RFC §24.6).
    ///
    /// # Errors
    /// Returns a path error for an escaping directory, or a filesystem read error.
    pub fn has_scripts(&self) -> Result<bool, SkillFileError> {
        self.has_entries("scripts", "script")
    }

    /// Whether `references/` contains any entry (RFC §24.6).
    ///
    /// # Errors
    /// Returns a path error for an escaping directory, or a filesystem read error.
    pub fn has_references(&self) -> Result<bool, SkillFileError> {
        self.has_entries("references", "reference")
    }

    /// Sorted filenames in `scripts/`, retaining native filename bytes (RFC §24.6).
    /// Listing a symlink does not authorize resolving or executing it.
    ///
    /// # Errors
    /// Returns a path error for an escaping directory, or a filesystem read error.
    pub fn list_scripts(&self) -> Result<Vec<OsString>, SkillFileError> {
        self.list_files("scripts", "script")
    }

    /// Sorted filenames in `references/`, retaining native filename bytes (RFC §24.6).
    ///
    /// # Errors
    /// Returns a path error for an escaping directory, or a filesystem read error.
    pub fn list_references(&self) -> Result<Vec<OsString>, SkillFileError> {
        self.list_files("references", "reference")
    }

    /// Reads a reference as UTF-8 with universal newlines (RFC §24.6),
    /// through the handle its check opened, never by its name again: one
    /// swapped for a link out meanwhile is refused, not read
    /// ([`files::open_inside`]). This direct host API has no model-tool
    /// byte limit.
    ///
    /// # Errors
    /// Distinguishes a path violation (the file changed while it was read
    /// included), a missing/unreadable file, and invalid UTF-8.
    pub fn read_reference(&self, filename: &str) -> Result<String, SkillFileError> {
        let directory = self.path.join("references");
        let path = safe_join_filename(&directory, filename, Some("reference"))?;
        let base = contained_directory(&directory, "reference")?;
        let mut file = files::open_inside(&path, |target| target.starts_with(&base))
            .map_err(|failure| reference_refusal(filename, &path, failure))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let text = String::from_utf8(bytes)?;
        Ok(text.replace("\r\n", "\n").replace('\r', "\n"))
    }

    /// Resolves a script within `scripts/`; does not execute it (RFC §24.6).
    ///
    /// # Errors
    /// Returns a path error for unsafe names or escaping links and `Io(NotFound)`
    /// when the resolved path is not a file.
    pub fn resolve_script(&self, script_name: &str) -> Result<PathBuf, SkillFileError> {
        self.file("scripts", script_name, "script", "Script")
    }

    fn file(
        &self,
        directory: &str,
        name: &str,
        kind: &str,
        label: &str,
    ) -> Result<PathBuf, SkillFileError> {
        let path = safe_join_filename(&self.path.join(directory), name, Some(kind))?;
        if !file_metadata(&path)?.is_some_and(|metadata| metadata.is_file()) {
            return Err(
                io::Error::new(ErrorKind::NotFound, format!("{label} not found: {name}")).into(),
            );
        }
        Ok(path)
    }

    fn bundled_directory(
        &self,
        directory: &str,
        kind: &str,
    ) -> Result<Option<PathBuf>, SkillFileError> {
        let path = self.path.join(directory);
        if !file_metadata(&path)?.is_some_and(|metadata| metadata.is_dir()) {
            return Ok(None);
        }
        Ok(Some(contained_directory(&path, kind)?))
    }

    fn has_entries(&self, directory: &str, kind: &str) -> Result<bool, SkillFileError> {
        let Some(path) = self.bundled_directory(directory, kind)? else {
            return Ok(false);
        };
        Ok(std::fs::read_dir(path)?.next().transpose()?.is_some())
    }

    fn list_files(&self, directory: &str, kind: &str) -> Result<Vec<OsString>, SkillFileError> {
        let Some(path) = self.bundled_directory(directory, kind)? else {
            return Ok(Vec::new());
        };
        let mut names = Vec::new();
        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            if file_metadata(&entry.path())?.is_some_and(|metadata| metadata.is_file()) {
                names.push(entry.file_name());
            }
        }
        names.sort_by_cached_key(|name| filename_order(name));
        Ok(names)
    }
}

/// What [`Skill::read_reference`] returns for the reference `filename`,
/// resolved to `path`, refused for `failure`: absent as a missing file is,
/// moved out or changed as a path violation, a disk failure as itself.
fn reference_refusal(filename: &str, path: &Path, failure: OpenFailure) -> SkillFileError {
    let refused = |why: &str| -> SkillFileError {
        SkillPathError(format!(
            "Invalid reference filename: {}{why}",
            quoted_name(filename)
        ))
        .into()
    };
    match failure {
        OpenFailure::Missing | OpenFailure::NotAFile => not_found(filename),
        OpenFailure::Failed(error)
            if error.kind() == ErrorKind::NotADirectory || super::paths::is_symlink_loop(path) =>
        {
            not_found(filename)
        }
        OpenFailure::Escapes => refused(" escapes the bundled directory"),
        OpenFailure::Changed => refused(" changed while it was read"),
        OpenFailure::Unverified(_) => refused(" cannot be verified (/proc/self/fd unreadable)"),
        OpenFailure::Failed(error) => error.into(),
    }
}

fn not_found(filename: &str) -> SkillFileError {
    io::Error::new(
        ErrorKind::NotFound,
        format!("Reference not found: {filename}"),
    )
    .into()
}

/// Preserve stat failures rather than reporting an inaccessible path as absent.
fn file_metadata(path: &Path) -> io::Result<Option<std::fs::Metadata>> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if matches!(error.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
            Ok(None)
        }
        Err(_) if super::paths::is_symlink_loop(path) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Python sorts decoded code points, including surrogateescape for invalid bytes.
#[cfg(not(windows))]
fn filename_order(name: &OsStr) -> Vec<u32> {
    let mut remaining = name.as_encoded_bytes();
    let mut points = Vec::new();
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(text) => {
                points.extend(text.chars().map(u32::from));
                break;
            }
            Err(error) => {
                let (valid, invalid) = remaining.split_at(error.valid_up_to());
                points.extend(String::from_utf8_lossy(valid).chars().map(u32::from));
                let (invalid, rest) = invalid.split_at(error.error_len().unwrap_or(invalid.len()));
                points.extend(invalid.iter().map(|byte| 0xdc00 + u32::from(*byte)));
                remaining = rest;
            }
        }
    }
    points
}

#[cfg(windows)]
fn filename_order(name: &OsStr) -> Vec<u32> {
    use std::os::windows::ffi::OsStrExt;
    char::decode_utf16(name.encode_wide())
        .map(|value| value.map_or_else(|error| u32::from(error.unpaired_surrogate()), u32::from))
        .collect()
}
