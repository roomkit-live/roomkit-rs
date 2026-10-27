//! A skill's files: its `SKILL.md` and its references, each read within a
//! bound, a reference by the name the model gives, confined to the skill's
//! `references/` directory once links are followed (RFC §24.6, §17.8).
//!
//! The check is where a path ends up, never how it is spelled: the name is
//! checked first (one plain component of `[A-Za-z0-9._-]`, which no
//! character outside ASCII passes, so none that normalizes onto a
//! separator does), then the directory and the file are resolved with
//! their links followed, and each must still be inside the one above it.
//! These are blocking calls: an agent runs them off its turn's task.

use std::fs::{self, File};
use std::io::{self, Read as _};
use std::path::{Path, PathBuf};

use super::texts::quoted_name;

/// The most bytes a skill's `SKILL.md` takes: its body rides every system
/// prompt while the skill is active.
pub(crate) const SKILL_FILE_MAX: u64 = 256 * 1024;

/// The most bytes a reference read takes: what one call hands the model.
pub(crate) const REFERENCE_MAX: u64 = 256 * 1024;

/// The directory of a skill's references.
const REFERENCES: &str = "references";

/// The names a skill's instructions are read from, in the order they are
/// looked for; any other spelling of `skill.md` after them.
const SKILL_FILES: [&str; 2] = ["SKILL.md", "skill.md"];

/// Why a bounded read failed.
#[derive(Debug)]
pub(crate) enum ReadFailure {
    /// The file is longer than its bound.
    TooLarge,
    /// It is not UTF-8 text.
    NotText,
    /// It could not be read.
    Io(io::Error),
}

/// The text of the file at `path`, read up to `limit` bytes.
///
/// # Errors
///
/// Returns why it could not be read whole within the bound.
pub(crate) fn read_bounded(path: &Path, limit: u64) -> Result<String, ReadFailure> {
    let file = File::open(path).map_err(ReadFailure::Io)?;
    let mut bytes = Vec::new();
    file.take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(ReadFailure::Io)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limit {
        return Err(ReadFailure::TooLarge);
    }
    String::from_utf8(bytes).map_err(|_not_text| ReadFailure::NotText)
}

/// The `SKILL.md` of the skill directory `directory` (already resolved):
/// `None` when it holds none.
///
/// # Errors
///
/// Returns the error listing the directory failed with.
pub(crate) fn find_skill_file(directory: &Path) -> io::Result<Option<PathBuf>> {
    for name in SKILL_FILES {
        let candidate = directory.join(name);
        if candidate.is_file() {
            return Ok(Some(candidate));
        }
    }
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        let named = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.eq_ignore_ascii_case("skill.md"));
        if named && path.is_file() {
            return Ok(Some(path));
        }
    }
    Ok(None)
}

/// Whether `path` is inside `directory`, below it.
pub(crate) fn is_below(path: &Path, directory: &Path) -> bool {
    path != directory && path.starts_with(directory)
}

/// The reference `name` of the skill in `skill`, or the words the model
/// reads of why it cannot be read (the reference implementation's).
pub(crate) fn read_reference(skill: &Path, name: &str) -> Result<String, String> {
    checked_name(name)?;
    let directory = match references_of(skill) {
        Ok(directory) => directory,
        Err(Unresolved::Missing) => return Err(not_found(name)),
        Err(Unresolved::Escapes) => {
            return Err(
                "Invalid reference directory: 'references' escapes the skill directory".to_owned(),
            );
        }
        Err(Unresolved::Failed) => {
            return Err("Invalid reference directory: cannot be resolved".to_owned());
        }
    };
    let target = match directory.join(name).canonicalize() {
        Ok(target) => target,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Err(not_found(name)),
        Err(_unresolved) => return Err(invalid(name, " cannot be resolved")),
    };
    // Resolved, links followed: a link out is caught here.
    if target != directory && !target.starts_with(&directory) {
        return Err(invalid(name, &format!(" escapes {REFERENCES}/")));
    }
    if !target.is_file() {
        return Err(not_found(name));
    }
    read_bounded(&target, REFERENCE_MAX).map_err(|failure| match failure {
        ReadFailure::TooLarge => {
            format!("Reference too large: {name} (past {REFERENCE_MAX} bytes)")
        }
        ReadFailure::NotText => format!("Reference is not UTF-8 text: {name}"),
        ReadFailure::Io(_) => format!("Reference cannot be read: {name}"),
    })
}

/// The references a read of the skill in `skill` takes, sorted: each file
/// of its `references/` whose name passes the check and that resolves
/// inside it.
pub(crate) fn list_references(skill: &Path) -> Vec<String> {
    let Ok(directory) = references_of(skill) else {
        return Vec::new();
    };
    let Ok(entries) = fs::read_dir(&directory) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter(|name| checked_name(name).is_ok())
        .filter(|name| {
            directory
                .join(name)
                .canonicalize()
                .is_ok_and(|target| is_below(&target, &directory) && target.is_file())
        })
        .collect();
    names.sort();
    names
}

/// Why a skill's `references/` was not resolved.
enum Unresolved {
    /// It is not there.
    Missing,
    /// It resolves outside the skill.
    Escapes,
    /// It could not be resolved.
    Failed,
}

/// The skill's `references/`, resolved, links followed, and still inside
/// the skill, itself resolved.
fn references_of(skill: &Path) -> Result<PathBuf, Unresolved> {
    let resolve = |path: &Path| {
        path.canonicalize().map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Unresolved::Missing
            } else {
                Unresolved::Failed
            }
        })
    };
    let skill = resolve(skill)?;
    let directory = resolve(&skill.join(REFERENCES))?;
    if is_below(&directory, &skill) {
        Ok(directory)
    } else {
        Err(Unresolved::Escapes)
    }
}

/// Whether `name` is a reference's name, a read refuses otherwise, in the
/// reference implementation's order of causes: no name, a control
/// character, an absolute path in any platform's spelling, a separator,
/// dots alone; then any character but `[A-Za-z0-9._-]`.
fn checked_name(name: &str) -> Result<(), String> {
    if name.trim().is_empty() || name.chars().any(char::is_control) {
        return Err(invalid(name, ""));
    }
    if is_absolute_anywhere(name) {
        return Err(invalid(name, " must be relative"));
    }
    if name.contains(['/', '\\']) {
        return Err(invalid(name, " must not contain a path"));
    }
    if name.trim_matches(['.', ' ']).is_empty() {
        return Err(invalid(name, ""));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
    {
        return Err(invalid(
            name,
            " must hold only letters, digits, '.', '_' and '-'",
        ));
    }
    Ok(())
}

/// Whether `name` is absolute on POSIX or on Windows: a root, a drive
/// letter, a UNC path.
fn is_absolute_anywhere(name: &str) -> bool {
    let mut characters = name.chars();
    let first = characters.next();
    let drive = first.is_some_and(|c| c.is_ascii_alphabetic()) && characters.next() == Some(':');
    name.starts_with('/') || name.starts_with("\\\\") || drive
}

fn invalid(name: &str, why: &str) -> String {
    format!("Invalid reference filename: {}{why}", quoted_name(name))
}

fn not_found(name: &str) -> String {
    format!("Reference not found: {name}")
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;

    /// RFC §24.6: each name a read refuses before it touches the disk, in
    /// the reference implementation's words.
    #[test]
    fn a_name_is_refused_before_it_is_resolved() {
        let refused = [
            ("", "Invalid reference filename: ''"),
            ("..", "Invalid reference filename: '..'"),
            ("a\u{0}b", "Invalid reference filename: 'a\\x00b'"),
            (
                "../SKILL.md",
                "Invalid reference filename: '../SKILL.md' must not contain a path",
            ),
            (
                "/etc/passwd",
                "Invalid reference filename: '/etc/passwd' must be relative",
            ),
            (
                "C:\\x.md",
                "Invalid reference filename: 'C:\\\\x.md' must be relative",
            ),
            (
                "\\\\server\\share",
                "Invalid reference filename: '\\\\\\\\server\\\\share' must be relative",
            ),
            (
                "sub/deep.md",
                "Invalid reference filename: 'sub/deep.md' must not contain a path",
            ),
            (
                "\u{ff0e}\u{ff0e}\u{ff0f}SKILL.md",
                "Invalid reference filename: '\u{ff0e}\u{ff0e}\u{ff0f}SKILL.md' must hold only \
                 letters, digits, '.', '_' and '-'",
            ),
            (
                "my guide.md",
                "Invalid reference filename: 'my guide.md' must hold only letters, digits, '.', \
                 '_' and '-'",
            ),
        ];
        for (name, words) in refused {
            assert_eq!(checked_name(name), Err(words.to_owned()), "{name:?}");
        }
        assert_eq!(checked_name("return-policy_v2.md"), Ok(()));
    }
}
