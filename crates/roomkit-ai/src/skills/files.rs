//! A skill's files: its `SKILL.md` and its references, each read within a
//! bound, a reference by the name the model gives, confined to the skill's
//! `references/` directory once links are followed (RFC §24.6, §17.8).
//!
//! The check is where a path ends up, never how it is spelled: the name is
//! checked first (one plain component of `[A-Za-z0-9._-]`, which no
//! character outside ASCII passes, so none that normalizes onto a
//! separator does), then the directory and the file are resolved with
//! their links followed, and each must still be inside the one above it.
//! A file is opened once it is checked, then checked again through its
//! handle: the path, resolved anew, must still be inside and name the very
//! file the handle holds, so a file swapped for a link between the check
//! and the read is never read. These are blocking calls: an agent runs
//! them off its turn's task.

use std::fs::{self, File, Metadata};
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

/// Why a path was not opened as the regular file it resolves to.
#[derive(Debug)]
pub(crate) enum OpenFailure {
    /// It resolves to nothing.
    Missing,
    /// It resolves where it may not.
    Escapes,
    /// It resolves to no regular file.
    NotAFile,
    /// Once opened, it resolves elsewhere or names another file: it
    /// changed while it was opened.
    Changed,
    /// It could not be resolved or opened.
    Failed(io::Error),
}

/// The regular file `path` resolves to, open, when `inside` takes where it
/// resolves. The path is resolved with its links followed and checked,
/// then the file is opened, then the path is resolved again: it must
/// resolve to the same place, still inside, and name the very file the
/// handle holds (its device and inode, on Unix). A file swapped for a link
/// out between the check and the open is so never handed over. A FIFO
/// found there is refused before the open, which it would block.
pub(crate) fn open_inside(
    path: &Path,
    inside: impl Fn(&Path) -> bool,
) -> Result<File, OpenFailure> {
    let resolved = resolve(path)?;
    if !inside(&resolved) {
        return Err(OpenFailure::Escapes);
    }
    if !fs::metadata(&resolved).map_err(failure)?.is_file() {
        return Err(OpenFailure::NotAFile);
    }
    let file = File::open(&resolved).map_err(failure)?;
    let opened = file.metadata().map_err(failure)?;
    if !opened.is_file() {
        return Err(OpenFailure::NotAFile);
    }
    if !still_names(path, &resolved, &opened, &inside) {
        return Err(OpenFailure::Changed);
    }
    Ok(file)
}

/// Whether `path`, resolved anew, still resolves to `resolved`, inside,
/// and names the file `opened` describes.
fn still_names(
    path: &Path,
    resolved: &Path,
    opened: &Metadata,
    inside: impl Fn(&Path) -> bool,
) -> bool {
    resolve(path).is_ok_and(|again| again == resolved && inside(&again) && names(&again, opened))
}

/// `path`, resolved with its links followed.
fn resolve(path: &Path) -> Result<PathBuf, OpenFailure> {
    path.canonicalize().map_err(failure)
}

/// What an error of the disk is to an open: a path that resolves to
/// nothing, or a failure.
fn failure(error: io::Error) -> OpenFailure {
    if error.kind() == io::ErrorKind::NotFound {
        OpenFailure::Missing
    } else {
        OpenFailure::Failed(error)
    }
}

/// Whether `path` names the file `opened` describes: the same device and
/// inode.
#[cfg(unix)]
fn names(path: &Path, opened: &Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;
    fs::metadata(path).is_ok_and(|now| (now.dev(), now.ino()) == (opened.dev(), opened.ino()))
}

/// Whether `path` names the file `opened` describes: a regular file still,
/// the platform giving no inode to compare.
#[cfg(not(unix))]
fn names(path: &Path, opened: &Metadata) -> bool {
    fs::metadata(path).is_ok_and(|now| now.is_file() && now.len() == opened.len())
}

/// The text `file` holds, read up to `limit` bytes.
///
/// # Errors
///
/// Returns why it could not be read whole within the bound.
pub(crate) fn read_bounded(file: File, limit: u64) -> Result<String, ReadFailure> {
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
    let opened = open_inside(&directory.join(name), |target| target.starts_with(&directory))
        // `references/` must still be where it was, inside the skill.
        .and_then(|file| match references_of(skill) {
            Ok(again) if again == directory => Ok(file),
            Ok(_) | Err(_) => Err(OpenFailure::Changed),
        });
    let file = match opened {
        Ok(file) => file,
        Err(OpenFailure::Missing | OpenFailure::NotAFile) => return Err(not_found(name)),
        // Resolved, links followed: a link out is caught here.
        Err(OpenFailure::Escapes) => return Err(invalid(name, &format!(" escapes {REFERENCES}/"))),
        Err(OpenFailure::Changed) => return Err(invalid(name, " changed while it was read")),
        Err(OpenFailure::Failed(_)) => return Err(invalid(name, " cannot be resolved")),
    };
    read_bounded(file, REFERENCE_MAX).map_err(|failure| match failure {
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

    /// A directory of its own under the system's temporary one, removed
    /// with what it holds when dropped.
    struct Tree(PathBuf);

    impl Tree {
        fn new(name: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("roomkit-files-{name}-{}", std::process::id()));
            let _absent = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path.canonicalize().unwrap())
        }
    }

    impl Drop for Tree {
        fn drop(&mut self) {
            let _gone = fs::remove_dir_all(&self.0);
        }
    }

    /// How many reads a race test makes: some tenths of a second.
    const READS: usize = 10_000;

    /// RFC §24.6: a file opened, then replaced under its name by another,
    /// is no longer the file its path names, and is refused.
    #[cfg(unix)]
    #[test]
    fn a_file_replaced_once_open_is_not_the_one_its_path_names() {
        let tree = Tree::new("replaced");
        let path = tree.0.join("a.md");
        fs::write(&path, "first").unwrap();
        let resolved = path.canonicalize().unwrap();
        let opened = File::open(&resolved).unwrap().metadata().unwrap();
        let inside = |target: &Path| target.starts_with(&tree.0);
        assert!(still_names(&path, &resolved, &opened, inside));
        fs::write(tree.0.join("b.md"), "second").unwrap();
        fs::rename(tree.0.join("b.md"), &path).unwrap();
        assert!(!still_names(&path, &resolved, &opened, inside));
    }

    /// RFC §24.6: a reference swapped for a link out between the check and
    /// the read is never read out of `references/`.
    #[cfg(unix)]
    #[test]
    fn a_reference_swapped_for_a_link_out_while_it_is_read_is_never_read_out() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let tree = Tree::new("swap");
        let skill = tree.0.join("skill");
        let references = skill.join(REFERENCES);
        fs::create_dir_all(&references).unwrap();
        fs::write(references.join("a.md"), "INSIDE").unwrap();
        fs::create_dir_all(tree.0.join("outside")).unwrap();
        let secret = tree.0.join("outside/secret.md");
        fs::write(&secret, "SECRET").unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let flipper = {
            let (stop, references) = (Arc::clone(&stop), references.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    fs::write(references.join(".file"), "INSIDE").unwrap();
                    fs::rename(references.join(".file"), references.join("a.md")).unwrap();
                    let _absent = fs::remove_file(references.join(".link"));
                    std::os::unix::fs::symlink(&secret, references.join(".link")).unwrap();
                    fs::rename(references.join(".link"), references.join("a.md")).unwrap();
                }
            })
        };
        let (mut inside, mut out) = (0, 0);
        for _ in 0..READS {
            match read_reference(&skill, "a.md").as_deref() {
                Ok("SECRET") => out += 1,
                Ok(_) => inside += 1,
                Err(_) => {}
            }
        }
        stop.store(true, Ordering::Relaxed);
        flipper.join().unwrap();
        assert_eq!(out, 0, "{out} reads out of {READS} read the file outside");
        assert!(inside > 0);
    }

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
