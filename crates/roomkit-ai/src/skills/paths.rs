//! Filename normalization and confinement for the direct Skill API (RFC §24.6).

use std::collections::{HashSet, VecDeque};
use std::io::{self, ErrorKind};
use std::path::{Component, Path, PathBuf};

use unicode_normalization::UnicodeNormalization;

use super::SkillPathError;
use super::texts::quoted_name;

pub(super) fn contained_directory(directory: &Path, kind: &str) -> Result<PathBuf, SkillPathError> {
    let cannot_resolve =
        |_| SkillPathError(format!("Invalid {kind} directory: cannot be resolved"));
    let parent =
        resolve(directory.parent().unwrap_or_else(|| Path::new(""))).map_err(cannot_resolve)?;
    let resolved = resolve(directory).map_err(cannot_resolve)?;
    if resolved == parent || !resolved.starts_with(&parent) {
        return Err(SkillPathError(format!(
            "Invalid {kind} directory: escapes the skill directory"
        )));
    }
    Ok(resolved)
}

pub(super) fn safe_join_filename(
    directory: &Path,
    filename: &str,
    kind: &str,
) -> Result<PathBuf, SkillPathError> {
    let invalid = |reason: &str| {
        SkillPathError(format!(
            "Invalid {kind} filename: {}{reason}",
            quoted_name(filename)
        ))
    };
    if filename.trim().is_empty() {
        return Err(invalid(""));
    }
    let normalized: String = filename.nfkc().collect();
    if normalized.chars().any(char::is_control) {
        return Err(invalid(""));
    }
    if normalized.starts_with('/')
        || normalized.chars().nth(1) == Some(':')
        || normalized.starts_with("\\\\")
    {
        return Err(invalid(" must be relative"));
    }
    if normalized.contains(['/', '\\']) {
        return Err(invalid(" must not contain a path"));
    }
    if normalized.trim_matches(['.', ' ']).is_empty() {
        return Err(invalid(""));
    }
    let base = contained_directory(directory, kind)?;
    let target = resolve(&base.join(normalized)).map_err(|_| invalid(" cannot be resolved"))?;
    if !target.starts_with(&base) {
        return Err(invalid(" escapes the bundled directory"));
    }
    Ok(target)
}

enum Step {
    Component(PathBuf),
    LinkResolved(PathBuf),
}

fn prepend(path: &Path, steps: &mut VecDeque<Step>) {
    for component in path.components().rev() {
        steps.push_front(Step::Component(component.as_os_str().into()));
    }
}

/// Like Path.resolve(strict=False): follow even dangling links, allow missing
/// components, and reject loops. Canonicalizing only the nearest existing parent
/// would miss a dangling link that already points outside the bundled directory.
fn resolve(path: &Path) -> io::Result<PathBuf> {
    let path = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut steps = VecDeque::new();
    prepend(&path, &mut steps);
    let mut active_links = HashSet::new();
    let mut resolved = PathBuf::new();
    while let Some(step) = steps.pop_front() {
        match step {
            Step::LinkResolved(link) => {
                active_links.remove(&link);
            }
            Step::Component(part) => {
                resolve_component(&part, &mut resolved, &mut steps, &mut active_links)?;
            }
        }
    }
    Ok(resolved)
}

fn resolve_component(
    part: &Path,
    resolved: &mut PathBuf,
    steps: &mut VecDeque<Step>,
    active_links: &mut HashSet<PathBuf>,
) -> io::Result<()> {
    match part.components().next() {
        Some(Component::ParentDir) => {
            resolved.pop();
        }
        Some(Component::Normal(name)) => {
            resolved.push(name);
            match std::fs::read_link(&resolved) {
                Ok(target) => {
                    if !active_links.insert(resolved.clone()) {
                        return Err(io::Error::new(ErrorKind::InvalidInput, "symlink loop"));
                    }
                    steps.push_front(Step::LinkResolved(resolved.clone()));
                    resolved.pop();
                    prepend(&target, steps);
                }
                Err(error)
                    if matches!(
                        error.kind(),
                        ErrorKind::NotFound | ErrorKind::NotADirectory | ErrorKind::InvalidInput
                    ) => {}
                Err(error) => return Err(error),
            }
        }
        Some(Component::RootDir | Component::Prefix(_)) => resolved.push(part),
        Some(Component::CurDir) | None => {}
    }
    Ok(())
}
