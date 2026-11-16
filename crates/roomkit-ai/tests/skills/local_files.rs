//! The direct Skill API, independently of the bounded model-facing reader.

use std::ffi::OsString;
use std::io::ErrorKind;

use roomkit_ai::{Skill, SkillFileError, SkillMetadata};

use super::support::TempDir;

fn skill(files: &TempDir) -> Skill {
    Skill {
        metadata: SkillMetadata::new("local", "Local files"),
        instructions: "Body".into(),
        path: files.dir("local"),
    }
}

fn names(values: &[&str]) -> Vec<OsString> {
    values.iter().map(OsString::from).collect()
}

fn path_error<T: std::fmt::Debug>(result: Result<T, SkillFileError>) {
    assert!(matches!(result.unwrap_err(), SkillFileError::Path(_)));
}

fn missing<T: std::fmt::Debug>(result: Result<T, SkillFileError>) {
    assert!(
        matches!(result.unwrap_err(), SkillFileError::Io(error) if error.kind() == ErrorKind::NotFound)
    );
}

// Python: tests/test_skills.py::TestSkillModel::test_list_scripts
#[test]
fn test_list_scripts() {
    let files = TempDir::new();
    let skill = skill(&files);
    files.file("local/scripts/test.py", "Body");
    files.file("local/scripts/run.sh", "Body");
    assert_eq!(skill.list_scripts().unwrap(), names(&["run.sh", "test.py"]));
    assert!(skill.has_scripts().unwrap());
}

// Python: tests/test_skills.py::TestSkillModel::test_list_references
#[test]
fn test_list_references() {
    let files = TempDir::new();
    let skill = skill(&files);
    files.file("local/references/schema.json", "{}");
    files.file("local/references/api.md", "# API");
    assert_eq!(
        skill.list_references().unwrap(),
        names(&["api.md", "schema.json"])
    );
    assert!(skill.has_references().unwrap());
}

// Python: tests/test_skills.py::TestSkillModel::test_read_reference
#[test]
fn test_read_reference() {
    let files = TempDir::new();
    let skill = skill(&files);
    files.file("local/references/data.txt", "Some data");
    assert_eq!(skill.read_reference("data.txt").unwrap(), "Some data");
}

// Python: tests/test_skills.py::TestSkillModel::test_resolve_script_returns_path
#[test]
fn test_resolve_script_returns_path() {
    let files = TempDir::new();
    let skill = skill(&files);
    let path = files.file("local/scripts/run.sh", "Body");
    assert_eq!(
        skill.resolve_script("run.sh").unwrap(),
        path.canonicalize().unwrap()
    );
}

// Python: tests/test_skills.py::TestSkillModel::test_has_scripts_empty
#[test]
fn test_has_scripts_empty() {
    let files = TempDir::new();
    assert!(!skill(&files).has_scripts().unwrap());
}

// Python: tests/test_skills.py::TestSkillModel::test_has_references_empty
#[test]
fn test_has_references_empty() {
    let files = TempDir::new();
    assert!(!skill(&files).has_references().unwrap());
}

// Python: tests/test_skills.py::TestSkillModel::test_read_reference_not_found
#[test]
fn test_read_reference_not_found() {
    let files = TempDir::new();
    missing(skill(&files).read_reference("nope.txt"));
}

// Python: tests/test_skills.py::TestSkillModel::test_resolve_script_missing
#[test]
fn test_resolve_script_missing() {
    let files = TempDir::new();
    missing(skill(&files).resolve_script("nope.sh"));
}

// Python: tests/test_skills.py::TestSkillModel::test_read_reference_rejects_fullwidth_traversal
#[test]
fn test_read_reference_rejects_fullwidth_traversal() {
    let files = TempDir::new();
    path_error(skill(&files).read_reference("．．／secret.txt"));
}

// Python: tests/test_skills.py::TestSkillModel::test_read_reference_rejects_absolute
#[test]
fn test_read_reference_rejects_absolute() {
    let files = TempDir::new();
    let error = skill(&files).read_reference("/etc/passwd").unwrap_err();
    assert!(matches!(error, SkillFileError::Path(_)));
    assert!(error.to_string().contains("relative"));
}

// Python: tests/test_skills.py::TestSkillModel::test_read_reference_path_error_is_value_error
// Rust catches the typed Path branch instead of Python's ValueError inheritance (HITL 45d6fce8).
#[test]
fn test_read_reference_path_error_is_value_error() {
    let files = TempDir::new();
    path_error(skill(&files).read_reference("../etc/passwd"));
}

// Python: tests/test_skills.py::TestSkillModel::test_read_reference_traversal_blocked
#[test]
fn test_read_reference_traversal_blocked() {
    let files = TempDir::new();
    let skill = skill(&files);
    for name in ["../etc/passwd", "sub/file.txt"] {
        path_error(skill.read_reference(name));
    }
}

// Python: tests/test_skills.py::TestSkillModel::test_resolve_script_traversal_blocked
#[test]
fn test_resolve_script_traversal_blocked() {
    let files = TempDir::new();
    path_error(skill(&files).resolve_script("../../bin/sh"));
}

#[cfg(unix)]
mod symlinks {
    use super::*;
    use crate::support::link;

    fn escape(directory: bool) -> (TempDir, Skill) {
        let files = TempDir::new();
        let skill = skill(&files);
        let target = files.file("outside/payload", "Outside");
        for name in ["scripts", "references"] {
            if directory {
                link(target.parent().unwrap(), &skill.path.join(name));
            } else {
                files.dir(&format!("local/{name}"));
                link(&target, &skill.path.join(name).join("payload"));
            }
        }
        (files, skill)
    }

    // Python: tests/test_skills.py::TestSkillModel::test_read_reference_symlink_escape_blocked
    #[test]
    fn test_read_reference_symlink_escape_blocked() {
        let (_files, skill) = escape(false);
        path_error(skill.read_reference("payload"));
    }

    // Python: tests/test_skills.py::TestSkillModel::test_resolve_script_symlink_escape_blocked
    #[test]
    fn test_resolve_script_symlink_escape_blocked() {
        let (_files, skill) = escape(false);
        path_error(skill.resolve_script("payload"));
    }

    // Python: tests/test_skills.py::TestSkillModel::test_read_reference_directory_symlink_escape_blocked
    #[test]
    fn test_read_reference_directory_symlink_escape_blocked() {
        let (_files, skill) = escape(true);
        path_error(skill.read_reference("payload"));
        path_error(skill.list_references());
        path_error(skill.has_references());
    }

    // Python: tests/test_skills.py::TestSkillModel::test_resolve_script_directory_symlink_escape_blocked
    #[test]
    fn test_resolve_script_directory_symlink_escape_blocked() {
        let (_files, skill) = escape(true);
        path_error(skill.resolve_script("payload"));
        path_error(skill.list_scripts());
        path_error(skill.has_scripts());
    }
}
