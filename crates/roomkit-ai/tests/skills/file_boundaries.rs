//! Python's public filesystem boundary witnesses, mirrored without model tools.

use super::support::TempDir;
use roomkit_ai::{Skill, SkillFileError, SkillMetadata};
use std::io::ErrorKind;

fn skill(files: &TempDir) -> Skill {
    Skill {
        metadata: SkillMetadata::new("local", "Files"),
        instructions: "Body".into(),
        path: files.dir("local"),
    }
}

// Python: tests/skills/test_local_files.py::test_direct_file_helpers_resolve_safe_names_after_nfkc
#[test]
fn test_direct_file_helpers_resolve_safe_names_after_nfkc() {
    let files = TempDir::new();
    let skill = skill(&files);
    for (requested, stored) in [
        ("café.md", "café.md"),
        ("my guide.md", "my guide.md"),
        ("ｎｏｔｅｓ.md", "notes.md"),
        ("e\u{301}.md", "é.md"),
    ] {
        for directory in ["scripts", "references"] {
            files.file(&format!("local/{directory}/{stored}"), "Contents");
        }
        assert_eq!(skill.read_reference(requested).unwrap(), "Contents");
        assert_eq!(
            skill.resolve_script(requested).unwrap(),
            skill.path.join("scripts").join(stored)
        );
    }
}

// Python: tests/skills/test_local_files.py::test_missing_bundled_directories_are_empty_but_reads_raise
#[test]
fn test_missing_bundled_directories_are_empty_but_reads_raise() {
    let files = TempDir::new();
    let skill = skill(&files);
    assert!(!skill.has_scripts().unwrap());
    assert!(!skill.has_references().unwrap());
    assert_eq!(
        skill.list_scripts().unwrap(),
        Vec::<std::ffi::OsString>::new()
    );
    assert_eq!(
        skill.list_references().unwrap(),
        Vec::<std::ffi::OsString>::new()
    );
    assert!(
        matches!(skill.read_reference("missing.md"), Err(SkillFileError::Io(error)) if error.kind() == ErrorKind::NotFound)
    );
    assert!(
        matches!(skill.resolve_script("missing.py"), Err(SkillFileError::Io(error)) if error.kind() == ErrorKind::NotFound)
    );
}

// Python: tests/skills/test_local_files.py::test_a_bundled_directory_entry_counts_without_being_a_listed_file
#[test]
fn test_a_bundled_directory_entry_counts_without_being_a_listed_file() {
    let files = TempDir::new();
    let skill = skill(&files);
    for directory in ["scripts", "references"] {
        files.dir(&format!("local/{directory}"));
    }
    assert!(!skill.has_scripts().unwrap());
    assert!(!skill.has_references().unwrap());
    for directory in ["scripts", "references"] {
        files.dir(&format!("local/{directory}/nested"));
    }
    assert!(skill.has_scripts().unwrap());
    assert!(skill.has_references().unwrap());
    assert_eq!(
        skill.list_scripts().unwrap(),
        Vec::<std::ffi::OsString>::new()
    );
    assert_eq!(
        skill.list_references().unwrap(),
        Vec::<std::ffi::OsString>::new()
    );
}

// Python: tests/skills/test_local_files.py::test_direct_reference_read_is_not_the_model_tool_bounded_read
#[test]
fn test_direct_reference_read_is_not_the_model_tool_bounded_read() {
    let files = TempDir::new();
    let skill = skill(&files);
    let text = "é".repeat(256 * 1024);
    files.file("local/references/large.md", &text);
    assert_eq!(skill.read_reference("large.md").unwrap(), text);
}

// Python: tests/skills/test_local_files.py::test_direct_reference_read_preserves_an_encoding_failure
#[test]
fn test_direct_reference_read_preserves_an_encoding_failure() {
    let files = TempDir::new();
    let skill = skill(&files);
    let path = files.file("local/references/invalid.md", "");
    std::fs::write(path, [0xff]).unwrap();
    assert!(matches!(
        skill.read_reference("invalid.md"),
        Err(SkillFileError::Encoding(_))
    ));
}

// Python: tests/skills/test_local_files.py::test_direct_file_helpers_reject_unsafe_names
#[test]
fn test_direct_file_helpers_reject_unsafe_names() {
    let files = TempDir::new();
    let skill = skill(&files);
    for name in [
        "",
        " ",
        ".",
        "..",
        "... ",
        "a/b",
        "a\\b",
        "/etc/passwd",
        "C:guide.md",
        "．．／secret",
        "null\0.md",
        "line\n.md",
        "control\u{85}.md",
    ] {
        assert!(
            matches!(skill.read_reference(name), Err(SkillFileError::Path(_))),
            "{name:?}"
        );
        assert!(
            matches!(skill.resolve_script(name), Err(SkillFileError::Path(_))),
            "{name:?}"
        );
    }
}

// Python: tests/skills/test_local_files.py::test_direct_reference_read_uses_utf8_with_universal_newlines
#[test]
fn test_direct_reference_read_uses_utf8_with_universal_newlines() {
    let files = TempDir::new();
    let skill = skill(&files);
    for text in ["one\r\ntwo\rthree\n", "\u{feff}Header\n"] {
        files.file("local/references/text.md", text);
        assert_eq!(
            skill.read_reference("text.md").unwrap(),
            text.replace("\r\n", "\n").replace('\r', "\n")
        );
    }
}

#[cfg(unix)]
mod symlinks {
    use super::*;
    use crate::support::link;

    // Python: tests/skills/test_local_files.py::test_listing_propagates_permission_errors_on_a_link_target
    #[test]
    fn test_listing_propagates_permission_errors_on_a_link_target() {
        use std::os::unix::fs::PermissionsExt;
        let files = TempDir::new();
        let skill = skill(&files);
        let target = files.file("local/hidden/body", "Body");
        for directory in ["scripts", "references"] {
            let path = files.dir(&format!("local/{directory}"));
            link(&target, &path.join("link"));
        }
        let hidden = skill.path.join("hidden");
        std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o0)).unwrap();
        if std::fs::metadata(&target).is_ok() {
            std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o700)).unwrap();
            return; // Root can traverse a mode-zero directory.
        }
        let results = [skill.list_scripts(), skill.list_references()];
        std::fs::set_permissions(&hidden, std::fs::Permissions::from_mode(0o700)).unwrap();
        for result in results {
            assert!(
                matches!(result, Err(SkillFileError::Io(error)) if error.kind() == ErrorKind::PermissionDenied)
            );
        }
    }

    // Python: tests/skills/test_local_files.py::test_directory_stat_permission_errors_are_not_empty_results
    #[test]
    fn test_directory_stat_permission_errors_are_not_empty_results() {
        use std::os::unix::fs::PermissionsExt;
        let files = TempDir::new();
        let skill = skill(&files);
        for directory in ["scripts", "references"] {
            files.dir(&format!("local/{directory}"));
        }
        std::fs::set_permissions(&skill.path, std::fs::Permissions::from_mode(0o0)).unwrap();
        let probe = std::fs::metadata(skill.path.join("scripts"));
        if probe.is_ok() {
            std::fs::set_permissions(&skill.path, std::fs::Permissions::from_mode(0o700)).unwrap();
            return; // Root can traverse a mode-zero directory.
        }
        let results = [
            skill.has_scripts(),
            skill.has_references(),
            skill.list_scripts().map(|_| true),
            skill.list_references().map(|_| true),
            skill.read_reference("body").map(|_| true),
            skill.resolve_script("body").map(|_| true),
        ];
        std::fs::set_permissions(&skill.path, std::fs::Permissions::from_mode(0o700)).unwrap();
        for result in results {
            assert!(
                matches!(result, Err(SkillFileError::Io(error)) if error.kind() == ErrorKind::PermissionDenied)
            );
        }
    }

    // Python: tests/skills/test_local_files.py::test_direct_file_helpers_follow_links_that_stay_inside_the_skill
    #[test]
    fn test_direct_file_helpers_follow_links_that_stay_inside_the_skill() {
        let files = TempDir::new();
        let skill = skill(&files);
        let body = files.file("local/content/body", "Contents");
        link(
            std::path::Path::new("body"),
            &skill.path.join("content/alias"),
        );
        for directory in ["scripts", "references"] {
            link(std::path::Path::new("content"), &skill.path.join(directory));
        }
        assert_eq!(skill.read_reference("alias").unwrap(), "Contents");
        assert_eq!(skill.resolve_script("alias").unwrap(), body);
        assert!(skill.has_scripts().unwrap());
        assert!(skill.has_references().unwrap());
    }

    // Python: tests/skills/test_local_files.py::test_direct_file_helpers_refuse_a_symlink_loop
    #[test]
    fn test_direct_file_helpers_refuse_a_symlink_loop() {
        let files = TempDir::new();
        let skill = skill(&files);
        for directory in ["scripts", "references"] {
            let path = files.dir(&format!("local/{directory}"));
            link(std::path::Path::new("second"), &path.join("first"));
            link(std::path::Path::new("first"), &path.join("second"));
        }
        assert!(matches!(
            skill.read_reference("first"),
            Err(SkillFileError::Path(_))
        ));
        assert!(matches!(
            skill.resolve_script("first"),
            Err(SkillFileError::Path(_))
        ));
    }

    // Python: tests/skills/test_local_files.py::test_direct_file_helpers_reject_a_dangling_symlink_escape
    #[test]
    fn test_direct_file_helpers_reject_a_dangling_symlink_escape() {
        let files = TempDir::new();
        let skill = skill(&files);
        for directory in ["scripts", "references"] {
            let path = files.dir(&format!("local/{directory}"));
            link(&files.path().join("absent"), &path.join("dangling"));
        }
        assert!(matches!(
            skill.read_reference("dangling"),
            Err(SkillFileError::Path(_))
        ));
        assert!(matches!(
            skill.resolve_script("dangling"),
            Err(SkillFileError::Path(_))
        ));
    }

    // Python: tests/skills/test_local_files.py::test_lists_preserve_child_names_but_reading_still_checks_containment
    #[test]
    fn test_lists_preserve_child_names_but_reading_still_checks_containment() {
        let files = TempDir::new();
        let skill = skill(&files);
        let outside = files.file("outside", "Outside");
        for directory in ["scripts", "references"] {
            let path = files.dir(&format!("local/{directory}"));
            link(&outside, &path.join("escape"));
            files.file(&format!("local/{directory}/safe name"), "Safe");
        }
        let expected: Vec<std::ffi::OsString> = ["escape", "safe name"].map(Into::into).to_vec();
        assert_eq!(skill.list_scripts().unwrap(), expected);
        assert_eq!(skill.list_references().unwrap(), expected);
        assert!(matches!(
            skill.read_reference("escape"),
            Err(SkillFileError::Path(_))
        ));
        assert!(matches!(
            skill.resolve_script("escape"),
            Err(SkillFileError::Path(_))
        ));
    }

    // Python: tests/skills/test_local_files.py::test_lists_preserve_non_utf8_filenames_and_python_sort_order
    #[test]
    fn test_lists_preserve_non_utf8_filenames_and_python_sort_order() {
        use std::os::unix::ffi::OsStringExt;
        let files = TempDir::new();
        let skill = skill(&files);
        let names: Vec<std::ffi::OsString> = vec![
            "a".into(),
            std::ffi::OsString::from_vec(vec![0xff]),
            "\u{e000}".into(),
            "😀".into(),
        ];
        for directory in ["scripts", "references"] {
            let path = files.dir(&format!("local/{directory}"));
            for name in &names {
                std::fs::write(path.join(name), "Body").unwrap();
            }
        }
        assert_eq!(skill.list_scripts().unwrap(), names);
        assert_eq!(skill.list_references().unwrap(), names);
    }
}
