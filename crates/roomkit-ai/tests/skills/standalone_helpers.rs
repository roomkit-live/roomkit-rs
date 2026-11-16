//! Direct public helper contracts from Python, separate from registry admission.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use roomkit_ai::{
    RequiresMatch, SkillMetadata, ToolError, missing_required_tools, parse_tool_names,
    safe_join_filename, serves_exactly,
};
use serde_json::json;

use super::support::TempDir;

// Python: tests/skills/test_standalone_helpers.py::test_parse_tool_names_preserves_forms_order_and_duplicates
#[test]
fn test_parse_tool_names_preserves_forms_order_and_duplicates() {
    for (value, expected) in [
        (
            json!(" [ 'search_*', \"fetch\" , '', search_* ] "),
            vec!["search_*", "fetch", "search_*"],
        ),
        (
            json!([" a,b ", "'search_*'", "", " search_* "]),
            vec!["a,b", "search_*", "search_*"],
        ),
        (json!(["first", "second"]), vec!["first", "second"]),
        (json!("[ab]"), vec!["ab"]),
        (json!("[sw]earch_*, fetch]"), vec!["[sw]earch_*", "fetch]"]),
        (json!("[[a,b]]"), vec!["[[a", "b]]"]),
        (json!(" , '' , \"\" "), vec![]),
        (json!(null), vec![]),
        (json!(42), vec![]),
        (json!(true), vec![]),
        (json!({"name": "lookup"}), vec![]),
        (
            json!(["", null, true, false, 42, 1.5]),
            vec!["None", "True", "False", "42", "1.5"],
        ),
        (
            json!([["nested"], {"flag": false}]),
            vec!["['nested']", "{'flag': False}"],
        ),
    ] {
        assert_eq!(parse_tool_names(&value), expected, "{value}");
    }
}

// Python: tests/skills/test_standalone_helpers.py::test_serves_exactly_is_case_sensitive_membership
#[test]
fn test_serves_exactly_is_case_sensitive_membership() {
    for (required, offered, expected) in [
        ("lookup", vec![], false),
        ("lookup", vec!["lookup", "lookup"], true),
        ("lookup", vec!["lookup", "other"], true),
        ("lookup", vec!["LOOKUP", "lookup_extra"], false),
        ("lookup*", vec!["lookup_extra"], false),
        ("", vec![""], true),
    ] {
        assert_eq!(serves_exactly(required, &offered), expected);
        assert_eq!(
            serves_exactly(required, offered.into_iter().collect::<HashSet<_>>()),
            expected
        );
    }
}

fn requires(names: &str) -> SkillMetadata {
    let mut metadata = SkillMetadata::new("local", "");
    metadata
        .extra_metadata
        .insert("requires".into(), json!(names));
    metadata
}

// Python: tests/skills/test_standalone_helpers.py::test_missing_required_tools_keeps_order_duplicates_and_defaults
#[test]
fn test_missing_required_tools_keeps_order_duplicates_and_defaults() {
    assert_eq!(
        missing_required_tools(
            &requires("second, first, second, served"),
            ["served", "served"],
            &[],
            None
        )
        .unwrap(),
        ["second", "first", "second"]
    );
    assert_eq!(
        missing_required_tools(
            &SkillMetadata::new("local", ""),
            std::iter::empty::<&str>(),
            &[],
            None
        )
        .unwrap(),
        Vec::<String>::new()
    );
}

// Python: tests/skills/test_standalone_helpers.py::test_missing_required_tools_consumes_iterables_and_filters_closed_patterns
#[test]
fn test_missing_required_tools_consumes_iterables_and_filters_closed_patterns() {
    let mut admitted = ["lookup", "lookup", "fetch", "Lookup"].into_iter();
    let closed = ["look*", "f?tch"].map(String::from);
    assert_eq!(
        missing_required_tools(
            &requires("lookup, fetch, Lookup"),
            &mut admitted,
            &closed,
            None
        )
        .unwrap(),
        ["lookup", "fetch"]
    );
    assert_eq!(admitted.next(), None);
}

// Python: tests/skills/test_standalone_helpers.py::test_missing_required_tools_passes_only_open_unique_names_to_the_matcher
#[test]
fn test_missing_required_tools_passes_only_open_unique_names_to_the_matcher() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::clone(&seen);
    let matcher = RequiresMatch::new(move |required, offered| {
        calls
            .lock()
            .unwrap()
            .push((required.to_owned(), offered.clone()));
        Ok(offered
            .iter()
            .any(|name| name.starts_with(&format!("{required}_"))))
    });
    assert_eq!(
        missing_required_tools(
            &requires("boards, missing, boards"),
            ["boards_read", "boards_read", "boards_delete"],
            &["*_delete".into()],
            Some(&matcher),
        )
        .unwrap(),
        ["missing"]
    );
    let expected: Vec<_> = ["boards", "missing", "boards"]
        .map(|name| (name.to_owned(), HashSet::from(["boards_read".to_owned()])))
        .into();
    assert_eq!(*seen.lock().unwrap(), expected);
}

// Python: tests/skills/test_standalone_helpers.py::test_missing_required_tools_propagates_the_matcher_error_without_more_calls
#[test]
fn test_missing_required_tools_propagates_the_matcher_error_without_more_calls() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::clone(&seen);
    let matcher = RequiresMatch::new(move |required, _| {
        calls.lock().unwrap().push(required.to_owned());
        Err(ToolError::Failed("matcher unavailable".into()))
    });
    let result = missing_required_tools(
        &requires("first, second"),
        std::iter::empty::<&str>(),
        &[],
        Some(&matcher),
    );
    assert!(matches!(result, Err(ToolError::Failed(reason)) if reason == "matcher unavailable"));
    assert_eq!(*seen.lock().unwrap(), ["first"]);
}

// Python: tests/skills/test_standalone_helpers.py::test_safe_join_filename_uses_the_default_or_custom_error_kind
#[test]
fn test_safe_join_filename_uses_the_default_or_custom_error_kind() {
    let tree = TempDir::new();
    for kind in [None, Some("reference"), Some("custom attachment")] {
        let error =
            safe_join_filename(&tree.path().join("references"), "../outside", kind).unwrap_err();
        assert!(
            error
                .to_string()
                .contains(&format!("Invalid {} filename", kind.unwrap_or("file")))
        );
    }
}

// Python: tests/skills/test_standalone_helpers.py::test_safe_join_filename_accepts_punctuation_that_is_not_a_drive
#[test]
fn test_safe_join_filename_accepts_punctuation_that_is_not_a_drive() {
    let tree = TempDir::new();
    let directory = tree.path().join("references");
    for name in ["guide:notes.md", " a ", "...x"] {
        assert_eq!(
            safe_join_filename(&directory, name, None).unwrap(),
            directory.join(name)
        );
    }
}

// Python: tests/skills/test_standalone_helpers.py::test_safe_join_filename_rejects_windows_drives_on_every_platform
#[test]
fn test_safe_join_filename_rejects_windows_drives_on_every_platform() {
    let tree = TempDir::new();
    for name in [
        "C:guide.md",
        "z:guide.md",
        "Z:/guide.md",
        "1:guide.md",
        "é:guide.md",
    ] {
        let error = safe_join_filename(&tree.path().join("references"), name, None).unwrap_err();
        assert!(error.to_string().contains("must be relative"));
    }
}

// Python: tests/skills/test_local_files.py::test_safe_join_can_resolve_a_file_that_does_not_exist
#[test]
fn test_safe_join_can_resolve_a_file_that_does_not_exist() {
    let tree = TempDir::new();
    let directory = tree.path().join("missing/references");
    assert_eq!(
        safe_join_filename(&directory, "guide.md", None).unwrap(),
        directory.join("guide.md")
    );
}

// Python: tests/skills/test_standalone_helpers.py::test_safe_join_filename_refuses_a_directory_escape_and_a_dangling_child
#[cfg(unix)]
#[test]
fn test_safe_join_filename_refuses_a_directory_escape_and_a_dangling_child() {
    use std::os::unix::fs::symlink;
    let tree = TempDir::new();
    let local = tree.dir("local");
    let outside = tree.dir("outside");
    let directory = local.join("references");
    symlink(&outside, &directory).unwrap();
    assert!(
        safe_join_filename(&directory, "guide.md", None)
            .unwrap_err()
            .to_string()
            .contains("escapes")
    );
    std::fs::remove_file(&directory).unwrap();
    std::fs::create_dir(&directory).unwrap();
    symlink(outside.join("missing.md"), directory.join("guide.md")).unwrap();
    assert!(
        safe_join_filename(&directory, "guide.md", None)
            .unwrap_err()
            .to_string()
            .contains("escapes")
    );
}
