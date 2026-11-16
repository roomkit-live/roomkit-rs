//! Public served-skill entry and resource contracts (RFC §24.7).
//! Python: `tests/test_mcp_skill_entry.py` and the retained direct-model witnesses.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use pretty_assertions::assert_eq;
use roomkit::model::Metadata;
use roomkit_ai::{SkillEntry, SkillError, SkillResource, SkillResources, verify_skill_resource};
use serde_json::{Value, json};

const URI: &str = "skill://acme/check/SKILL.md";
const HELLO: &str = "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";

#[path = "served_skill_entry/boundaries.rs"]
mod boundaries;
#[path = "served_skill_entry/parsing.rs"]
mod parsing;

fn frontmatter() -> Metadata {
    json!({"name": "check", "description": "Check"})
        .as_object()
        .unwrap()
        .clone()
}

fn entry(resources: Vec<SkillResource>) -> SkillEntry {
    SkillEntry::new(URI, frontmatter(), SkillResources::Listed(resources))
}

// Python: tests/test_mcp_skill_entry.py::TestAValidEntry::test_reads_its_name_path_and_files
#[test]
fn a_direct_entry_reads_its_name_path_and_files() {
    let resource = SkillResource::new(URI, HELLO, 5);
    let entry = entry(vec![resource.clone()]);
    assert_eq!(entry.uri(), URI);
    assert_eq!(entry.name().unwrap(), "check");
    assert_eq!(entry.description().unwrap(), "Check");
    assert_eq!(entry.root(), "skill://acme/check");
    assert_eq!(entry.path(), "acme/check");
    assert!(!entry.dynamic());
    assert_eq!(entry.total_size(), 5);
    assert_eq!(entry.resource(URI), Some(&resource));
    assert_eq!(entry.resource("absent"), None);
    assert_eq!(entry.limit_exceeded(), None);
}

// Python: tests/test_mcp_skill_entry.py::TestAValidEntry::test_a_dynamic_entry_lists_nothing_and_has_no_fingerprint
#[test]
fn a_dynamic_entry_lists_nothing_and_has_no_fingerprint() {
    let entry = SkillEntry::new(URI, frontmatter(), SkillResources::Dynamic);
    assert!(entry.dynamic());
    assert_eq!(entry.resources(), &SkillResources::Dynamic);
    assert_eq!(entry.resource(URI), None);
    assert_eq!(entry.total_size(), 0);
    assert_eq!(entry.fingerprint(), None);
    assert_eq!(entry.limit_exceeded(), None);
}

#[test]
fn direct_construction_is_not_entry_validation() {
    let entry = SkillEntry::new(
        "odd",
        Metadata::new(),
        SkillResources::Listed(vec![SkillResource::new("elsewhere", "not a digest", -7)]),
    );
    assert_eq!(entry.total_size(), -7);
    assert_eq!(entry.name().unwrap_err().key, "name");
    assert_eq!(
        entry.description().unwrap_err().to_string(),
        "'description'"
    );
    assert_eq!(entry.root(), "");
    assert_eq!(entry.path(), "");
    assert_eq!(entry.resource("elsewhere").unwrap().size(), -7);
}

#[test]
fn owned_frontmatter_can_change_through_the_entry() {
    // The owned constructor is the explicit HITL450fa510 language mapping.
    let mut original = frontmatter();
    let mut entry = SkillEntry::new(URI, original.clone(), SkillResources::Dynamic);
    original.insert("name".into(), json!("outside"));
    assert_eq!(entry.name().unwrap(), "check");
    entry.frontmatter_mut().insert("name".into(), json!(12));
    entry
        .frontmatter_mut()
        .insert("description".into(), Value::Null);
    assert_eq!(entry.name().unwrap(), "12");
    assert_eq!(entry.description().unwrap(), "None");
    entry
        .frontmatter_mut()
        .insert("name".into(), json!([true, "x", null]));
    assert_eq!(entry.name().unwrap(), "[True, 'x', None]");
}

#[test]
fn direct_duplicates_keep_the_records_and_look_up_the_last() {
    let first = SkillResource::new(URI, "first", 2);
    let last = SkillResource::new(URI, "last", 3);
    let entry = entry(vec![first.clone(), last.clone()]);
    assert_eq!(
        entry.resources(),
        &SkillResources::Listed(vec![first, last.clone()])
    );
    assert_eq!(entry.resource(URI), Some(&last));
    assert_eq!(entry.total_size(), 5);
    // Python hashlib.sha256 of both sorted URI/digest lines, not just the last record.
    assert_eq!(
        entry.fingerprint().as_deref(),
        Some("sha256:5f09750f4531de449d212dd89d10b02470b4a1351bb8b9f1e2ec50275179af27")
    );
}

#[test]
fn sizes_cover_the_signed_and_unsigned_json_integer_range() {
    let entry = entry(vec![
        SkillResource::new("a", "", u64::MAX),
        SkillResource::new("b", "", u64::MAX),
        SkillResource::new("c", "", i64::MIN),
    ]);
    assert_eq!(entry.total_size(), 27_670_116_110_564_327_422);
    assert_eq!(
        SkillResource::new("a", "", 1_i32),
        SkillResource::new("a", "", 1_u64)
    );
}

// Python: tests/test_mcp_skill_entry.py::TestResolve::test_a_relative_path_resolves_against_the_skill
#[test]
fn a_relative_path_resolves_against_the_skill() {
    for scheme in ["skill", "Skill", "custom+v1.2"] {
        let root = format!("{scheme}://acme/check");
        let mut raw = parsing::raw();
        raw["uri"] = json!(format!("{root}/SKILL.md"));
        raw["resources"] = json!("dynamic");
        let entry = roomkit_ai::parse_entry(&raw).unwrap();
        assert_eq!(entry.root(), root);
        assert_eq!(
            entry.path(),
            if scheme == "skill" {
                "acme/check"
            } else {
                &root
            }
        );
        for path in [
            "examples/email.md",
            "%2e%2e/file",
            "a?query",
            "a#fragment",
            "é/😀",
        ] {
            let uri = entry.resolve(path).unwrap();
            assert_eq!(uri, format!("{root}/{path}"));
            assert_eq!(entry.relative(&uri), path);
        }
    }
}

// Python: tests/test_mcp_skill_entry.py::TestResolve::test_a_path_leaving_the_skill_is_refused
#[test]
fn a_path_leaving_the_skill_is_refused() {
    let entry = roomkit_ai::parse_entry(&parsing::raw()).unwrap();
    for path in [
        "",
        "/etc/passwd",
        "../other/SKILL.md",
        "a/../../x",
        "a//b",
        "./x",
        "a\\b",
    ] {
        assert!(
            entry
                .resolve(path)
                .unwrap_err()
                .to_string()
                .starts_with("Not a path inside the skill: ")
        );
    }
}

#[test]
fn direct_uri_helpers_use_character_slices_without_validation() {
    let entry = SkillEntry::new("é😀abcdefghij", Metadata::new(), SkillResources::Dynamic);
    assert_eq!(entry.root(), "é😀a");
    assert_eq!(entry.path(), "é😀a");
    assert_eq!(entry.relative("é😀abTAIL"), "TAIL");
    assert_eq!(entry.relative("x"), "");
    assert_eq!(
        SkillEntry::new("é😀", Metadata::new(), SkillResources::Dynamic).root(),
        ""
    );
}

// Python: tests/test_mcp_skill_entry.py::TestVerifyResource::test_matching_content_passes
#[test]
fn matching_content_passes() {
    let resource = SkillResource::new(URI, HELLO, 5);
    verify_skill_resource(&resource, b"hello").unwrap();
}

// Python: tests/test_mcp_skill_entry.py::TestVerifyResource::test_a_length_that_differs_fails_before_the_digest
#[test]
fn a_length_that_differs_fails_before_the_digest() {
    let resource = SkillResource::new(URI, "bad digest", 5);
    let error = verify_skill_resource(&resource, b"hello!").unwrap_err();
    assert!(matches!(error, SkillError::Verification { .. }));
    assert_eq!(
        error.to_string(),
        format!("{URI}: 6 bytes where the entry lists 5")
    );
}

// Python: tests/test_mcp_skill_entry.py::TestVerifyResource::test_a_digest_that_differs_fails
#[test]
fn a_digest_that_differs_fails() {
    let resource = SkillResource::new(URI, HELLO, 5);
    assert_eq!(
        verify_skill_resource(&resource, b"hullo")
            .unwrap_err()
            .to_string(),
        format!("{URI}: its digest is not the entry's")
    );
}
