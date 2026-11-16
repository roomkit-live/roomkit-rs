//! Python: `tests/test_mcp_skill_entry.py`, entry admission and file-list contracts.

use pretty_assertions::assert_eq;
use roomkit_ai::{SkillError, parse_entry};
use serde_json::{Value, json};

use super::{HELLO, URI};

pub(super) fn raw() -> Value {
    json!({
        "uri": URI,
        "frontmatter": {"name": "check", "description": "Check"},
        "resources": [{"uri": URI, "digest": HELLO, "size": 5}]
    })
}

fn refused(value: &Value, reason: &str) {
    let error = parse_entry(value).unwrap_err();
    assert!(matches!(error, SkillError::EntryValidation { .. }));
    assert!(error.to_string().contains(reason), "{error}");
}

// Python: tests/test_mcp_skill_entry.py::TestAValidEntry::test_reads_its_name_path_and_files
#[test]
fn reads_its_name_path_and_files() {
    let mut value = raw();
    let reference = "skill://acme/check/reference.md";
    value["resources"]
        .as_array_mut()
        .unwrap()
        .push(json!({"uri": reference, "digest": HELLO, "size": 5}));
    let entry = parse_entry(&value).unwrap();
    assert_eq!(entry.name().unwrap(), "check");
    assert_eq!(entry.description().unwrap(), "Check");
    assert_eq!(entry.root(), "skill://acme/check");
    assert_eq!(entry.path(), "acme/check");
    assert!(!entry.dynamic());
    assert_eq!(entry.resource(URI).unwrap().digest(), HELLO);
    assert_eq!(entry.resource(reference).unwrap().size(), 5);
    assert_eq!(entry.resource("skill://acme/check/absent"), None);
}

// Python: tests/test_mcp_skill_entry.py::TestAValidEntry::test_a_path_under_another_scheme_keeps_it
#[test]
fn a_path_under_another_scheme_keeps_it() {
    let mut value = raw();
    value["uri"] = json!("github://owner/check/SKILL.md");
    value["resources"] = json!("dynamic");
    let entry = parse_entry(&value).unwrap();
    assert_eq!(entry.path(), "github://owner/check");
}

// Python: tests/test_mcp_skill_entry.py::TestAValidEntry::test_a_dynamic_entry_lists_nothing_and_has_no_fingerprint
#[test]
fn a_dynamic_entry_lists_nothing_and_has_no_fingerprint() {
    let mut value = raw();
    value["resources"] = json!("dynamic");
    let entry = parse_entry(&value).unwrap();
    assert!(entry.dynamic());
    assert_eq!(entry.total_size(), 0);
    assert_eq!(entry.fingerprint(), None);
    assert_eq!(entry.resource(URI), None);
}

// Python: tests/test_mcp_skill_entry.py::TestAValidEntry::test_extra_frontmatter_passes_through_unchanged
#[test]
fn extra_frontmatter_passes_through_unchanged() {
    let mut value = raw();
    value["frontmatter"]["license"] = json!("MIT");
    value["frontmatter"]["metadata"] = json!({"a": [1]});
    let entry = parse_entry(&value).unwrap();
    value["frontmatter"]["metadata"]["a"] = json!([1, 2]);
    assert_eq!(
        entry.frontmatter().get("metadata"),
        Some(&json!({"a": [1]}))
    );
    assert_eq!(entry.frontmatter().get("license"), Some(&json!("MIT")));
}

// Python: tests/test_mcp_skill_entry.py::TestARefusedEntry::test_without_a_skill_uri_or_frontmatter
#[test]
fn without_a_skill_uri_or_frontmatter() {
    for (value, reason) in [
        (json!("not an object"), "not an object"),
        (json!({"frontmatter": {}}), "no URI"),
        (json!(null), "not an object"),
    ] {
        refused(&value, reason);
    }
    for (uri, reason) in [
        ("skill://acme/check/README.md", "not the URI of a SKILL.md"),
        ("check/SKILL.md", "not the URI of a SKILL.md"),
        ("skill://acme/../check/SKILL.md", "not a plain path"),
    ] {
        let mut value = raw();
        value["uri"] = json!(uri);
        refused(&value, reason);
    }
    let mut value = raw();
    value["frontmatter"] = Value::Null;
    refused(&value, "no frontmatter");
}

// Python: tests/test_mcp_skill_entry.py::TestARefusedEntry::test_whose_name_is_not_the_uris
#[test]
fn whose_name_is_not_the_uris() {
    let mut value = raw();
    value["frontmatter"]["name"] = json!("other");
    assert_eq!(
        parse_entry(&value).unwrap_err().to_string(),
        format!("{URI}: the frontmatter name 'other' is not the URI's")
    );
}

// Python: tests/test_mcp_skill_entry.py::TestARefusedEntry::test_whose_name_is_not_a_skill_name
#[test]
fn whose_name_is_not_a_skill_name() {
    for name in [
        json!("Check"),
        json!("ch_eck"),
        json!(""),
        Value::Null,
        json!("a".repeat(65)),
    ] {
        let mut value = raw();
        value["frontmatter"]["name"] = name;
        refused(&value, "not a skill name");
    }
}

// Python: tests/test_mcp_skill_entry.py::TestARefusedEntry::test_without_a_description
#[test]
fn without_a_description() {
    for description in [json!(""), Value::Null, json!("d".repeat(1025))] {
        let mut value = raw();
        value["frontmatter"]["description"] = description;
        refused(&value, "description is missing or too long");
    }
}

// Python: tests/test_mcp_skill_entry.py::TestARefusedEntry::test_whose_gating_key_is_neither_a_text_nor_a_list_of_texts
#[test]
fn whose_gating_key_is_neither_a_text_nor_a_list_of_texts() {
    for key in ["requires", "allowed_tools"] {
        for bad in [
            json!({"search_web":true}),
            json!([["search_web", "fetch"]]),
            json!([{}]),
        ] {
            let mut value = raw();
            value["frontmatter"][key] = bad;
            refused(&value, &format!("{key} is neither a text"));
        }
    }
}

// Python: tests/test_mcp_skill_entry.py::TestARefusedEntry::test_whose_resources_are_neither_a_list_nor_dynamic
#[test]
fn whose_resources_are_neither_a_list_nor_dynamic() {
    for resources in [Value::Null, json!("static"), json!({"uri":"x"}), json!(3)] {
        let mut value = raw();
        value["resources"] = resources;
        refused(&value, "resources is neither a list nor 'dynamic'");
    }
    let mut value = raw();
    value.as_object_mut().unwrap().shift_remove("resources");
    refused(&value, "resources is neither a list nor 'dynamic'");
}

// Python: tests/test_mcp_skill_entry.py::TestARefusedEntry::test_that_omits_its_own_skill_md
#[test]
fn that_omits_its_own_skill_md() {
    let mut value = raw();
    value["resources"][0]["uri"] = json!("skill://acme/check/reference.md");
    refused(&value, "does not list the skill's own SKILL.md");
}

// Python: tests/test_mcp_skill_entry.py::TestARefusedEntry::test_that_lists_a_file_outside_the_skill
#[test]
fn that_lists_a_file_outside_the_skill() {
    for uri in [
        "skill://acme/other/SKILL.md",
        "skill://acme/check-old/x.md",
        "skill://acme/check/../other/x.md",
        "skill://acme/check//x.md",
        "github://acme/check/x.md",
    ] {
        let mut value = raw();
        value["resources"][0]["uri"] = json!(uri);
        refused(&value, "is outside the skill");
    }
}

// Python: tests/test_mcp_skill_entry.py::TestARefusedEntry::test_that_lists_a_file_twice
#[test]
fn that_lists_a_file_twice() {
    let mut value = raw();
    let file = value["resources"][0].clone();
    value["resources"].as_array_mut().unwrap().push(file);
    refused(&value, "is listed twice");
}

// Python: tests/test_mcp_skill_entry.py::TestARefusedEntry::test_whose_file_is_malformed
#[test]
fn whose_file_is_malformed() {
    for (key, bad) in [
        ("digest", json!("sha256:ABC")),
        ("digest", json!(format!("md5:{}", "0".repeat(64)))),
        ("size", json!(-1)),
        ("size", json!(true)),
        ("size", json!("3")),
        ("size", json!(1.0)),
        ("uri", Value::Null),
    ] {
        let mut value = raw();
        value["resources"][0][key] = bad;
        refused(&value, URI);
    }
    let mut value = raw();
    value["resources"][0] = json!("not an object");
    refused(&value, "a listed file is not an object");
}
