//! Python source limits and additional retained direct-model witnesses.

use pretty_assertions::assert_eq;
use roomkit::model::Metadata;
use roomkit_ai::{
    MAX_RESOURCES_PER_SKILL, MAX_TOTAL_SIZE, SkillEntry, SkillResource, SkillResources,
    parse_entry, verify_skill_resource,
};
use serde_json::{Value, json};

use super::{HELLO, URI, entry, frontmatter, parsing::raw};

fn vectors() -> Value {
    serde_json::from_str(include_str!("reference.json")).unwrap()
}

// Python: tests/test_mcp_skill_entry.py::TestFingerprint::test_binds_every_uri_and_digest_whatever_the_order
#[test]
fn binds_every_uri_and_digest_whatever_the_order() {
    for row in vectors()["fingerprints"].as_array().unwrap() {
        let files = row["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|file| {
                SkillResource::new(
                    file["uri"].as_str().unwrap(),
                    file["digest"].as_str().unwrap(),
                    file["size"].as_u64().unwrap(),
                )
            })
            .collect();
        let entry = entry(files);
        assert_eq!(entry.fingerprint().as_deref(), row["fingerprint"].as_str());
        if !row["files"].as_array().unwrap().is_empty() {
            let mut value = raw();
            value["resources"] = row["files"].clone();
            assert_eq!(
                parse_entry(&value).unwrap().fingerprint(),
                entry.fingerprint()
            );
        }
    }
}

// Python: tests/test_mcp_skill_entry.py::TestFingerprint::test_changes_when_a_file_changes
#[test]
fn changes_when_a_file_changes() {
    let mut value = raw();
    let first = parse_entry(&value).unwrap();
    value["resources"][0]["digest"] = json!(format!("sha256:{}", "1".repeat(64)));
    let changed = parse_entry(&value).unwrap();
    value["resources"][0]["digest"] = json!(HELLO);
    value["resources"][0]["size"] = json!(55);
    let resized = parse_entry(&value).unwrap();
    assert_ne!(first.fingerprint(), changed.fingerprint());
    assert_eq!(first.fingerprint(), resized.fingerprint());
}

// Python: tests/test_mcp_skill_entry.py::TestLimits::test_an_entry_at_the_limits_is_within_them
#[test]
fn an_entry_at_the_limits_is_within_them() {
    let mut value = raw();
    for index in 1..MAX_RESOURCES_PER_SKILL {
        value["resources"].as_array_mut().unwrap().push(json!({
            "uri": format!("skill://acme/check/f{index}"), "digest":HELLO, "size":0
        }));
    }
    value["resources"][0]["size"] = json!(MAX_TOTAL_SIZE);
    let entry = parse_entry(&value).unwrap();
    assert_eq!(entry.total_size(), i128::from(MAX_TOTAL_SIZE));
    assert_eq!(entry.limit_exceeded(), None);
}

// Python: tests/test_mcp_skill_entry.py::TestLimits::test_an_entry_past_the_file_count_says_so
#[test]
fn an_entry_past_the_file_count_says_so() {
    let mut value = raw();
    for index in 0..MAX_RESOURCES_PER_SKILL {
        value["resources"].as_array_mut().unwrap().push(json!({
            "uri": format!("skill://acme/check/f{index}"), "digest": HELLO, "size": MAX_TOTAL_SIZE
        }));
    }
    assert_eq!(
        parse_entry(&value).unwrap().limit_exceeded().as_deref(),
        Some("it lists 513 files, past 512")
    );
}

// Python: tests/test_mcp_skill_entry.py::TestLimits::test_an_entry_past_the_total_size_says_so
#[test]
fn an_entry_past_the_total_size_says_so() {
    let mut value = raw();
    value["resources"][0]["size"] = json!(MAX_TOTAL_SIZE + 1);
    assert_eq!(
        parse_entry(&value).unwrap().limit_exceeded().as_deref(),
        Some("its files hold 16777217 bytes, past 16777216")
    );
}

// Python: tests/test_mcp_skill_entry.py::test_an_entry_whose_frontmatter_is_too_large_is_refused
#[test]
fn the_frontmatter_bound_counts_the_default_json_characters() {
    let base =
        usize::try_from(vectors()["frontmatter_empty_notes_chars"].as_u64().unwrap()).unwrap();
    for (character, width) in [("x", 1), ("é", 6), ("😀", 12), ("\0", 6)] {
        let available = 65536 - base;
        let notes = character.repeat(available / width) + &"a".repeat(available % width);
        let mut value = raw();
        value["frontmatter"]["notes"] = json!(notes);
        let entry = parse_entry(&value).unwrap();
        value["frontmatter"]["notes"] = json!(format!("{notes}a"));
        assert!(
            parse_entry(&value)
                .unwrap_err()
                .to_string()
                .contains("past 65536 characters")
        );
        assert_eq!(entry.frontmatter().get("notes"), Some(&json!(notes)));
    }
}

// Python: tests/test_mcp_skill_entry.py::test_an_entry_whose_frontmatter_is_too_large_is_refused
#[test]
fn an_entry_whose_frontmatter_is_too_large_is_refused() {
    let mut value = raw();
    value["frontmatter"]["notes"] = json!("n".repeat(70_000));
    assert!(
        parse_entry(&value)
            .unwrap_err()
            .to_string()
            .contains("past 65536 characters")
    );
}

#[test]
fn gating_keys_preserve_scalars_and_flat_lists() {
    for key in ["requires", "allowed_tools"] {
        for contents in [
            Value::Null,
            json!(true),
            json!(0),
            json!(1.5),
            json!("read"),
            json!([]),
            json!(["read", null, 2]),
        ] {
            let mut value = raw();
            value["frontmatter"][key] = contents.clone();
            assert_eq!(
                parse_entry(&value).unwrap().frontmatter().get(key),
                Some(&contents)
            );
        }
    }
}

#[test]
fn entry_equality_uses_python_numbers_without_losing_integer_precision() {
    for (left, right) in [
        (json!(true), json!(1)),
        (json!(false), json!(-0.0)),
        (json!(-0.0), json!(0.0)),
        (json!(1), json!(1.0)),
    ] {
        let make = |v| {
            SkillEntry::new(
                URI,
                json!({"name":"check", "extra":[{"value":v}]})
                    .as_object()
                    .unwrap()
                    .clone(),
                SkillResources::Dynamic,
            )
        };
        let first = make(left);
        let mut second = make(right);
        let name = second.frontmatter_mut().shift_remove("name").unwrap();
        second.frontmatter_mut().insert("name".into(), name);
        assert_eq!(first, second);
        assert_ne!(
            first,
            SkillEntry::new(
                URI,
                first.frontmatter().clone(),
                SkillResources::Listed(vec![])
            )
        );
    }
    for (left, right) in [
        (
            json!(9_007_199_254_740_993_u64),
            json!(9_007_199_254_740_992.0),
        ),
        (json!(u64::MAX), json!(18_446_744_073_709_551_616.0)),
        (json!(1), json!("1")),
    ] {
        let make = |v| {
            SkillEntry::new(
                URI,
                Metadata::from_iter([("v".into(), v)]),
                SkillResources::Dynamic,
            )
        };
        assert_ne!(make(left), make(right));
    }
}

#[test]
fn resource_verification_uses_exact_bytes() {
    for row in vectors()["resources"].as_array().unwrap() {
        let content: Vec<u8> = row["bytes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| u8::try_from(b.as_u64().unwrap()).unwrap())
            .collect();
        let resource = SkillResource::new(URI, row["digest"].as_str().unwrap(), content.len());
        verify_skill_resource(&resource, &content).unwrap();
        let wrong = SkillResource::new(URI, format!("sha256:{}", "0".repeat(64)), content.len());
        assert!(
            verify_skill_resource(&wrong, &content)
                .unwrap_err()
                .to_string()
                .contains("digest is not")
        );
        let mut changed = content;
        changed.push(0);
        assert!(
            verify_skill_resource(&resource, &changed)
                .unwrap_err()
                .to_string()
                .contains("bytes where")
        );
    }
}

#[test]
fn parsing_keeps_the_digest_regexs_final_newline() {
    let mut value = raw();
    value["resources"][0]["digest"] = json!(format!("{HELLO}\n"));
    let entry = parse_entry(&value).unwrap();
    let resource = entry.resource(URI).unwrap();
    assert_eq!(resource.digest(), format!("{HELLO}\n"));
    assert!(
        verify_skill_resource(resource, b"hello")
            .unwrap_err()
            .to_string()
            .contains("digest is not")
    );
}

/// Keep the borrowed input's own destruction safe even when a test assertion fails.
struct Input(Value);
impl Drop for Input {
    fn drop(&mut self) {
        let mut pending = vec![std::mem::take(&mut self.0)];
        while let Some(value) = pending.pop() {
            match value {
                Value::Array(mut items) => pending.append(&mut items),
                Value::Object(fields) => pending.extend(fields.into_values()),
                Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
            }
        }
    }
}

fn nested(depth: usize, object: bool) -> Input {
    let mut deep = Value::Null;
    for _ in 0..depth {
        deep = if object {
            Value::Object(Metadata::from_iter([("d".into(), deep)]))
        } else {
            Value::Array(vec![deep])
        };
    }
    let mut value = raw();
    value
        .get_mut("frontmatter")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("deep".into(), deep);
    Input(value)
}

// Python: tests/test_mcp_skill_entry.py::test_an_entry_whose_frontmatter_nests_too_deep_is_refused
#[test]
fn an_entry_whose_frontmatter_nests_too_deep_is_refused() {
    let value = nested(100_000, true);
    assert!(
        parse_entry(&value.0)
            .unwrap_err()
            .to_string()
            .contains("past 65536")
    );
}

#[test]
fn deeply_nested_json_below_the_size_limit_is_copied_compared_and_dropped() {
    // HITL1566ac3f: use the size bound, not a Python runtime's recursion limit.
    for depth in [128, 129, 256, 900, 2000, 10_000] {
        let mut value = nested(depth, false);
        let entry = parse_entry(&value.0).unwrap();
        let mut current = entry.frontmatter().get("deep").unwrap();
        for _ in 0..depth {
            let items = current.as_array().unwrap();
            assert_eq!(items.len(), 1);
            current = &items[0];
        }
        assert!(current.is_null());
        let copied = entry.clone();
        assert_eq!(entry, copied);
        value.0["frontmatter"]["name"] = json!("outside");
        assert_eq!(entry.name().unwrap(), "check");
        drop(entry);
        drop(copied);
    }
}

#[test]
fn mutating_owned_frontmatter_does_not_mutate_a_cloned_entry() {
    let mut entry = SkillEntry::new(URI, frontmatter(), SkillResources::Dynamic);
    let copied = entry.clone();
    entry
        .frontmatter_mut()
        .insert("name".into(), json!("changed"));
    assert_eq!(copied.name().unwrap(), "check");
}
