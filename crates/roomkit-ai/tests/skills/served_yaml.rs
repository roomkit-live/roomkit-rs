//! Field verification is tested against the Python-green shared corpus (RFC §24.7).

use roomkit_ai::{parse_entry, verify_frontmatter};
use serde_json::Value;

#[test]
fn served_frontmatter_matches_the_shared_positive_and_negative_corpus() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/scenarios/served-skills");
    let mut tested = 0;
    for file in std::fs::read_dir(root).unwrap() {
        let file = file.unwrap();
        let filename = file.file_name();
        let filename = filename.to_str().unwrap();
        if !filename.starts_with("frontmatter-") {
            continue;
        }
        let scenario: Value =
            serde_json::from_str(&std::fs::read_to_string(file.path()).unwrap()).unwrap();
        let replies = &scenario["steps"][0]["servers"]["remote"]["replies"];
        let entry = parse_entry(&replies[0]["result"]["skills"][0]).unwrap();
        let text = replies[1]["result"]["contents"][0]["text"]
            .as_str()
            .unwrap();
        let result = verify_frontmatter(&entry, text);
        if filename.starts_with("frontmatter-accepts-") {
            assert_eq!(result.unwrap(), "Follow the steps.", "{filename}");
        } else {
            assert!(result.is_err(), "{filename}");
        }
        tested += 1;
    }
    assert_eq!(tested, 16);
}

/// Python `read_frontmatter_yaml`, the same bounded SafeLoader used by
/// `verify_frontmatter`, frozen before the Rust comparison. Finite values only:
/// a non-finite number cannot be admitted as a JSON SkillEntry value.
#[test]
fn served_yaml_scalar_and_key_normalization_matches_python() {
    let cases: Vec<Value> = serde_json::from_str(include_str!("served_yaml_cases.json")).unwrap();
    let mut differences = Vec::new();
    for case in cases {
        let frontmatter = case
            .get("frontmatter")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"name":"guide","description":"Read the guide"}));
        let entry = parse_entry(&serde_json::json!({"name":"guide","description":"Read the guide","uri":"skill://guide/SKILL.md","frontmatter":frontmatter,"resources":"dynamic"})).unwrap();
        let text = format!("---\n{}---\nBody", case["yaml"].as_str().unwrap());
        let got = verify_frontmatter(&entry, &text);
        if got.is_ok() != case["valid"].as_bool().unwrap() {
            differences.push(format!("{}: {got:?}", case["expression"]));
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n"));
}
