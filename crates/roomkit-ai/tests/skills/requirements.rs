//! Python prerequisite metadata and bounded frontmatter reads (RFC §24.2).

use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannelOptions, AiTool, SkillError, SkillMetadata, SkillRegistry, SkillToolNames, ToolResult,
    Tools,
};
use serde_json::json;

use super::support::{Room, TempDir, skill_md, tool_results};

// Python: tests/test_skill_tool_gating.py::test_requires_reads_a_value_as_allowed_tools_does.
#[test]
fn requires_reads_text_and_list_values_without_changing_metadata() {
    for (value, expected) in [
        (
            json!(["search_*", " fetch_* "]),
            vec!["search_*", "fetch_*"],
        ),
        (json!("search_*, fetch_*"), vec!["search_*", "fetch_*"]),
        (json!("[search_*, fetch_*]"), vec!["search_*", "fetch_*"]),
        (
            json!("'search_*', \"fetch_*\""),
            vec!["search_*", "fetch_*"],
        ),
        (
            json!("['search_*', 'fetch_*']"),
            vec!["search_*", "fetch_*"],
        ),
        (json!("search_*, , "), vec!["search_*"]),
        (json!(""), vec![]),
        (json!(null), vec![]),
    ] {
        let mut metadata = SkillMetadata::new("research", "d");
        metadata.allowed_tools = match &value {
            serde_json::Value::String(text) => Some(SkillToolNames::Text(text.clone())),
            serde_json::Value::Array(items) => Some(SkillToolNames::List(
                items
                    .iter()
                    .map(|item| item.as_str().unwrap().to_owned())
                    .collect(),
            )),
            serde_json::Value::Null
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_)
            | serde_json::Value::Object(_) => None,
        };
        metadata.extra_metadata.insert("requires".into(), value);
        let before = metadata.clone();
        assert_eq!(metadata.required_tool_names(), expected);
        assert_eq!(metadata.gated_tool_names(), expected);
        assert_eq!(metadata, before);
    }
}

// Python: tests/test_skill_tool_gating.py::test_requires_reads_a_value_as_allowed_tools_does;
// tests/test_skill_frontmatter_lines.py::test_each_form_reads_as_yaml_reads_it.
#[test]
fn frontmatter_keeps_requires_as_text_in_extra_metadata() {
    for (value, text, names) in [
        ("tool_a, tool_b", "tool_a, tool_b", vec!["tool_a", "tool_b"]),
        (
            "[tool_a, 'tool_b']",
            "tool_a, tool_b",
            vec!["tool_a", "tool_b"],
        ),
        (
            "\n  - tool_a\n  - tool_b",
            "tool_a, tool_b",
            vec!["tool_a", "tool_b"],
        ),
        ("[]", "", vec![]),
        ("[null, tool_a]", "tool_a", vec!["tool_a"]),
        ("'calendar_*'", "calendar_*", vec!["calendar_*"]),
        ("\n  - 'a,b'", "a,b", vec!["a", "b"]),
    ] {
        let tree = TempDir::new();
        let path = tree.skill_with(
            "skills",
            "a",
            &skill_md("a", "d", &format!("requires: {value}\n")),
        );
        let parsed = SkillRegistry::default().register(path);
        assert!(parsed.is_ok(), "{value}: {parsed:?}");
        let metadata = parsed.unwrap();
        assert_eq!(metadata.extra_metadata.get("requires"), Some(&json!(text)));
        assert_eq!(metadata.required_tool_names(), names);
        assert!(metadata.allowed_tools.is_none());
    }
}

// Python: tests/test_skill_frontmatter_lines.py::test_the_line_reader_never_gates_less_than_yaml;
// tests/test_skill_frontmatter_lines.py::test_a_gating_key_a_line_cannot_hold_whole_refuses_the_skill.
#[test]
fn repeated_requirements_cannot_erase_dependencies_and_unreadable_ones_are_refused() {
    let tree = TempDir::new();
    for extra in ["requires: tool_b\n", "requires: []\n", "requires: null\n"] {
        let path = tree.skill_with(
            "skills",
            "a",
            &skill_md("a", "d", &format!("requires: tool_a\n{extra}")),
        );
        let metadata = SkillRegistry::default().register(path).unwrap();
        assert!(
            metadata
                .required_tool_names()
                .contains(&"tool_a".to_owned())
        );
    }
    for value in [
        "[tool_a, [tool_b]]",
        "{tool_a: true}",
        "'tool_[ab]'",
        ">-\n  tool_a",
        "yes",
    ] {
        let path = tree.skill_with(
            "skills",
            "a",
            &skill_md("a", "d", &format!("requires: {value}\n")),
        );
        assert!(SkillRegistry::default().register(path).is_err(), "{value}");
    }
}

// Python: tests/test_skill_tool_gating.py::test_a_bracket_within_an_entry_refuses_the_skill;
// skills/models.py::bracketed_pattern_errors uses tools/policy.py::bracket_error for both keys.
#[test]
fn bracketed_requirements_and_gates_explain_the_supported_pattern_syntax() {
    let tree = TempDir::new();
    for key in ["requires", "allowed_tools"] {
        let path = tree.skill_with(
            "skills",
            "a",
            &skill_md("a", "d", &format!("{key}: 'tool_[ab]'\n")),
        );
        assert_eq!(
            SkillRegistry::default().register(path),
            Err(SkillError::Validation {
                skill: "a".into(),
                reasons: vec![format!(
                    "{key}: tool pattern 'tool_[ab]' holds a bracket: a pattern reads * and ? alone",
                )],
            }),
        );
    }
}

// Python: tests/test_skill_requires_parity.py::test_an_offered_required_tool_lets_the_activation_through.
#[tokio::test]
async fn an_offered_required_tool_returns_the_exact_instructions() {
    let tree = TempDir::new();
    let path = tree.skill_with(
        "skills",
        "cal",
        "---\nname: cal\ndescription: cal skill\nrequires: calendar\n---\nUse calendar.",
    );
    let mut registry = SkillRegistry::default();
    registry.register(path).unwrap();
    let tools = Tools::new().tool(
        AiTool::new("calendar", "Read calendar").unwrap(),
        |_call, _context| async { Ok(ToolResult::from("ok")) },
    );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "activate_skill", r#"{"name":"cal"}"#)]),
        ScriptedRound::answering("Done."),
    ]);
    let room = Room::new(model.clone(), registry, |options| AiChannelOptions {
        tools: tools.into(),
        ..options
    })
    .await;
    room.says("Go").await;
    let results = tool_results(&model);
    let result: serde_json::Value = serde_json::from_str(&results[1][0]).unwrap();
    assert_eq!(result["instructions"], "Use calendar.");
}
