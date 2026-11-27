//! Python's local skill metadata and frontmatter gate tests (RFC §24.1–24.2).

use roomkit_ai::{SkillMetadata, SkillRegistry, SkillToolNames, ToolPolicy};

use super::support::{TempDir, skill_md};

// Python: tests/test_skill_tool_gating.py::TestFrontmatterKeepsTheList;
// tests/test_skill_frontmatter_lines.py::test_each_form_reads_as_yaml_reads_it.
#[test]
fn each_form_keeps_its_public_metadata_type() {
    let list = || SkillToolNames::List(vec!["search_*".into(), "fetch_*".into()]);
    let cases = [
        ("", None),
        ("allowed_tools: null\n", None),
        ("allowed_tools: []\n", Some(SkillToolNames::List(vec![]))),
        (
            "allowed_tools: ''\n",
            Some(SkillToolNames::Text(String::new())),
        ),
        (
            "allowed_tools: search_*, fetch_*\n",
            Some(SkillToolNames::Text("search_*, fetch_*".into())),
        ),
        ("allowed_tools: [search_*, 'fetch_*']\n", Some(list())),
        (
            "allowed_tools:\n  - search_*\n  - 'fetch_*' # note\n",
            Some(list()),
        ),
        ("'allowed_tools':\n- search_*\n- fetch_*\n", Some(list())),
        (
            "allowed_tools: [3d_render, 123abc*]\n",
            Some(SkillToolNames::List(vec![
                "3d_render".into(),
                "123abc*".into(),
            ])),
        ),
        (
            "allowed_tools: [null, search_*, ~, fetch_*]\n",
            Some(list()),
        ),
        (
            "allowed_tools:\n  - \"issue_refund: money\"\n",
            Some(SkillToolNames::List(vec!["issue_refund: money".into()])),
        ),
        (
            "allowed_tools: 'issue_refund: money'\n",
            Some(SkillToolNames::Text("issue_refund: money".into())),
        ),
        (
            "allowed_tools: [issue_refund:money, 'void: *']\n",
            Some(SkillToolNames::List(vec![
                "issue_refund:money".into(),
                "void: *".into(),
            ])),
        ),
    ];
    for (frontmatter, expected) in cases {
        let root = TempDir::new();
        let path = root.skill_with(
            "skills",
            "research",
            &skill_md("research", "d", frontmatter),
        );
        let registry = SkillRegistry::default();
        let metadata = registry.register(&path).unwrap();
        assert_eq!(metadata.allowed_tools, expected, "{frontmatter}");
        assert_eq!(registry.get_metadata("research"), Some(metadata));
    }
}

// Python: tests/test_channels/test_ai_skill_gating.py::TestGatedToolNames;
// tests/test_skill_tool_gating.py::test_requires_reads_a_value_as_allowed_tools_does
// (allowed_tools half; requirements.rs verifies both on the same values).
#[test]
fn gated_tool_names_reads_text_and_list_without_changing_them() {
    let cases = [
        (None, vec![]),
        (Some(SkillToolNames::Text(String::new())), vec![]),
        (
            Some(SkillToolNames::Text("my_tool".into())),
            vec!["my_tool"],
        ),
        (
            Some(SkillToolNames::Text("a, b, c".into())),
            vec!["a", "b", "c"],
        ),
        (
            Some(SkillToolNames::Text("  x , y ,  ".into())),
            vec!["x", "y"],
        ),
        (
            Some(SkillToolNames::Text(
                " search_* , , 'fetch_*' , \"lookup\" ".into(),
            )),
            vec!["search_*", "fetch_*", "lookup"],
        ),
        (
            Some(SkillToolNames::Text("['search_*', 'fetch_*']".into())),
            vec!["search_*", "fetch_*"],
        ),
        (Some(SkillToolNames::Text("[ab]".into())), vec!["ab"]),
        (
            Some(SkillToolNames::List(vec![
                " search_* ".into(),
                "a,b".into(),
                String::new(),
            ])),
            vec!["search_*", "a,b"],
        ),
    ];
    for (value, expected) in cases {
        let mut metadata = SkillMetadata::new("research", "d");
        assert!(metadata.allowed_tools.is_none());
        metadata.allowed_tools.clone_from(&value);
        assert_eq!(metadata.gated_tool_names(), expected);
        assert_eq!(metadata.allowed_tools, value);
    }
}

// Python: tests/test_skill_tool_gating.py::TestGlobsGate,
// TestGatingSitesUseTheSameMatcher, test_a_bracketed_text_reads_as_a_list.
#[test]
fn a_skill_and_a_policy_agree_on_their_globs() {
    for value in [
        SkillToolNames::Text("search_*, fetch_v?, run_report".into()),
        SkillToolNames::List(vec![
            "search_*".into(),
            "fetch_v?".into(),
            "run_report".into(),
        ]),
    ] {
        let mut metadata = SkillMetadata::new("research", "d");
        metadata.allowed_tools = Some(value);
        let policy = ToolPolicy::new(roomkit_ai::ToolPolicyOptions {
            allow: metadata.gated_tool_names(),
            deny: vec![],
            ..Default::default()
        })
        .expect("valid tool policy");
        for (name, expected) in [
            ("search_web", true),
            ("search_", true),
            ("fetch_vé", true),
            ("fetch_v12", false),
            ("run_report", true),
            ("run_report_v2", false),
            ("send_email", false),
        ] {
            assert_eq!(metadata.gates(name), expected, "{name}");
            assert_eq!(metadata.gates(name), policy.is_allowed(name));
        }
    }
    let mut metadata = SkillMetadata::new("research", "d");
    assert!(!metadata.gates("anything"));
    metadata.allowed_tools = Some(SkillToolNames::Text("[ab]".into()));
    assert!(metadata.gates("ab"));
    assert!(!metadata.gates("a") && !metadata.gates("b"));
}

// Python: tests/test_skill_tool_gating.py::TestGlobsGate (all five methods).
#[test]
fn globs_gate_the_names_the_skill_names() {
    for allowed in [
        SkillToolNames::Text("search_*".into()),
        SkillToolNames::List(vec!["search_*".into()]),
    ] {
        let mut metadata = SkillMetadata::new("research", "d");
        metadata.allowed_tools = Some(allowed);
        for (name, expected) in [
            ("search_web", true),
            ("search_docs", true),
            ("delete_everything", false),
            ("fetch_url", false),
        ] {
            assert_eq!(metadata.gates(name), expected, "{name}");
        }
    }
    let root = TempDir::new();
    let path = root.skill_with(
        "skills",
        "research",
        &skill_md(
            "research",
            "d",
            "allowed_tools:\n  - search_*\n  - fetch_*\n",
        ),
    );
    let metadata = SkillRegistry::default().register(path).unwrap();
    assert!(metadata.gates("search_web") && metadata.gates("fetch_url"));
    assert!(!metadata.gates("send_email"));
}

// Python: tests/test_skill_frontmatter_lines.py::test_the_line_reader_never_gates_less_than_yaml;
// tests/test_skill_tool_gating.py::test_a_bracketed_text_reads_as_a_list (repeated-key paths).
#[test]
fn repeated_keys_cannot_erase_an_earlier_gate() {
    for extra in [
        "allowed_tools: other\n",
        "allowed_tools:\n  - other\n",
        "allowed_tools: []\n",
        "allowed_tools: null\n",
        "metadata:\n  allowed_tools: other\n",
    ] {
        let root = TempDir::new();
        let text = skill_md(
            "research",
            "Use it: for research",
            &format!("allowed_tools: '[ab]'\n{extra}"),
        );
        let path = root.skill_with("skills", "research", &text);
        let metadata = SkillRegistry::default().register(path).unwrap();
        assert!(metadata.gates("ab"), "{extra}");
        assert!(!metadata.gates("a") && !metadata.gates("b"));
    }
}

// Python: tests/test_skill_frontmatter_lines.py::test_a_gating_key_a_line_cannot_hold_whole_refuses_the_skill,
// test_a_line_it_cannot_place_under_a_list_key_refuses_the_skill,
// test_the_line_reader_never_gates_less_than_yaml;
// tests/test_skill_tool_gating.py::test_a_bracket_within_an_entry_refuses_the_skill.
#[test]
fn a_gate_that_cannot_be_read_whole_refuses_the_skill() {
    for gate in [
        "[\n search_*,\n fetch_*\n]",
        ">-\n  search_*,\n  fetch_*",
        "|\n  search_*",
        "\nsearch_*",
        "search_*\n  - fetch_*",
        "[search_*, [fetch_*]]",
        "{search: true}",
        "\n  - search_*\n  scope: research",
        "\n  - search_*\n   - fetch_*",
        "\n  - &tools search_*",
        "!tool search_*",
        "'search_*",
        "\"search_*\" trailing",
        "rm_[a-z]*",
        "'fetch]'",
        "\"[sw]earch_*, fetch_v?\"",
        "issue_refund: money",
        "search_*, issue_refund: money",
        "\n  - issue_refund: money",
        "[search_*, issue_refund: money]",
        "\n  - issue_refund:\tmoney",
        "issue_refund:",
        "\n  - issue_refund:",
        "[issue_refund:, search_*]",
    ] {
        let root = TempDir::new();
        let path = root.skill_with(
            "skills",
            "research",
            &skill_md("research", "d", &format!("allowed_tools: {gate}\n")),
        );
        let registry = SkillRegistry::default();
        assert!(registry.register(path).is_err(), "accepted {gate:?}");
        assert!(!registry.has_entries());
    }
}
