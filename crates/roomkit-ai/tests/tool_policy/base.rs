use super::*;

// Python: tests/test_tool_policy.py::TestEmptyPolicy::test_empty_allows_all
#[test]
fn test_empty_allows_all() {
    let policy = ToolPolicy::default();
    assert!(policy.is_allowed("anything"));
    assert!(policy.is_allowed("mcp_search"));
}

// Python: tests/test_tool_policy.py::TestEmptyPolicy::test_empty_lists_allows_all
#[test]
fn test_empty_lists_allows_all() {
    assert!(policy(&[], &[]).is_allowed("anything"));
}

macro_rules! judgement {
    ($name:ident, $source:literal, $allow:expr, $deny:expr, $cases:expr) => {
        #[doc = concat!("Python: tests/test_tool_policy.py::", $source)]
        #[test]
        fn $name() {
            let policy = policy($allow, $deny);
            for (name, expected) in $cases {
                assert_eq!(policy.is_allowed(name), expected, "{name}");
            }
        }
    };
}

judgement!(
    test_exact_match,
    "TestAllowPatterns::test_exact_match",
    &["search"],
    &[],
    [("search", true), ("delete", false)]
);
judgement!(
    test_glob_match,
    "TestAllowPatterns::test_glob_match",
    &["mcp_*"],
    &[],
    [
        ("mcp_search", true),
        ("mcp_delete", true),
        ("local_search", false)
    ]
);
judgement!(
    test_a_pattern_reads_star_and_question_mark_alone,
    "TestAllowPatterns::test_a_pattern_reads_star_and_question_mark_alone",
    &["look?p", "mcp_*"],
    &[],
    [
        ("lookup", true),
        ("lookuup", false),
        ("mcp_", true),
        ("mcp_x", true)
    ]
);
judgement!(
    test_multiple_allow_patterns,
    "TestAllowPatterns::test_multiple_allow_patterns",
    &["search", "read_*"],
    &[],
    [
        ("search", true),
        ("read_file", true),
        ("delete_file", false)
    ]
);
judgement!(
    test_exact_deny,
    "TestDenyPatterns::test_exact_deny",
    &[],
    &["delete"],
    [("delete", false), ("search", true)]
);
judgement!(
    test_glob_deny,
    "TestDenyPatterns::test_glob_deny",
    &[],
    &["delete_*"],
    [
        ("delete_file", false),
        ("delete_user", false),
        ("search", true)
    ]
);
judgement!(
    test_deny_wins,
    "TestDenyOverridesAllow::test_deny_wins",
    &["*"],
    &["delete"],
    [("search", true), ("delete", false)]
);
judgement!(
    test_deny_glob_wins_over_allow_glob,
    "TestDenyOverridesAllow::test_deny_glob_wins_over_allow_glob",
    &["mcp_*"],
    &["mcp_delete*"],
    [
        ("mcp_search", true),
        ("mcp_delete", false),
        ("mcp_delete_user", false)
    ]
);
judgement!(
    test_deny_only_blocks_matched,
    "TestEdgeCases::test_deny_only_blocks_matched",
    &[],
    &["bad_*"],
    [("good_tool", true), ("bad_tool", false)]
);
judgement!(
    test_question_mark_glob,
    "TestEdgeCases::test_question_mark_glob",
    &["tool_?"],
    &[],
    [("tool_a", true), ("tool_ab", false)]
);

// Python: tests/test_tool_policy.py::TestAllowPatterns::test_a_pattern_holding_a_bracket_is_refused
#[test]
fn test_a_pattern_holding_a_bracket_is_refused() {
    for field in ["allow", "deny"] {
        let mut input = json!({});
        input[field] = json!(["rm_[a-z]*"]);
        assert!(
            serde_json::from_value::<ToolPolicy>(input.clone())
                .unwrap_err()
                .to_string()
                .contains("holds a bracket")
        );
        assert!(
            serde_json::from_value::<RoleOverride>(input)
                .unwrap_err()
                .to_string()
                .contains("holds a bracket")
        );
    }
    assert!(
        ToolPolicy::new(ToolPolicyOptions {
            allow: strings(&["["]),
            ..Default::default()
        })
        .is_err()
    );
    assert!(
        ToolPolicy::new(ToolPolicyOptions {
            deny: strings(&["]"]),
            ..Default::default()
        })
        .is_err()
    );
    assert!(
        RoleOverride::new(RoleOverrideOptions {
            allow: strings(&["["]),
            ..Default::default()
        })
        .is_err()
    );
    assert!(
        RoleOverride::new(RoleOverrideOptions {
            deny: strings(&["]"]),
            ..Default::default()
        })
        .is_err()
    );
}

// Python: tests/test_tool_policy.py::TestAsFilter::test_as_filter_callable
#[test]
fn test_as_filter_callable() {
    let policy = policy(&["a", "b"], &["b"]);
    let filter = policy.as_filter();
    assert!(filter("a"));
    assert!(!filter("b"));
    assert!(!filter("c"));
}

// Python: tests/test_tool_policy.py::TestAsFilter::test_as_filter_with_builtin_filter
#[test]
fn test_as_filter_with_builtin_filter() {
    let policy = policy(&["search", "read_*"], &[]);
    let filter = policy.as_filter();
    let names: Vec<_> = ["search", "read_file", "delete", "read_log"]
        .into_iter()
        .filter(|name| filter(name))
        .collect();
    assert_eq!(names, ["search", "read_file", "read_log"]);
}

// Python: tests/test_tool_policy.py::TestEdgeCases::test_serialization_roundtrip
#[test]
fn test_serialization_roundtrip() {
    let original = policy(&["a*"], &["ab"]);
    let restored: ToolPolicy =
        serde_json::from_value(serde_json::to_value(&original).unwrap()).unwrap();
    assert_eq!(restored, original);
    assert!(restored.is_allowed("ac"));
    assert!(!restored.is_allowed("ab"));
}
