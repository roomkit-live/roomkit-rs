use super::*;

// Python: tests/tools/test_role_policy.py::test_alias_admission_applies_both_allow_constraints
#[test]
fn test_alias_admission_applies_both_allow_constraints() {
    let policy: ToolPolicy = serde_json::from_value(serde_json::json!({
        "allow": ["mcp__a__*", "mcp__b__*"],
        "role_overrides": {"reader": {"allow": ["mcp__b__read_*"]}}
    }))
    .unwrap();
    let policy = policy.resolve(Some("reader")).unwrap();
    assert!(!alias_admitted(&policy, "mcp__a__read_file"));
    assert!(alias_admitted(&policy, "mcp__b__read_file"));
    assert!(!alias_admitted(&policy, "mcp__b__write_file"));
}

// Python: tests/tools/test_role_policy.py::test_alias_admission_keeps_each_server_constraint
#[test]
fn test_alias_admission_keeps_each_server_constraint() {
    type Case<'a> = (&'a [&'a str], &'a [&'a str], &'a [&'a str], [bool; 2]);
    let cases: &[Case<'_>] = &[
        (&["mcp__a__*"], &[], &[], [true, false]),
        (&[], &["mcp__a__*"], &[], [true, false]),
        (&["read_*"], &["search_*"], &[], [true, true]),
        (&["read_*", "mcp__a__*"], &[], &[], [true, false]),
        (&[], &["read_*", "mcp__a__*"], &[], [true, false]),
        (
            &["mcp__*"],
            &["mcp__a__*"],
            &["mcp__a__read_*"],
            [false, false],
        ),
        (&["mcp__a__*"], &["mcp__b__*"], &[], [false, false]),
    ];
    for (base, secondary, deny, expected) in cases {
        let policy: ToolPolicy = serde_json::from_value(serde_json::json!({
            "allow": base, "deny": deny,
            "role_overrides": {"member": {"allow": secondary}}
        }))
        .unwrap();
        let policy = policy.resolve(Some("member")).unwrap();
        assert_eq!(
            ["a", "b"].map(|server| alias_admitted(&policy, &format!("mcp__{server}__read_file"))),
            *expected
        );
    }
}

// Python: tests/tools/test_role_policy.py::test_alias_call_must_pass_both_alias_and_bare_names_after_resolution
#[test]
fn test_alias_call_must_pass_both_alias_and_bare_names_after_resolution() {
    let policy: ToolPolicy = serde_json::from_value(serde_json::json!({
        "allow": ["*"],
        "role_overrides": {"reader": {"allow": ["mcp__a__*", "read_*"]}}
    }))
    .unwrap();
    let policy = policy.resolve(Some("reader")).unwrap();
    assert!(call_admitted(&policy, "mcp__a__read_file"));
    assert!(!call_admitted(&policy, "mcp__a__write_file"));
    assert!(!call_admitted(&policy, "mcp__b__read_file"));
    assert!(call_admitted(&policy, "read_file"));
}
