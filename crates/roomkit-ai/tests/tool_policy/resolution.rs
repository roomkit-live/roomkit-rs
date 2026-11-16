use super::*;

// Python: tests/test_tool_policy.py::TestRoleOverrideResolve::test_no_overrides_returns_self
#[test]
fn test_no_overrides_returns_self() {
    let policy = policy(&["search"], &[]);
    assert!(
        matches!(policy.resolve(Some("member")).unwrap(), Cow::Borrowed(value) if std::ptr::eq(value, std::ptr::from_ref(&policy)))
    );
}

// Python: tests/test_tool_policy.py::TestRoleOverrideResolve::test_none_role_returns_self
#[test]
fn test_none_role_returns_self() {
    let policy = with_role(
        policy(&["search"], &[]),
        "member",
        role(&[], &["x"], RoleOverrideMode::Restrict),
    );
    assert!(
        matches!(policy.resolve(None).unwrap(), Cow::Borrowed(value) if std::ptr::eq(value, std::ptr::from_ref(&policy)))
    );
}

// Python: tests/test_tool_policy.py::TestRoleOverrideResolve::test_unknown_role_returns_self
#[test]
fn test_unknown_role_returns_self() {
    let policy = with_role(
        policy(&["search"], &[]),
        "member",
        role(&[], &["x"], RoleOverrideMode::Restrict),
    );
    assert!(
        matches!(policy.resolve(Some("unknown_role")).unwrap(), Cow::Borrowed(value) if std::ptr::eq(value, std::ptr::from_ref(&policy)))
    );
}

// Python: tests/test_tool_policy.py::TestRoleOverrideRestrict::test_deny_union
#[test]
fn test_deny_union() {
    let policy = with_role(
        policy(&[], &["delete"]),
        "observer",
        role(&[], &["write_*"], RoleOverrideMode::Restrict),
    );
    let resolved = resolved(&policy, "observer");
    assert!(!resolved.is_allowed("delete"));
    assert!(!resolved.is_allowed("write_file"));
    assert!(resolved.is_allowed("read_file"));
}

// Python: tests/test_tool_policy.py::TestRoleOverrideRestrict::test_allow_intersection
#[test]
fn test_allow_intersection() {
    let policy = with_role(
        policy(&["mcp_*", "local_*"], &[]),
        "member",
        role(&["mcp_search", "mcp_read"], &[], RoleOverrideMode::Restrict),
    );
    let resolved = resolved(&policy, "member");
    assert!(resolved.is_allowed("mcp_search"));
    assert!(!resolved.is_allowed("local_tool"));
    assert!(!resolved.is_allowed("mcp_delete"));
}

// Python: tests/test_tool_policy.py::TestRoleOverrideRestrict::test_deny_plus_allow_intersection
#[test]
fn test_deny_plus_allow_intersection() {
    let policy = with_role(
        policy(&["*"], &["admin_*"]),
        "observer",
        role(
            &["read_*", "search"],
            &["read_secret"],
            RoleOverrideMode::Restrict,
        ),
    );
    let resolved = resolved(&policy, "observer");
    for name in ["read_file", "search"] {
        assert!(resolved.is_allowed(name));
    }
    for name in ["write_file", "admin_panel", "read_secret"] {
        assert!(!resolved.is_allowed(name));
    }
}

// Python: tests/test_tool_policy.py::TestRoleOverrideRestrict::test_restrict_empty_override_allow_no_restriction
#[test]
fn test_restrict_empty_override_allow_no_restriction() {
    let policy = with_role(
        policy(&["read_*"], &[]),
        "member",
        role(&[], &["read_secret"], RoleOverrideMode::Restrict),
    );
    let resolved = resolved(&policy, "member");
    assert!(resolved.is_allowed("read_file"));
    assert!(!resolved.is_allowed("read_secret"));
}

// Python: tests/test_tool_policy.py::TestRoleOverrideReplace::test_replace_mode
#[test]
fn test_replace_mode() {
    let policy = with_role(
        policy(&["*"], &["admin_*"]),
        "bot",
        role(&["ping"], &[], RoleOverrideMode::Replace),
    );
    let resolved = resolved(&policy, "bot");
    assert!(resolved.is_allowed("ping"));
    assert!(!resolved.is_allowed("search"));
    assert!(!resolved.is_allowed("admin_panel"));
}

// Python: tests/test_tool_policy.py::TestRoleOverrideReplace::test_replace_mode_no_allow
#[test]
fn test_replace_mode_no_allow() {
    let policy = with_role(
        policy(&[], &["dangerous"]),
        "owner",
        role(&[], &[], RoleOverrideMode::Replace),
    );
    let resolved = resolved(&policy, "owner");
    assert!(resolved.is_allowed("dangerous"));
    assert!(resolved.is_allowed("anything"));
}

// Python: tests/test_tool_policy.py::TestRoleOverrideSerialization::test_roundtrip
#[test]
fn test_roundtrip() {
    let policy = with_role(
        with_role(
            policy(&["search"], &[]),
            "observer",
            role(&["search"], &["write_*"], RoleOverrideMode::Restrict),
        ),
        "bot",
        role(&["ping"], &[], RoleOverrideMode::Replace),
    );
    let restored: ToolPolicy =
        serde_json::from_value(serde_json::to_value(&policy).unwrap()).unwrap();
    assert_eq!(
        restored
            .role_overrides
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["bot", "observer"]
    );
    assert_eq!(
        restored.role_overrides["bot"].mode,
        RoleOverrideMode::Replace
    );
    assert_eq!(restored, policy);
    let resolved = resolved(&restored, "observer");
    assert!(resolved.is_allowed("search"));
    assert!(!resolved.is_allowed("write_file"));
}
