use super::*;

// Python: tests/tools/test_role_policy.py::test_an_empty_role_bypasses_even_its_own_override
#[test]
fn test_an_empty_role_bypasses_even_its_own_override() {
    let policy = with_role(
        ToolPolicy::default(),
        "",
        role(&[], &["*"], RoleOverrideMode::Restrict),
    );
    assert!(
        matches!(policy.resolve(Some("")).unwrap(), Cow::Borrowed(value) if std::ptr::eq(value, std::ptr::from_ref(&policy)))
    );
    assert!(policy.resolve(None).unwrap().is_allowed("anything"));
}

// Python: tests/tools/test_role_policy.py::test_resolving_detaches_lists_and_discards_overrides
#[test]
fn test_resolving_detaches_lists_and_discards_overrides() {
    let override_ = role(
        &["read_*"],
        &["blocked", "extra", "extra"],
        RoleOverrideMode::Restrict,
    );
    let mut policy = with_role(policy(&["*"], &["blocked", "blocked"]), "member", override_);
    let resolved = resolved(&policy, "member");
    assert_eq!(resolved.deny, ["blocked", "blocked", "extra", "extra"]);
    assert!(resolved.role_overrides.is_empty());
    assert!(
        matches!(resolved.resolve(Some("member")).unwrap(), Cow::Borrowed(value) if std::ptr::eq(value, std::ptr::from_ref(&resolved)))
    );
    policy.allow.clear();
    policy.deny.clear();
    let override_ = policy.role_overrides.get_mut("member").unwrap();
    override_.allow.clear();
    override_.deny.clear();
    assert_eq!(resolved.allow, ["*"]);
    assert!(!resolved.is_allowed("write_file"));
    assert!(resolved.is_allowed("read_file"));
}

// Python: tests/tools/test_role_policy.py::test_serialization_omits_the_private_intersection
#[test]
fn test_serialization_omits_the_private_intersection() {
    let policy = with_role(
        policy(&["*"], &[]),
        "reader",
        role(&["read_*"], &[], RoleOverrideMode::Restrict),
    );
    let resolved = resolved(&policy, "reader");
    assert!(!resolved.is_allowed("write_file"));
    let value = serde_json::to_value(&resolved).unwrap();
    assert_eq!(
        value,
        json!({"allow": ["*"], "deny": [], "role_overrides": {}})
    );
    assert!(
        serde_json::from_value::<ToolPolicy>(value)
            .unwrap()
            .is_allowed("write_file")
    );
}

// Python: tests/tools/test_role_policy.py::test_the_filter_reads_later_policy_mutations
// Rust borrows the policy: release the predicate before editing it (approved API form).
#[test]
fn test_the_filter_reads_later_policy_mutations() {
    let mut policy = policy(&["read_*"], &[]);
    assert!(policy.as_filter()("read_file"));
    policy.deny.push("read_file".to_owned());
    assert!(!policy.as_filter()("read_file"));
}

// Python: tests/tools/test_role_policy.py::test_resolution_validates_only_the_rules_it_constructs
#[test]
fn test_resolution_validates_only_the_rules_it_constructs() {
    let mut policy = with_role(
        with_role(
            ToolPolicy::default(),
            "replace",
            role(&[], &[], RoleOverrideMode::Replace),
        ),
        "restrict",
        RoleOverride::default(),
    );
    policy.deny.push("rm_[a-z]*".to_owned());
    assert!(
        matches!(policy.resolve(None).unwrap(), Cow::Borrowed(value) if std::ptr::eq(value, std::ptr::from_ref(&policy)))
    );
    assert!(resolved(&policy, "replace").is_allowed("rm_file"));
    assert!(
        policy
            .resolve(Some("restrict"))
            .unwrap_err()
            .to_string()
            .contains("holds a bracket")
    );
}

// Python: tests/tools/test_role_policy.py::test_a_retained_override_is_not_revalidated_by_the_policy_constructor
// Rust moves the retained override; edits through the policy map remain observable.
#[test]
fn test_a_retained_override_is_not_revalidated_by_the_policy_constructor() {
    let mut override_ = RoleOverride::default();
    override_.allow.push("rm_[a-z]*".to_owned());
    let policy = ToolPolicy::new(ToolPolicyOptions {
        role_overrides: BTreeMap::from([("member".to_owned(), override_)]),
        ..Default::default()
    })
    .unwrap();
    let resolved = resolved(&policy, "member");
    assert!(resolved.is_allowed("rm_[a-z]file"));
    assert!(!resolved.is_allowed("rm_file"));
}

// Python: tests/tools/test_role_policy.py::test_a_raw_override_is_validated_on_construction
#[test]
fn test_a_raw_override_is_validated_on_construction() {
    for field in ["allow", "deny"] {
        let mut input = json!({"role_overrides": {"member": {}}});
        input["role_overrides"]["member"][field] = json!(["rm_[a-z]*"]);
        assert!(
            serde_json::from_value::<ToolPolicy>(input)
                .unwrap_err()
                .to_string()
                .contains("holds a bracket")
        );
    }
}

// Python: tests/tools/test_role_policy.py::test_resolving_revalidates_constructed_lists_from_a_mutated_override
#[test]
fn test_resolving_revalidates_constructed_lists_from_a_mutated_override() {
    for (mode, allow) in [
        (RoleOverrideMode::Replace, true),
        (RoleOverrideMode::Replace, false),
        (RoleOverrideMode::Restrict, false),
    ] {
        let mut override_ = role(&[], &[], mode);
        let list = if allow {
            &mut override_.allow
        } else {
            &mut override_.deny
        };
        list.push("rm_[a-z]*".to_owned());
        let policy = with_role(ToolPolicy::default(), "member", override_);
        assert!(
            policy
                .resolve(Some("member"))
                .unwrap_err()
                .to_string()
                .contains("holds a bracket")
        );
    }
}

// Python: tests/tools/test_role_policy.py::test_clone_and_equality_keep_the_private_intersection
#[test]
fn test_clone_and_equality_keep_the_private_intersection() {
    let policy = with_role(
        policy(&["*"], &[]),
        "reader",
        role(&["read_*"], &[], RoleOverrideMode::Restrict),
    );
    let resolved = resolved(&policy, "reader");
    let mut clone = resolved.clone();
    let plain: ToolPolicy =
        serde_json::from_value(serde_json::to_value(&resolved).unwrap()).unwrap();
    assert_eq!(clone, resolved);
    assert_ne!(plain, resolved);
    assert!(!clone.is_allowed("write_file"));
    assert!(plain.is_allowed("write_file"));
    clone.deny.push("read_file".to_owned());
    assert!(resolved.is_allowed("read_file"));
    assert!(!clone.is_allowed("read_file"));
}

// Python: tests/tools/test_role_policy.py::test_resolving_a_new_role_replaces_the_previous_private_constraint
#[test]
fn test_resolving_a_new_role_replaces_the_previous_private_constraint() {
    let policy = with_role(
        policy(&["*"], &[]),
        "reader",
        role(&["read_*"], &[], RoleOverrideMode::Restrict),
    );
    let mut reader = resolved(&policy, "reader");
    for role in [None, Some("unknown")] {
        assert!(
            matches!(reader.resolve(role).unwrap(), Cow::Borrowed(value) if std::ptr::eq(value, std::ptr::from_ref(&reader)))
        );
    }
    reader.role_overrides.insert(
        "writer".to_owned(),
        role(&["write_*"], &[], RoleOverrideMode::Restrict),
    );
    let writer = resolved(&reader, "writer");
    assert!(writer.is_allowed("write_file"));
    assert!(!writer.is_allowed("read_file"));
    assert!(!reader.is_allowed("write_file"));
}

// Python: tests/tools/test_role_policy.py::test_a_mutated_base_allow_is_validated_only_when_restrict_constructs_it
#[test]
fn test_a_mutated_base_allow_is_validated_only_when_restrict_constructs_it() {
    let mut policy = with_role(
        with_role(ToolPolicy::default(), "reader", RoleOverride::default()),
        "owner",
        role(&[], &[], RoleOverrideMode::Replace),
    );
    policy.allow.push("[invalid]".to_owned());
    for role in [None, Some("unknown")] {
        assert!(
            matches!(policy.resolve(role).unwrap(), Cow::Borrowed(value) if std::ptr::eq(value, std::ptr::from_ref(&policy)))
        );
    }
    assert!(resolved(&policy, "owner").is_allowed("anything"));
    assert!(
        policy
            .resolve(Some("reader"))
            .unwrap_err()
            .to_string()
            .contains("holds a bracket")
    );
}

// Python: tests/tools/test_role_policy.py::test_resolve_reads_later_mutations_through_the_role_map
#[test]
fn test_resolve_reads_later_mutations_through_the_role_map() {
    let mut policy = with_role(ToolPolicy::default(), "member", RoleOverride::default());
    let before = resolved(&policy, "member");
    policy
        .role_overrides
        .get_mut("member")
        .unwrap()
        .allow
        .push("read_*".to_owned());
    let after = resolved(&policy, "member");
    assert!(before.is_allowed("write_file"));
    assert!(!after.is_allowed("write_file"));
    assert!(after.is_allowed("read_file"));
}

// Retained Python reference: role-policy/test_role_policy_model_boundaries.py.
#[test]
fn test_model_defaults_and_unknown_keys() {
    let override_: RoleOverride = serde_json::from_value(json!({"unknown": true})).unwrap();
    assert_eq!(override_, RoleOverride::default());
    assert_eq!(
        serde_json::to_value(override_).unwrap(),
        json!({"allow": [], "deny": [], "mode": "restrict"})
    );
    let policy: ToolPolicy =
        serde_json::from_value(json!({"unknown": true, "_restrict_allow": ["none"]})).unwrap();
    assert_eq!(
        serde_json::to_value(&policy).unwrap(),
        json!({"allow": [], "deny": [], "role_overrides": {}})
    );
    assert!(policy.is_allowed("anything"));
}

// Retained Python reference: role-policy/test_role_policy_model_boundaries.py.
#[test]
fn test_model_accepts_the_two_modes() {
    for mode in ["restrict", "replace"] {
        let policy: ToolPolicy =
            serde_json::from_value(json!({"role_overrides": {"reader": {"mode": mode}}})).unwrap();
        assert_eq!(
            serde_json::to_value(policy.role_overrides["reader"].mode).unwrap(),
            json!(mode)
        );
        assert!(resolved(&policy, "reader").is_allowed("anything"));
    }
}

// Retained Python reference: role-policy/test_role_policy_model_boundaries.py.
#[test]
fn test_model_rejects_other_modes() {
    for mode in [
        json!("unknown"),
        json!("RESTRICT"),
        json!(""),
        json!(null),
        json!(0),
    ] {
        assert!(serde_json::from_value::<RoleOverride>(json!({"mode": mode})).is_err());
        assert!(
            serde_json::from_value::<ToolPolicy>(
                json!({"role_overrides": {"reader": {"mode": mode}}})
            )
            .is_err()
        );
    }
}

// Retained Python reference: role-policy/test_role_policy_boundaries.py::test_override_lists_are_not_nullable
#[test]
fn test_override_lists_are_not_nullable() {
    for field in ["allow", "deny"] {
        let mut input = json!({});
        input[field] = json!(null);
        assert!(serde_json::from_value::<RoleOverride>(input).is_err());
    }
}

// Retained Python reference: role-policy/test_role_policy_model_boundaries.py::test_policy_fields_are_not_nullable
#[test]
fn test_policy_fields_are_not_nullable() {
    for field in ["allow", "deny", "role_overrides"] {
        let mut input = json!({});
        input[field] = json!(null);
        assert!(serde_json::from_value::<ToolPolicy>(input).is_err());
    }
}
