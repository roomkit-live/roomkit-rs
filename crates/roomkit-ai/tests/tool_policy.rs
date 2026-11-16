//! Public policy contracts from Python's `tests/test_tool_policy.py` (RFC §21.1).

#![expect(clippy::expect_used, reason = "a test fails on invalid fixture rules")]

use std::borrow::Cow;
use std::collections::BTreeMap;

use roomkit_ai::{
    RoleOverride, RoleOverrideMode, RoleOverrideOptions, ToolPolicy, ToolPolicyOptions,
};
use serde_json::json;

#[path = "tool_policy/base.rs"]
mod base;
#[path = "tool_policy/boundaries.rs"]
mod boundaries;
#[path = "tool_policy/resolution.rs"]
mod resolution;

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn policy(allow: &[&str], deny: &[&str]) -> ToolPolicy {
    ToolPolicy::new(ToolPolicyOptions {
        allow: strings(allow),
        deny: strings(deny),
        ..Default::default()
    })
    .expect("valid policy")
}

fn role(allow: &[&str], deny: &[&str], mode: RoleOverrideMode) -> RoleOverride {
    RoleOverride::new(RoleOverrideOptions {
        allow: strings(allow),
        deny: strings(deny),
        mode,
    })
    .expect("valid override")
}

fn with_role(mut policy: ToolPolicy, name: &str, override_: RoleOverride) -> ToolPolicy {
    policy.role_overrides.insert(name.to_owned(), override_);
    policy
}

fn resolved(policy: &ToolPolicy, role: &str) -> ToolPolicy {
    let resolved = policy.resolve(Some(role)).expect("valid resolution");
    assert!(
        matches!(resolved, Cow::Owned(_)),
        "known role produces a new policy"
    );
    resolved.into_owned()
}
