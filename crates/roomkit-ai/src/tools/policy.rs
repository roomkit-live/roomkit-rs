//! Which tools an agent may call (RFC §21.1 `ToolPolicy`).

mod patterns;
mod role_override;

use crate::AiError;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;

pub(crate) use patterns::glob_matches;
pub use role_override::{RoleOverride, RoleOverrideMode, RoleOverrideOptions};

/// Python's `ToolPolicy` constructor keywords (RFC §21.1).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ToolPolicyOptions {
    /// Allowed name patterns; empty imposes no restriction.
    pub allow: Vec<String>,
    /// Denied name patterns, which always win.
    pub deny: Vec<String>,
    /// Overrides selected by participant role.
    pub role_overrides: BTreeMap<String, RoleOverride>,
}

/// Allow and deny patterns over tool names, with per-role overrides
/// (RFC §21.1). `*` matches any characters and `?` one character.
/// Empty allow lists impose no restriction; deny always wins.
///
/// ```
/// use roomkit_ai::{ToolPolicy, ToolPolicyOptions};
/// let policy = ToolPolicy::new(ToolPolicyOptions {
///     allow: vec!["search_*".into(), "get_?".into()],
///     deny: vec!["search_admin*".into()],
///     ..Default::default()
/// })?;
/// assert!(policy.is_allowed("search_orders"));
/// assert!(policy.is_allowed("get_x"));
/// assert!(!policy.is_allowed("search_admin_users"));
/// assert!(!policy.is_allowed("delete_orders"));
/// # Ok::<(), roomkit_ai::AiError>(())
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "ToolPolicyOptions")]
pub struct ToolPolicy {
    /// Allowed name patterns; empty imposes no restriction.
    pub allow: Vec<String>,
    /// Denied name patterns, which always win.
    pub deny: Vec<String>,
    /// Overrides selected by participant role.
    pub role_overrides: BTreeMap<String, RoleOverride>,
    // Python's private attribute survives cloning but not model_dump.
    #[serde(skip)]
    restrict_allow: Vec<String>,
}

impl ToolPolicy {
    /// Construct a policy from Python's keywords (RFC §21.1).
    /// Already constructed overrides are retained without revalidation.
    ///
    /// # Errors
    /// Returns [`AiError::ToolPattern`] if an allow or deny rule holds a bracket.
    pub fn new(options: ToolPolicyOptions) -> Result<Self, AiError> {
        validate_patterns(&options.allow, &options.deny)?;
        Ok(Self {
            allow: options.allow,
            deny: options.deny,
            role_overrides: options.role_overrides,
            restrict_allow: Vec::new(),
        })
    }

    /// The effective policy for `role` (RFC §21.1). None, empty and unknown
    /// roles borrow this policy; a known role produces an owned policy without
    /// overrides. Restrict intersects allows and merges denies; replace uses
    /// only the override's rules.
    ///
    /// # Errors
    /// Returns [`AiError::ToolPattern`] if public edits introduced a bracket
    /// into a list reconstructed by resolution. Restrict reconstructs base
    /// allow and merged deny, replace reconstructs the override's lists.
    pub fn resolve(&self, role: Option<&str>) -> Result<Cow<'_, Self>, AiError> {
        let Some(override_) = role
            .filter(|role| !role.is_empty())
            .and_then(|role| self.role_overrides.get(role))
        else {
            return Ok(Cow::Borrowed(self));
        };
        if override_.mode == RoleOverrideMode::Replace {
            return Self::new(ToolPolicyOptions {
                allow: override_.allow.clone(),
                deny: override_.deny.clone(),
                ..Default::default()
            })
            .map(Cow::Owned);
        }
        self.restrict(override_).map(Cow::Owned)
    }

    fn restrict(&self, override_: &RoleOverride) -> Result<Self, AiError> {
        // Test membership against the original base: keep duplicates of a
        // new override rule as well as duplicates already in the base.
        let deny = self
            .deny
            .iter()
            .chain(
                override_
                    .deny
                    .iter()
                    .filter(|pattern| !self.deny.contains(pattern)),
            )
            .cloned()
            .collect();
        let mut resolved = Self::new(ToolPolicyOptions {
            allow: self.allow.clone(),
            deny,
            ..Default::default()
        })?;
        // Python copies this private constraint without revalidating it.
        resolved.restrict_allow.clone_from(&override_.allow);
        Ok(resolved)
    }

    /// Whether `tool_name` passes every allow constraint and no deny rule.
    pub fn is_allowed(&self, tool_name: &str) -> bool {
        let matches = |pattern: &String| glob_matches(pattern, tool_name);
        !self.deny.iter().any(matches)
            && [&self.allow, &self.restrict_allow]
                .into_iter()
                .all(|allowed| allowed.is_empty() || allowed.iter().any(matches))
    }

    /// A predicate borrowing this policy, Python's `as_filter` (RFC §21.1).
    /// The borrow keeps the policy alive and prevents mutation while retained.
    pub fn as_filter(&self) -> impl Fn(&str) -> bool + '_ {
        |tool_name| self.is_allowed(tool_name)
    }
}

impl TryFrom<ToolPolicyOptions> for ToolPolicy {
    type Error = AiError;
    fn try_from(options: ToolPolicyOptions) -> Result<Self, Self::Error> {
        Self::new(options)
    }
}

fn validate_patterns(allow: &[String], deny: &[String]) -> Result<(), AiError> {
    match allow
        .iter()
        .chain(deny)
        .find(|pattern| pattern.contains(['[', ']']))
    {
        Some(pattern) => Err(AiError::ToolPattern {
            pattern: pattern.clone(),
        }),
        None => Ok(()),
    }
}

/// The server in `mcp__<server>__<tool>`, for the MCP crate (RFC §21.2).
#[doc(hidden)]
pub fn alias_server(name: &str) -> Option<&str> {
    let (server, _tool) = name.strip_prefix("mcp__")?.split_once("__")?;
    Some(server)
}

/// The bare tool an MCP alias runs, or the unchanged name (RFC §21.2).
#[doc(hidden)]
pub fn served_tool_name(name: &str) -> &str {
    name.strip_prefix("mcp__")
        .and_then(|rest| rest.split_once("__"))
        .map_or(name, |(_server, tool)| tool)
}

/// An alias call must pass under both its alias and bare name (RFC §21.1).
pub(crate) fn call_admitted(policy: &ToolPolicy, name: &str) -> bool {
    policy.is_allowed(name) && policy.is_allowed(served_tool_name(name))
}

/// Server selection for a bare call (RFC §21.2). Each allow list naming
/// an MCP alias must match; a list of bare names imposes no server constraint.
pub(crate) fn alias_admitted(policy: &ToolPolicy, alias: &str) -> bool {
    let covers = |pattern: &String| glob_matches(pattern, alias);
    !policy.deny.iter().any(covers)
        && [&policy.allow, &policy.restrict_allow]
            .into_iter()
            .all(|allowed| {
                !allowed.iter().any(|pattern| pattern.starts_with("mcp__"))
                    || allowed.iter().any(covers)
            })
}

#[cfg(test)]
mod tests;
