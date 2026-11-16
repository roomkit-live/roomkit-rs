use super::validate_patterns;
use crate::AiError;
use serde::{Deserialize, Serialize};

/// How a role combines with the base policy (RFC §21.1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleOverrideMode {
    /// Intersect allow predicates and merge deny rules.
    #[default]
    Restrict,
    /// Replace the base rules completely.
    Replace,
}

/// Python's `RoleOverride` constructor keywords (RFC §21.1).
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct RoleOverrideOptions {
    /// Allowed name patterns; empty imposes no restriction.
    pub allow: Vec<String>,
    /// Denied name patterns, which always win.
    pub deny: Vec<String>,
    /// How to combine with the base policy; defaults to restrict.
    pub mode: RoleOverrideMode,
}

/// Per-role tool policy rules (RFC §21.1). Fields remain editable;
/// construction and deserialization reject bracket-containing patterns.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RoleOverrideOptions")]
pub struct RoleOverride {
    /// Allowed name patterns; empty imposes no restriction.
    pub allow: Vec<String>,
    /// Denied name patterns, which always win.
    pub deny: Vec<String>,
    /// How to combine with the base policy; defaults to restrict.
    pub mode: RoleOverrideMode,
}

impl RoleOverride {
    /// Construct an override from Python's keywords (RFC §21.1).
    ///
    /// # Errors
    /// Returns [`AiError::ToolPattern`] if an allow or deny rule holds a bracket.
    pub fn new(options: RoleOverrideOptions) -> Result<Self, AiError> {
        validate_patterns(&options.allow, &options.deny)?;
        Ok(Self {
            allow: options.allow,
            deny: options.deny,
            mode: options.mode,
        })
    }
}

impl TryFrom<RoleOverrideOptions> for RoleOverride {
    type Error = AiError;
    fn try_from(options: RoleOverrideOptions) -> Result<Self, Self::Error> {
        Self::new(options)
    }
}
