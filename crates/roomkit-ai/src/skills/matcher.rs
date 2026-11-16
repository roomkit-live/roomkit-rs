//! The host's synchronous reading of a prerequisite name (RFC §24.3).

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use crate::ToolError;

type MatchFn = dyn Fn(&str, &HashSet<String>) -> Result<bool, ToolError> + Send + Sync;

/// Whether an offered tool has this exact, case-sensitive name (RFC §24.3).
/// Wildcards in `required` are literal characters.
pub fn serves_exactly(required: &str, offered: impl IntoIterator<Item = impl AsRef<str>>) -> bool {
    offered.into_iter().any(|name| name.as_ref() == required)
}

/// Python's `RequiresMatch` callable. Defaults to exact name membership.
/// Clones retain the same callback and its state; equality compares callback
/// identity, with all default matchers equal (HITL 73da84cd).
#[derive(Clone, Default)]
pub struct RequiresMatch {
    callback: Option<Arc<MatchFn>>,
}

impl RequiresMatch {
    /// Wrap the host's synchronous matcher; no registry lock is held when called.
    pub fn new(
        callback: impl Fn(&str, &HashSet<String>) -> Result<bool, ToolError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            callback: Some(Arc::new(callback)),
        }
    }

    /// Whether `required` is served by the admitted, non-closed `offered` names.
    ///
    /// # Errors
    ///
    /// Returns the callback's error unchanged. A failed check activates nothing.
    pub fn call(&self, required: &str, offered: &HashSet<String>) -> Result<bool, ToolError> {
        match &self.callback {
            Some(callback) => callback(required, offered),
            None => Ok(offered.contains(required)),
        }
    }
}

impl fmt::Debug for RequiresMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RequiresMatch")
            .field("custom", &self.callback.is_some())
            .finish_non_exhaustive()
    }
}

impl PartialEq for RequiresMatch {
    fn eq(&self, other: &Self) -> bool {
        match (&self.callback, &other.callback) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}

impl Eq for RequiresMatch {}

/// Python's `SkillRegistry(requires_match=serves_exactly)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SkillRegistryOptions {
    /// Exact matching by default; the host may supply its own reading.
    pub requires_match: RequiresMatch,
}

/// Python's keyword options for `SkillRegistry.copy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CopyOptions {
    /// Preserve selected availability marks. Defaults to true.
    pub marks: bool,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self { marks: true }
    }
}
