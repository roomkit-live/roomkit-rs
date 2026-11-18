//! Regular expressions for application-supplied provider tool names (RFC §6.7).

use fancy_regex::{Expr, Regex};
use roomkit_core::rules::python_repr;

use crate::{AiError, ProviderError, ProviderErrorOptions};

/// A provider's whole-name rule, checked before it sends a request.
///
/// Rust uses the native syntax of [`fancy_regex`]. The standard vendor rules,
/// alternatives, backreferences and lookaround work in both Rust and Python.
/// Custom Python expressions can differ: scoped ASCII mode, named conditional
/// groups and Unicode character names have different support. Rust also accepts
/// Unicode properties such as `\p{L}`.
#[derive(Clone, Debug)]
pub struct ToolNameRule {
    vendor: String,
    pattern: String,
    compiled: Regex,
}

impl PartialEq for ToolNameRule {
    fn eq(&self, other: &Self) -> bool {
        self.vendor == other.vendor && self.pattern == other.pattern
    }
}

impl Eq for ToolNameRule {}

impl ToolNameRule {
    /// Compile a provider's expression for use throughout its lifetime.
    ///
    /// # Errors
    ///
    /// Returns the compiler's diagnostic if the pattern is invalid.
    pub fn new(vendor: impl Into<String>, pattern: impl Into<String>) -> Result<Self, AiError> {
        let pattern = pattern.into();
        let invalid = |error: fancy_regex::Error| AiError::ToolNamePattern {
            reason: error.to_string(),
        };
        // Validate the expression on its own before adding our non-capturing
        // group. Otherwise an unmatched parenthesis could close that group.
        Expr::parse_tree(&pattern).map_err(invalid)?;
        let compiled = Regex::new(&format!(r"\A(?:{pattern})\z"))
            .or_else(|original| {
                // In extended mode a trailing comment swallows the closing group.
                // A newline ends that comment and is ignored in extended mode.
                Regex::new(&format!("\\A(?:{pattern}\n)\\z")).map_err(|_| original)
            })
            .map_err(invalid)?;
        Ok(Self {
            vendor: vendor.into(),
            pattern,
            compiled,
        })
    }

    /// The vendor named in a failed preflight check.
    pub fn vendor(&self) -> &str {
        &self.vendor
    }

    /// The expression as supplied by the application.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }

    /// Check the names in iteration order.
    ///
    /// # Errors
    ///
    /// The first rejected name yields a non-retryable error before the request.
    /// If the engine reaches a matching limit, the error retains its cause.
    pub fn check(
        &self,
        names: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<(), ProviderError> {
        for name in names {
            let name = name.as_ref();
            match self.compiled.find(name) {
                Ok(Some(found)) if found.start() == 0 && found.end() == name.len() => {}
                Ok(_) => return Err(self.refused(name)),
                Err(cause) => return Err(self.refused(name).with_source(cause)),
            }
        }
        Ok(())
    }

    fn refused(&self, name: &str) -> ProviderError {
        ProviderError::new(
            format!(
                "tool {}: {} accepts tool names matching {}",
                python_repr(name),
                self.vendor,
                self.pattern
            ),
            ProviderErrorOptions {
                provider: self.vendor.clone(),
                context_overflow: Some(false),
                before_request: true,
                ..Default::default()
            },
        )
    }
}
