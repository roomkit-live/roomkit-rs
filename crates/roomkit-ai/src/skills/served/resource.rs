//! A listed file's immutable identity and verification (RFC §24.7).

use std::fmt::Write as _;

use sha2::{Digest as _, Sha256};

use crate::SkillError;

/// Input adapter for signed/unsigned JSON integers: -2^63 through 2^64-1.
///
/// Plain integer literals and primitives through i64/u64 are accepted. The
/// private storage keeps direct negative sizes and exact totals without
/// admitting unrestricted i128 inputs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SkillResourceSize(i128);

macro_rules! sizes {
    ($($integer:ty),+ $(,)?) => {$(
        impl From<$integer> for SkillResourceSize {
            fn from(value: $integer) -> Self {
                Self(i128::from(value))
            }
        }
    )+};
}
sizes!(i8, i16, i32, i64, u8, u16, u32, u64);

impl From<usize> for SkillResourceSize {
    fn from(value: usize) -> Self {
        Self(value as i128)
    }
}

impl From<isize> for SkillResourceSize {
    fn from(value: isize) -> Self {
        Self(value as i128)
    }
}

/// One file of a served skill: URI, SHA-256 digest and byte length (RFC §24.7).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkillResource {
    uri: String,
    digest: String,
    size: SkillResourceSize,
}

impl SkillResource {
    /// Construct a record without validating its fields, as Python's dataclass does.
    pub fn new(
        uri: impl Into<String>,
        digest: impl Into<String>,
        size: impl Into<SkillResourceSize>,
    ) -> Self {
        Self {
            uri: uri.into(),
            digest: digest.into(),
            size: size.into(),
        }
    }

    /// The URI of this file.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// The digest as supplied by the entry.
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// Its signed or unsigned JSON integer, represented exactly.
    pub fn size(&self) -> i128 {
        self.size.0
    }
}

/// Check bytes against their listed length, then SHA-256 digest (RFC §24.7).
///
/// Matching a server's own entry establishes consistency, never trust.
///
/// # Errors
/// Returns [`SkillError::Verification`] if the length or digest differs.
pub fn verify_skill_resource(resource: &SkillResource, content: &[u8]) -> Result<(), SkillError> {
    if content.len() as i128 != resource.size() {
        return Err(SkillError::Verification {
            message: format!(
                "{}: {} bytes where the entry lists {}",
                resource.uri(),
                content.len(),
                resource.size()
            ),
        });
    }
    if digest(content) != resource.digest() {
        return Err(SkillError::Verification {
            message: format!("{}: its digest is not the entry's", resource.uri()),
        });
    }
    Ok(())
}

pub(super) fn digest(content: &[u8]) -> String {
    let mut text = String::with_capacity(71);
    text.push_str("sha256:");
    for byte in Sha256::digest(content) {
        let _infallible = write!(text, "{byte:02x}");
    }
    text
}
