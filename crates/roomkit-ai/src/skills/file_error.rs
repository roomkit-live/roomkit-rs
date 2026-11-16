//! Direct file operations preserve path, filesystem and text-decoding failures.

/// A bundled filename or directory escapes its boundary or cannot be resolved.
/// Python's `SkillPathError`, a `ValueError` (RFC §24.6).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SkillPathError(pub(crate) String);

/// Why a direct [`super::Skill`] file operation failed (RFC §24.6).
/// The variants preserve Python's path, filesystem and UTF-8 exception categories.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SkillFileError {
    /// The filename or directory violates containment.
    #[error(transparent)]
    Path(#[from] SkillPathError),
    /// A filesystem failure, including `ErrorKind::NotFound` for an absent file.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A reference exists but its contents are not UTF-8.
    #[error(transparent)]
    Encoding(#[from] std::string::FromUtf8Error),
}
