//! What loading skills fails with (RFC §24.5).

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

/// Why a local skill could not be loaded (RFC §24.5), or an entry or file
/// served by an MCP server could not be admitted or verified (RFC §24.7).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum SkillError {
    /// A directory handed to discovery cannot be scanned, or a skill
    /// directory found in it leads out of it.
    Discovery {
        /// The directory.
        path: PathBuf,
        /// What is wrong with it.
        reason: String,
    },
    /// A skill's `SKILL.md` cannot be found, read or parsed (RFC §24.1).
    Parse {
        /// The skill, by its directory's name.
        skill: String,
        /// Why.
        reason: String,
    },
    /// A standalone frontmatter document could not be read.
    Frontmatter {
        /// The parsing error, without a skill directory prefix.
        message: String,
    },
    /// A skill's metadata does not validate.
    Validation {
        /// The skill, by its directory's name.
        skill: String,
        /// Each rule it breaks.
        reasons: Vec<String>,
    },
    /// The skills' manifest, which every system prompt carries, would be
    /// longer than its bound.
    ManifestTooLarge {
        /// What it would take.
        characters: usize,
        /// Its bound.
        limit: usize,
    },
    /// A server failure with its original concrete cause retained.
    ServerDiscovery {
        /// The sanitized public message.
        message: String,
        /// The original error, available through `Error::source`.
        source: Arc<dyn Error + Send + Sync>,
    },
    /// A verification failure with its original parsing or transport cause.
    VerificationCaused {
        /// The sanitized message presented to an integrator or model.
        message: String,
        /// The concrete diagnostic cause, not part of the public message.
        source: Arc<dyn Error + Send + Sync>,
    },
    /// A discovery failure that has no underlying transport error.
    ServerUnavailable {
        /// The public discovery message.
        message: String,
    },
    /// A file invalidated an active served skill across every registry copy.
    Changed {
        /// The origin-qualified registry name.
        name: String,
        /// The failed verification that retired the held skill, when available.
        source: Option<Arc<SkillError>>,
    },
    /// Python's `KeyError` when reading a skill that has not been loaded.
    NotFound {
        /// The missing registry name.
        name: String,
    },
    /// A concrete local path, I/O or encoding failure while reading a file.
    File(Arc<super::SkillFileError>),
    /// A served entry violates the extension's manifest contract.
    EntryValidation {
        /// The reference validation message, including its URI when available.
        message: String,
    },
    /// Served bytes differ from the entry that describes them.
    Verification {
        /// The reference verification message.
        message: String,
    },
}

impl SkillError {
    pub(crate) fn discovery(path: impl Into<PathBuf>, reason: &str) -> Self {
        Self::Discovery {
            path: path.into(),
            reason: reason.to_owned(),
        }
    }

    pub(crate) fn parse(skill: &str, reason: impl Into<String>) -> Self {
        Self::Parse {
            skill: skill.to_owned(),
            reason: reason.into(),
        }
    }
}

impl PartialEq for SkillError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Discovery { path: a, reason: b }, Self::Discovery { path: c, reason: d }) => {
                a == c && b == d
            }
            (
                Self::Parse {
                    skill: a,
                    reason: b,
                },
                Self::Parse {
                    skill: c,
                    reason: d,
                },
            ) => a == c && b == d,
            (
                Self::Validation {
                    skill: a,
                    reasons: b,
                },
                Self::Validation {
                    skill: c,
                    reasons: d,
                },
            ) => a == c && b == d,
            (
                Self::ManifestTooLarge {
                    characters: a,
                    limit: b,
                },
                Self::ManifestTooLarge {
                    characters: c,
                    limit: d,
                },
            ) => a == c && b == d,
            (
                Self::ServerDiscovery {
                    message: a,
                    source: b,
                },
                Self::ServerDiscovery {
                    message: c,
                    source: d,
                },
            )
            | (
                Self::VerificationCaused {
                    message: a,
                    source: b,
                },
                Self::VerificationCaused {
                    message: c,
                    source: d,
                },
            ) => a == c && Arc::ptr_eq(b, d),
            (Self::ServerUnavailable { message: a }, Self::ServerUnavailable { message: b })
            | (Self::Frontmatter { message: a }, Self::Frontmatter { message: b })
            | (Self::EntryValidation { message: a }, Self::EntryValidation { message: b })
            | (Self::Verification { message: a }, Self::Verification { message: b })
            | (Self::Changed { name: a, .. }, Self::Changed { name: b, .. })
            | (Self::NotFound { name: a }, Self::NotFound { name: b }) => a == b,
            (Self::File(a), Self::File(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}
impl Eq for SkillError {}
impl From<super::SkillFileError> for SkillError {
    fn from(error: super::SkillFileError) -> Self {
        Self::File(Arc::new(error))
    }
}
impl From<super::SkillPathError> for SkillError {
    fn from(error: super::SkillPathError) -> Self {
        Self::from(super::SkillFileError::from(error))
    }
}

impl std::fmt::Display for SkillError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Discovery { path, reason } => write!(f, "{reason}: {}", path.display()),
            Self::Parse { skill, reason } => write!(f, "Parsing failed for {skill}: {reason}"),
            Self::Validation { skill, reasons } => {
                write!(f, "Validation failed for {skill}: {}", reasons.join("; "))
            }
            Self::ManifestTooLarge { characters, limit } => write!(
                f,
                "the skills' manifest would take {characters} characters, past {limit}"
            ),
            Self::ServerDiscovery { message, .. }
            | Self::VerificationCaused { message, .. }
            | Self::ServerUnavailable { message }
            | Self::Frontmatter { message }
            | Self::EntryValidation { message }
            | Self::Verification { message } => f.write_str(message),
            Self::Changed { name, .. } => write!(
                f,
                "Skill {} changed on its server or could not be verified: it is no longer active. Activate it again to load its current version.",
                super::texts::quoted_name(name)
            ),
            Self::NotFound { name } => f.write_str(&super::texts::quoted_name(name)),
            Self::File(source) => std::fmt::Display::fmt(source, f),
        }
    }
}

impl Error for SkillError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        // Expose the concrete error rather than its Arc wrapper, whose Error
        // implementation would skip the wrapped error when traversing sources.
        match self {
            Self::ServerDiscovery { source, .. } | Self::VerificationCaused { source, .. } => {
                Some(source.as_ref())
            }
            Self::Changed { source, .. } => source
                .as_deref()
                .map(|error| -> &(dyn Error + 'static) { error }),
            Self::File(source) => Some(source.as_ref()),
            Self::Discovery { .. }
            | Self::Parse { .. }
            | Self::Validation { .. }
            | Self::ManifestTooLarge { .. }
            | Self::ServerUnavailable { .. }
            | Self::NotFound { .. }
            | Self::Frontmatter { .. }
            | Self::EntryValidation { .. }
            | Self::Verification { .. } => None,
        }
    }
}
impl From<Arc<super::SkillFileError>> for SkillError {
    fn from(error: Arc<super::SkillFileError>) -> Self {
        Self::File(error)
    }
}
