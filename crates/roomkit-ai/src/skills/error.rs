//! What loading skills fails with (RFC §24.5).

use std::path::PathBuf;

/// Why skills could not be loaded: a deployment error, never a runtime
/// condition (RFC §24.5). Discovery and registration fail on the first one,
/// name the skill or the directory, and leave the registry as it was.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SkillError {
    /// A directory handed to discovery cannot be scanned, or a skill
    /// directory found in it leads out of it.
    #[error("{reason}: {}", path.display())]
    Discovery {
        /// The directory.
        path: PathBuf,
        /// What is wrong with it.
        reason: String,
    },
    /// A skill's `SKILL.md` cannot be found, read or parsed (RFC §24.1).
    #[error("Parsing failed for {skill}: {reason}")]
    Parse {
        /// The skill, by its directory's name.
        skill: String,
        /// Why.
        reason: String,
    },
    /// A skill's metadata does not validate.
    #[error("Validation failed for {skill}: {}", reasons.join("; "))]
    Validation {
        /// The skill, by its directory's name.
        skill: String,
        /// Each rule it breaks.
        reasons: Vec<String>,
    },
    /// The skills' manifest, which every system prompt carries, would be
    /// longer than its bound.
    #[error("the skills' manifest would take {characters} characters, past {limit}")]
    ManifestTooLarge {
        /// What it would take.
        characters: usize,
        /// Its bound.
        limit: usize,
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
