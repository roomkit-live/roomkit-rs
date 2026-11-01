//! The host's skills, discovered in its directories (RFC §24.3, §24.5).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::error::SkillError;
use super::files::{self, OpenFailure, ReadFailure, SKILL_FILE_MAX};
use super::frontmatter::{self, Fields};
use super::model::{DiscoverOptions, Skill, SkillMetadata};
use super::texts::{escaped, quoted_name};

/// The most characters a skill's name takes (RFC §24.1).
const NAME_MAX: usize = 64;

/// The most characters a skill's description takes.
const DESCRIPTION_MAX: usize = 1024;

/// The most characters the manifest takes: every system prompt carries it.
pub(crate) const MANIFEST_MAX: usize = 64 * 1024;

/// The skills an agent serves (RFC §24.3), read from the host's own
/// directories: each a directory holding a `SKILL.md` whose frontmatter
/// gives its `name` and `description`, read line by line within RFC
/// §24.1's bounds, with no YAML reader (step 4, decision 11).
///
/// Discovery and registration are strict and atomic (RFC §24.5): the first
/// skill that cannot be loaded fails the call, which names it and why, and
/// leaves the registry as it was. A skill is refused when:
///
/// - its `SKILL.md` is missing, longer than 256 KiB, not UTF-8, a link out
///   of its directory, or has no frontmatter;
/// - its frontmatter is past 65,536 characters or 64 brackets open at
///   once, holds a line that may hide a key or a field the registry reads
///   that cannot be read whole line by line, or declares `allowed_tools`
///   or `requires`, since this implementation gates no tool (another key
///   it cannot read whole is left out of `extra_metadata`);
/// - its name is missing, not kebab-case, longer than 64 characters or not
///   its directory's; its description is missing or longer than 1,024
///   characters;
/// - the manifest of all its skills would pass 65,536 characters.
///
/// Discovery that is not strict ([`DiscoverOptions::strict`]) logs and
/// skips each directory it cannot scan and each skill it cannot load; the
/// manifest's bound still fails it. A skill registered under a name the
/// registry holds replaces it, keeping its place.
///
/// ```
/// use roomkit_ai::{DiscoverOptions, SkillRegistry};
///
/// let mut skills = SkillRegistry::new();
/// let error = skills
///     .discover(&["no/such/directory"], DiscoverOptions::default())
///     .unwrap_err();
/// assert!(error.to_string().starts_with("Skill directory not found"));
/// assert!(!skills.has_entries());
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SkillRegistry {
    skills: Vec<Skill>,
}

impl SkillRegistry {
    /// A registry with no skill.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers every skill found in `directories`: each subdirectory
    /// holding a `SKILL.md`, in name order, a link to a directory inside
    /// the one scanned followed. Returns how many skills it registered.
    ///
    /// # Errors
    ///
    /// When [`DiscoverOptions::strict`], returns [`SkillError::Discovery`]
    /// for a directory that does not exist or cannot be scanned, or a
    /// subdirectory that leads out of it, and [`SkillError::Parse`] and
    /// [`SkillError::Validation`] for the first skill that cannot be
    /// loaded; in either mode, [`SkillError::ManifestTooLarge`] past the
    /// manifest's bound. The registry is then as it was.
    ///
    /// The directories are a slice, so that one directory is never read as
    /// the components of its path:
    ///
    /// ```compile_fail,E0308
    /// use roomkit_ai::{DiscoverOptions, SkillRegistry};
    ///
    /// let directory = std::path::PathBuf::from("skills");
    /// let _found = SkillRegistry::new().discover(&directory, DiscoverOptions::default());
    /// ```
    pub fn discover<P: AsRef<Path>>(
        &mut self,
        directories: &[P],
        options: DiscoverOptions,
    ) -> Result<usize, SkillError> {
        let mut found: Vec<PathBuf> = Vec::new();
        for directory in directories {
            for candidate in candidates(directory.as_ref(), options.strict)? {
                if !found.contains(&candidate) {
                    found.push(candidate);
                }
            }
        }
        let mut skills = Vec::new();
        for directory in &found {
            match load(directory) {
                Ok(skill) => skills.push(skill),
                Err(error) => skipped(options.strict, error)?,
            }
        }
        self.commit(skills)
    }

    /// Registers the skill in `skill_dir`, a link to it followed, and
    /// returns its metadata.
    ///
    /// # Errors
    ///
    /// Returns [`SkillError::Parse`] for a directory whose `SKILL.md`
    /// cannot be read, the directory missing included, or parsed,
    /// [`SkillError::Validation`] for metadata that does not validate, and
    /// [`SkillError::ManifestTooLarge`] past the manifest's bound. The
    /// registry is then as it was.
    pub fn register(&mut self, skill_dir: impl AsRef<Path>) -> Result<SkillMetadata, SkillError> {
        let skill_dir = skill_dir.as_ref();
        let resolved = skill_dir.canonicalize().map_err(|error| {
            SkillError::parse(
                &label(skill_dir),
                format!("Unable to read SKILL.md: {error}"),
            )
        })?;
        let skill = load(&resolved)?;
        let metadata = skill.metadata.clone();
        self.commit(vec![skill])?;
        Ok(metadata)
    }

    /// The metadata of the skill `name`.
    pub fn get_metadata(&self, name: &str) -> Option<&SkillMetadata> {
        self.get_skill(name).map(|skill| &skill.metadata)
    }

    /// The skill `name` whole, with its instructions.
    pub fn get_skill(&self, name: &str) -> Option<&Skill> {
        self.skills.iter().find(|skill| skill.name() == name)
    }

    /// The metadata of its skills, in the order they were registered.
    pub fn all_metadata(&self) -> Vec<&SkillMetadata> {
        self.skills.iter().map(|skill| &skill.metadata).collect()
    }

    /// The names of its skills, in the order they were registered.
    pub fn skill_names(&self) -> Vec<&str> {
        self.skills.iter().map(Skill::name).collect()
    }

    /// How many skills it holds.
    pub fn skill_count(&self) -> usize {
        self.skills.len()
    }

    /// Whether the model has anything to be told: a skill it can activate.
    pub fn has_entries(&self) -> bool {
        !self.skills.is_empty()
    }

    /// Copies the registry, optionally narrowed to `names` (RFC §24.3).
    /// Instructions and resolved paths are kept without reading files again.
    /// Unknown names are omitted; the source registry is unchanged.
    #[must_use]
    pub fn copy(&self, names: Option<&[String]>) -> Self {
        Self {
            skills: self
                .skills
                .iter()
                .filter(|skill| {
                    names.is_none_or(|names| names.iter().any(|name| name == skill.name()))
                })
                .cloned()
                .collect(),
        }
    }

    /// The tools an AI channel given this registry declares
    /// ([`AiChannelOptions::skills`](crate::AiChannelOptions::skills)).
    /// An empty registry registers no tools (RFC §24.3).
    pub fn tool_definitions(&self) -> Vec<crate::AiTool> {
        if self.has_entries() {
            super::tools::SkillTools::definitions()
        } else {
            Vec::new()
        }
    }

    /// The manifest every system prompt carries (RFC §24.3
    /// `to_prompt_xml()`): an `<available_skills>` block naming each skill
    /// with its description, license and compatibility, their text escaped
    /// so that none reads as markup; empty with no skill.
    pub fn to_prompt_xml(&self) -> String {
        manifest(&self.skills)
    }

    /// Takes `skills`, each replacing the one of its name, when the
    /// manifest stays within its bound; returns how many it took.
    fn commit(&mut self, skills: Vec<Skill>) -> Result<usize, SkillError> {
        let mut next = self.skills.clone();
        let mut names: Vec<String> = Vec::new();
        for skill in skills {
            if !names.iter().any(|name| name == skill.name()) {
                names.push(skill.name().to_owned());
            }
            match next.iter_mut().find(|held| held.name() == skill.name()) {
                Some(held) => *held = skill,
                None => next.push(skill),
            }
        }
        let characters = manifest(&next).chars().count();
        if characters > MANIFEST_MAX {
            return Err(SkillError::ManifestTooLarge {
                characters,
                limit: MANIFEST_MAX,
            });
        }
        self.skills = next;
        Ok(names.len())
    }
}

/// Fails with `error` when discovery is strict; otherwise logs it, and
/// what it names is skipped.
fn skipped(strict: bool, error: SkillError) -> Result<(), SkillError> {
    if strict {
        return Err(error);
    }
    tracing::warn!(target: "roomkit::skills", %error, "skill discovery skipped what it could not load");
    Ok(())
}

/// The skill directories of `directory`, resolved, in name order: each
/// subdirectory holding a `SKILL.md`; when discovery is not `strict`, none
/// for a directory it cannot scan, and none for a subdirectory it cannot
/// take.
fn candidates(directory: &Path, strict: bool) -> Result<Vec<PathBuf>, SkillError> {
    let (resolved, children) = match children(directory) {
        Ok(read) => read,
        Err(error) => return skipped(strict, error).map(|()| Vec::new()),
    };
    let mut found = Vec::new();
    for child in children {
        match candidate(&child, &resolved) {
            Ok(Some(skill)) => found.push(skill),
            Ok(None) => {}
            Err(error) => skipped(strict, error)?,
        }
    }
    Ok(found)
}

/// `directory`, resolved, and its entries, in name order.
fn children(directory: &Path) -> Result<(PathBuf, Vec<PathBuf>), SkillError> {
    let not_found = || SkillError::discovery(directory, "Skill directory not found");
    let resolved = directory
        .canonicalize()
        .map_err(|_unresolved| not_found())?;
    if !resolved.is_dir() {
        return Err(not_found());
    }
    let scanned = || SkillError::discovery(&resolved, "Skill directory cannot be scanned");
    let mut children: Vec<PathBuf> = fs::read_dir(&resolved)
        .map_err(|_unreadable| scanned())?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<io::Result<_>>()
        .map_err(|_unreadable| scanned())?;
    children.sort();
    Ok((resolved, children))
}

/// `child`, an entry of the directory `scanned`, resolved, when it is a
/// skill directory: a directory holding a `SKILL.md`.
fn candidate(child: &Path, scanned: &Path) -> Result<Option<PathBuf>, SkillError> {
    if !child.is_dir() {
        return Ok(None);
    }
    let inspected = || SkillError::discovery(child, "Skill candidate cannot be inspected");
    let skill = child.canonicalize().map_err(|_unresolved| inspected())?;
    // A link to a directory is followed, never out of the one scanned.
    if !files::is_below(&skill, scanned) {
        return Err(SkillError::discovery(
            child,
            "Skill candidate escapes discovery directory",
        ));
    }
    let held = files::find_skill_file(&skill).map_err(|_unreadable| inspected())?;
    Ok(held.map(|_file| skill))
}

/// The name of `directory`, which a failure names the skill by.
fn label(directory: &Path) -> String {
    directory
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// The skill in `directory`, resolved: its `SKILL.md` read and parsed, its
/// metadata validated.
fn load(directory: &Path) -> Result<Skill, SkillError> {
    let label = label(directory);
    let content = skill_file(directory, &label)?;
    let (text, body) =
        frontmatter::split(&content).map_err(|reason| SkillError::parse(&label, reason))?;
    let fields = frontmatter::read(text).map_err(|reason| SkillError::parse(&label, reason))?;
    let reasons = invalid(&fields, &label);
    if !reasons.is_empty() {
        return Err(SkillError::Validation {
            skill: label,
            reasons,
        });
    }
    Ok(Skill {
        metadata: SkillMetadata {
            name: fields.name.unwrap_or_default(),
            description: fields.description.unwrap_or_default(),
            license: fields.license.filter(|license| !license.is_empty()),
            compatibility: fields.compatibility.filter(|text| !text.is_empty()),
            extra_metadata: fields
                .extra
                .into_iter()
                .map(|(key, text)| (key, text.into()))
                .collect(),
        },
        instructions: body.to_owned(),
        path: directory.to_path_buf(),
    })
}

/// The text of the `SKILL.md` of `directory`, a regular file inside it once
/// its links are followed, at most 256 KiB of UTF-8, read through the
/// handle checked once it is open ([`files::open_inside`]).
fn skill_file(directory: &Path, label: &str) -> Result<String, SkillError> {
    let failed = |reason: String| SkillError::parse(label, reason);
    let found = files::find_skill_file(directory)
        .map_err(|error| failed(format!("Unable to read SKILL.md: {error}")))?
        .ok_or_else(|| failed("No SKILL.md found".to_owned()))?;
    let file = files::open_inside(&found, |target| files::is_below(target, directory)).map_err(
        |failure| {
            failed(match failure {
                OpenFailure::Missing => "No SKILL.md found".to_owned(),
                OpenFailure::Escapes => "SKILL.md escapes its skill directory".to_owned(),
                OpenFailure::NotAFile => "SKILL.md is not a regular file".to_owned(),
                OpenFailure::Changed => "SKILL.md changed while it was read".to_owned(),
                OpenFailure::Unverified(error) => {
                    format!("SKILL.md cannot be verified (/proc/self/fd unreadable): {error}")
                }
                OpenFailure::Failed(error) => format!("Unable to read SKILL.md: {error}"),
            })
        },
    )?;
    files::read_bounded(file, SKILL_FILE_MAX).map_err(|failure| {
        failed(match failure {
            ReadFailure::TooLarge => format!("SKILL.md is past {SKILL_FILE_MAX} bytes"),
            ReadFailure::NotText => "SKILL.md is not UTF-8 text".to_owned(),
            ReadFailure::Io(error) => format!("Unable to read SKILL.md: {error}"),
        })
    })
}

/// What is wrong with `fields`, the frontmatter of the skill in the
/// directory `label`, in the reference implementation's words and order.
fn invalid(fields: &Fields, label: &str) -> Vec<String> {
    let mut reasons = Vec::new();
    let name = fields.name.as_deref().unwrap_or_default();
    let characters = name.chars().count();
    if name.is_empty() {
        reasons.push("Missing required field: name".to_owned());
    } else if !is_kebab_case(name) {
        reasons.push(format!(
            "Invalid name {}: must be kebab-case (lowercase alphanumeric + hyphens)",
            quoted_name(name)
        ));
    } else if characters > NAME_MAX {
        reasons.push(format!(
            "Name too long: {characters} chars (max {NAME_MAX})"
        ));
    }
    if !name.is_empty() && name != label {
        reasons.push(format!(
            "Name {} does not match directory name {}",
            quoted_name(name),
            quoted_name(label)
        ));
    }
    let description = fields.description.as_deref().unwrap_or_default();
    let characters = description.chars().count();
    if description.is_empty() {
        reasons.push("Missing required field: description".to_owned());
    } else if characters > DESCRIPTION_MAX {
        reasons.push(format!(
            "Description too long: {characters} chars (max {DESCRIPTION_MAX})"
        ));
    }
    reasons
}

/// Whether `name` is lowercase ASCII letters and digits in runs a hyphen
/// separates.
fn is_kebab_case(name: &str) -> bool {
    name.split('-').all(|run| {
        !run.is_empty()
            && run
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
    })
}

/// The `<available_skills>` block of `skills`, as the reference
/// implementation writes it; empty with none.
fn manifest(skills: &[Skill]) -> String {
    if skills.is_empty() {
        return String::new();
    }
    let mut lines = vec!["<available_skills>".to_owned()];
    for skill in skills {
        let metadata = &skill.metadata;
        lines.push(format!("  <skill name=\"{}\">", escaped(&metadata.name)));
        let fields = [
            ("description", Some(metadata.description.as_str())),
            ("license", metadata.license.as_deref()),
            ("compatibility", metadata.compatibility.as_deref()),
        ];
        for (tag, value) in fields {
            if let Some(value) = value.filter(|value| !value.is_empty()) {
                lines.push(format!("    <{tag}>{}</{tag}>", escaped(value)));
            }
        }
        lines.push("  </skill>".to_owned());
    }
    lines.push("</available_skills>".to_owned());
    lines.join("\n")
}
