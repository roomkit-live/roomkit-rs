//! The host's skills, discovered in its directories (RFC §24.3, §24.5).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::error::SkillError;
use super::files::{self, OpenFailure, ReadFailure, SKILL_FILE_MAX};
use super::frontmatter::{self, Fields};
use super::texts::{escaped, quoted_name};

/// The most characters a skill's name takes (RFC §24.1).
const NAME_MAX: usize = 64;

/// The most characters a skill's description takes.
const DESCRIPTION_MAX: usize = 1024;

/// The most characters the manifest takes: every system prompt carries it.
pub(crate) const MANIFEST_MAX: usize = 64 * 1024;

/// A skill (RFC §24.2): its metadata, read from its `SKILL.md`'s
/// frontmatter, its instructions, the file's body, and its directory,
/// resolved, where its references are read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Skill {
    name: String,
    description: String,
    license: Option<String>,
    compatibility: Option<String>,
    instructions: String,
    path: PathBuf,
}

impl Skill {
    /// Its name: lowercase letters and digits in hyphen-separated runs, at
    /// most 64 characters, its directory's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// What it does, at most 1,024 characters.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Its license, when its frontmatter names one.
    pub fn license(&self) -> Option<&str> {
        self.license.as_deref()
    }

    /// What it needs to run, as free text, when its frontmatter says.
    pub fn compatibility(&self) -> Option<&str> {
        self.compatibility.as_deref()
    }

    /// Its instructions: its `SKILL.md`'s body.
    pub fn instructions(&self) -> &str {
        &self.instructions
    }

    /// Its directory, resolved.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

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
///   once, cannot be read whole line by line, or declares `allowed_tools`
///   or `requires`, since this implementation gates no tool;
/// - its name is missing, not kebab-case, longer than 64 characters or not
///   its directory's; its description is missing or longer than 1,024
///   characters;
/// - the manifest of all its skills would pass 65,536 characters.
///
/// A skill registered under a name the registry holds replaces it, keeping
/// its place.
///
/// ```
/// use roomkit_ai::SkillRegistry;
///
/// let mut skills = SkillRegistry::new();
/// let error = skills.discover(&["no/such/directory"]).unwrap_err();
/// assert!(error.to_string().starts_with("Skill directory not found"));
/// assert!(skills.is_empty());
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
    /// Returns [`SkillError::Discovery`] for a directory that does not
    /// exist or cannot be scanned, or a subdirectory that leads out of it;
    /// [`SkillError::Parse`] and [`SkillError::Validation`] for the first
    /// skill that cannot be loaded; [`SkillError::ManifestTooLarge`] past
    /// the manifest's bound. The registry is then as it was.
    ///
    /// The directories are a slice, so that one directory is never read as
    /// the components of its path:
    ///
    /// ```compile_fail,E0308
    /// let directory = std::path::PathBuf::from("skills");
    /// let _found = roomkit_ai::SkillRegistry::new().discover(&directory);
    /// ```
    pub fn discover<P: AsRef<Path>>(&mut self, directories: &[P]) -> Result<usize, SkillError> {
        let mut found: Vec<PathBuf> = Vec::new();
        for directory in directories {
            for candidate in candidates(directory.as_ref())? {
                if !found.contains(&candidate) {
                    found.push(candidate);
                }
            }
        }
        let skills = found
            .iter()
            .map(|directory| load(directory))
            .collect::<Result<Vec<Skill>, SkillError>>()?;
        let count = self.commit(skills)?;
        Ok(count)
    }

    /// Registers the skill in `directory`, a link to it followed, and
    /// returns it.
    ///
    /// # Errors
    ///
    /// Returns [`SkillError::Discovery`] for a directory that does not
    /// exist, [`SkillError::Parse`] and [`SkillError::Validation`] for a
    /// skill that cannot be loaded, and [`SkillError::ManifestTooLarge`]
    /// past the manifest's bound. The registry is then as it was.
    pub fn register(&mut self, directory: impl AsRef<Path>) -> Result<&Skill, SkillError> {
        let directory = directory.as_ref();
        let resolved = directory
            .canonicalize()
            .map_err(|_unresolved| SkillError::discovery(directory, "Skill directory not found"))?;
        let skill = load(&resolved)?;
        let name = skill.name.clone();
        self.commit(vec![skill])?;
        self.get(&name)
            .ok_or_else(|| SkillError::parse(&name, "the skill was registered and then not found"))
    }

    /// The skill `name`.
    pub fn get(&self, name: &str) -> Option<&Skill> {
        self.skills.iter().find(|skill| skill.name == name)
    }

    /// Its skills, in the order they were registered.
    pub fn skills(&self) -> &[Skill] {
        &self.skills
    }

    /// How many skills it holds.
    pub fn len(&self) -> usize {
        self.skills.len()
    }

    /// Whether it holds no skill.
    pub fn is_empty(&self) -> bool {
        self.skills.is_empty()
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
                .filter(|skill| names.is_none_or(|names| names.contains(&skill.name)))
                .cloned()
                .collect(),
        }
    }

    /// The tools [`crate::AiAgent::with_skills`] registers for this registry.
    /// An empty registry registers no tools (RFC §24.3).
    pub fn tool_definitions(&self) -> Vec<crate::ToolDefinition> {
        if self.is_empty() {
            Vec::new()
        } else {
            super::tools::SkillTools::definitions()
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
            if !names.contains(&skill.name) {
                names.push(skill.name.clone());
            }
            match next.iter_mut().find(|held| held.name == skill.name) {
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

/// The skill directories of `directory`, resolved, in name order: each
/// subdirectory holding a `SKILL.md`.
fn candidates(directory: &Path) -> Result<Vec<PathBuf>, SkillError> {
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
    let mut found = Vec::new();
    for child in children {
        if !child.is_dir() {
            continue;
        }
        let inspected = || SkillError::discovery(&child, "Skill candidate cannot be inspected");
        let skill = child.canonicalize().map_err(|_unresolved| inspected())?;
        // A link to a directory is followed, never out of the one scanned.
        if !files::is_below(&skill, &resolved) {
            return Err(SkillError::discovery(
                &child,
                "Skill candidate escapes discovery directory",
            ));
        }
        if files::find_skill_file(&skill)
            .map_err(|_unreadable| inspected())?
            .is_some()
        {
            found.push(skill);
        }
    }
    Ok(found)
}

/// The skill in `directory`, resolved: its `SKILL.md` read and parsed, its
/// metadata validated.
fn load(directory: &Path) -> Result<Skill, SkillError> {
    let label = directory
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
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
        name: fields.name.unwrap_or_default(),
        description: fields.description.unwrap_or_default(),
        license: fields.license.filter(|license| !license.is_empty()),
        compatibility: fields.compatibility.filter(|text| !text.is_empty()),
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
        lines.push(format!("  <skill name=\"{}\">", escaped(&skill.name)));
        let fields = [
            ("description", Some(skill.description.as_str())),
            ("license", skill.license.as_deref()),
            ("compatibility", skill.compatibility.as_deref()),
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
