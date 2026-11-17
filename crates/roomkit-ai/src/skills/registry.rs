//! The host's skills, discovered in its directories (RFC §24.3, §24.5).

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

use super::availability::Marks;
use super::error::SkillError;
use super::files::{self, OpenFailure, ReadFailure, SKILL_FILE_MAX};
use super::frontmatter;
use super::model::{DiscoverOptions, Skill, SkillMetadata};
use super::served::catalog::Catalog;
use super::validation::{invalid, memory_errors};
use super::{CopyOptions, LoadedSkill, McpSkill, RequiresMatch, SkillRegistryOptions};

/// The most characters the manifest takes: every system prompt carries it.
pub(crate) const MANIFEST_MAX: usize = 64 * 1024;

/// The skills an agent serves (RFC §24.3), read from the host's own
/// directories: each a directory holding a `SKILL.md` whose frontmatter
/// gives its `name` and `description`, read line by line within RFC
/// §24.1's bounds, with no YAML reader (step 4, decision 11).
///
/// Directory discovery and registration are strict and atomic (RFC §24.5): the first
/// skill that cannot be loaded fails the call, which names it and why, and
/// leaves the registry as it was. A skill is refused when:
///
/// - its `SKILL.md` is missing, longer than 256 KiB, not UTF-8, a link out
///   of its directory, or has no frontmatter;
/// - its frontmatter is past 65,536 characters or 64 brackets open at
///   once, holds a line that may hide a key or a field the registry reads
///   that cannot be read whole line by line, or an unreadable
///   `allowed_tools` or `requires` (another key it cannot read whole
///   is left out of `extra_metadata`);
/// - its name is missing, not kebab-case, longer than 64 characters or not
///   its directory's; its description is missing or longer than 1,024
///   characters;
/// - the manifest of all its skills would pass 65,536 characters.
///
/// Discovery that is not strict ([`DiscoverOptions::strict`]) logs and
/// skips each directory it cannot scan and each skill it cannot load; the
/// manifest's bound still fails it. A skill registered under a name the
/// registry holds replaces it, keeping its place. [`Self::add`] accepts an already
/// built skill without reading a file or imposing directory metadata constraints.
///
/// ```
/// use roomkit_ai::{DiscoverOptions, SkillRegistry};
///
/// let mut skills = SkillRegistry::default();
/// let error = skills
///     .discover(&["no/such/directory"], DiscoverOptions::default())
///     .unwrap_err();
/// assert!(error.to_string().starts_with("Skill directory not found"));
/// assert!(!skills.has_entries());
/// ```
#[derive(Debug, Default)]
pub struct SkillRegistry {
    /// The host's reading of a prerequisite against offered tool names.
    pub requires_match: RequiresMatch,
    pub(super) state: Mutex<RegistryState>,
    pub(super) catalog: Arc<Mutex<Catalog>>,
}

#[derive(Clone, Debug, Default)]
pub(super) struct RegistryState {
    // Served placeholders hold metadata only; they are never returned as local skills.
    pub(super) skills: Vec<Skill>,
    pub(super) marks: Marks,
    pub(super) loaded: HashMap<String, Arc<Mutex<McpSkill>>>,
    pub(super) owned_labels: HashSet<String>,
    pub(super) withheld: HashSet<String>,
}

pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Clone for SkillRegistry {
    fn clone(&self) -> Self {
        Self {
            requires_match: self.requires_match.clone(),
            state: Mutex::new(lock(&self.state).clone()),
            catalog: Arc::clone(&self.catalog),
        }
    }
}
impl PartialEq for SkillRegistry {
    fn eq(&self, other: &Self) -> bool {
        let a = lock(&self.state).clone();
        let b = lock(&other.state).clone();
        let catalogs_match = Arc::ptr_eq(&self.catalog, &other.catalog) || {
            let a_empty = !lock(&self.catalog).has_servers();
            let b_empty = !lock(&other.catalog).has_servers();
            a_empty && b_empty
        };
        self.requires_match == other.requires_match
            && a.skills == b.skills
            && a.marks == b.marks
            && a.owned_labels == b.owned_labels
            && a.withheld == b.withheld
            && a.loaded.len() == b.loaded.len()
            && a.loaded.iter().all(|(name, skill)| {
                b.loaded.get(name).is_some_and(|other| {
                    let first = lock(skill).clone();
                    let second = lock(other).clone();
                    first == second
                })
            })
            && catalogs_match
    }
}
impl Eq for SkillRegistry {}

impl SkillRegistry {
    /// A registry with no skill.
    pub fn new(options: SkillRegistryOptions) -> Self {
        Self {
            requires_match: options.requires_match,
            ..Self::default()
        }
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
    /// let _found = SkillRegistry::default().discover(&directory, DiscoverOptions::default());
    /// ```
    pub fn discover<P: AsRef<Path>>(
        &self,
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
    pub fn register(&self, skill_dir: impl AsRef<Path>) -> Result<SkillMetadata, SkillError> {
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

    /// Adds a skill built in memory, retaining its instructions and path (RFC §24.3).
    /// Replaces an earlier skill of the same name and clears its availability marks.
    /// No file is read and directory metadata constraints do not apply.
    ///
    /// # Errors
    /// Returns [`SkillError::Validation`] for a name containing `:` (reserved for
    /// served skills) or a bracketed tool pattern, and [`SkillError::ManifestTooLarge`]
    /// past the manifest bound. A rejected addition leaves the registry unchanged.
    pub fn add(&self, skill: Skill) -> Result<(), SkillError> {
        let reasons = memory_errors(&skill.metadata);
        if !reasons.is_empty() {
            return Err(SkillError::Validation {
                skill: skill.name().to_owned(),
                reasons,
            });
        }
        self.commit(vec![skill])?;
        Ok(())
    }

    /// An owned metadata snapshot, with no network or catalog-following read.
    pub fn get_metadata(&self, name: &str) -> Option<SkillMetadata> {
        lock(&self.state)
            .skills
            .iter()
            .find(|skill| skill.name() == name)
            .map(|skill| skill.metadata.clone())
    }

    /// An owned skill snapshot; served content is available only after loading.
    ///
    /// # Errors
    /// A shared catalog refusal that cannot fit the local manifest is returned
    /// before any cached value, leaving local state unchanged (RFC §24.5).
    pub fn get_skill(&self, name: &str) -> Result<Option<LoadedSkill>, SkillError> {
        if self.is_served(name) {
            self.follow_catalog(name)?;
            let loaded = lock(&self.state).loaded.get(name).cloned();
            return Ok(loaded.map(|skill| LoadedSkill::Served(lock(&skill).clone())));
        }
        Ok(lock(&self.state)
            .skills
            .iter()
            .find(|skill| skill.name() == name)
            .cloned()
            .map(LoadedSkill::Local))
    }

    /// Owned metadata snapshots in registration order.
    pub fn all_metadata(&self) -> Vec<SkillMetadata> {
        lock(&self.state)
            .skills
            .iter()
            .map(|skill| skill.metadata.clone())
            .collect()
    }
    /// Owned names in registration order.
    pub fn skill_names(&self) -> Vec<String> {
        lock(&self.state)
            .skills
            .iter()
            .map(|skill| skill.name().to_owned())
            .collect()
    }
    /// The number of available metadata entries.
    pub fn skill_count(&self) -> usize {
        lock(&self.state).skills.len()
    }
    /// Whether the model has an available skill or unavailable reason to read.
    pub fn has_entries(&self) -> bool {
        let state = lock(&self.state);
        !state.skills.is_empty() || state.marks.has_entries()
    }

    /// Copies the registry, optionally narrowed to `names` (RFC §24.3).
    /// Instructions and resolved paths are kept without reading files again.
    /// Unknown names are omitted; the source registry is unchanged.
    /// With `marks=false`, copied available skills are listed and no unavailable
    /// reason or closed pattern survives. Removed skills are never restored.
    ///
    /// # Errors
    /// Returns [`SkillError::ManifestTooLarge`] if the copied manifest would
    /// exceed 65,536 characters, including skills made listed by dropping marks.
    pub fn copy(&self, names: Option<&[String]>, options: CopyOptions) -> Result<Self, SkillError> {
        let state = lock(&self.state);
        let wanted = |name: &str| names.is_none_or(|names| names.iter().any(|held| held == name));
        let skills: Vec<Skill> = state
            .skills
            .iter()
            .filter(|skill| wanted(skill.name()))
            .cloned()
            .collect();
        let marks = if options.marks {
            state.marks.subset(names, &skills)
        } else {
            Marks::default()
        };
        marks.check_manifest(&skills)?;
        let mut withheld = state.withheld.clone();
        if !options.marks {
            withheld.extend(
                state
                    .marks
                    .unavailable
                    .iter()
                    .filter(|(name, _)| name.contains(':'))
                    .map(|(name, _)| name.clone()),
            );
        }
        let next = RegistryState {
            skills,
            marks,
            loaded: state
                .loaded
                .iter()
                .filter(|(name, _)| wanted(name))
                .map(|(name, skill)| (name.clone(), Arc::clone(skill)))
                .collect(),
            owned_labels: if names.is_none() {
                state.owned_labels.clone()
            } else {
                HashSet::new()
            },
            withheld,
        };
        Ok(Self {
            requires_match: self.requires_match.clone(),
            state: Mutex::new(next),
            catalog: Arc::clone(&self.catalog),
        })
    }

    /// The tools an AI channel given this registry declares
    /// ([`AiChannelOptions::skills`](crate::AiChannelOptions::skills)).
    /// An empty registry declares none, the channel keeping their names (RFC
    /// §24.3).
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
        {
            let state = lock(&self.state);
            state.marks.manifest(&state.skills)
        }
    }

    /// Takes `skills`, each replacing the one of its name, when the
    /// manifest stays within its bound; returns how many it took.
    fn commit(&self, skills: Vec<Skill>) -> Result<usize, SkillError> {
        let mut state = lock(&self.state);
        let mut next = state.skills.clone();
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
        let mut marks = state.marks.clone();
        for name in &names {
            marks.clear(name);
        }
        marks.check_manifest(&next)?;
        state.skills = next;
        state.marks = marks;
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
            allowed_tools: fields.allowed_tools,
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
