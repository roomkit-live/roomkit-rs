//! A scripted agent's skills (README, "Scripted agents"): each skill written
//! to a directory of its own, as a host's are, and discovered from there,
//! the directories removed with the run.

use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit_ai::{DiscoverOptions, RequiresMatch, SkillRegistry, SkillRegistryOptions, ToolError};
use serde_json::Value;

use crate::run::lock;

/// The directories a run's registries read.
#[derive(Default)]
pub(crate) struct SkillDirectories(Mutex<Vec<PathBuf>>);

impl SkillDirectories {
    /// The registry holding `skills`, written to a new directory.
    pub(crate) fn registry(
        &self,
        skills: &Value,
        matcher: &Value,
    ) -> Result<SkillRegistry, String> {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let directory = std::env::temp_dir().join(format!(
            "roomkit-conformance-skills-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&directory).map_err(|e| e.to_string())?;
        lock(&self.0).push(directory.clone());
        for skill in skills.as_array().ok_or("skills: not a list")? {
            write_skill(&directory, skill).map_err(|e| e.to_string())?;
        }
        let mut registry = SkillRegistry::new(SkillRegistryOptions {
            requires_match: requires_match(matcher)?,
        });
        registry
            .discover(&[directory], DiscoverOptions::default())
            .map_err(|e| e.to_string())?;
        for skill in skills.as_array().into_iter().flatten() {
            let name = skill["name"].as_str().unwrap_or_default();
            if let Some(reason) = skill["unavailable"].as_str() {
                registry.mark_unavailable(name, reason);
            }
            if skill["unlisted"].as_bool().unwrap_or(false) {
                registry.mark_unlisted(name);
            }
        }
        Ok(registry)
    }
}

fn requires_match(value: &Value) -> Result<RequiresMatch, String> {
    let mode = if value.is_null() {
        "exact"
    } else {
        value.as_str().ok_or("skills_requires_match: not text")?
    };
    match mode {
        "exact" => Ok(RequiresMatch::default()),
        "hub" => Ok(RequiresMatch::new(|required, offered| {
            Ok(offered
                .iter()
                .any(|name| name == required || name.starts_with(&format!("{required}_"))))
        })),
        "fail" => Ok(RequiresMatch::new(|_, _| {
            Err(ToolError::Raised {
                kind: "RuntimeError".into(),
                message: "Scripted skill matcher failed".into(),
            })
        })),
        _ => Err(format!("Unknown skill matcher: {mode}")),
    }
}

impl Drop for SkillDirectories {
    fn drop(&mut self) {
        for directory in lock(&self.0).drain(..) {
            drop(std::fs::remove_dir_all(directory));
        }
    }
}

/// Writes `skill`'s `SKILL.md` and its references under `root`.
fn write_skill(root: &std::path::Path, skill: &Value) -> std::io::Result<()> {
    let text = |key: &str| skill[key].as_str().unwrap_or_default();
    let directory = root.join(text("name"));
    let references = directory.join("references");
    std::fs::create_dir_all(&references)?;
    let requirements: String = ["allowed_tools", "requires"]
        .iter()
        .filter_map(|key| skill.get(key).map(|names| format!("{key}: {names}\n")))
        .collect();
    std::fs::write(
        directory.join("SKILL.md"),
        format!(
            "---\nname: {}\ndescription: {}\n{requirements}---\n{}",
            text("name"),
            text("description"),
            text("instructions")
        ),
    )?;
    for (name, reference) in skill["references"].as_object().into_iter().flatten() {
        std::fs::write(
            references.join(name),
            reference.as_str().unwrap_or_default(),
        )?;
    }
    Ok(())
}
