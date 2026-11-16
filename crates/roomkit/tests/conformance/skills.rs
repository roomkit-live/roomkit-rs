//! A scripted agent's skills (README, "Scripted agents"): each skill written
//! to a directory of its own, discovered there or registered in memory,
//! the directories removed with the run.

use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit_ai::{
    DiscoverOptions, RequiresMatch, Skill, SkillMetadata, SkillRegistry, SkillRegistryOptions,
    SkillToolNames, ToolError,
};
use serde_json::Value;

use crate::run::lock;

#[path = "skills/scripts.rs"]
mod scripts;

/// The directories a run's registries read.
#[derive(Default)]
pub(crate) struct SkillDirectories {
    paths: Mutex<Vec<PathBuf>>,
    executors: scripts::Executors,
}

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
        lock(&self.paths).push(directory.clone());
        let skills = skills.as_array().ok_or("skills: not a list")?;
        let directories = skills
            .iter()
            .enumerate()
            .map(|(index, skill)| write_skill(&directory, index, skill))
            .collect::<Result<Vec<_>, _>>()?;
        let mut registry = SkillRegistry::new(SkillRegistryOptions {
            requires_match: requires_match(matcher)?,
        });
        registry
            .discover(&[directory], DiscoverOptions::default())
            .map_err(|error| error.to_string())?;
        for (skill, path) in skills.iter().zip(directories) {
            if in_memory(skill)? {
                registry
                    .add(memory_skill(skill, path)?)
                    .map_err(|error| error.to_string())?;
            }
        }
        apply_marks(&mut registry, skills)?;
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
        for directory in lock(&self.paths).drain(..) {
            drop(std::fs::remove_dir_all(directory));
        }
    }
}

fn apply_marks(registry: &mut SkillRegistry, skills: &[Value]) -> Result<(), String> {
    for skill in skills {
        let name = skill["name"].as_str().unwrap_or_default();
        if let Some(reason) = skill["unavailable"].as_str() {
            registry
                .mark_unavailable(name, reason)
                .map_err(|error| error.to_string())?;
        }
        if skill["unlisted"].as_bool().unwrap_or(false) {
            registry.mark_unlisted(name);
        }
    }
    Ok(())
}

fn in_memory(skill: &Value) -> Result<bool, String> {
    skill.get("in_memory").map_or(Ok(false), |value| {
        value
            .as_bool()
            .ok_or_else(|| "in_memory: expected a boolean".into())
    })
}

/// References have a real path even when the registry entry has no SKILL.md.
fn write_skill(root: &std::path::Path, index: usize, skill: &Value) -> Result<PathBuf, String> {
    let memory = in_memory(skill)?;
    let directory = if memory {
        root.join("_in_memory").join(index.to_string())
    } else {
        root.join(skill["name"].as_str().unwrap_or_default())
    };
    let references = directory.join("references");
    std::fs::create_dir_all(&references).map_err(|error| error.to_string())?;
    if !memory {
        write_frontmatter(&directory, skill).map_err(|error| error.to_string())?;
    }
    for (name, reference) in skill["references"].as_object().into_iter().flatten() {
        std::fs::write(
            references.join(name),
            reference.as_str().unwrap_or_default(),
        )
        .map_err(|error| error.to_string())?;
    }
    write_scripts(&directory, &skill["scripts"])?;
    Ok(directory)
}

fn write_scripts(directory: &std::path::Path, scripts: &Value) -> Result<(), String> {
    if scripts.is_null() {
        return Ok(());
    }
    let directory = directory.join("scripts");
    std::fs::create_dir_all(&directory).map_err(|error| error.to_string())?;
    for (name, content) in scripts.as_object().ok_or("scripts: not an object")? {
        let path = roomkit_ai::safe_join_filename(&directory, name, Some("script"))
            .map_err(|error| error.to_string())?;
        let content = content.as_str().ok_or("script contents: not text")?;
        std::fs::write(path, content).map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn write_frontmatter(directory: &std::path::Path, skill: &Value) -> std::io::Result<()> {
    let text = |key: &str| skill[key].as_str().unwrap_or_default();
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
    )
}

fn memory_skill(skill: &Value, path: PathBuf) -> Result<Skill, String> {
    let text = |key: &str| skill[key].as_str().unwrap_or_default();
    let mut metadata = SkillMetadata::new(text("name"), text("description"));
    metadata.allowed_tools = skill.get("allowed_tools").map(tool_names).transpose()?;
    if let Some(value) = skill.get("requires") {
        let text = match tool_names(value)? {
            SkillToolNames::Text(text) => text,
            SkillToolNames::List(items) => items.join(", "),
        };
        metadata
            .extra_metadata
            .insert("requires".into(), Value::String(text));
    }
    Ok(Skill {
        metadata,
        instructions: text("instructions").into(),
        path,
    })
}

fn tool_names(value: &Value) -> Result<SkillToolNames, String> {
    if let Some(text) = value.as_str() {
        return Ok(SkillToolNames::Text(text.into()));
    }
    let items = value
        .as_array()
        .ok_or("tool names: expected text or a list")?;
    let names = items
        .iter()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| "tool names: expected text entries".into())
        })
        .collect::<Result<_, String>>()?;
    Ok(SkillToolNames::List(names))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Python: tests/conformance/test_skill_fixture.py::test_the_fixture_adds_an_in_memory_skill_without_a_skill_md
    #[test]
    fn test_the_fixture_adds_an_in_memory_skill_without_a_skill_md() {
        let directories = SkillDirectories::default();
        let skills = json!([{"name":"Store Handbook", "description":"Stored", "instructions":"From memory", "in_memory":true, "references":{"notes.md":"Reference"}}]);
        let registry = directories.registry(&skills, &Value::Null).unwrap();
        let skill = registry.get_skill("Store Handbook").unwrap();
        assert_eq!(skill.instructions, "From memory");
        assert_eq!(skill.read_reference("notes.md").unwrap(), "Reference");
        assert!(!skill.path.join("SKILL.md").exists());
    }
}
