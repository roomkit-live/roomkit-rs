//! Public in-memory registration, without manufacturing a SKILL.md.

use std::collections::HashSet;

use roomkit_ai::{Skill, SkillError, SkillMetadata, SkillRegistry, SkillToolNames};

use super::support::TempDir;

fn skill(name: &str, path: std::path::PathBuf, description: &str) -> Skill {
    let mut metadata = SkillMetadata::new(name, description);
    metadata.allowed_tools = Some(SkillToolNames::Text("calendar_*".into()));
    Skill {
        metadata,
        instructions: "Body".into(),
        path,
    }
}

// Python: tests/skills/test_local_registration.py::test_add_preserves_metadata_instructions_and_a_path_without_skill_md
#[test]
fn test_add_preserves_metadata_instructions_and_a_path_without_skill_md() {
    let files = TempDir::new();
    let stored = skill("Store Handbook", files.path().join("not-created"), "");
    let registry = SkillRegistry::default();
    registry.add(stored.clone()).unwrap();
    assert_eq!(
        registry.get_skill(stored.name()).unwrap(),
        Some(roomkit_ai::LoadedSkill::Local(stored.clone()))
    );
    assert_eq!(
        registry.get_metadata(stored.name()),
        Some(stored.metadata.clone())
    );
    assert_eq!(registry.skill_names(), ["Store Handbook"]);
    assert!(!stored.path.exists());
}

// Python: tests/skills/test_local_registration.py::test_add_replaces_in_place_and_clears_both_marks_and_closed_gates
#[test]
fn test_add_replaces_in_place_and_clears_both_marks_and_closed_gates() {
    let files = TempDir::new();
    let registry = SkillRegistry::default();
    for name in ["first", "second"] {
        registry
            .add(skill(name, files.path().join(name), "Stored"))
            .unwrap();
    }
    let mut replacement = skill("first", files.path().join("replacement"), "New");
    replacement.instructions = "New body".into();
    registry.add(replacement.clone()).unwrap();
    assert_eq!(registry.skill_names(), ["first", "second"]);
    assert_eq!(
        registry.get_skill("first").unwrap(),
        Some(roomkit_ai::LoadedSkill::Local(replacement.clone()))
    );
    registry.mark_unlisted("first");
    registry.mark_unavailable("first", "not here").unwrap();
    assert!(
        registry
            .closed_tool_names(&HashSet::new())
            .contains("calendar_*")
    );
    registry.add(replacement.clone()).unwrap();
    assert_eq!(
        registry.get_skill("first").unwrap(),
        Some(roomkit_ai::LoadedSkill::Local(replacement.clone()))
    );
    assert_eq!(registry.listed_names().len(), 2);
    assert!(registry.unavailable_skills().is_empty());
    assert!(registry.closed_tool_names(&HashSet::new()).is_empty());
}

// Python: tests/skills/test_local_registration.py::test_a_refused_add_preserves_the_previous_skill_and_marks
#[test]
fn test_a_refused_add_preserves_the_previous_skill_and_marks() {
    let files = TempDir::new();
    for marked in [false, true] {
        let registry = SkillRegistry::default();
        registry
            .add(skill("stored", files.path().join("original"), "Stored"))
            .unwrap();
        if marked {
            registry.mark_unavailable("stored", "offline").unwrap();
        }
        let before = registry.clone();
        let error = registry
            .add(skill(
                "stored",
                files.path().join("rejected"),
                &"&".repeat(65_536),
            ))
            .unwrap_err();
        assert!(matches!(error, SkillError::ManifestTooLarge { .. }));
        assert_eq!(registry, before);
    }
}

// Python: tests/skills/test_local_registration.py::test_add_drops_the_replaced_unavailable_reason_before_checking_the_bound
#[test]
fn test_add_drops_the_replaced_unavailable_reason_before_checking_the_bound() {
    let files = TempDir::new();
    let registry = SkillRegistry::default();
    registry.mark_unavailable("stored", "").unwrap();
    let size = registry.to_prompt_xml().chars().count();
    registry
        .mark_unavailable("stored", &"é".repeat(65_536 - size))
        .unwrap();
    assert_eq!(registry.to_prompt_xml().chars().count(), 65_536);
    let stored = skill("stored", files.path().to_owned(), "Stored");
    registry.add(stored.clone()).unwrap();
    assert_eq!(
        registry.get_skill("stored").unwrap(),
        Some(roomkit_ai::LoadedSkill::Local(stored.clone()))
    );
    assert!(registry.unavailable_skills().is_empty());
}

// Python: tests/test_mcp_skill_registry.py::TestOrigins::test_a_host_skill_cannot_take_a_served_name
#[test]
fn test_a_host_skill_cannot_take_a_served_name() {
    let files = TempDir::new();
    let registry = SkillRegistry::default();
    let error = registry
        .add(skill("server:reports", files.path().to_owned(), "Stored"))
        .unwrap_err();
    assert!(matches!(error, SkillError::Validation { .. }));
    assert!(error.to_string().contains("holds ':'"));
    assert!(!registry.has_entries());
}

// Python: tests/test_skill_tool_gating.py::test_a_skill_added_with_a_bracketed_gate_is_refused
#[test]
fn test_a_skill_added_with_a_bracketed_gate_is_refused() {
    let files = TempDir::new();
    for field in ["allowed_tools", "requires"] {
        let registry = SkillRegistry::default();
        let mut stored = skill("stored", files.path().to_owned(), "Stored");
        if field == "allowed_tools" {
            stored.metadata.allowed_tools = Some(SkillToolNames::Text("calendar_[ab]".into()));
        } else {
            stored
                .metadata
                .extra_metadata
                .insert("requires".into(), serde_json::json!("calendar_[ab]"));
        }
        let error = registry.add(stored).unwrap_err();
        assert!(matches!(error, SkillError::Validation { .. }));
        assert!(error.to_string().contains("holds a bracket"));
        assert!(!registry.has_entries());
    }
}

// Python: tests/test_skill_registry_copy.py::test_a_skill_built_in_memory_is_registered_and_found
#[test]
fn test_a_skill_built_in_memory_is_registered_and_found() {
    let files = TempDir::new();
    let path = files.file("reports/scripts/run.py", "Body");
    let stored = skill("reports", files.path().join("reports"), "Reports");
    let registry = SkillRegistry::default();
    registry.add(stored.clone()).unwrap();
    assert_eq!(registry.skill_names(), ["reports"]);
    let found = registry.get_skill("reports").unwrap().unwrap();
    assert_eq!(found, roomkit_ai::LoadedSkill::Local(stored));
    assert_eq!(found.resolve_script("run.py").unwrap(), path);
}
