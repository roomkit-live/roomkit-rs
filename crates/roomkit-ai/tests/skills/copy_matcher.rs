//! Registry copies retain the host's matcher and selected marks (RFC §24.3).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit_ai::{CopyOptions, RequiresMatch, SkillRegistry, SkillRegistryOptions, ToolError};

use super::support::{TempDir, skill_md};

// Python: tests/test_skill_registry_copy.py::test_a_copy_keeps_the_marks_unless_asked_not_to.
#[test]
fn a_copy_without_marks_never_restores_an_unavailable_skill() {
    let tree = TempDir::new();
    let source = SkillRegistry::default();
    for name in ["reports", "invoices", "legacy"] {
        let path = tree.skill_with(
            "skills",
            name,
            &skill_md(name, "d", "allowed_tools: use_tool\n"),
        );
        source.register(path).unwrap();
    }
    source.mark_unlisted("invoices");
    source.mark_unavailable("legacy", "not here").unwrap();
    let marked = source.copy(None, CopyOptions::default()).unwrap();
    assert_eq!(marked.listed_names(), ["reports"]);
    assert_eq!(
        marked.get_unavailable_reason("legacy").as_deref(),
        Some("not here")
    );
    assert_eq!(
        marked.closed_tool_names(&HashSet::new()),
        HashSet::from(["use_tool".into()])
    );
    for names in [None, Some(["invoices".into(), "legacy".into()].as_slice())] {
        let plain = source.copy(names, CopyOptions { marks: false }).unwrap();
        assert!(plain.get_skill("legacy").unwrap().is_none());
        assert!(plain.get_unavailable_reason("legacy").as_deref().is_none());
        assert_eq!(plain.unavailable_skills().len(), 0);
        assert_eq!(plain.closed_tool_names(&HashSet::new()).len(), 0);
        assert_eq!(plain.listed_names(), plain.skill_names());
        assert!(plain.get_skill("invoices").unwrap().is_some());
    }
    assert_eq!(
        source.get_unavailable_reason("legacy").as_deref(),
        Some("not here")
    );
    assert_eq!(source.listed_names(), ["reports"]);
}

// Python: tests/test_skill_requires_parity.py::test_a_copy_keeps_the_host_matcher.
#[test]
fn copies_and_clones_keep_the_same_host_callback_and_state() {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    let matcher = RequiresMatch::new(move |required, offered| {
        counter.fetch_add(1, Ordering::SeqCst);
        Ok(offered
            .iter()
            .any(|name| name.starts_with(&format!("{required}_"))))
    });
    let source = SkillRegistry::new(SkillRegistryOptions {
        requires_match: matcher.clone(),
    });
    let names = HashSet::from(["boards_get_card".to_owned()]);
    for copy in [
        source.clone(),
        source.copy(None, CopyOptions::default()).unwrap(),
        source.copy(None, CopyOptions { marks: false }).unwrap(),
    ] {
        assert_eq!(copy.requires_match, matcher);
        assert!(copy.requires_match.call("boards", &names).unwrap());
        assert_eq!(copy, source);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_ne!(matcher, RequiresMatch::new(|_, _| Ok(true)));
    assert_ne!(matcher, RequiresMatch::default());
    assert_eq!(RequiresMatch::default(), RequiresMatch::default());
}

// Python: skills/models.py::serves_exactly and RequiresMatch (a callable can raise).
#[test]
fn the_default_is_exact_and_a_callback_error_is_preserved() {
    let matcher = RequiresMatch::default();
    let offered = HashSet::from(["boards_get_card".to_owned()]);
    assert!(matcher.call("boards_get_card", &offered).unwrap());
    assert!(!matcher.call("boards", &offered).unwrap());
    assert!(!matcher.call("boards_*", &offered).unwrap());
    let failing = RequiresMatch::new(|_, _| Err(ToolError::Failed("matcher failed".into())));
    assert!(
        matches!(failing.call("boards", &offered), Err(ToolError::Failed(reason)) if reason == "matcher failed")
    );
}

// Python: tests/test_skill_requires_parity.py::test_the_registry_keeps_an_unavailable_skill_s_gates;
// test_an_activated_skill_opens_a_gate_an_unavailable_one_shares.
#[test]
fn public_gate_views_distinguish_identical_patterns_from_overlapping_ones() {
    let tree = TempDir::new();
    let registry = SkillRegistry::default();
    for (name, pattern) in [
        ("closed", "calendar_*"),
        ("same", "calendar_*"),
        ("overlap", "calendar_read"),
    ] {
        let path = tree.skill_with(
            "skills",
            name,
            &skill_md(name, "d", &format!("allowed_tools: {pattern}\n")),
        );
        registry.register(path).unwrap();
    }
    registry.mark_unavailable("closed", "not here").unwrap();
    let empty = HashSet::new();
    assert_eq!(
        registry.closed_tool_names(&empty),
        HashSet::from(["calendar_*".into()])
    );
    assert_eq!(registry.unopenable_tool_names().len(), 0);
    assert_eq!(
        registry.gated_tool_names(&empty),
        HashSet::from(["calendar_*".into(), "calendar_read".into()])
    );
    assert_eq!(
        registry.closed_tool_names(&HashSet::from(["overlap".into()])),
        registry.closed_tool_names(&empty)
    );
    let active = HashSet::from(["same".into()]);
    assert_eq!(registry.closed_tool_names(&active).len(), 0);
    assert_eq!(
        registry.gated_tool_names(&active),
        HashSet::from(["calendar_read".into()])
    );
    registry.mark_unavailable("same", "also gone").unwrap();
    assert_eq!(
        registry.unopenable_tool_names(),
        HashSet::from(["calendar_*".into()])
    );
}

// Python: tests/test_skill_requires_parity.py::test_the_registry_keeps_an_unavailable_skill_s_gates.
#[test]
fn all_unavailable_gates_survive_a_copy_unless_marks_are_dropped() {
    let tree = TempDir::new();
    let registry = SkillRegistry::default();
    let path = tree.skill_with(
        "skills",
        "cal",
        &skill_md("cal", "d", "allowed_tools: gated_cal\n"),
    );
    registry.register(path).unwrap();
    registry
        .mark_unavailable("cal", "not granted here")
        .unwrap();
    let empty = HashSet::new();
    let expected = HashSet::from(["gated_cal".into()]);
    assert_eq!(registry.gated_tool_names(&empty), expected);
    assert_eq!(
        registry
            .copy(None, CopyOptions::default())
            .unwrap()
            .gated_tool_names(&empty),
        expected
    );
    assert!(
        registry
            .copy(None, CopyOptions { marks: false })
            .unwrap()
            .gated_tool_names(&empty)
            .is_empty()
    );
}
