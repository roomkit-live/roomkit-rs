//! Python registry marks and views, through its public API (RFC §24.3).

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::model::ToolCallOutcome;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiChannelOptions, AiTool, SkillError, SkillRegistry, ToolResult, Tools};

use super::support::{Room, TempDir, skill_md, tool_results};

// Python: tests/test_skills.py::TestSkillRegistry::test_mark_unavailable_removes_from_available
// and test_register_clears_unavailable_mark.
#[test]
fn an_unavailable_skill_leaves_every_available_view_and_registration_restores_it() {
    let root = TempDir::new();
    let path = root.skill_with("skills", "calendar", &skill_md("calendar", "d", ""));
    let registry = SkillRegistry::default();
    registry.register(&path).unwrap();
    registry
        .mark_unavailable("calendar", "not granted")
        .unwrap();
    assert!(registry.get_skill("calendar").unwrap().is_none());
    assert!(registry.get_metadata("calendar").is_none());
    assert_eq!(registry.skill_names(), Vec::<String>::new());
    assert_eq!(
        registry.all_metadata(),
        Vec::<roomkit_ai::SkillMetadata>::new()
    );
    assert_eq!(registry.skill_count(), 0);
    assert!(registry.has_entries());
    assert_eq!(registry.tool_definitions().len(), 2);
    assert_eq!(
        registry.get_unavailable_reason("calendar").as_deref(),
        Some("not granted")
    );
    let mut snapshot = registry.unavailable_skills();
    snapshot.clear();
    assert_eq!(
        registry
            .unavailable_skills()
            .get("calendar")
            .map(String::as_str),
        Some("not granted")
    );
    registry.register(&path).unwrap();
    assert!(
        registry
            .get_unavailable_reason("calendar")
            .as_deref()
            .is_none()
    );
    assert!(registry.get_skill("calendar").unwrap().is_some());
    assert_eq!(registry.skill_names(), ["calendar"]);
}

// Python: tests/test_skills.py::TestSkillRegistry::test_to_prompt_xml_unavailable_block
// and test_to_prompt_xml_unavailable_only.
#[test]
fn an_unavailable_only_manifest_escapes_names_and_reasons_and_keeps_order() {
    let registry = SkillRegistry::default();
    registry
        .mark_unavailable("a<\"", "Needs <approval> & access")
        .unwrap();
    registry.mark_unavailable("b", "later").unwrap();
    registry
        .mark_unavailable("a<\"", "Updated <approval> & access")
        .unwrap();
    assert_eq!(
        registry.to_prompt_xml(),
        "<unavailable_skills>\n  <skill name=\"a&lt;&quot;\">\n    <reason>Updated &lt;approval&gt; &amp; access</reason>\n  </skill>\n  <skill name=\"b\">\n    <reason>later</reason>\n  </skill>\n</unavailable_skills>"
    );
    let root = TempDir::new();
    let path = root.skill_with("skills", "usable", &skill_md("usable", "d", ""));
    registry.register(path).unwrap();
    let manifest = registry.to_prompt_xml();
    assert!(manifest.starts_with("<available_skills>\n  <skill name=\"usable\">"));
    assert!(manifest.contains("</available_skills>\n<unavailable_skills>"));
}

// Python: tests/test_skills.py::TestSkillRegistry::test_mark_unlisted_hides_from_manifest_but_keeps_activation,
// test_mark_unlisted_unknown_name_is_ignored, test_all_unlisted_collapses_the_available_block,
// test_register_clears_unlisted_mark.
#[test]
fn unlisted_skills_keep_their_available_reads_and_regain_listing_on_registration() {
    let root = TempDir::new();
    let registry = SkillRegistry::default();
    let path = root.skill_with("skills", "quiet", &skill_md("quiet", "d", ""));
    registry.mark_unlisted("ghost");
    assert_eq!(registry.listed_names(), Vec::<String>::new());
    registry.register(&path).unwrap();
    registry.mark_unlisted("quiet");
    assert_eq!(registry.listed_names(), Vec::<String>::new());
    assert_eq!(registry.to_prompt_xml(), "");
    assert!(registry.get_skill("quiet").unwrap().is_some());
    assert!(registry.get_metadata("quiet").is_some());
    assert_eq!(registry.skill_names(), ["quiet"]);
    assert!(
        registry
            .get_unavailable_reason("quiet")
            .as_deref()
            .is_none()
    );
    registry.register(&path).unwrap();
    assert_eq!(registry.listed_names(), ["quiet"]);
}

// Python: tests/test_skill_registry_copy.py::test_a_copy_keeps_the_marks_unless_asked_not_to
// (default marks=true only; the explicit false option is a separate witness).
#[test]
fn a_subset_copy_preserves_selected_marks_without_mutating_its_source() {
    let root = TempDir::new();
    let source = SkillRegistry::default();
    for name in ["loud", "quiet", "gone"] {
        let path = root.skill_with("skills", name, &skill_md(name, "d", ""));
        source.register(path).unwrap();
    }
    source.mark_unlisted("quiet");
    source.mark_unavailable("gone", "not here").unwrap();
    let selected = ["quiet".to_owned(), "gone".to_owned()];
    let copied = source
        .copy(Some(&selected), roomkit_ai::CopyOptions::default())
        .unwrap();
    assert_eq!(copied.skill_names(), ["quiet"]);
    assert_eq!(copied.listed_names(), Vec::<String>::new());
    assert_eq!(
        copied.get_unavailable_reason("gone").as_deref(),
        Some("not here")
    );
    copied.mark_unavailable("quiet", "only in copy").unwrap();
    assert!(source.get_skill("quiet").unwrap().is_some());
    assert!(source.get_unavailable_reason("quiet").as_deref().is_none());
    assert_eq!(
        source
            .copy(None, roomkit_ai::CopyOptions::default())
            .unwrap(),
        source
    );
}

// Python: tests/test_skill_requires_parity.py::test_a_subset_copy_keeps_only_its_skills_closed_gates.
// Repeating the unavailable mark must retain the gates before either copy.
#[tokio::test]
async fn repeated_marks_and_subset_copies_keep_the_right_tools_closed() {
    let root = TempDir::new();
    let source = SkillRegistry::default();
    for name in ["a", "b"] {
        let path = root.skill_with(
            "skills",
            name,
            &skill_md(name, "d", &format!("allowed_tools: tool_{name}\n")),
        );
        source.register(path).unwrap();
        source.mark_unavailable(name, "first reason").unwrap();
        source.mark_unavailable(name, "updated reason").unwrap();
    }
    for (registry, b_runs) in [
        (
            source
                .copy(None, roomkit_ai::CopyOptions::default())
                .unwrap(),
            false,
        ),
        (
            source
                .copy(Some(&["a".to_owned()]), roomkit_ai::CopyOptions::default())
                .unwrap(),
            true,
        ),
    ] {
        let expected = if b_runs {
            HashSet::from(["tool_a".into()])
        } else {
            HashSet::from(["tool_a".into(), "tool_b".into()])
        };
        assert_eq!(registry.closed_tool_names(&HashSet::new()), expected);
        assert_closed_calls(registry, b_runs).await;
    }
}

async fn assert_closed_calls(registry: SkillRegistry, b_runs: bool) {
    let executed = Arc::new(AtomicUsize::new(0));
    let mut tools = Tools::new();
    for name in ["tool_a", "tool_b"] {
        let executed = Arc::clone(&executed);
        tools = tools.tool(AiTool::new(name, "Run").unwrap(), move |_call, _context| {
            let executed = Arc::clone(&executed);
            async move {
                executed.fetch_add(1, Ordering::SeqCst);
                Ok(ToolResult::from("ran"))
            }
        });
    }
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "tool_a", "{}"), ("c2", "tool_b", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let room = Room::new(model.clone(), registry, |options| AiChannelOptions {
        tools: tools.into(),
        ..options
    })
    .await;
    room.says("Go").await;
    let ends = room.ends().await;
    assert_eq!(
        ends.first().unwrap().outcome,
        Some(ToolCallOutcome::Refused)
    );
    assert_eq!(
        ends.get(1).unwrap().outcome,
        Some(if b_runs {
            ToolCallOutcome::Served
        } else {
            ToolCallOutcome::Refused
        })
    );
    assert_eq!(executed.load(Ordering::SeqCst), usize::from(b_runs));
    let requests = model.requests();
    let declared = &requests.first().unwrap().tools;
    assert!(!declared.iter().any(|tool| tool.name == "tool_a"));
    assert_eq!(declared.iter().any(|tool| tool.name == "tool_b"), b_runs);
}

// Python: tests/test_skills.py::TestSkillRegistry::test_a_manifest_past_its_bound_registers_nothing,
// with marks that a successful registration would clear.
#[test]
fn a_failed_registration_preserves_unavailable_and_unlisted_marks() {
    let root = TempDir::new();
    let path = root.skill_with(
        "skills",
        "held",
        &skill_md("held", "d", "allowed_tools: held_tool\n"),
    );
    for unavailable in [true, false] {
        root.file(
            "skills/held/SKILL.md",
            &skill_md("held", "d", "allowed_tools: held_tool\n"),
        );
        let registry = SkillRegistry::default();
        registry.register(&path).unwrap();
        registry.mark_unlisted("held");
        if unavailable {
            registry.mark_unavailable("held", "not here").unwrap();
        }
        registry
            .mark_unavailable("other", &"r".repeat(65_000))
            .unwrap();
        assert!(registry.to_prompt_xml().chars().count() < 65_536);
        let before = registry.clone();
        root.file(
            "skills/held/SKILL.md",
            &skill_md("held", &"d".repeat(1024), ""),
        );
        assert!(matches!(
            registry.register(&path),
            Err(SkillError::ManifestTooLarge { .. })
        ));
        assert_eq!(registry, before);
    }
}

// Python: tests/test_skills.py::TestSkillRegistry::test_register_clears_unavailable_mark;
// the removed reason is excluded from the prospective manifest bound.
#[test]
fn registration_removes_the_old_reason_before_checking_the_manifest_size() {
    let root = TempDir::new();
    let path = root.skill_with("skills", "held", &skill_md("held", "d", ""));
    let registry = SkillRegistry::default();
    registry.mark_unavailable("held", "").unwrap();
    let remaining = 65_536 - registry.to_prompt_xml().chars().count();
    registry
        .mark_unavailable("held", &"r".repeat(remaining))
        .unwrap();
    assert_eq!(registry.to_prompt_xml().chars().count(), 65_536);
    registry.register(path).unwrap();
    assert_eq!(registry.skill_names(), ["held"]);
    assert!(registry.get_unavailable_reason("held").as_deref().is_none());
    assert!(registry.to_prompt_xml().chars().count() < 65_536);
}

// Python: tests/test_skill_requires_parity.py::test_re_registering_an_unavailable_skill_reopens_its_gates_to_activation.
#[tokio::test]
async fn registration_makes_an_unavailable_gate_openable_again() {
    let tree = TempDir::new();
    let path = tree.skill_with(
        "skills",
        "a",
        &skill_md("a", "d", "allowed_tools: tool_a\n"),
    );
    let registry = SkillRegistry::default();
    registry.register(&path).unwrap();
    registry.mark_unavailable("a", "not here").unwrap();
    tree.file(
        "skills/a/SKILL.md",
        &skill_md("a", "d", "allowed_tools: tool_b\n"),
    );
    registry.register(path).unwrap();
    assert!(registry.closed_tool_names(&HashSet::new()).is_empty());
    assert!(
        registry
            .gated_tool_names(&HashSet::from(["a".into()]))
            .is_empty()
    );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "tool_a", "{}")]),
        ScriptedRound::calling("", [("c2", "tool_b", "{}")]),
        ScriptedRound::calling("", [("c3", "activate_skill", r#"{"name":"a"}"#)]),
        ScriptedRound::calling("", [("c4", "tool_b", "{}")]),
        ScriptedRound::answering("Done."),
    ]);
    let mut tools = Tools::new();
    for name in ["tool_a", "tool_b"] {
        tools = tools.tool(AiTool::new(name, "Run").unwrap(), |_call, _context| async {
            Ok(ToolResult::from("ran"))
        });
    }
    let room = Room::new(model.clone(), registry, |options| AiChannelOptions {
        tools: tools.into(),
        ..options
    })
    .await;
    room.says("Go").await;
    assert_eq!(tool_results(&model)[1][0], "ran");
    assert!(tool_results(&model)[2][1].contains("Activate the skill first using activate_skill"));
    let outcomes: Vec<_> = room.ends().await.iter().map(|row| row.outcome).collect();
    assert_eq!(
        outcomes,
        [
            Some(ToolCallOutcome::Served),
            Some(ToolCallOutcome::Refused),
            Some(ToolCallOutcome::Served),
            Some(ToolCallOutcome::Served),
        ]
    );
    assert_eq!(tool_results(&model)[4][3], "ran");
}
