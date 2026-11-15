//! The complete escaped manifest stays bounded after every local operation.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::model::ToolCallOutcome;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{
    AiChannelOptions, AiTool, CopyOptions, SkillError, SkillRegistry, ToolResult, Tools,
};

use super::support::{Room, TempDir, skill_md, tool_results};

const LIMIT: usize = 65_536;

// Python: tests/skills/test_manifest_bound.py::test_a_refused_reason_update_preserves_order_and_closed_gates
#[test]
fn an_oversized_reason_cannot_change_the_manifest() {
    let mut registry = SkillRegistry::default();
    registry.mark_unavailable("held", "short").unwrap();
    let before = registry.to_prompt_xml();
    assert!(matches!(
        registry.mark_unavailable("held", &"&".repeat(14_000)),
        Err(SkillError::ManifestTooLarge { .. })
    ));
    assert_eq!(registry.to_prompt_xml(), before);
    assert!(registry.to_prompt_xml().chars().count() <= LIMIT);
}

// Python: tests/skills/test_manifest_bound.py::test_an_unavailable_reason_counts_escaped_characters_at_the_boundary
#[test]
fn an_unavailable_reason_counts_escaped_characters_at_the_boundary() {
    for prefix in ["", "<&\"'😀"] {
        let mut registry = SkillRegistry::default();
        let name = "missing<&\"";
        registry.mark_unavailable(name, prefix).unwrap();
        let reason = format!(
            "{prefix}{}",
            "é".repeat(LIMIT - registry.to_prompt_xml().chars().count())
        );
        registry.mark_unavailable(name, &reason).unwrap();
        let before = registry.clone();
        assert_eq!(registry.to_prompt_xml().chars().count(), LIMIT);
        assert!(registry.to_prompt_xml().len() > LIMIT);
        let error = registry
            .mark_unavailable(name, &format!("{reason}x"))
            .unwrap_err();
        assert!(matches!(
            error,
            SkillError::ManifestTooLarge {
                characters: 65_537,
                limit: LIMIT
            }
        ));
        assert_eq!(
            error.to_string(),
            "the skills' manifest would take 65537 characters, past 65536"
        );
        assert_eq!(registry, before);
        registry.mark_unavailable(name, "short").unwrap();
        assert_eq!(registry.get_unavailable_reason(name), Some("short"));
    }
}

// Python: tests/skills/test_manifest_bound.py::test_a_refused_mark_preserves_the_available_skill_and_its_gates
#[test]
fn a_refused_mark_preserves_the_available_skill_and_its_gates() {
    let tree = TempDir::new();
    let path = tree.skill_with(
        "skills",
        "cal",
        &skill_md("cal", "d", "allowed_tools: calendar_*\n"),
    );
    for unlisted in [false, true] {
        let mut registry = SkillRegistry::default();
        registry.register(&path).unwrap();
        if unlisted {
            registry.mark_unlisted("cal");
        }
        let before = registry.clone();
        assert!(matches!(
            registry.mark_unavailable("cal", &"x".repeat(LIMIT)),
            Err(SkillError::ManifestTooLarge { .. })
        ));
        assert_eq!(registry, before);
        assert!(registry.get_skill("cal").is_some());
        assert_eq!(
            registry.gated_tool_names(&HashSet::new()),
            HashSet::from(["calendar_*".into()])
        );
        assert!(registry.closed_tool_names(&HashSet::new()).is_empty());
    }
}

// Python: tests/skills/test_manifest_bound.py::test_a_refused_reason_update_preserves_order_and_closed_gates
#[test]
fn a_refused_reason_update_preserves_order_and_closed_gates() {
    let tree = TempDir::new();
    let path = tree.skill_with(
        "skills",
        "cal",
        &skill_md("cal", "d", "allowed_tools: calendar_*\n"),
    );
    let mut registry = SkillRegistry::default();
    registry.register(path).unwrap();
    registry.mark_unavailable("cal", "first").unwrap();
    registry.mark_unavailable("other", "second").unwrap();
    let before = registry.clone();
    let manifest = registry.to_prompt_xml();
    for name in ["cal", "new"] {
        assert!(matches!(
            registry.mark_unavailable(name, &"&".repeat(14_000)),
            Err(SkillError::ManifestTooLarge { .. })
        ));
        assert_eq!(registry, before);
        assert_eq!(registry.to_prompt_xml(), manifest);
        assert_eq!(
            registry.closed_tool_names(&HashSet::new()),
            HashSet::from(["calendar_*".into()])
        );
    }
}

// Python: tests/skills/test_manifest_bound.py::test_copy_cannot_relist_an_oversized_hidden_catalogue
#[test]
fn copy_cannot_relist_an_oversized_hidden_catalogue() {
    let tree = TempDir::new();
    let mut registry = SkillRegistry::default();
    for i in 0..70 {
        let name = format!("skill-{i:02}");
        let path = tree.skill_with("skills", &name, &skill_md(&name, &"d".repeat(1024), ""));
        registry.register(path).unwrap();
        registry.mark_unlisted(&name);
    }
    let before = registry.clone();
    assert!(matches!(
        registry.copy(None, CopyOptions { marks: false }),
        Err(SkillError::ManifestTooLarge { .. })
    ));
    assert_eq!(registry, before);
    assert_eq!(registry.to_prompt_xml(), "");
    assert_eq!(
        registry
            .copy(None, CopyOptions::default())
            .unwrap()
            .to_prompt_xml(),
        ""
    );
    let selected = registry
        .copy(Some(&["skill-00".into()]), CopyOptions { marks: false })
        .unwrap();
    assert_eq!(selected.listed_names(), ["skill-00"]);
    assert_eq!(
        selected.get_skill("skill-00"),
        registry.get_skill("skill-00")
    );
    assert!(selected.to_prompt_xml().chars().count() < LIMIT);
}

// Python: tests/skills/test_manifest_bound.py::test_a_rejected_mark_keeps_activation_and_tool_execution_working
#[tokio::test]
async fn a_rejected_mark_keeps_activation_and_tool_execution_working() {
    let tree = TempDir::new();
    let path = tree.skill_with(
        "skills",
        "cal",
        &skill_md("cal", "d", "allowed_tools: calendar\n"),
    );
    let mut registry = SkillRegistry::default();
    registry.register(path).unwrap();
    assert!(
        registry
            .mark_unavailable("cal", &"x".repeat(LIMIT))
            .is_err()
    );
    let executed = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&executed);
    let tools = Tools::new().tool(
        AiTool::new("calendar", "Run").unwrap(),
        move |_call, _ctx| {
            let counter = Arc::clone(&counter);
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(ToolResult::from("ran"))
            }
        },
    );
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("before", "calendar", "{}")]),
        ScriptedRound::calling("", [("activate", "activate_skill", r#"{"name":"cal"}"#)]),
        ScriptedRound::calling("", [("after", "calendar", "{}")]),
        ScriptedRound::answering("done"),
    ]);
    let room = Room::new(model.clone(), registry, |options| AiChannelOptions {
        tools: tools.into(),
        ..options
    })
    .await;
    room.says("Go").await;
    let outcomes: Vec<_> = room.ends().await.iter().map(|row| row.outcome).collect();
    assert_eq!(
        outcomes,
        [
            Some(ToolCallOutcome::Refused),
            Some(ToolCallOutcome::Served),
            Some(ToolCallOutcome::Served)
        ]
    );
    assert_eq!(executed.load(Ordering::SeqCst), 1);
    assert_eq!(tool_results(&model)[3][2], "ran");
}
