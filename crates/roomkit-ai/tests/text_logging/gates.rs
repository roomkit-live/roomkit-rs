use super::capture::Lines;
use super::ordinary::Switch;
use super::support::{SERIAL, Turn, call};
use roomkit::model::HookResult;
use roomkit::model::hook::{BlockOptions, ModifyOptions};
use roomkit::{HookOptions, HookTrigger, ToolCallEvent};
use roomkit_ai::{AiGenerationEvent, SkillRegistry, ToolHandler, ToolResult};
use serde_json::json;
use std::path::PathBuf;

struct SkillDir(PathBuf);
impl SkillDir {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("roomkit-text-logging-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(path.join("synthetic")).unwrap();
        std::fs::write(path.join("synthetic/SKILL.md"), "---\nname: synthetic\ndescription: Synthetic skill\nallowed_tools: lookup\n---\nBody.\n").unwrap();
        Self(path)
    }
    fn registry(&self) -> SkillRegistry {
        let registry = SkillRegistry::default();
        registry.register(self.0.join("synthetic")).unwrap();
        registry
    }
}
impl Drop for SkillDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[tokio::test]
async fn gates_log_once_at_the_source_level_without_running_a_tool() {
    let _serial = SERIAL.lock().await;
    let _switch = Switch::set(true);
    let skill = SkillDir::new();
    for streaming in [false, true] {
        for (gate, expected) in [
            ("unknown", "Provider requested undeclared tool unknown"),
            ("cut", "Provider cut tool call lookup (c1): it does not run"),
            (
                "garbled",
                "Provider sent unreadable arguments for tool call lookup (c1): it does not run",
            ),
            (
                "withdrawn",
                "Provider called lookup, withdrawn for this turn",
            ),
            ("skill", "Tool lookup blocked by skill gating"),
            ("before", "Tool lookup denied by BEFORE_TOOL_USE hook"),
        ] {
            let lines = Lines::default();
            let _capture = tracing::subscriber::set_default(lines.clone());
            let mut requested = call(json!({}));
            if gate == "unknown" {
                requested.name = "unknown".into();
            }
            requested.partial = gate == "cut" || gate == "garbled";
            requested.garbled = gate == "garbled";
            let handler = ToolHandler::new(|_, _| async {
                panic!("a gated tool must not run");
                #[expect(unreachable_code, reason = "the handler must never run")]
                Ok(ToolResult::Text(String::new()))
            });
            let turn = Turn::with_skills(
                streaming,
                vec![requested],
                handler,
                None,
                (gate == "skill").then(|| skill.registry()),
            )
            .await;
            if gate == "withdrawn" {
                turn.kit
                    .hook(
                        HookTrigger::BeforeAiGeneration,
                        |mut event: AiGenerationEvent, _context| {
                            event.ai_context.tools.clear();
                            async { HookResult::modify(event, ModifyOptions::default()) }
                        },
                        HookOptions {
                            name: "withdraw".into(),
                            ..Default::default()
                        },
                    )
                    .unwrap();
            }
            if gate == "before" {
                turn.kit
                    .hook(
                        HookTrigger::BeforeToolUse,
                        |_event: ToolCallEvent, _context| async {
                            HookResult::block("synthetic-policy", BlockOptions::default())
                        },
                        HookOptions {
                            name: "refuse".into(),
                            ..Default::default()
                        },
                    )
                    .unwrap();
            }
            let parts = turn.run().await;
            assert!(parts[0].is_error);
            let records = lines.read();
            let matches: Vec<_> = records
                .iter()
                .filter(|line| line.contains(expected))
                .collect();
            assert_eq!(matches.len(), 1, "{gate}: {records:?}");
            assert!(matches[0].starts_with(if gate == "before" { "INFO" } else { "WARN" }));
            assert!(
                !records
                    .iter()
                    .any(|line| line.contains("a tool call did not serve"))
            );
        }
    }
}
