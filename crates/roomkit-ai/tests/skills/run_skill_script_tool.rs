//! The standalone tool an application can hand to the ordinary tools API.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::model::Metadata;
use roomkit_ai::{
    DiscoverOptions, RunSkillScriptTool, ScriptExecutor, ScriptResult, Skill, SkillRegistry, Tool,
    ToolEntry, ToolError, ToolResult, extract_tools,
};
use serde_json::{Value, json};

use super::support::TempDir;

#[derive(Default)]
struct Executor(AtomicUsize);

impl ScriptExecutor for Executor {
    fn execute<'a>(
        &'a self,
        skill: &'a Skill,
        script: &'a str,
        arguments: Option<&'a Metadata>,
    ) -> Pin<Box<dyn Future<Output = Result<ScriptResult, ToolError>> + Send + 'a>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        assert_eq!(skill.metadata.name, "reports");
        assert_eq!(script, "build.py");
        assert_eq!(arguments.unwrap()["q"], "1");
        let result = ScriptResult {
            stdout: "OK".into(),
            ..ScriptResult::new(0)
        };
        Box::pin(std::future::ready(Ok(result)))
    }
}

fn args(skill: &str, script: &str) -> Metadata {
    serde_json::from_value(json!({"skill_name": skill, "script_name": script})).unwrap()
}

#[test]
fn standalone_definitions_own_their_nested_schema() {
    let skills = Arc::new(SkillRegistry::default());
    let executor = Arc::new(Executor::default());
    let first = RunSkillScriptTool::new(Arc::clone(&skills), Arc::<Executor>::clone(&executor));
    let second = RunSkillScriptTool::new(skills, executor);
    let mut definition = first.definition();
    definition.parameters["properties"]["skill_name"]["description"] = json!("Custom wording");
    definition.parameters["required"] = json!([]);
    for definition in [first.definition(), second.definition()] {
        assert_eq!(
            definition.parameters["properties"]["skill_name"]["description"],
            "Name of the skill."
        );
        assert_eq!(
            definition.parameters["required"],
            json!(["skill_name", "script_name"])
        );
    }
}

#[tokio::test]
async fn standalone_tool_serves_the_registry_supplied_by_the_host() {
    let skills = Arc::new(SkillRegistry::default());
    let executor = Arc::new(Executor::default());
    let tool = Arc::new(RunSkillScriptTool::new(
        Arc::clone(&skills),
        Arc::<Executor>::clone(&executor),
    ));
    let (definitions, handler) = extract_tools(vec![ToolEntry::Tool(
        Arc::<RunSkillScriptTool>::clone(&tool),
    )])
    .unwrap();
    assert_eq!(definitions.len(), 1);
    let definition = &definitions[0];
    assert_eq!(definition.name, RunSkillScriptTool::NAME);
    assert_eq!(definition.name, "run_skill_script");
    assert_eq!(
        definition.parameters["required"],
        json!(["skill_name", "script_name"])
    );

    let directory = TempDir::new();
    directory.skill("available", "reports", "Build reports.");
    directory.file("available/reports/scripts/build.py", "");
    skills
        .discover(
            &[directory.path().join("available")],
            DiscoverOptions::default(),
        )
        .unwrap();
    let mut arguments = args("reports", "build.py");
    arguments.insert("arguments".into(), json!({"q":"1"}));
    let result = handler
        .unwrap()
        .call(definition.name.clone(), arguments)
        .await
        .unwrap();
    let ToolResult::Text(result) = result else {
        panic!("expected the script result as JSON text")
    };
    assert_eq!(
        serde_json::from_str::<Value>(&result).unwrap()["stdout"],
        "OK"
    );

    let unknown = tool
        .handler(definition.name.clone(), args("nowhere", "build.py"))
        .await;
    assert!(matches!(unknown, Err(ToolError::Refused(reason)) if reason.contains("nowhere")));
    let missing = tool
        .handler(definition.name.clone(), args("reports", "absent"))
        .await
        .unwrap();
    let ToolResult::Text(error) = missing else {
        panic!("expected the checked path error text")
    };
    assert!(serde_json::from_str::<Value>(&error).unwrap()["error"].is_string());
    assert_eq!(executor.0.load(Ordering::SeqCst), 1);
}
