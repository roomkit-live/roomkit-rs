//! Private helper failures retain their identity until the channel handles them.
use super::*;
use crate::SkillMetadata;

struct FailingExecutor;
impl ScriptExecutor for FailingExecutor {
    fn execute<'a>(
        &'a self,
        _: &'a Skill,
        _: &'a str,
        _: Option<&'a Metadata>,
    ) -> BoxFuture<'a, Result<ScriptResult, ToolError>> {
        Box::pin(std::future::ready(Err(ToolError::raised_as(
            "ConnectionError",
            "private executor failure",
        ))))
    }
}

struct Directory(std::path::PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _removed = std::fs::remove_dir_all(&self.0);
    }
}

// Python: tests/test_tool_failure_text.py::test_a_skill_script_that_raised_raises_on_to_the_channel
#[tokio::test]
async fn a_skill_script_that_raised_raises_on_to_the_channel() {
    let directory = Directory(
        std::env::temp_dir().join(format!("roomkit-script-helper-{}", std::process::id())),
    );
    std::fs::create_dir_all(directory.0.join("scripts")).unwrap();
    std::fs::write(directory.0.join("scripts/run"), "").unwrap();
    let registry = SkillRegistry::default();
    registry
        .add(Skill {
            metadata: SkillMetadata::new("s", ""),
            instructions: String::new(),
            path: directory.0.clone(),
        })
        .unwrap();
    let call = ToolCall::new(
        "s1",
        "run_skill_script",
        json!({"skill_name": "s", "script_name": "run"})
            .as_object()
            .unwrap()
            .clone(),
    );
    assert!(matches!(
        run(&registry, &FailingExecutor, &call).await,
        Err(ToolError::Raised { kind, message })
            if kind == "ConnectionError" && message == "private executor failure"
    ));
}
