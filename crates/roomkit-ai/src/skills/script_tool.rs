//! A host-supplied script tool using the same handler as an AI channel.

use std::fmt;
use std::sync::Arc;

use roomkit::model::Metadata;

use super::{ScriptExecutor, SkillRegistry, executor, texts};
use crate::{AiTool, Tool, ToolFuture};

/// Python's `RunSkillScriptTool(skills, executor)`. The handler uses the
/// supplied registry and executor when the tool is called (RFC §24.6).
pub struct RunSkillScriptTool {
    skills: Arc<SkillRegistry>,
    executor: Arc<dyn ScriptExecutor>,
}

impl RunSkillScriptTool {
    /// The name declared to the model, Python's `RunSkillScriptTool.name`.
    pub const NAME: &str = texts::RUN_SKILL_SCRIPT;

    /// Serve scripts from `skills` using the host's execution policy.
    pub fn new(skills: Arc<SkillRegistry>, executor: Arc<dyn ScriptExecutor>) -> Self {
        Self { skills, executor }
    }
}

impl fmt::Debug for RunSkillScriptTool {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunSkillScriptTool")
            .finish_non_exhaustive()
    }
}

impl Tool for RunSkillScriptTool {
    fn definition(&self) -> AiTool {
        executor::definition()
    }

    fn handler(&self, _name: String, arguments: Metadata) -> ToolFuture {
        let skills = Arc::clone(&self.skills);
        let executor = Arc::clone(&self.executor);
        Box::pin(async move { executor::run(&skills, executor.as_ref(), &arguments).await })
    }
}
