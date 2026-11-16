//! The run's auditors (README, "Setup" and "Scripted agents"): the
//! implementation's JSONL tool and session auditors, each over a file of
//! its own, and a scripted agent's tool handler wrapped to record to one
//! of them.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use roomkit::orchestration::{JsonlSessionAuditor, JsonlToolAuditor, SessionAuditor, ToolAuditor};
use roomkit_ai::orchestration::audit_tool_handler;
use roomkit_ai::{
    AiChannelOptions, Tool, ToolEntry, ToolError, ToolHandler, compose_tool_handlers,
};
use serde::Serialize;
use serde_json::Value;

use crate::run::Run;

/// The run's tool auditor and session auditor, each made at its first use,
/// their files removed with the run.
#[derive(Default)]
pub(crate) struct Auditors {
    directory: OnceLock<PathBuf>,
    tool: OnceLock<Arc<JsonlToolAuditor>>,
    session: OnceLock<Arc<JsonlSessionAuditor>>,
}

impl Auditors {
    fn path(&self, name: &str) -> PathBuf {
        static RUNS: AtomicUsize = AtomicUsize::new(0);
        let directory = self.directory.get_or_init(|| {
            let run = RUNS.fetch_add(1, Ordering::Relaxed);
            let directory = std::env::temp_dir().join(format!(
                "roomkit-conformance-audit-{}-{run}",
                std::process::id()
            ));
            // A directory a killed run left under a reused pid holds none of
            // this run's lines.
            drop(std::fs::remove_dir_all(&directory));
            directory
        });
        directory.join(name)
    }

    fn tool(&self) -> Arc<JsonlToolAuditor> {
        let made = self
            .tool
            .get_or_init(|| Arc::new(JsonlToolAuditor::new(self.path("tools.jsonl")).unwrap()));
        Arc::clone(made)
    }

    fn session(&self) -> Arc<JsonlSessionAuditor> {
        let made = self.session.get_or_init(|| {
            Arc::new(JsonlSessionAuditor::new(self.path("session.jsonl")).unwrap())
        });
        Arc::clone(made)
    }

    /// The setup's `session_auditor`: made, and attached to the kit when it
    /// says so.
    pub(crate) fn set_up(&self, spec: &Value, kit: &crate::Kit) -> Result<(), String> {
        if spec["attached"] == true {
            self.session().attach(kit).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// The tool auditor the agent's `audit` names.
    fn named(&self, audit: &str) -> Option<Arc<dyn ToolAuditor>> {
        match audit {
            "tool_auditor" => Some(self.tool()),
            "session_auditor" => Some(self.session().tool_auditor()),
            _ => None,
        }
    }

    /// Each auditor's entries, and how its JSONL file differs from them.
    pub(crate) fn observe(&self, problems: &mut Vec<String>) -> (Vec<Value>, Vec<Value>) {
        let tools = self.tool.get().map(|auditor| auditor.entries());
        let sessions = self.session.get().map(|auditor| auditor.entries());
        (
            self.read(tools, "tools.jsonl", "tool_audit", problems),
            self.read(sessions, "session.jsonl", "session_audit", problems),
        )
    }

    fn read<T: Serialize>(
        &self,
        entries: Option<Vec<T>>,
        file: &str,
        label: &str,
        problems: &mut Vec<String>,
    ) -> Vec<Value> {
        let Some(entries) = entries else {
            return Vec::new();
        };
        let entries: Vec<Value> = entries
            .iter()
            .map(|e| serde_json::to_value(e).unwrap())
            .collect();
        // Both sides read through one parser: serde_json's default float
        // parsing may miss a written float by its last bit.
        let reread = |text: &str| serde_json::from_str::<Value>(text).unwrap();
        let held: Vec<Value> = entries
            .iter()
            .map(|entry| reread(&entry.to_string()))
            .collect();
        let written = std::fs::read_to_string(self.path(file)).unwrap_or_default();
        let lines: Vec<Value> = written.lines().map(reread).collect();
        if lines != held {
            problems.push(format!(
                "{label}: its JSONL file holds {lines:?}, its entries {held:?}"
            ));
        }
        entries
    }
}

impl Drop for Auditors {
    fn drop(&mut self) {
        if let Some(directory) = self.directory.get() {
            drop(std::fs::remove_dir_all(directory));
        }
    }
}

/// A scripted agent whose `audit` names an auditor: its scripted tools
/// declared as definitions and served by one tool handler, as Python's
/// runner serves them, that handler wrapped in `audit_tool_handler` under
/// the agent's id (README, "Scripted agents").
pub(crate) fn audit_tools(
    spec: &Value,
    run: &Arc<Run>,
    options: &mut AiChannelOptions,
) -> Result<(), String> {
    let Some(auditor) = spec["audit"]
        .as_str()
        .and_then(|audit| run.auditors.named(audit))
    else {
        return Ok(());
    };
    let scripted = serving_definitions(&mut options.tools);
    let handler = match options.tool_handler.take() {
        Some(prior) => compose_tool_handlers(vec![prior, scripted]).map_err(|e| e.to_string())?,
        None => scripted,
    };
    let agent_id = spec["id"].as_str().unwrap_or_default();
    options.tool_handler = Some(audit_tool_handler(handler, auditor, agent_id));
    Ok(())
}

/// Keep every definition and its traits, including MCP's, and serve each
/// scripted tool through the audited handler instead of bypassing it.
fn serving_definitions(entries: &mut [ToolEntry]) -> ToolHandler {
    let mut tools: HashMap<String, Arc<dyn Tool>> = HashMap::new();
    for entry in entries {
        if let ToolEntry::Tool(tool) = entry {
            let definition = tool.definition();
            tools.insert(definition.name.clone(), Arc::clone(tool));
            *entry = ToolEntry::Definition(definition);
        }
    }
    ToolHandler::new(move |name, arguments| {
        let tool = tools.get(&name).cloned();
        async move {
            let tool =
                tool.ok_or_else(|| ToolError::unserved(format!("{name} is not this handler's")))?;
            tool.handler(name, arguments).await
        }
    })
}
