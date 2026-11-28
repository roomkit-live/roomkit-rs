//! The run's auditors (README, "Setup" and "Scripted agents"): the
//! implementation's JSONL tool and session auditors, each over a file of
//! its own, the runner's own tool auditor, and a scripted agent's tool
//! handler wrapped to record to one of them.
//!
//! `audit_files` sets how the files stand before the run (RFC §15.8.3):
//! under directories not made yet, made unwritable, or holding lines of an
//! earlier run.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use roomkit::orchestration::{
    JsonlSessionAuditor, JsonlToolAuditor, SessionAuditEntry, SessionAuditor, ToolAuditEntry,
    ToolAuditor,
};
use roomkit_ai::orchestration::audit_tool_handler;
use roomkit_ai::{
    AiChannelOptions, Tool, ToolEntry, ToolError, ToolHandler, compose_tool_handlers,
};
use serde::Serialize;
use serde_json::Value;

use crate::calls::{Called, Unreadable, field};
use crate::run::Run;

/// Each auditor's expectation and the name of its file.
const FILES: [(&str, &str); 2] = [
    ("tool_audit", "tools.jsonl"),
    ("session_audit", "session.jsonl"),
];

/// A `ToolAuditor` of the runner's own, as an integrator's backend implements
/// the interface (README, `own_auditor`): it keeps each entry it is handed.
#[derive(Default)]
pub(crate) struct OwnToolAuditor {
    entries: Mutex<Vec<ToolAuditEntry>>,
}

impl ToolAuditor for OwnToolAuditor {
    fn record(&self, entry: ToolAuditEntry) {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(entry);
    }

    fn summary(&self) -> String {
        format!("{} tool calls", self.entries().len())
    }

    fn entries(&self) -> Vec<ToolAuditEntry> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// The run's tool auditor and session auditor, each made at its first use,
/// their files removed with the run, and its own tool auditor.
#[derive(Default)]
pub(crate) struct Auditors {
    directory: OnceLock<PathBuf>,
    files: OnceLock<Value>,
    tool: OnceLock<Arc<JsonlToolAuditor>>,
    session: OnceLock<Arc<JsonlSessionAuditor>>,
    own: Arc<OwnToolAuditor>,
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
        if self.file_setting("nested") == Some(&Value::Bool(true)) {
            // Directories of its own the auditor makes itself (§15.8.3).
            let stem = name.split_once('.').map_or(name, |(stem, _)| stem);
            return directory
                .join(format!("missing-{stem}"))
                .join("one")
                .join(name);
        }
        directory.join(name)
    }

    /// A member of the setup's `audit_files`.
    fn file_setting(&self, member: &str) -> Option<&Value> {
        self.files.get().and_then(|files| files.get(member))
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

    /// The setup's `audit_files`, then its `session_auditor`: made, and
    /// attached to the kit when it says so.
    pub(crate) fn set_up(
        &self,
        spec: &Value,
        files: &Value,
        kit: &crate::Kit,
    ) -> Result<(), String> {
        if files.is_object() {
            self.set_up_files(files)?;
        }
        if spec["attached"] == true {
            self.session().attach(kit).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Each file as `audit_files` says it stands, then both auditors made.
    fn set_up_files(&self, files: &Value) -> Result<(), String> {
        drop(self.files.set(files.clone()));
        for (kind, name) in FILES {
            let path = self.path(name);
            if files["unwritable"] == true {
                std::fs::create_dir_all(&path).map_err(|e| e.to_string())?;
            }
            if let Some(lines) = files["lines"][kind].as_array() {
                let parent = path.parent().ok_or("an audit file has no directory")?;
                std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
                let text: String = lines.iter().map(|line| line.to_string() + "\n").collect();
                std::fs::write(&path, text).map_err(|e| e.to_string())?;
            }
        }
        drop((self.tool(), self.session()));
        Ok(())
    }

    /// What the runner's own tool auditor was handed (README, `own_audit`).
    pub(crate) fn own_entries(&self) -> Vec<Value> {
        self.own
            .entries()
            .iter()
            .map(|entry| serde_json::to_value(entry).unwrap())
            .collect()
    }

    /// The tool auditor the agent's `audit` names.
    fn named(&self, audit: &str) -> Option<Arc<dyn ToolAuditor>> {
        match audit {
            "tool_auditor" => Some(self.tool()),
            "session_auditor" => Some(self.session().tool_auditor()),
            "own_auditor" => Some(Arc::<OwnToolAuditor>::clone(&self.own)),
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
        if self.file_setting("unwritable") == Some(&Value::Bool(true)) {
            return entries;
        }
        // Both sides read through one parser: serde_json's default float
        // parsing may miss a written float by its last bit.
        let reread = |text: &str| serde_json::from_str::<Value>(text).unwrap();
        let earlier = self
            .file_setting("lines")
            .and_then(|lines| lines[label].as_array())
            .cloned()
            .unwrap_or_default();
        let held: Vec<Value> = earlier
            .iter()
            .chain(&entries)
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

/// The application records an entry of its own on the run's session
/// auditor (README, `session_audit.record`).
pub(crate) fn record(run: &Arc<Run>, spec: &Value) -> Result<Called, Unreadable> {
    let entry: SessionAuditEntry = field(spec, "entry")?;
    run.auditors.session().record(entry);
    Ok(Ok(Value::Null))
}
