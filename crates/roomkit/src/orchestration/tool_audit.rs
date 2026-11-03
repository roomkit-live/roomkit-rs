//! Tool execution auditing (RFC §15.8.1): Python's
//! `roomkit.orchestration.tool_audit`. A [`ToolAuditEntry`] records one
//! tool call, its input, output, timing and status; a [`ToolAuditor`]
//! keeps them, the JSONL file's and the log's built in.
//! `roomkit_ai::orchestration::audit_tool_handler` wraps a tool handler to
//! record each of its calls.

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use serde::{Deserialize, Serialize};
use tracing::Level;

use crate::model::Metadata;
use crate::tasks::json::json;

/// A single tool execution record (RFC §15.8.1): Python's `ToolAuditEntry`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolAuditEntry {
    /// When the call ended: an RFC 3339 time in UTC.
    pub ts: String,
    /// The agent that made the call.
    pub agent_id: String,
    /// The tool called.
    pub tool_name: String,
    /// The call's arguments.
    pub arguments: Metadata,
    /// What the call answered, as text, cut at 500 characters by the
    /// wrapper that records it.
    pub result: String,
    /// `ok`, `failed`, `error` or `cancelled`.
    pub status: String,
    /// How long the call took, in milliseconds.
    pub duration_ms: f64,
    /// Extra data.
    #[serde(default)]
    pub metadata: Metadata,
}

impl ToolAuditEntry {
    /// The record of a call of `tool_name` by `agent_id` with `arguments`,
    /// which answered `result` with `status` after `duration_ms`, ended at
    /// `ts`; no metadata, as Python's default.
    pub fn new(
        ts: impl Into<String>,
        agent_id: impl Into<String>,
        tool_name: impl Into<String>,
        arguments: Metadata,
        result: impl Into<String>,
        status: impl Into<String>,
        duration_ms: f64,
    ) -> Self {
        Self {
            ts: ts.into(),
            agent_id: agent_id.into(),
            tool_name: tool_name.into(),
            arguments,
            result: result.into(),
            status: status.into(),
            duration_ms,
            metadata: Metadata::new(),
        }
    }
}

/// What records tool calls and sums them up (RFC §15.8.1): Python's
/// `ToolAuditor`. It is shared by the calls it records, which may run at
/// once: an implementation keeps its entries behind a lock.
pub trait ToolAuditor: Send + Sync {
    /// Records `entry`.
    fn record(&self, entry: ToolAuditEntry);

    /// A summary of every entry, for a person to read.
    fn summary(&self) -> String;

    /// Every entry recorded, in order: a copy.
    fn entries(&self) -> Vec<ToolAuditEntry>;

    /// Logs the summary at INFO, as Python's module logger does.
    fn print_summary(&self) {
        tracing::info!("{}", self.summary());
    }
}

/// An auditor writing each entry to a JSONL file and keeping it in memory:
/// Python's `JSONLToolAuditor(path)`.
#[derive(Debug)]
pub struct JsonlToolAuditor {
    path: PathBuf,
    entries: Mutex<Vec<ToolAuditEntry>>,
}

impl JsonlToolAuditor {
    /// The auditor of the file `path`, its parent directories created, as
    /// Python's `JSONLToolAuditor(path)` makes them.
    ///
    /// # Errors
    ///
    /// Returns the error creating the directories, where Python raises
    /// `OSError`.
    pub fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        create_parent(&path)?;
        Ok(Self {
            path,
            entries: Mutex::new(Vec::new()),
        })
    }
}

impl ToolAuditor for JsonlToolAuditor {
    /// Keeps `entry` and appends it to the file, one JSON line; a write
    /// that fails is logged and the entry kept, as Python's.
    fn record(&self, entry: ToolAuditEntry) {
        let mut entries = locked(&self.entries);
        append_line(&self.path, &entry, "audit entry");
        entries.push(entry);
    }

    fn summary(&self) -> String {
        let entries = self.entries();
        if entries.is_empty() {
            return "\nNo tool calls recorded.".to_owned();
        }
        let mut lines = vec![
            format!("\nTool Audit ({})", self.path.display()),
            "=".repeat(60),
        ];
        let mut by_tool = BTreeMap::<&str, usize>::new();
        let mut total_ms = 0.0;
        for (at, entry) in entries.iter().enumerate() {
            lines.push(format!(
                "  {:2}. [{}] {}  ({:.0}ms)",
                at + 1,
                status_icon(&entry.status),
                entry.tool_name,
                entry.duration_ms
            ));
            lines.push(format!("      → {}", preview(&entry.result, 100)));
            *by_tool.entry(&entry.tool_name).or_default() += 1;
            total_ms += entry.duration_ms;
        }
        let tools: Vec<String> = by_tool
            .iter()
            .map(|(tool, count)| format!("{tool}({count})"))
            .collect();
        lines.push(format!(
            "\n  Total: {} calls, {total_ms:.0}ms",
            entries.len()
        ));
        lines.push(format!("  Tools: {}", tools.join(", ")));
        lines.join("\n")
    }

    fn entries(&self) -> Vec<ToolAuditEntry> {
        locked(&self.entries).clone()
    }
}

/// What a [`ConsoleToolAuditor`] is built with: Python's keywords of
/// `ConsoleToolAuditor(*, level=logging.INFO)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConsoleToolAuditorOptions {
    /// The level each entry is logged at; INFO by default.
    pub level: Level,
}

impl Default for ConsoleToolAuditorOptions {
    fn default() -> Self {
        Self { level: Level::INFO }
    }
}

/// An auditor logging each entry as it is recorded, and keeping it in
/// memory: Python's `ConsoleToolAuditor`.
#[derive(Debug)]
pub struct ConsoleToolAuditor {
    level: Level,
    entries: Mutex<Vec<ToolAuditEntry>>,
}

impl ConsoleToolAuditor {
    /// The auditor logging at `options.level`.
    pub fn new(options: ConsoleToolAuditorOptions) -> Self {
        Self {
            level: options.level,
            entries: Mutex::new(Vec::new()),
        }
    }
}

impl ToolAuditor for ConsoleToolAuditor {
    fn record(&self, entry: ToolAuditEntry) {
        let sign = if entry.status == "ok" { "+" } else { "x" };
        let line = format!(
            "[AUDIT] [{sign}] {}.{} → {} ({:.0}ms) {}",
            entry.agent_id,
            entry.tool_name,
            entry.status,
            entry.duration_ms.trunc(),
            first_chars(&entry.result, 80)
        );
        log_at(self.level, &line);
        locked(&self.entries).push(entry);
    }

    fn summary(&self) -> String {
        let entries = self.entries();
        if entries.is_empty() {
            return "\nNo tool calls recorded.".to_owned();
        }
        let mut lines = vec!["\nTool Audit (console)".to_owned(), "=".repeat(60)];
        for (at, entry) in entries.iter().enumerate() {
            lines.push(format!(
                "  {:2}. [{}] {}.{}  ({:.0}ms)",
                at + 1,
                status_icon(&entry.status),
                entry.agent_id,
                entry.tool_name,
                entry.duration_ms
            ));
        }
        let total_ms: f64 = entries.iter().map(|entry| entry.duration_ms).sum();
        lines.push(format!(
            "\n  Total: {} calls, {total_ms:.0}ms",
            entries.len()
        ));
        lines.join("\n")
    }

    fn entries(&self) -> Vec<ToolAuditEntry> {
        locked(&self.entries).clone()
    }
}

/// `line` logged at `level`, as Python's `logger.log(level, ...)`.
fn log_at(level: Level, line: &str) {
    match level {
        Level::TRACE => tracing::trace!("{line}"),
        Level::DEBUG => tracing::debug!("{line}"),
        Level::INFO => tracing::info!("{line}"),
        Level::WARN => tracing::warn!("{line}"),
        _ => tracing::error!("{line}"),
    }
}

/// `OK` for a call that went well, `FAIL` for any other.
fn status_icon(status: &str) -> &'static str {
    if status == "ok" { "OK" } else { "FAIL" }
}

/// The first `limit` characters of `text` on one line, `...` after them
/// when it is longer, as Python's summaries preview a text.
pub(super) fn preview(text: &str, limit: usize) -> String {
    let mut shown = first_chars(text, limit).replace('\n', " ");
    if text.chars().count() > limit {
        shown.push_str("...");
    }
    shown
}

/// The first `limit` characters of `text`: Python's `text[:limit]`.
pub(super) fn first_chars(text: &str, limit: usize) -> &str {
    text.char_indices()
        .nth(limit)
        .and_then(|(end, _)| text.get(..end))
        .unwrap_or(text)
}

/// Creates the parent directories of `path`, as Python's
/// `path.parent.mkdir(parents=True, exist_ok=True)`.
pub(super) fn create_parent(path: &Path) -> std::io::Result<()> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => std::fs::create_dir_all(parent),
        _ => Ok(()),
    }
}

/// Appends `entry` to the file `path` as one JSON line, as Python's
/// `json.dumps(entry.model_dump(), default=str)` writes it; a failure is
/// logged, naming what was written (`what`).
pub(super) fn append_line(path: &Path, entry: &impl Serialize, what: &str) {
    let written = serde_json::to_value(entry).map(|value| {
        let mut line = String::new();
        json(&mut line, &value, true);
        line.push('\n');
        line
    });
    let appended = written.map_err(std::io::Error::other).and_then(|line| {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?
            .write_all(line.as_bytes())
    });
    if let Err(error) = appended {
        tracing::error!("Failed to write {what} to {}: {error}", path.display());
    }
}

/// The entries behind `lock`, a poisoned lock's included: a record is
/// plain data, whole after any panic.
pub(super) fn locked<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(PoisonError::into_inner)
}
