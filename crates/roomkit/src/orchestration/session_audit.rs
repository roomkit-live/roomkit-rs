//! Full session auditing (RFC §15.8.2): Python's
//! `roomkit.orchestration.session_audit`. A [`SessionAuditor`] records
//! the conversation's events in one timeline, a tool call among them
//! through its tool auditor bridge, and attaches to a kit to record the
//! sessions it starts; [`JsonlSessionAuditor`] writes them to a JSONL file.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use roomkit_core::ops::{Environment, SystemEnvironment};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::session_transcript::transcript;
use super::tool_audit::{ToolAuditEntry, ToolAuditor, append_line, create_parent, locked};
use crate::model::{Metadata, RoomContext};
use crate::store::ConversationStore;
use crate::{HookExecution, HookOptions, HookTrigger, RoomKit, RoomKitError, SessionStartedEvent};

/// The metadata keys a tool call's session entry carries of its tool entry.
const TOOL_KEYS: [&str; 4] = ["tool_name", "arguments", "status", "agent_id"];

/// A single event of the session's timeline (RFC §15.8.2): Python's
/// `SessionAuditEntry`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SessionAuditEntry {
    /// When it happened: an RFC 3339 time in UTC.
    pub ts: String,
    /// `speech`, `tool_call`, `vision`, `barge_in`, `session` or `error`.
    pub r#type: String,
    /// Who produced it: `user`, `assistant` or `system`.
    pub role: Option<String>,
    /// Its text: a transcription, a tool's result, a description.
    pub content: String,
    /// How long it took, for a timed event (a tool call).
    pub duration_ms: Option<f64>,
    /// Extra data: a tool's name and arguments, a session's channel.
    #[serde(default)]
    pub metadata: Metadata,
}

impl SessionAuditEntry {
    /// The event of `type` saying `content` at `ts`, no role, duration nor
    /// metadata, as Python's defaults.
    pub fn new(
        ts: impl Into<String>,
        r#type: impl Into<String>,
        content: impl Into<String>,
    ) -> Self {
        Self {
            ts: ts.into(),
            r#type: r#type.into(),
            role: None,
            content: content.into(),
            duration_ms: None,
            metadata: Metadata::new(),
        }
    }
}

/// What records a session's timeline (RFC §15.8.2): Python's
/// `SessionAuditor`. It is shared by the hooks and the tool calls that
/// record to it: an implementation keeps its entries behind a lock.
pub trait SessionAuditor: Send + Sync + 'static {
    /// Records `entry`.
    fn record(&self, entry: SessionAuditEntry);

    /// The conversation's transcript, for a person to read.
    fn summary(&self) -> String;

    /// Every entry recorded, in order: a copy.
    fn entries(&self) -> Vec<SessionAuditEntry>;

    /// Logs the summary at INFO, as Python's module logger does.
    fn print_summary(&self) {
        tracing::info!("{}", self.summary());
    }

    /// Records the tool call `entry` as a `tool_call` event of the
    /// assistant, its result the content and its tool, arguments, status
    /// and agent in the metadata, the entry's own after them.
    fn record_tool(&self, entry: ToolAuditEntry) {
        self.record(tool_call_entry(entry));
    }

    /// A [`ToolAuditor`] recording into this session's timeline, for
    /// `audit_tool_handler`: Python's `tool_auditor` property.
    fn tool_auditor(self: Arc<Self>) -> Arc<dyn ToolAuditor> {
        Arc::new(ToolAuditorBridge(self))
    }

    /// Registers on `kit` the hooks recording its conversation's events:
    /// Python's `attach(kit)`. `ON_SESSION_STARTED` records each session
    /// the kit starts, its channel and participant in the metadata; the
    /// speech, interruption and vision hooks come with the voice channels.
    /// A tool call is recorded through [`tool_auditor`](Self::tool_auditor).
    ///
    /// # Errors
    ///
    /// Returns [`RoomKitError::KitClosed`] once the kit's close started, as
    /// [`RoomKit::hook`].
    fn attach<S: ConversationStore>(self: Arc<Self>, kit: &RoomKit<S>) -> Result<(), RoomKitError>
    where
        Self: Sized,
    {
        let name = format!("session_audit_session_{:x}", Arc::as_ptr(&self).addr());
        kit.hook(
            HookTrigger::OnSessionStarted,
            move |session: SessionStartedEvent, _context: RoomContext| {
                self.record(session_entry(&session));
                async {}
            },
            HookOptions {
                execution: HookExecution::Async,
                name,
                ..HookOptions::default()
            },
        )
    }
}

/// An auditor writing each entry to a JSONL file and keeping it in memory:
/// Python's `JSONLSessionAuditor(path)`.
#[derive(Debug)]
pub struct JsonlSessionAuditor {
    path: PathBuf,
    entries: Mutex<Vec<SessionAuditEntry>>,
}

impl JsonlSessionAuditor {
    /// The auditor of the file `path`, its parent directories created, as
    /// Python's `JSONLSessionAuditor(path)` makes them.
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

impl SessionAuditor for JsonlSessionAuditor {
    /// Keeps `entry` and appends it to the file, one JSON line; a write
    /// that fails is logged and the entry kept, as Python's.
    fn record(&self, entry: SessionAuditEntry) {
        let mut entries = locked(&self.entries);
        append_line(&self.path, &entry, "session entry");
        entries.push(entry);
    }

    fn summary(&self) -> String {
        let entries = self.entries();
        if entries.is_empty() {
            return "\nNo session events recorded.".to_owned();
        }
        transcript(
            &format!("Session Audit ({})", self.path.display()),
            &entries,
        )
    }

    fn entries(&self) -> Vec<SessionAuditEntry> {
        locked(&self.entries).clone()
    }
}

/// The tool auditor feeding a session auditor: Python's
/// `_SessionToolAuditorBridge`.
struct ToolAuditorBridge<A: ?Sized>(Arc<A>);

impl<A: SessionAuditor + ?Sized> ToolAuditor for ToolAuditorBridge<A> {
    fn record(&self, entry: ToolAuditEntry) {
        self.0.record_tool(entry);
    }

    fn summary(&self) -> String {
        self.0.summary()
    }

    /// The session's tool calls read back as tool entries.
    fn entries(&self) -> Vec<ToolAuditEntry> {
        self.0
            .entries()
            .into_iter()
            .filter(|entry| entry.r#type == "tool_call")
            .map(|entry| tool_entry(&entry))
            .collect()
    }
}

/// The session entry of the tool call `entry`: Python's `record_tool`.
fn tool_call_entry(entry: ToolAuditEntry) -> SessionAuditEntry {
    let mut metadata = Metadata::new();
    metadata.insert("tool_name".to_owned(), Value::String(entry.tool_name));
    metadata.insert("arguments".to_owned(), Value::Object(entry.arguments));
    metadata.insert("status".to_owned(), Value::String(entry.status));
    metadata.insert("agent_id".to_owned(), Value::String(entry.agent_id));
    metadata.extend(entry.metadata);
    SessionAuditEntry {
        role: Some("assistant".to_owned()),
        duration_ms: Some(entry.duration_ms),
        metadata,
        ..SessionAuditEntry::new(entry.ts, "tool_call", entry.result)
    }
}

/// The tool entry a session's `tool_call` entry reads back as: Python's
/// bridge `entries`.
fn tool_entry(entry: &SessionAuditEntry) -> ToolAuditEntry {
    let text = |key: &str, default: &str| {
        entry
            .metadata
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or(default)
            .to_owned()
    };
    let arguments = entry
        .metadata
        .get("arguments")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let metadata = entry
        .metadata
        .iter()
        .filter(|(key, _)| !TOOL_KEYS.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    ToolAuditEntry {
        metadata,
        ..ToolAuditEntry::new(
            entry.ts.clone(),
            text("agent_id", ""),
            text("tool_name", ""),
            arguments,
            entry.content.clone(),
            text("status", "ok"),
            entry.duration_ms.unwrap_or(0.0),
        )
    }
}

/// The entry of a session the kit started: Python's `ON_SESSION_STARTED`
/// hook of `attach`.
fn session_entry(session: &SessionStartedEvent) -> SessionAuditEntry {
    let metadata = json!({
        "channel_id": session.channel_id.as_str(),
        "participant_id": session.participant_id,
    });
    SessionAuditEntry {
        role: Some("system".to_owned()),
        metadata: metadata.as_object().cloned().unwrap_or_default(),
        ..SessionAuditEntry::new(
            SystemEnvironment.now().to_iso_offset(),
            "session",
            "Session started",
        )
    }
}
