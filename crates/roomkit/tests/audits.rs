//! The tool and session auditors (RFC §15.8): Python's
//! `tests/orchestration/test_tool_audit.py` and `test_session_audit.py`,
//! the entries, their JSONL lines and the summaries, pinned to Python's
//! own.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use roomkit::orchestration::{
    ConsoleToolAuditor, ConsoleToolAuditorOptions, JsonlSessionAuditor, JsonlToolAuditor,
    SessionAuditEntry, SessionAuditor, ToolAuditEntry, ToolAuditor,
};
use roomkit::{HasHooksOptions, HookTrigger, RoomKit, RoomKitOptions};
use serde_json::json;

/// A directory of the test's own, removed with it.
struct Dir(PathBuf);

impl Dir {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "roomkit-audits-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        );
        let path = std::env::temp_dir().join(name);
        let _absent = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn join(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _gone = std::fs::remove_dir_all(&self.0);
    }
}

fn tool_entry(tool_name: &str, result: &str, status: &str, duration_ms: f64) -> ToolAuditEntry {
    let arguments = json!({ "q": "x" }).as_object().cloned().unwrap();
    ToolAuditEntry::new("t", "a", tool_name, arguments, result, status, duration_ms)
}

fn session_entry(ts: &str, r#type: &str, role: &str, content: &str) -> SessionAuditEntry {
    SessionAuditEntry {
        role: Some(role.to_owned()),
        ..SessionAuditEntry::new(ts, r#type, content)
    }
}

fn lines(path: &Path) -> Vec<String> {
    std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

// Python: tests/orchestration/test_tool_audit.py::test_jsonl_auditor_record_and_summary
#[test]
fn jsonl_auditor_record_and_summary() {
    let dir = Dir::new();
    let path = dir.join("audit.jsonl");
    let auditor = JsonlToolAuditor::new(&path).unwrap();
    auditor.record(tool_entry("search", "done", "ok", 10.0));
    let second = ToolAuditEntry {
        agent_id: "b".to_owned(),
        arguments: serde_json::Map::new(),
        ..tool_entry("lookup", &format!("é\n{}", "y".repeat(120)), "failed", 2.5)
    };
    auditor.record(second);

    assert_eq!(auditor.entries().len(), 2);
    // The line Python's `json.dumps(entry.model_dump(), default=str)` writes.
    assert_eq!(
        lines(&path)[0],
        r#"{"ts": "t", "agent_id": "a", "tool_name": "search", "arguments": {"q": "x"}, "result": "done", "status": "ok", "duration_ms": 10.0, "metadata": {}}"#
    );
    assert!(lines(&path)[1].starts_with(r#"{"ts": "t", "agent_id": "b", "tool_name": "lookup", "arguments": {}, "result": "\u00e9\nyyy"#));
    let expected = format!(
        "\nTool Audit ({})\n{}\n   1. [OK] search  (10ms)\n      → done\n   2. [FAIL] lookup  (2ms)\n      → é {}...\n\n  Total: 2 calls, 12ms\n  Tools: lookup(1), search(1)",
        path.display(),
        "=".repeat(60),
        "y".repeat(98)
    );
    assert_eq!(auditor.summary(), expected);
}

// Python: tests/orchestration/test_tool_audit.py::test_jsonl_auditor_empty_summary
#[test]
fn jsonl_auditor_empty_summary() {
    let dir = Dir::new();
    let auditor = JsonlToolAuditor::new(dir.join("audit.jsonl")).unwrap();
    assert_eq!(auditor.summary(), "\nNo tool calls recorded.");
}

/// No Python test: the console auditor's summary, pinned to Python's.
#[test]
fn a_console_auditor_keeps_and_sums_its_entries() {
    let auditor = ConsoleToolAuditor::new(ConsoleToolAuditorOptions::default());
    auditor.record(tool_entry("search", "done", "ok", 10.6));
    auditor.record(ToolAuditEntry {
        agent_id: "b".to_owned(),
        ..tool_entry("click", "no", "error", 3.0)
    });
    let expected = format!(
        "\nTool Audit (console)\n{}\n   1. [OK] a.search  (11ms)\n   2. [FAIL] b.click  (3ms)\n\n  Total: 2 calls, 14ms",
        "=".repeat(60)
    );
    assert_eq!(auditor.summary(), expected);
    assert_eq!(auditor.entries().len(), 2);
}

// Python: tests/orchestration/test_session_audit.py::test_jsonl_session_auditor_records_entries
#[test]
fn jsonl_session_auditor_records_entries() {
    let dir = Dir::new();
    let path = dir.join("session.jsonl");
    let auditor = JsonlSessionAuditor::new(&path).unwrap();
    auditor.record(session_entry(
        "2026-03-20T12:00:00+00:00",
        "speech",
        "user",
        "Hello",
    ));
    auditor.record(session_entry(
        "2026-03-20T12:00:01+00:00",
        "speech",
        "assistant",
        "Hi there!",
    ));

    let roles: Vec<_> = auditor
        .entries()
        .into_iter()
        .map(|entry| entry.role)
        .collect();
    assert_eq!(
        roles,
        [Some("user".to_owned()), Some("assistant".to_owned())]
    );
    assert_eq!(
        lines(&path)[0],
        r#"{"ts": "2026-03-20T12:00:00+00:00", "type": "speech", "role": "user", "content": "Hello", "duration_ms": null, "metadata": {}}"#
    );
    assert_eq!(lines(&path).len(), 2);
}

// Python: tests/orchestration/test_session_audit.py::test_jsonl_session_auditor_creates_parent_dirs
#[test]
fn jsonl_session_auditor_creates_parent_dirs() {
    let dir = Dir::new();
    let path = dir.join("nested/deep/session.jsonl");
    let auditor = JsonlSessionAuditor::new(&path).unwrap();
    auditor.record(session_entry("t", "session", "system", "started"));
    assert!(path.exists());
}

// Python: tests/orchestration/test_session_audit.py::test_record_tool_adds_to_timeline
#[test]
fn record_tool_adds_to_timeline() {
    let dir = Dir::new();
    let path = dir.join("s.jsonl");
    let auditor = JsonlSessionAuditor::new(&path).unwrap();
    auditor.record(session_entry(
        "2026-03-20T12:00:00+00:00",
        "speech",
        "user",
        "Search",
    ));
    let arguments = json!({ "q": "é" }).as_object().cloned().unwrap();
    let mut tool = ToolAuditEntry::new(
        "2026-03-20T12:00:08+00:00",
        "screen",
        "describe_screen",
        arguments,
        "Chrome with Google open",
        "ok",
        5886.0,
    );
    tool.metadata.insert("frame".to_owned(), json!(3));
    auditor.record_tool(tool);

    let entry = auditor.entries().pop().unwrap();
    assert_eq!(
        (
            entry.r#type.as_str(),
            entry.role.as_deref(),
            entry.content.as_str(),
            entry.duration_ms
        ),
        (
            "tool_call",
            Some("assistant"),
            "Chrome with Google open",
            Some(5886.0)
        )
    );
    // The tool's keys first, its own metadata after them, as Python's
    // `{..., **entry.metadata}`.
    assert_eq!(
        lines(&path)[1],
        r#"{"ts": "2026-03-20T12:00:08+00:00", "type": "tool_call", "role": "assistant", "content": "Chrome with Google open", "duration_ms": 5886.0, "metadata": {"tool_name": "describe_screen", "arguments": {"q": "\u00e9"}, "status": "ok", "agent_id": "screen", "frame": 3}}"#
    );
}

// Python: tests/orchestration/test_session_audit.py::test_tool_auditor_bridge_entries
#[test]
fn tool_auditor_bridge_entries() {
    let dir = Dir::new();
    let auditor = Arc::new(JsonlSessionAuditor::new(dir.join("s.jsonl")).unwrap());
    let mut tool = tool_entry("search", "found", "ok", 42.0);
    tool.metadata.insert("frame".to_owned(), json!(3));
    auditor.record_tool(tool.clone());
    auditor.record(session_entry("t", "speech", "user", "Hello"));

    let bridge = Arc::clone(&auditor).tool_auditor();
    assert_eq!(bridge.entries(), [tool]);
}

// Python: tests/orchestration/test_session_audit.py::test_tool_auditor_bridge_record
#[test]
fn tool_auditor_bridge_record() {
    let dir = Dir::new();
    let auditor = Arc::new(JsonlSessionAuditor::new(dir.join("s.jsonl")).unwrap());
    let bridge = Arc::clone(&auditor).tool_auditor();
    bridge.record(tool_entry("click", "ok", "ok", 10.0));

    let types: Vec<_> = auditor
        .entries()
        .into_iter()
        .map(|entry| entry.r#type)
        .collect();
    assert_eq!(types, ["tool_call"]);
    assert_eq!(bridge.summary(), auditor.summary());
}

// Python: tests/orchestration/test_session_audit.py::test_summary_empty
#[test]
fn summary_empty() {
    let dir = Dir::new();
    let auditor = JsonlSessionAuditor::new(dir.join("s.jsonl")).unwrap();
    assert_eq!(auditor.summary(), "\nNo session events recorded.");
}

// Python: tests/orchestration/test_session_audit.py::test_summary_full_conversation
// and test_summary_duration_formatting
#[test]
fn summary_full_conversation() {
    let dir = Dir::new();
    let path = dir.join("s.jsonl");
    let auditor = JsonlSessionAuditor::new(&path).unwrap();
    auditor.record(session_entry(
        "2026-03-20T12:00:00+00:00",
        "session",
        "system",
        "Session started",
    ));
    auditor.record(session_entry(
        "2026-03-20T12:00:05+00:00",
        "speech",
        "user",
        "Open Chrome",
    ));
    let arguments = json!({ "q": "é" }).as_object().cloned().unwrap();
    auditor.record_tool(ToolAuditEntry::new(
        "2026-03-20T12:00:08+00:00",
        "screen",
        "describe_screen",
        arguments,
        "Chrome with Google open",
        "ok",
        5886.0,
    ));
    auditor.record(session_entry(
        "2026-03-20T13:03:42+00:00",
        "barge_in",
        "user",
        "User interrupted",
    ));

    let at = |ts: &str| local_time(ts);
    let expected = format!(
        "\nSession Audit ({})\n{}\n  [{}] SESSION Session started\n  [{}] USER: \"Open Chrome\"\n  [{}] TOOL describe_screen {{\"q\": \"\\u00e9\"}} → OK (5886ms)\n           Chrome with Google open\n  [{}] BARGE-IN User interrupted\n\n  Duration: 1h 3m 42s | Turns: 1 user, 0 assistant | Tool calls: 1 (5886ms) | Interruptions: 1\n  Tools: describe_screen(1)",
        path.display(),
        "=".repeat(60),
        at("2026-03-20T12:00:00+00:00"),
        at("2026-03-20T12:00:05+00:00"),
        at("2026-03-20T12:00:08+00:00"),
        at("2026-03-20T13:03:42+00:00"),
    );
    assert_eq!(auditor.summary(), expected);
}

// Python: tests/orchestration/test_session_audit.py::test_summary_duration_formatting
#[test]
fn summary_duration_formatting() {
    let dir = Dir::new();
    let auditor = JsonlSessionAuditor::new(dir.join("s.jsonl")).unwrap();
    auditor.record(session_entry(
        "2026-03-20T12:00:00+00:00",
        "session",
        "system",
        "started",
    ));
    auditor.record(session_entry(
        "2026-03-20T12:03:42+00:00",
        "speech",
        "user",
        "done",
    ));
    assert!(auditor.summary().contains("Duration: 3m 42s"));
}

// Python: tests/orchestration/test_session_audit.py::test_attach_registers_hooks
// (ON_SESSION_STARTED: the speech, interruption and vision triggers come
// with the voice channels)
#[tokio::test]
async fn attach_registers_hooks() {
    let dir = Dir::new();
    let kit = RoomKit::new(RoomKitOptions::default());
    let auditor = Arc::new(JsonlSessionAuditor::new(dir.join("s.jsonl")).unwrap());
    Arc::clone(&auditor).attach(&kit).unwrap();

    let on = |trigger| HasHooksOptions {
        trigger: Some(trigger),
    };
    assert!(
        kit.hook_engine()
            .has_hooks(on(HookTrigger::OnSessionStarted))
    );
    kit.close().await.unwrap();
}

// Python: tests/orchestration/test_session_audit.py::test_a_transcript_shows_a_utc_entry_in_local_time
#[test]
fn a_transcript_shows_a_utc_entry_in_local_time() {
    let dir = Dir::new();
    let auditor = JsonlSessionAuditor::new(dir.join("s.jsonl")).unwrap();
    let stored = "2026-03-20T12:00:00+00:00";
    auditor.record(session_entry(stored, "session", "system", "started"));
    assert!(
        auditor
            .summary()
            .contains(&format!("[{}] SESSION started", local_time(stored)))
    );
}

/// `ts` as `HH:MM:SS` in the machine's time zone, as Python's
/// `datetime.fromisoformat(ts).astimezone()` shows it.
fn local_time(ts: &str) -> String {
    let instant: jiff::Timestamp = ts.parse().unwrap();
    instant
        .to_zoned(jiff::tz::TimeZone::system())
        .strftime("%H:%M:%S")
        .to_string()
}
