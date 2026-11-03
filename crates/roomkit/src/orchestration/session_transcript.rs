//! The transcript a session auditor's summary is (RFC §15.8.2): each entry
//! on a line at its local time, then the session's figures, as Python's
//! `session_audit` formatting helpers write them.

use std::collections::BTreeMap;

use jiff::civil::DateTime;
use jiff::tz::TimeZone;
use serde_json::Value;

use super::session_audit::SessionAuditEntry;
use super::tool_audit::{first_chars, preview};
use crate::tasks::json::json;

/// The transcript titled `title` of `entries`, which are not empty: each
/// entry on a line, then the figures.
pub(super) fn transcript(title: &str, entries: &[SessionAuditEntry]) -> String {
    let mut lines = vec![format!("\n{title}"), "=".repeat(60)];
    lines.extend(
        entries
            .iter()
            .map(|entry| line(entry, &local_time(&entry.ts))),
    );
    lines.push(String::new());
    let figures = Figures::of(entries);
    let duration = match figures.duration_s {
        Some(seconds) if seconds != 0.0 => duration(seconds),
        _ => "?".to_owned(),
    };
    let mut parts = vec![format!("Duration: {duration}")];
    if figures.user_turns > 0 || figures.assistant_turns > 0 {
        parts.push(format!(
            "Turns: {} user, {} assistant",
            figures.user_turns, figures.assistant_turns
        ));
    }
    if figures.tool_calls > 0 {
        parts.push(format!(
            "Tool calls: {} ({:.0}ms)",
            figures.tool_calls, figures.tool_ms
        ));
    }
    if figures.vision_count > 0 {
        parts.push(format!("Vision: {}", figures.vision_count));
    }
    if figures.barge_ins > 0 {
        parts.push(format!("Interruptions: {}", figures.barge_ins));
    }
    lines.push(format!("  {}", parts.join(" | ")));
    if !figures.by_tool.is_empty() {
        let tools: Vec<String> = figures
            .by_tool
            .iter()
            .map(|(tool, count)| format!("{tool}({count})"))
            .collect();
        lines.push(format!("  Tools: {}", tools.join(", ")));
    }
    lines.join("\n")
}

/// `ts`, an instant stored in UTC (RFC §1.4), as `HH:MM:SS` in the
/// machine's time zone, for a person to read; its first 8 characters when
/// it reads as no instant. Python's `_format_time`.
fn local_time(ts: &str) -> String {
    if let Ok(instant) = ts.parse::<jiff::Timestamp>() {
        return instant
            .to_zoned(TimeZone::system())
            .strftime("%H:%M:%S")
            .to_string();
    }
    // An instant with no offset is the machine's own time already, as
    // Python's `astimezone` reads a naive one.
    match ts.parse::<DateTime>() {
        Ok(civil) => civil.strftime("%H:%M:%S").to_string(),
        Err(_) => first_chars(ts, 8).to_owned(),
    }
}

/// `entry`'s line at `ts`: Python's `_format_entry`.
fn line(entry: &SessionAuditEntry, ts: &str) -> String {
    let content = &entry.content;
    match entry.r#type.as_str() {
        "speech" => {
            let label = if entry.role.as_deref() == Some("user") {
                "USER"
            } else {
                "ASSISTANT"
            };
            let mut text = first_chars(content, 200).to_owned();
            if content.chars().count() > 200 {
                text.push_str("...");
            }
            format!("  [{ts}] {label}: \"{text}\"")
        }
        "tool_call" => tool_call_line(entry, ts),
        "vision" => format!("  [{ts}] VISION {}", preview(content, 150)),
        "barge_in" => format!("  [{ts}] BARGE-IN {content}"),
        "session" => format!("  [{ts}] SESSION {content}"),
        "error" => format!("  [{ts}] ERROR {}", first_chars(content, 200)),
        other => format!(
            "  [{ts}] {} {}",
            other.to_uppercase(),
            first_chars(content, 200)
        ),
    }
}

/// A tool call's line, its result's preview on the next.
fn tool_call_line(entry: &SessionAuditEntry, ts: &str) -> String {
    let metadata = &entry.metadata;
    let tool_name = metadata
        .get("tool_name")
        .map_or_else(|| "?".to_owned(), shown);
    let status = metadata
        .get("status")
        .map_or_else(|| "ok".to_owned(), shown)
        .to_uppercase();
    let duration = match entry.duration_ms {
        Some(ms) if ms != 0.0 => format!(" ({ms:.0}ms)"),
        _ => String::new(),
    };
    let arguments = match metadata.get("arguments") {
        Some(arguments) if truthy(arguments) => {
            let mut written = " ".to_owned();
            json(&mut written, arguments, true);
            written
        }
        _ => String::new(),
    };
    let mut line = format!("  [{ts}] TOOL {tool_name}{arguments} → {status}{duration}");
    if !entry.content.is_empty() {
        line.push_str("\n           ");
        line.push_str(&preview(&entry.content, 120));
    }
    line
}

/// `value` as Python's f-string shows it: a text as itself.
fn shown(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), str::to_owned)
}

/// Whether `value` reads as true, as Python's `if value`.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(flag) => *flag,
        Value::Number(number) => number.as_f64().is_some_and(|n| n != 0.0),
        Value::String(text) => !text.is_empty(),
        Value::Array(items) => !items.is_empty(),
        Value::Object(members) => !members.is_empty(),
    }
}

/// The session's figures: Python's `_compute_stats`.
#[derive(Default)]
struct Figures {
    user_turns: usize,
    assistant_turns: usize,
    tool_calls: usize,
    tool_ms: f64,
    by_tool: BTreeMap<String, usize>,
    vision_count: usize,
    barge_ins: usize,
    duration_s: Option<f64>,
}

impl Figures {
    fn of(entries: &[SessionAuditEntry]) -> Self {
        let mut figures = Self::default();
        for entry in entries {
            match entry.r#type.as_str() {
                "speech" if entry.role.as_deref() == Some("user") => figures.user_turns += 1,
                "speech" => figures.assistant_turns += 1,
                "tool_call" => {
                    figures.tool_calls += 1;
                    figures.tool_ms += entry.duration_ms.unwrap_or(0.0);
                    let tool = entry
                        .metadata
                        .get("tool_name")
                        .map_or_else(|| "?".to_owned(), shown);
                    *figures.by_tool.entry(tool).or_default() += 1;
                }
                "vision" => figures.vision_count += 1,
                "barge_in" => figures.barge_ins += 1,
                _ => {}
            }
        }
        if let [first, .., last] = entries {
            figures.duration_s = seconds_between(&first.ts, &last.ts);
        }
        figures
    }
}

/// The seconds from `first` to `last`, both instants with an offset or
/// both without; `None` otherwise, as Python's subtraction refuses an
/// aware and a naive one.
fn seconds_between(first: &str, last: &str) -> Option<f64> {
    let elapsed = match (Instant::read(first)?, Instant::read(last)?) {
        (Instant::Aware(first), Instant::Aware(last)) => last.duration_since(first),
        (Instant::Naive(first), Instant::Naive(last)) => last.duration_since(first),
        _ => return None,
    };
    Some(elapsed.as_secs_f64())
}

/// An entry's instant as Python's `fromisoformat` reads it: with an
/// offset, or naive.
enum Instant {
    Aware(jiff::Timestamp),
    Naive(DateTime),
}

impl Instant {
    /// `ts` read with its offset first: the civil reading ignores one.
    fn read(ts: &str) -> Option<Self> {
        if let Ok(instant) = ts.parse::<jiff::Timestamp>() {
            return Some(Self::Aware(instant));
        }
        ts.parse::<DateTime>().ok().map(Self::Naive)
    }
}

/// `seconds` for a person to read: Python's `_format_duration`.
fn duration(seconds: f64) -> String {
    if seconds < 60.0 {
        return format!("{seconds:.0}s");
    }
    let minutes = (seconds / 60.0).floor();
    let secs = (seconds % 60.0).floor();
    if minutes < 60.0 {
        return format!("{minutes:.0}m {secs:.0}s");
    }
    let hours = (minutes / 60.0).floor();
    let mins = minutes % 60.0;
    format!("{hours:.0}h {mins:.0}m {secs:.0}s")
}
