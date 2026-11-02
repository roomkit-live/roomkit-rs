//! The status bus (RFC §19.8): Python's `StatusBus`, `StatusEntry` and
//! `StatusLevel`.

use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use roomkit_core::model::Metadata;
use roomkit_core::ops::Environment;
use serde::{Deserialize, Serialize};

use super::backend::{self, StatusBackend, StatusCallback};
use super::memory::{InMemoryStatusBackend, InMemoryStatusBackendOptions};
use crate::context::ExecutionContext;
use crate::environment::SystemEnvironment;
use crate::error::{RoomKitError, repr};

/// How many entries [`StatusBus::recent`] reads by default, Python's
/// `n=10`.
const RECENT: usize = 10;

/// How many completed entries [`StatusBus::has_completed`] reads, as
/// Python's reads 50.
const COMPLETED_READ: usize = 50;

/// How many entries [`StatusBus::print_summary`] reads, as Python's.
const SUMMARY_READ: usize = 500;

/// A status entry's level: Python's `StatusLevel`, written `ok`, `failed`,
/// `pending`, `info` and `completed` (RFC §19.8).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StatusLevel {
    /// What the agent did went well.
    Ok,
    /// What the agent did failed.
    Failed,
    /// The agent's work is under way.
    Pending,
    /// A note.
    Info,
    /// The agent's work ended.
    Completed,
}

impl StatusLevel {
    /// The level as it is written: Python's `StatusLevel.value`.
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
            Self::Pending => "pending",
            Self::Info => "info",
            Self::Completed => "completed",
        }
    }
}

impl fmt::Display for StatusLevel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Python's `StatusLevel(value)`: the level a text names, a
/// [`RoomKitError::Value`] (Python's `ValueError`) for any other text.
impl FromStr for StatusLevel {
    type Err = RoomKitError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        [
            Self::Ok,
            Self::Failed,
            Self::Pending,
            Self::Info,
            Self::Completed,
        ]
        .into_iter()
        .find(|level| level.as_str() == value)
        .ok_or_else(|| RoomKitError::Value(format!("{} is not a valid StatusLevel", repr(value))))
    }
}

/// One status an agent posted: Python's `StatusEntry(ts, agent_id, action,
/// status, detail='', metadata={})` (RFC §19.8). Its `metadata.room_id`
/// names the room it is about, whose `ON_STATUS_POSTED` hooks it fires.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StatusEntry {
    /// When it was posted, an RFC 3339 instant in UTC.
    pub ts: String,
    /// The agent it is about.
    pub agent_id: String,
    /// What the agent is doing or did.
    pub action: String,
    /// Its level.
    pub status: StatusLevel,
    /// What it says, for whoever follows the bus.
    #[serde(default)]
    pub detail: String,
    /// What else it carries; `room_id` names its room.
    #[serde(default)]
    pub metadata: Metadata,
}

impl StatusEntry {
    /// The entry of `agent_id`'s `action` at `status`, posted at `ts`, no
    /// detail and no metadata.
    pub fn new(
        ts: impl Into<String>,
        agent_id: impl Into<String>,
        action: impl Into<String>,
        status: StatusLevel,
    ) -> Self {
        Self {
            ts: ts.into(),
            agent_id: agent_id.into(),
            action: action.into(),
            status,
            detail: String::new(),
            metadata: Metadata::new(),
        }
    }

    /// The entry posted now, by the system clock, as Python's bus stamps
    /// it.
    pub(crate) fn posted(
        agent_id: String,
        action: String,
        status: StatusLevel,
        detail: String,
        metadata: Option<Metadata>,
    ) -> Self {
        Self {
            detail,
            metadata: metadata.unwrap_or_default(),
            ..Self::new(
                SystemEnvironment.now().to_string(),
                agent_id,
                action,
                status,
            )
        }
    }

    /// The room its metadata names, when it names one with a non-empty
    /// text.
    pub(crate) fn room_id(&self) -> Option<&str> {
        self.metadata
            .get("room_id")
            .and_then(serde_json::Value::as_str)
            .filter(|room_id| !room_id.is_empty())
    }
}

/// What [`StatusBus::new`] takes: Python's `StatusBus(*, backend=None,
/// persist_path=None)`.
#[derive(Clone, Default)]
pub struct StatusBusOptions {
    /// Python `backend=None`: the backend that keeps and carries the
    /// entries; `None` is an [`InMemoryStatusBackend`].
    pub backend: Option<Arc<dyn StatusBackend>>,
    /// Python `persist_path=None`: with no backend, the JSONL file the
    /// in-memory backend appends each entry to.
    pub persist_path: Option<PathBuf>,
}

impl fmt::Debug for StatusBusOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StatusBusOptions")
            .field("backend", &self.backend.as_ref().map(|_| "StatusBackend"))
            .field("persist_path", &self.persist_path)
            .finish()
    }
}

/// What [`StatusBus::post`] takes beside the agent, its action and its
/// level: Python's `post(..., *, detail='', metadata=None)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PostOptions {
    /// Python `detail=''`: what the entry says.
    pub detail: String,
    /// Python `metadata=None`: what else it carries, `room_id` its room.
    pub metadata: Option<Metadata>,
}

/// What [`StatusBus::post_async`] takes beside the agent, its action and
/// its level: Python's `post_async(..., *, detail='', metadata=None)`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PostAsyncOptions {
    /// Python `detail=''`: what the entry says.
    pub detail: String,
    /// Python `metadata=None`: what else it carries, `room_id` its room.
    pub metadata: Option<Metadata>,
}

/// What [`StatusBus::recent`] takes: Python's `recent(n=10, *,
/// agent_id=None, status=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecentOptions {
    /// Python `n=10`: how many of the newest entries.
    pub n: usize,
    /// Python `agent_id=None`: only this agent's entries.
    pub agent_id: Option<String>,
    /// Python `status=None`: only the entries of this level.
    pub status: Option<StatusLevel>,
}

impl Default for RecentOptions {
    fn default() -> Self {
        Self {
            n: RECENT,
            agent_id: None,
            status: None,
        }
    }
}

/// What [`StatusBus::has_completed`] takes: Python's
/// `has_completed(agent_id=None)`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HasCompletedOptions {
    /// Python `agent_id=None`: only this agent's completions.
    pub agent_id: Option<String>,
}

/// The shared log of the agents' statuses (RFC §19.8): Python's
/// `StatusBus`. Every agent posts to it, any code subscribes to it or reads
/// its recent entries; a kit holds one (`RoomKit::status_bus`), whose
/// entries naming a room fire that room's `ON_STATUS_POSTED` hooks. Cloning
/// it is cheap, and every clone is the same bus.
#[derive(Clone)]
pub struct StatusBus {
    backend: Arc<dyn StatusBackend>,
}

impl fmt::Debug for StatusBus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("StatusBus").finish_non_exhaustive()
    }
}

impl Default for StatusBus {
    fn default() -> Self {
        Self::new(StatusBusOptions::default())
    }
}

impl StatusBus {
    /// A bus over `options.backend`, or over an in-memory backend that
    /// appends to `options.persist_path` when it names one.
    pub fn new(options: StatusBusOptions) -> Self {
        let backend = options.backend.unwrap_or_else(|| {
            Arc::new(InMemoryStatusBackend::new(InMemoryStatusBackendOptions {
                persist_path: options.persist_path,
                ..InMemoryStatusBackendOptions::default()
            }))
        });
        Self { backend }
    }

    /// Posts `agent_id`'s `action` at `status` and returns its entry at
    /// once: its publication, which hands it to the subscribers, runs on
    /// the current tokio runtime in the background, and none runs outside
    /// one, as Python's `post` publishes nothing without a running loop.
    pub fn post(
        &self,
        agent_id: impl Into<String>,
        action: impl Into<String>,
        status: StatusLevel,
        options: PostOptions,
    ) -> StatusEntry {
        let entry = StatusEntry::posted(
            agent_id.into(),
            action.into(),
            status,
            options.detail,
            options.metadata,
        );
        log_post(&entry);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn(self.publication(entry.clone())));
        }
        entry
    }

    /// Posts `agent_id`'s `action` at `status` and returns its entry once
    /// every subscriber ran: Python's `post_async`.
    pub async fn post_async(
        &self,
        agent_id: impl Into<String>,
        action: impl Into<String>,
        status: StatusLevel,
        options: PostAsyncOptions,
    ) -> StatusEntry {
        let entry = StatusEntry::posted(
            agent_id.into(),
            action.into(),
            status,
            options.detail,
            options.metadata,
        );
        log_post(&entry);
        self.backend.publish(entry.clone()).await;
        entry
    }

    /// The publication of `entry`, to run where its poster says: the kit
    /// runs its own posts' as work it tracks. It runs at least as deep as
    /// its poster, read now: a hook it fires sends at least that deep, as
    /// Python's `post` publishes in a task that copies the poster's context,
    /// so that a status hook that tells an agent again ends at the depth
    /// limit (RFC §8.3, RMK-1187).
    pub(crate) fn publication(
        &self,
        entry: StatusEntry,
    ) -> impl Future<Output = ()> + Send + use<> {
        let backend = Arc::clone(&self.backend);
        let poster = ExecutionContext::current().turn_depth().unwrap_or(0);
        let within = ExecutionContext::default().at_least(poster);
        within.scope(async move { backend.publish(entry).await })
    }

    /// Hands every entry posted from now on to `callback`.
    pub async fn subscribe(&self, callback: StatusCallback) {
        self.backend.subscribe(callback).await;
    }

    /// Hands no more entry to `callback`, the `Arc` that subscribed or a
    /// clone of it.
    pub async fn unsubscribe(&self, callback: StatusCallback) {
        self.backend.unsubscribe(callback).await;
    }

    /// The newest `options.n` entries, oldest first, kept to an agent and
    /// a level when `options` names them.
    pub async fn recent(&self, options: RecentOptions) -> Vec<StatusEntry> {
        let filters = backend::RecentOptions {
            agent_id: options.agent_id,
            status: options.status,
        };
        self.backend.recent(options.n, filters).await
    }

    /// Whether a `completed` entry, of `options.agent_id` when it names
    /// one, is among the last 50 completed ones.
    pub async fn has_completed(&self, options: HasCompletedOptions) -> bool {
        let filters = backend::RecentOptions {
            agent_id: None,
            status: Some(StatusLevel::Completed),
        };
        let completed = self.backend.recent(COMPLETED_READ, filters).await;
        completed.iter().any(|entry| {
            options
                .agent_id
                .as_deref()
                .is_none_or(|agent| entry.agent_id == agent)
        })
    }

    /// Releases what the bus's backend holds; the kit's close calls it.
    pub async fn close(&self) {
        self.backend.close().await;
    }

    /// Logs a summary of the last 500 entries, at the `info` level: one
    /// line each, then the counts of the `ok`, `failed` and `completed`
    /// ones, as Python's `print_summary` does.
    pub async fn print_summary(&self) {
        let entries = self
            .backend
            .recent(SUMMARY_READ, backend::RecentOptions::default())
            .await;
        tracing::info!(target: "roomkit::orchestration", "{}", summary(&entries));
    }
}

/// The text [`StatusBus::print_summary`] logs.
fn summary(entries: &[StatusEntry]) -> String {
    if entries.is_empty() {
        return "No status entries.".to_owned();
    }
    let mut lines = vec!["\nStatus Log".to_owned(), "=".repeat(60)];
    for (position, entry) in entries.iter().enumerate() {
        let icon = match entry.status {
            StatusLevel::Ok => "+",
            StatusLevel::Failed => "x",
            StatusLevel::Info => "~",
            StatusLevel::Completed => "*",
            StatusLevel::Pending => "?",
        };
        lines.push(format!(
            "  {:2}. [{icon}] {:6} {}",
            position + 1,
            entry.agent_id,
            entry.action
        ));
        if !entry.detail.is_empty() {
            let detail: String = entry.detail.chars().take(100).collect();
            lines.push(format!("      {detail}"));
        }
    }
    let count = |level: StatusLevel| entries.iter().filter(|entry| entry.status == level).count();
    lines.push(format!(
        "\n  Total: {} entries ({} ok, {} fail, {} done)",
        entries.len(),
        count(StatusLevel::Ok),
        count(StatusLevel::Failed),
        count(StatusLevel::Completed)
    ));
    lines.join("\n")
}

/// Logs a post at the `debug` level, as Python's bus does.
fn log_post(entry: &StatusEntry) {
    tracing::debug!(
        target: "roomkit::orchestration",
        agent = %entry.agent_id,
        action = %entry.action,
        status = %entry.status,
        "a status is posted"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;

    fn bus() -> StatusBus {
        StatusBus::default()
    }

    fn heard() -> (StatusCallback, Arc<Mutex<Vec<StatusEntry>>>) {
        let heard: Arc<Mutex<Vec<StatusEntry>>> = Arc::default();
        let keeps = Arc::clone(&heard);
        let callback: StatusCallback = Arc::new(move |entry| {
            keeps.lock().unwrap().push(entry);
            Box::pin(async {})
        });
        (callback, heard)
    }

    // Python: tests/orchestration/test_status_bus.py::test_bus_post_accepts_string_status
    #[test]
    fn a_level_is_read_from_its_text_and_another_text_is_refused() {
        assert_eq!(
            (
                "completed".parse::<StatusLevel>().ok(),
                "done"
                    .parse::<StatusLevel>()
                    .err()
                    .map(|e| (e.class_name(), e.to_string()))
            ),
            (
                Some(StatusLevel::Completed),
                // Python's enum, its value quoted as `!r` writes it.
                Some((
                    "ValueError".to_owned(),
                    "'done' is not a valid StatusLevel".to_owned()
                ))
            )
        );
    }

    // Python: tests/orchestration/test_status_bus.py::test_backend_eviction
    #[tokio::test]
    async fn the_backend_keeps_the_newest_entries_past_its_bound() {
        let backend = InMemoryStatusBackend::new(InMemoryStatusBackendOptions {
            max_entries: 3,
            ..InMemoryStatusBackendOptions::default()
        });
        for action in ["a", "b", "c", "d", "e"] {
            backend
                .publish(StatusEntry::new("t", "agent", action, StatusLevel::Ok))
                .await;
        }
        let kept: Vec<String> = backend
            .recent(10, backend::RecentOptions::default())
            .await
            .into_iter()
            .map(|entry| entry.action)
            .collect();
        assert_eq!(kept, ["c", "d", "e"]);
    }

    // Python: tests/orchestration/test_status_bus.py::test_backend_subscribe_receives_entries
    #[tokio::test]
    async fn a_subscriber_receives_each_entry_until_it_unsubscribes() {
        let bus = bus();
        let (callback, heard) = heard();
        bus.subscribe(Arc::clone(&callback)).await;
        bus.post_async("a", "one", StatusLevel::Ok, PostAsyncOptions::default())
            .await;
        bus.unsubscribe(callback).await;
        bus.post_async("a", "two", StatusLevel::Ok, PostAsyncOptions::default())
            .await;
        let actions: Vec<String> = heard
            .lock()
            .unwrap()
            .iter()
            .map(|entry| entry.action.clone())
            .collect();
        assert_eq!(actions, ["one"]);
    }

    // Python: tests/orchestration/test_status_bus.py::test_backend_persistence
    #[tokio::test]
    async fn the_backend_appends_each_entry_to_its_file() {
        let directory =
            std::env::temp_dir().join(format!("roomkit-status-{}", uuid::Uuid::new_v4().simple()));
        let path = directory.join("log").join("status.jsonl");
        let bus = StatusBus::new(StatusBusOptions {
            persist_path: Some(path.clone()),
            ..StatusBusOptions::default()
        });
        bus.post_async(
            "exec",
            "search",
            StatusLevel::Ok,
            PostAsyncOptions {
                detail: "found 3".to_owned(),
                metadata: None,
            },
        )
        .await;
        bus.post_async(
            "exec",
            "close",
            StatusLevel::Completed,
            PostAsyncOptions::default(),
        )
        .await;
        let lines: Vec<serde_json::Value> = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        std::fs::remove_dir_all(&directory).unwrap();
        assert_eq!(
            lines
                .iter()
                .map(|line| (
                    line["action"].clone(),
                    line["status"].clone(),
                    line["detail"].clone()
                ))
                .collect::<Vec<_>>(),
            vec![
                (json!("search"), json!("ok"), json!("found 3")),
                (json!("close"), json!("completed"), json!("")),
            ]
        );
    }

    // Python: tests/orchestration/test_status_bus.py::test_bus_post_sync_schedules
    #[tokio::test]
    async fn post_returns_its_entry_and_publishes_it_in_the_background() {
        let bus = bus();
        let (callback, heard) = heard();
        bus.subscribe(callback).await;
        let entry = bus.post("voice", "greet", StatusLevel::Info, PostOptions::default());
        assert!(heard.lock().unwrap().is_empty());
        tokio::task::yield_now().await;
        assert_eq!(heard.lock().unwrap().clone(), vec![entry]);
    }

    // Python: tests/orchestration/test_status_bus.py::test_bus_subscribe_async_callback
    #[tokio::test]
    async fn post_async_returns_once_the_subscribers_ran() {
        let bus = bus();
        let (callback, heard) = heard();
        bus.subscribe(callback).await;
        let entry = bus
            .post_async("a", "act", StatusLevel::Ok, PostAsyncOptions::default())
            .await;
        assert_eq!(heard.lock().unwrap().clone(), vec![entry]);
    }

    #[tokio::test]
    async fn a_failing_subscriber_does_not_keep_the_others_from_the_entry() {
        let bus = bus();
        let failing: StatusCallback = Arc::new(|_entry| Box::pin(async { panic!("down") }));
        let (callback, heard) = heard();
        bus.subscribe(failing).await;
        bus.subscribe(callback).await;
        bus.post_async("a", "act", StatusLevel::Ok, PostAsyncOptions::default())
            .await;
        assert_eq!(heard.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn has_completed_reads_the_completed_entries_of_the_agent_it_names() {
        let bus = bus();
        bus.post_async("a1", "work", StatusLevel::Ok, PostAsyncOptions::default())
            .await;
        bus.post_async(
            "a2",
            "work",
            StatusLevel::Completed,
            PostAsyncOptions::default(),
        )
        .await;
        let of = |agent: Option<&str>| HasCompletedOptions {
            agent_id: agent.map(str::to_owned),
        };
        assert_eq!(
            (
                bus.has_completed(of(None)).await,
                bus.has_completed(of(Some("a2"))).await,
                bus.has_completed(of(Some("a1"))).await
            ),
            (true, true, false)
        );
    }

    // Python: tests/orchestration/test_status_bus.py::test_bus_print_summary,
    // test_bus_print_summary_empty
    #[test]
    fn the_summary_lists_each_entry_and_counts_them() {
        let mut entry = StatusEntry::new("t", "exec", "search", StatusLevel::Ok);
        entry.detail = "found 7".to_owned();
        let failed = StatusEntry::new("t", "exec", "write", StatusLevel::Failed);
        assert_eq!(
            (summary(&[]), summary(&[entry, failed])),
            (
                "No status entries.".to_owned(),
                format!(
                    "\nStatus Log\n{}\n   1. [+] exec   search\n      found 7\n   2. [x] exec   write\n\n  Total: 2 entries (1 ok, 1 fail, 0 done)",
                    "=".repeat(60)
                )
            )
        );
    }
}
