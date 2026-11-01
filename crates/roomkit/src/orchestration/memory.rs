//! The in-memory status backend: Python's `InMemoryStatusBackend`, the
//! entries in a list and the subscribers called in turn, in one process.

use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, PoisonError};

use super::backend::{RecentOptions, StatusBackend, StatusCallback, StatusFuture, same_callback};
use super::status::StatusEntry;
use crate::unwind::{message, unwinding};

/// How many entries the backend keeps by default, Python's
/// `max_entries=500`.
const MAX_ENTRIES: usize = 500;

/// What [`InMemoryStatusBackend::new`] takes: Python's
/// `InMemoryStatusBackend(*, max_entries=500, persist_path=None)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InMemoryStatusBackendOptions {
    /// Python `max_entries=500`: how many entries it keeps, the oldest let
    /// go past it.
    pub max_entries: usize,
    /// Python `persist_path=None`: a JSONL file each entry is appended to,
    /// one JSON object a line, its directory created; a write that fails is
    /// logged and the entry kept all the same.
    pub persist_path: Option<PathBuf>,
}

impl Default for InMemoryStatusBackendOptions {
    fn default() -> Self {
        Self {
            max_entries: MAX_ENTRIES,
            persist_path: None,
        }
    }
}

/// A status backend in one process: Python's `InMemoryStatusBackend`. It
/// keeps the newest `max_entries` entries, appends each to its JSONL file
/// when it has one, and hands each to its subscribers in the order they
/// subscribed, each awaited, one that panics logged. For several processes,
/// a backend over Redis or NATS implements [`StatusBackend`] instead.
pub struct InMemoryStatusBackend {
    max_entries: usize,
    persist_path: Option<PathBuf>,
    entries: Mutex<Vec<StatusEntry>>,
    subscribers: Mutex<Vec<StatusCallback>>,
}

impl std::fmt::Debug for InMemoryStatusBackend {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("InMemoryStatusBackend")
            .field("max_entries", &self.max_entries)
            .field("persist_path", &self.persist_path)
            .finish_non_exhaustive()
    }
}

impl Default for InMemoryStatusBackend {
    fn default() -> Self {
        Self::new(InMemoryStatusBackendOptions::default())
    }
}

impl InMemoryStatusBackend {
    /// A backend keeping `options.max_entries` entries, persisted to
    /// `options.persist_path` when it names one, whose directory is created
    /// now (a failure logged, as the file's writes are).
    pub fn new(options: InMemoryStatusBackendOptions) -> Self {
        if let Some(parent) = options.persist_path.as_deref().and_then(Path::parent)
            && let Err(error) = std::fs::create_dir_all(parent)
        {
            tracing::warn!(target: "roomkit::orchestration", %error, "the status log's directory cannot be created");
        }
        Self {
            max_entries: options.max_entries,
            persist_path: options.persist_path,
            entries: Mutex::new(Vec::new()),
            subscribers: Mutex::new(Vec::new()),
        }
    }

    fn entries(&self) -> MutexGuard<'_, Vec<StatusEntry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn subscribers(&self) -> MutexGuard<'_, Vec<StatusCallback>> {
        self.subscribers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Keeps `entry`, the oldest let go past the bound.
    fn keep(&self, entry: StatusEntry) {
        let mut entries = self.entries();
        entries.push(entry);
        let excess = entries.len().saturating_sub(self.max_entries);
        entries.drain(..excess);
    }

    /// Appends `entry` to the backend's file, off the runtime's threads; a
    /// failure is logged.
    async fn persist(&self, entry: &StatusEntry) {
        let Some(path) = self.persist_path.clone() else {
            return;
        };
        let line = match serde_json::to_string(entry) {
            Ok(line) => line,
            Err(error) => {
                tracing::error!(target: "roomkit::orchestration", %error, "a status entry cannot be persisted");
                return;
            }
        };
        let written = tokio::task::spawn_blocking(move || append_line(&path, &line)).await;
        match written {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::error!(target: "roomkit::orchestration", %error, "a status entry was not persisted");
            }
            Err(error) => {
                tracing::error!(target: "roomkit::orchestration", %error, "a status entry was not persisted");
            }
        }
    }
}

/// Appends `line` and a line break to the file at `path`.
fn append_line(path: &Path, line: &str) -> std::io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{line}")
}

impl StatusBackend for InMemoryStatusBackend {
    fn publish(&self, entry: StatusEntry) -> StatusFuture<'_, ()> {
        Box::pin(async move {
            self.keep(entry.clone());
            self.persist(&entry).await;
            let subscribers = self.subscribers().clone();
            for callback in subscribers {
                if let Err(panic) = unwinding(callback(entry.clone())).await {
                    let panic = message(panic.as_ref());
                    tracing::error!(target: "roomkit::orchestration", %panic, "a status bus subscriber failed");
                }
            }
        })
    }

    /// Python's `entries[-n:]` once filtered: `n` of 0 reads them all.
    fn recent(&self, n: usize, options: RecentOptions) -> StatusFuture<'_, Vec<StatusEntry>> {
        Box::pin(async move {
            let kept: Vec<StatusEntry> = self
                .entries()
                .iter()
                .filter(|entry| {
                    options
                        .agent_id
                        .as_deref()
                        .is_none_or(|agent| entry.agent_id == agent)
                        && options.status.is_none_or(|status| entry.status == status)
                })
                .cloned()
                .collect();
            let skipped = if n == 0 {
                0
            } else {
                kept.len().saturating_sub(n)
            };
            kept.into_iter().skip(skipped).collect()
        })
    }

    fn subscribe(&self, callback: StatusCallback) -> StatusFuture<'_, ()> {
        Box::pin(async move {
            self.subscribers().push(callback);
        })
    }

    /// Takes off the first subscription of `callback`, as Python's
    /// `list.remove`.
    fn unsubscribe(&self, callback: StatusCallback) -> StatusFuture<'_, ()> {
        Box::pin(async move {
            let mut subscribers = self.subscribers();
            if let Some(position) = subscribers
                .iter()
                .position(|subscribed| same_callback(subscribed, &callback))
            {
                subscribers.remove(position);
            }
        })
    }
}
