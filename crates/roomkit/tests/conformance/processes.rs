//! What the runner's fixture processes report (roomkit-specs
//! conformance/README.md, "Scripted ACP agents", `acp.process`, and
//! "Scripted agents", `mcp_servers` `stdio`).
//!
//! A fixture process, `roomkit-conformance-fixture`, is given the path of
//! its report as its first own argument, and appends to it one JSON line
//! each: its start, with the arguments it received after its own and the
//! variables of its environment the scenario names; each request the run
//! reads of it; and its exit.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde_json::{Value, json};

use crate::run::lock;

/// The fixture process's program.
pub(crate) const FIXTURE: &str = env!("CARGO_BIN_EXE_roomkit-conformance-fixture");

/// How long a fixture process is waited for to end, once its owner closed.
const EXIT_WAIT: Duration = Duration::from_secs(5);

/// Tells apart the report directories of the runs of one process.
static RUNS: AtomicU64 = AtomicU64::new(0);

/// The reports of a run's fixture processes, each under its name.
#[derive(Debug, Default)]
pub(crate) struct ProcessReports {
    directory: Mutex<Option<PathBuf>>,
    paths: Mutex<Vec<PathBuf>>,
    /// The fixture processes, by name: an ACP channel's id, an MCP
    /// server's label.
    processes: Mutex<BTreeMap<String, PathBuf>>,
}

impl ProcessReports {
    /// A new report path for the fixture process named `name`.
    pub(crate) fn report(&self, name: &str) -> Result<PathBuf, String> {
        let mut directory = lock(&self.directory);
        if directory.is_none() {
            // A name of its own, drawn at random, created anew: never a
            // directory another process prepared.
            let drawn = getrandom::u64().map_err(|error| error.to_string())?;
            let created = std::env::temp_dir().join(format!(
                "roomkit-conformance-{}-{}-{drawn:016x}",
                std::process::id(),
                RUNS.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&created).map_err(|error| error.to_string())?;
            *directory = Some(created);
        }
        let mut paths = lock(&self.paths);
        let path = directory
            .as_ref()
            .map(|directory| directory.join(format!("{}.jsonl", paths.len())))
            .ok_or("no report directory")?;
        paths.push(path.clone());
        lock(&self.processes).insert(name.to_owned(), path.clone());
        Ok(path)
    }

    /// `expect.processes`, once every fixture process that started ended
    /// or [`EXIT_WAIT`] passed.
    pub(crate) async fn observe(&self) -> BTreeMap<String, Vec<Value>> {
        let processes = lock(&self.processes).clone();
        let _ended = tokio::time::timeout(EXIT_WAIT, async {
            while !processes.values().all(|path| ended(&lines(path))) {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        processes
            .into_iter()
            .map(|(name, path)| (name, starts(&lines(&path))))
            .collect()
    }

    /// Removes the reports.
    pub(crate) fn close(&self) {
        if let Some(directory) = lock(&self.directory).take() {
            let _removed = std::fs::remove_dir_all(directory);
        }
    }
}

/// A run that returns before its observation, its setup failing, still
/// removes its reports.
impl Drop for ProcessReports {
    fn drop(&mut self) {
        self.close();
    }
}

/// The lines of the report at `path`.
pub(crate) fn lines(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

/// What a fixture ACP agent received: `{method, params}` each.
pub(crate) fn received(path: &Path) -> Vec<Value> {
    lines(path)
        .into_iter()
        .filter(|line| line["event"] == "received")
        .map(|line| json!({ "method": line["method"], "params": line["params"] }))
        .collect()
}

/// The `tools/call` requests a fixture MCP server was sent:
/// `{name, arguments, _meta}` each.
pub(crate) fn calls(path: &Path) -> Vec<Value> {
    lines(path)
        .into_iter()
        .filter(|line| line["event"] == "call")
        .map(|line| json!({ "name": line["name"], "arguments": line["arguments"], "_meta": line["_meta"] }))
        .collect()
}

/// Whether each start of the process is followed by its exit.
fn ended(lines: &[Value]) -> bool {
    let mut open = 0_usize;
    for line in lines {
        if line["event"] == "start" {
            open += 1;
        } else if line["event"] == "exit" {
            open = open.saturating_sub(1);
        }
    }
    open == 0
}

/// Each start, in order: `{argv, env, exited}`.
fn starts(lines: &[Value]) -> Vec<Value> {
    let mut starts: Vec<Value> = Vec::new();
    for line in lines {
        if line["event"] == "start" {
            starts.push(json!({ "argv": line["argv"], "env": line["env"], "exited": false }));
        } else if line["event"] == "exit"
            && let Some(last) = starts.last_mut()
        {
            last["exited"] = json!(true);
        }
    }
    starts
}
