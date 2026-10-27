//! The server's process: started, its standard error read as it writes,
//! watched so that it is waited for the moment it exits, stopped at its
//! close, killed when anything else ends it (RFC §21.2).

use std::time::Duration;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::command::McpCommand;
use crate::error::McpError;
use crate::group::Group;
use crate::texts::bounded_message;

/// The longest line of a server's standard error a log line keeps, in
/// bytes: the rest of the line is read and dropped.
const STDERR_LINE_LIMIT: usize = 4096;

/// A started server: its process, and the two pipes the protocol runs on.
pub(crate) struct Started {
    pub(crate) process: ServerProcess,
    pub(crate) stdin: ChildStdin,
    pub(crate) stdout: ChildStdout,
}

/// How the watcher of a process is asked to end it.
#[derive(Clone, Copy, Debug)]
enum Stop {
    /// Its input closed already, it is given this long to exit, then its
    /// group is ended.
    Grace(Duration),
    /// Its group is ended at once.
    Kill,
}

/// The process of a server, the leader of a process group of its own on
/// Unix, held by a task that waits for it: a process that exits on its own
/// is waited for at once, so that no zombie stays while its connection
/// does, and what it started is ended with it. Stopped or killed, its group
/// is ended (`SIGTERM`, the command's grace, `SIGKILL`) and the process
/// waited for before the call returns; dropped, its watcher does the same;
/// and if the runtime is gone, dropping the watcher sends the group
/// `SIGKILL`, the process killed too (the tokio runtime reaps it).
pub(crate) struct ServerProcess {
    pid: Option<u32>,
    stop: Option<oneshot::Sender<Stop>>,
    watcher: Option<JoinHandle<()>>,
}

impl ServerProcess {
    /// Starts `command`'s process, its standard error read as it writes.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Command`] for a command that cannot start a
    /// process, and [`McpError::Spawn`] when the program cannot be
    /// started; no process runs then.
    pub(crate) fn start(command: &McpCommand) -> Result<Started, McpError> {
        let program = command.program();
        let mut child = command
            .process()?
            .spawn()
            .map_err(|source| McpError::Spawn {
                program: program.clone(),
                source,
            })?;
        let pid = child.id();
        let (Some(stdin), Some(stdout), Some(stderr)) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take())
        else {
            // Each stream was asked piped: none is missing.
            return Err(McpError::Handshake {
                program,
                message: "the server's standard streams are not piped".to_owned(),
            });
        };
        tokio::spawn(read_stderr(stderr, program));
        let (stop, stopped) = oneshot::channel();
        let watcher = tokio::spawn(watch(child, pid, command.close_grace, stopped));
        Ok(Started {
            process: Self {
                pid,
                stop: Some(stop),
                watcher: Some(watcher),
            },
            stdin,
            stdout,
        })
    }

    /// The process's id, as it was started.
    pub(crate) fn id(&self) -> Option<u32> {
        self.pid
    }

    /// Gives the process `grace` to exit, its input closed already, then
    /// kills it; returns once it is waited for.
    pub(crate) async fn stop(mut self, grace: Duration) {
        self.end(Stop::Grace(grace)).await;
    }

    /// Kills the process; returns once it is waited for.
    pub(crate) async fn kill(mut self) {
        self.end(Stop::Kill).await;
    }

    async fn end(&mut self, how: Stop) {
        if let Some(stop) = self.stop.take() {
            // A watcher that already waited for its process takes no order.
            let _waited_already = stop.send(how);
        }
        if let Some(watcher) = self.watcher.take() {
            let _ended = watcher.await;
        }
    }
}

/// Holds `child` until it exits on its own, or until it is asked to end
/// it, or its handle is dropped, which ends it; then ends what is left of
/// its group, each wait between `SIGTERM` and `SIGKILL` lasting `grace`,
/// and waits for it.
async fn watch(
    mut child: Child,
    pid: Option<u32>,
    grace: Duration,
    stopped: oneshot::Receiver<Stop>,
) {
    let group = Group::led_by(pid);
    let mut guard = group.guard();
    let how = tokio::select! {
        _exited = child.wait() => None,
        how = stopped => Some(how.unwrap_or(Stop::Kill)),
    };
    if let Some(Stop::Grace(grace)) = how {
        let _exited = tokio::time::timeout(grace, child.wait()).await;
    }
    group.end(&mut child, grace).await;
    guard.disarm();
}

/// Reads a server's standard error to its end, so that it never fills the
/// pipe and blocks the server: each line, bounded, logged at debug level.
async fn read_stderr(stderr: ChildStderr, program: String) {
    let mut reader = BufReader::new(stderr);
    let mut line: Vec<u8> = Vec::new();
    loop {
        let available = match reader.fill_buf().await {
            Ok([]) | Err(_) => break,
            Ok(available) => available,
        };
        let read = available.len();
        for &byte in available {
            if byte == b'\n' {
                log_line(&program, &line);
                line.clear();
            } else if line.len() < STDERR_LINE_LIMIT {
                line.push(byte);
            }
        }
        reader.consume(read);
    }
    if !line.is_empty() {
        log_line(&program, &line);
    }
}

fn log_line(program: &str, line: &[u8]) {
    tracing::debug!(
        target: "roomkit::mcp",
        server = program,
        line = %bounded_message(&String::from_utf8_lossy(line)),
        "an MCP server's standard error"
    );
}
