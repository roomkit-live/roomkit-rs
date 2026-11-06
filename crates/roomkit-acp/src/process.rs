//! A spawned agent's process: started, its standard error drained, watched
//! so that it is waited for the moment it exits, stopped as the Python ACP
//! SDK stops it, killed when it is let go of (HITL 3f858b12).

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::Path;
use std::process::ExitStatus;
use std::process::Stdio;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::env::default_environment;
use crate::error::AcpError;

/// How long a closing agent is given at each step, as the Python SDK gives
/// it (`shutdown_timeout`): to exit once its input is closed, then once it
/// is sent `SIGTERM`, before it is killed.
pub(crate) const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// How a process exited, none until it was waited for.
type Status = Arc<Mutex<Option<ExitStatus>>>;

/// A started agent: its process and the two pipes the protocol runs on.
pub(crate) struct Started {
    pub(crate) process: AgentProcess,
    pub(crate) stdin: ChildStdin,
    pub(crate) stdout: ChildStdout,
}

/// The process of a spawned agent, held by a task that waits for it: an
/// agent that exits on its own is waited for at once, so that no zombie
/// stays and [`has_exited`](Self::has_exited) tells. Stopped, it is given
/// the grace to exit, then `SIGTERM`, then `SIGKILL`; dropped, it is killed
/// at once; and if the runtime is gone, its handle's drop kills it.
pub(crate) struct AgentProcess {
    pid: Option<u32>,
    /// How the process exited, once it was waited for.
    status: Status,
    stop: Option<oneshot::Sender<Duration>>,
    watcher: Option<JoinHandle<()>>,
    stderr: JoinHandle<()>,
}

impl AgentProcess {
    /// Starts `program` with `args` in `cwd`, without a shell, its
    /// environment the default one and `env` over it, its three streams
    /// piped.
    ///
    /// # Errors
    ///
    /// [`AcpError::Spawn`] when the program cannot be started.
    pub(crate) fn start(
        program: &str,
        args: &[String],
        cwd: &Path,
        env: Option<&BTreeMap<OsString, OsString>>,
    ) -> Result<Started, AcpError> {
        let mut command = tokio::process::Command::new(program);
        command
            .args(args)
            .env_clear()
            .envs(default_environment())
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(env) = env {
            command.envs(env);
        }
        let spawn_error = |source| AcpError::Spawn {
            program: program.to_owned(),
            source: Arc::new(source),
        };
        let mut child = command.spawn().map_err(|source: std::io::Error| {
            // Python's error names the directory that is missing, not the
            // program.
            if cwd.is_dir() {
                spawn_error(source)
            } else {
                spawn_error(std::io::Error::new(
                    source.kind(),
                    format!("{source}: {}", cwd.display()),
                ))
            }
        })?;
        let (Some(stdin), Some(stdout), Some(stderr)) =
            (child.stdin.take(), child.stdout.take(), child.stderr.take())
        else {
            // Each stream was asked piped: none is missing.
            return Err(spawn_error(std::io::Error::other(
                "the agent's standard streams are not piped",
            )));
        };
        let pid = child.id();
        let status = Status::default();
        let (stop, stopped) = oneshot::channel();
        let watcher = tokio::spawn(watch(child, Arc::clone(&status), stopped));
        Ok(Started {
            process: Self {
                pid,
                status,
                stop: Some(stop),
                watcher: Some(watcher),
                stderr: tokio::spawn(drain(stderr)),
            },
            stdin,
            stdout,
        })
    }

    /// The process's id, as it was started.
    pub(crate) fn id(&self) -> Option<u32> {
        self.pid
    }

    /// Whether the process has exited and was waited for.
    pub(crate) fn has_exited(&self) -> bool {
        self.status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    /// Stops the process, its input closed already: `grace` to exit, then
    /// `SIGTERM` and `grace` again, then `SIGKILL`; returns once it is
    /// waited for, its standard error's draining stopped.
    pub(crate) async fn stop(mut self, grace: Duration) {
        if let Some(stop) = self.stop.take() {
            // A watcher that already waited for its process takes no order.
            let _waited_already = stop.send(grace);
        }
        if let Some(watcher) = self.watcher.take() {
            let _ended = watcher.await;
        }
        self.stderr.abort();
    }
}

impl Drop for AgentProcess {
    /// Let go of without a stop: the watcher, its order's sender dropped,
    /// kills the process at once.
    fn drop(&mut self) {
        self.stderr.abort();
    }
}

/// Holds `child` until it exits on its own, or until it is asked to stop it
/// with a grace, or its order's sender is dropped, which kills it; then
/// waits for it and keeps how it exited in `status`.
async fn watch(mut child: Child, status: Status, stopped: oneshot::Receiver<Duration>) {
    let order = tokio::select! {
        _exited = child.wait() => None,
        order = stopped => Some(order.ok()),
    };
    match order {
        None => {}
        Some(Some(grace)) => stop_gracefully(&mut child, grace).await,
        Some(None) => kill(&mut child).await,
    }
    // Waited for already, the process answers its status at once.
    let exited = child.wait().await.ok();
    *status.lock().unwrap_or_else(PoisonError::into_inner) = exited.or(Some(ExitStatus::default()));
}

/// The SDK's stop: `grace` for the process to exit, its input closed, then
/// `SIGTERM` and `grace` again, then `SIGKILL`.
async fn stop_gracefully(child: &mut Child, grace: Duration) {
    if tokio::time::timeout(grace, child.wait()).await.is_ok() {
        return;
    }
    terminate(child);
    if tokio::time::timeout(grace, child.wait()).await.is_ok() {
        return;
    }
    kill(child).await;
}

async fn kill(child: &mut Child) {
    // A process already gone cannot be killed again.
    let _already_gone = child.start_kill();
    let _waited = child.wait().await;
}

/// Sends the process `SIGTERM`, the SDK's `process.terminate()`, while it
/// is not waited for yet, so that its id is still its own.
#[cfg(unix)]
fn terminate(child: &Child) {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let Some(pid) = child.id().and_then(|pid| i32::try_from(pid).ok()) else {
        return;
    };
    if let Err(error) = kill(Pid::from_raw(pid), Signal::SIGTERM) {
        tracing::debug!(target: "roomkit::acp", %error, "an ACP agent could not be sent SIGTERM");
    }
}

/// Elsewhere than on Unix, Python's `terminate()` is `TerminateProcess`,
/// which is the kill the next step sends.
#[cfg(not(unix))]
fn terminate(_child: &Child) {}

/// Reads the agent's standard error to its end and drops it, as the
/// Python implementation's transport drains it: so that it never fills the
/// pipe and blocks the agent.
async fn drain(mut stderr: ChildStderr) {
    let mut buffer = vec![0_u8; 8192];
    while matches!(stderr.read(&mut buffer).await, Ok(read) if read > 0) {}
}

#[cfg(all(test, unix))]
mod tests {
    use tokio::time::Instant;

    use super::*;

    const SHORT: Duration = Duration::from_millis(50);

    fn start(program: &str, args: &[&str]) -> Result<Started, AcpError> {
        let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
        AgentProcess::start(program, &args, Path::new("/"), None)
    }

    async fn exited(process: &AgentProcess) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !process.has_exited() {
            assert!(Instant::now() < deadline, "the process did not exit");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn an_agent_that_exits_on_its_own_is_waited_for() {
        let started = start("true", &[]).unwrap();
        exited(&started.process).await;
        assert!(started.process.id().is_some());
    }

    #[tokio::test]
    async fn an_agent_that_exits_once_its_input_closes_stops_within_the_grace() {
        let Started { process, stdin, .. } = start("cat", &[]).unwrap();
        drop(stdin);
        let begun = Instant::now();
        process.stop(Duration::from_secs(10)).await;
        assert!(begun.elapsed() < Duration::from_secs(5));
    }

    /// The signal that ended the process, once it was waited for.
    fn signal(status: &Status) -> Option<i32> {
        use std::os::unix::process::ExitStatusExt;
        status
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .and_then(|status| status.signal())
    }

    #[tokio::test]
    async fn an_agent_that_stays_is_sent_sigterm_after_the_grace() {
        let Started { process, stdin, .. } = start("sleep", &["30"]).unwrap();
        let status = Arc::clone(&process.status);
        drop(stdin);
        let begun = Instant::now();
        process.stop(SHORT).await;
        let took = begun.elapsed();
        assert!(took >= SHORT && took < Duration::from_secs(5), "{took:?}");
        assert_eq!(signal(&status), Some(15), "ended by SIGTERM");
    }

    #[tokio::test]
    async fn an_agent_that_ignores_sigterm_is_killed_after_the_second_grace() {
        // `SIGTERM` ignored is inherited across the `exec`.
        let Started {
            process,
            stdin,
            mut stdout,
        } = start("sh", &["-c", "trap '' TERM; echo ready; exec sleep 30"]).unwrap();
        drop(stdin);
        // The trap is set before the stop begins.
        let mut ready = [0_u8; 6];
        stdout.read_exact(&mut ready).await.unwrap();
        let status = Arc::clone(&process.status);
        let begun = Instant::now();
        process.stop(SHORT).await;
        let took = begun.elapsed();
        assert!(
            took >= SHORT * 2 && took < Duration::from_secs(5),
            "{took:?}"
        );
        assert_eq!(signal(&status), Some(9), "ended by SIGKILL");
    }

    #[tokio::test]
    async fn an_agent_let_go_of_is_killed() {
        let Started { process, stdin, .. } = start("sleep", &["30"]).unwrap();
        let status = Arc::clone(&process.status);
        drop(process);
        let deadline = Instant::now() + Duration::from_secs(10);
        while status.lock().unwrap().is_none() {
            assert!(Instant::now() < deadline, "the process was not killed");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        drop(stdin);
        assert_eq!(signal(&status), Some(9), "killed");
    }

    #[tokio::test]
    async fn a_missing_directory_is_named_in_the_error() {
        let started =
            AgentProcess::start("true", &[], Path::new("/nonexistent/roomkit-acp-cwd"), None);
        let Err(error) = started else {
            panic!("started in a missing directory");
        };
        assert!(
            error.to_string().contains("/nonexistent/roomkit-acp-cwd"),
            "{error}"
        );
    }
}
