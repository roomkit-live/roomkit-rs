//! The process group a server leads (RFC §21.2: a server is stopped, with
//! what it started): on Unix the server is started in a group of its own
//! (`process_group(0)`), whose id is its process's, and the group is
//! stopped whole, as the reference implementation's MCP SDK stops a
//! server's session: `SIGTERM` to the group, a wait for it to go, then
//! `SIGKILL`. The real server behind `npx` or `uvx` is the wrapper's child,
//! and goes with it.
//!
//! A group is signalled only while its id is the server's: while the
//! server's process is not waited for yet, or at once after, its members
//! holding the id; never the caller's own group (`0`) nor every process
//! (`-1`). Elsewhere than on Unix, the server's process alone is killed.

use std::time::Duration;

use tokio::process::Child;
use tokio::time::Instant;

/// How often a group is asked whether anything of it is left.
const POLL: Duration = Duration::from_millis(20);

/// How long a killed process is waited for: a process that a kill does not
/// end at once is stuck in the kernel, and is left to the runtime, which
/// reaps it once it ends.
const KILL_WAIT: Duration = Duration::from_secs(5);

/// A signal a group is sent.
#[derive(Clone, Copy, Debug)]
enum Signal {
    Term,
    Kill,
}

/// The process group a server leads.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Group {
    /// The group's id, the server's process's: none without an id, or one
    /// that names no single group.
    id: Option<i32>,
}

impl Group {
    /// The group the server whose process is `pid` leads.
    pub(crate) fn led_by(pid: Option<u32>) -> Self {
        let id = pid
            .and_then(|pid| i32::try_from(pid).ok())
            .filter(|&id| id > 1);
        Self { id }
    }

    /// Ends the group: `SIGTERM` to it, its leader `child` waited for as it
    /// exits, `SIGKILL` once `wait` is over and anything of it is left; the
    /// leader killed alone if it left the group; then the leader waited
    /// for, within [`KILL_WAIT`]. A group already gone is no error.
    pub(crate) async fn end(self, child: &mut Child, wait: Duration) {
        if self.signal(Some(Signal::Term)) {
            let deadline = Instant::now() + wait;
            // Asked before the leader is waited for: the id is the group's.
            while self.signal(None) {
                let _exited = child.try_wait();
                if Instant::now() >= deadline {
                    self.signal(Some(Signal::Kill));
                    break;
                }
                tokio::time::sleep(POLL).await;
            }
        }
        if matches!(child.try_wait(), Ok(None)) {
            let _already_gone = child.start_kill();
        }
        if tokio::time::timeout(KILL_WAIT, child.wait()).await.is_err() {
            tracing::warn!(
                target: "roomkit::mcp",
                group = self.id,
                "an MCP server's process did not end once killed"
            );
        }
    }

    /// A guard that sends the group `SIGKILL` when dropped armed.
    pub(crate) fn guard(self) -> GroupGuard {
        GroupGuard {
            group: self,
            armed: true,
        }
    }

    /// Sends the group `signal`, or with none asks whether anything of it
    /// is left; whether it was there.
    fn signal(self, signal: Option<Signal>) -> bool {
        let Some(id) = self.id else {
            return false;
        };
        send(id, signal)
    }
}

#[cfg(unix)]
fn send(id: i32, signal: Option<Signal>) -> bool {
    use nix::errno::Errno;
    use nix::sys::signal::{Signal as Unix, killpg};
    use nix::unistd::Pid;

    let signal = signal.map(|signal| match signal {
        Signal::Term => Unix::SIGTERM,
        Signal::Kill => Unix::SIGKILL,
    });
    match killpg(Pid::from_raw(id), signal) {
        Ok(()) => true,
        Err(Errno::ESRCH) => false,
        Err(error) => {
            tracing::warn!(
                target: "roomkit::mcp",
                group = id,
                %error,
                "an MCP server's process group could not be signalled"
            );
            false
        }
    }
}

#[cfg(not(unix))]
fn send(_id: i32, _signal: Option<Signal>) -> bool {
    false
}

/// Sends a server's group `SIGKILL` when dropped armed: the watcher holding
/// it is dropped before it waited for the server only when the runtime
/// goes, and nothing of the group stays then. It sends nothing once
/// disarmed, the server waited for.
#[derive(Debug)]
pub(crate) struct GroupGuard {
    group: Group,
    armed: bool,
}

impl GroupGuard {
    /// The group is ended, or its leader waited for: dropping sends
    /// nothing.
    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for GroupGuard {
    fn drop(&mut self) {
        if self.armed {
            self.group.signal(Some(Signal::Kill));
        }
    }
}
