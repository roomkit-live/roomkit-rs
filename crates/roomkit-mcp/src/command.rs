//! What starts an MCP server over stdio (RFC §21.2 `command`, `args`): a
//! program and its arguments, as a list, never through a shell.

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use crate::error::McpError;
use crate::server::McpServer;

/// How long connecting may take by default: the handshake and the listing
/// of the tools, a first start that fetches its package included.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);

/// How long a server is given by default to exit once its input is closed,
/// then once its group is sent `SIGTERM`, before `SIGKILL`: the reference
/// implementation's.
const DEFAULT_CLOSE_GRACE: Duration = Duration::from_secs(2);

/// The variables of the host's environment a server inherits, as the
/// reference implementation's MCP SDK gives them; any other the host passes
/// with [`McpCommand::env`]. A host's environment holds its own secrets (a
/// model's API key), which are not the server's.
#[cfg(not(windows))]
const INHERITED: &[&str] = &["HOME", "LOGNAME", "PATH", "SHELL", "TERM", "USER"];

#[cfg(windows)]
const INHERITED: &[&str] = &[
    "APPDATA",
    "HOMEDRIVE",
    "HOMEPATH",
    "LOCALAPPDATA",
    "PATH",
    "PATHEXT",
    "PROCESSOR_ARCHITECTURE",
    "SYSTEMDRIVE",
    "SYSTEMROOT",
    "TEMP",
    "USERNAME",
    "USERPROFILE",
];

/// What starts an MCP server over stdio (RFC §21.2): a program and its
/// arguments, passed as a list and never through a shell, so that no
/// argument is read as anything but itself.
///
/// The server inherits only `HOME`, `LOGNAME`, `PATH`, `SHELL`, `TERM` and
/// `USER` of the host's environment (on Windows, the variables a program
/// needs to run), and the variables given with [`env`](Self::env); its
/// standard error is read as it writes, each line logged at debug level
/// under the target `roomkit::mcp`, so that it never fills a pipe, and its
/// last 20 lines, 4 KiB at most, end the error of a connection that fails
/// its handshake, its listing or its connect bound.
///
/// On Unix the server leads a process group of its own, which is stopped
/// whole, so that what it starts (the real server, behind `npx` or `uvx`)
/// goes with it: `SIGTERM` to the group, then `SIGKILL` once the grace is
/// over. In its own group, the server no longer receives a terminal's
/// Ctrl+C: a host stops it through its close, its kit's shutdown or the
/// drop of its last handle, which a Ctrl+C handler that shuts the kit down
/// covers. Elsewhere, the server's process alone is killed.
///
/// ```
/// use std::time::Duration;
///
/// use roomkit_mcp::McpCommand;
///
/// let command = McpCommand::new("npx")
///     .args(["-y", "@modelcontextprotocol/server-filesystem", "/srv/shop"])
///     .env("LOG_LEVEL", "warning")
///     .connect_timeout(Duration::from_secs(120));
/// # let _ = command;
/// ```
#[derive(Clone)]
pub struct McpCommand {
    program: OsString,
    args: Vec<OsString>,
    env: Vec<(OsString, OsString)>,
    current_dir: Option<PathBuf>,
    pub(crate) connect_timeout: Duration,
    pub(crate) close_grace: Duration,
}

impl McpCommand {
    /// The command starting `program`, a path or a name the host's `PATH`
    /// resolves, with no argument, connecting within 60 seconds and given
    /// 2 seconds to exit at its close.
    pub fn new(program: impl Into<OsString>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            env: Vec::new(),
            current_dir: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            close_grace: DEFAULT_CLOSE_GRACE,
        }
    }

    /// The same command, `arg` after its arguments.
    #[must_use]
    pub fn arg(mut self, arg: impl Into<OsString>) -> Self {
        self.args.push(arg.into());
        self
    }

    /// The same command, `args` after its arguments.
    #[must_use]
    pub fn args<I, A>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = A>,
        A: Into<OsString>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// The same command, the server's environment holding `key` set to
    /// `value`, over an inherited one of that name.
    #[must_use]
    pub fn env(mut self, key: impl Into<OsString>, value: impl Into<OsString>) -> Self {
        self.env.push((key.into(), value.into()));
        self
    }

    /// The same command, the server started in `dir`.
    #[must_use]
    pub fn current_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.current_dir = Some(dir.into());
        self
    }

    /// The same command, connecting within `bound`: the handshake and the
    /// listing of the tools, past which the process is stopped and
    /// [`McpError::Timeout`] returned.
    #[must_use]
    pub fn connect_timeout(mut self, bound: Duration) -> Self {
        self.connect_timeout = bound;
        self
    }

    /// The same command, the server given `grace` to exit once its input
    /// is closed at its close, then `grace` again once its group is sent
    /// `SIGTERM` (at its close, a failed connection or a drop), before
    /// `SIGKILL`.
    #[must_use]
    pub fn close_grace(mut self, grace: Duration) -> Self {
        self.close_grace = grace;
        self
    }

    /// Starts the server, runs the MCP handshake and lists its tools, within
    /// the connect bound ([`connect_timeout`](Self::connect_timeout), 60
    /// seconds by default).
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Command`] or [`McpError::Spawn`] when no process
    /// could be started, and [`McpError::Handshake`], [`McpError::Listing`]
    /// or [`McpError::Timeout`] when the connection failed part-way: the
    /// process is then stopped and waited for before the error is returned
    /// (RFC §21.2). Each of the three carries the last lines the server
    /// wrote on its standard error, when it wrote any: a handshake's or a
    /// listing's message ends with them, and they are a timeout's `stderr`.
    pub async fn connect(self) -> Result<McpServer, McpError> {
        McpServer::connect(self).await
    }

    /// The program, as errors and logs name the server: never its
    /// arguments, which may hold secrets.
    pub(crate) fn program(&self) -> String {
        self.program.to_string_lossy().into_owned()
    }

    /// The process to start: the program and its arguments as a list, the
    /// environment cleared but for the inherited variables and the
    /// command's own, the three standard streams piped, killed if dropped.
    ///
    /// # Errors
    ///
    /// Returns [`McpError::Command`] when the command cannot start a
    /// process.
    pub(crate) fn process(&self) -> Result<tokio::process::Command, McpError> {
        self.check()?;
        let mut process = tokio::process::Command::new(&self.program);
        process
            .args(&self.args)
            .env_clear()
            .envs(inherited())
            .envs(self.env.iter().map(|(key, value)| (key, value)))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(dir) = &self.current_dir {
            process.current_dir(dir);
        }
        // The server leads a process group of its own, so that the
        // processes it starts (the real server, behind `npx` or `uvx`) are
        // stopped with it.
        #[cfg(unix)]
        process.process_group(0);
        Ok(process)
    }

    /// Whether the command can start a process.
    fn check(&self) -> Result<(), McpError> {
        let refuse = |reason: &str| {
            Err(McpError::Command {
                reason: reason.to_owned(),
            })
        };
        if self.program.is_empty() {
            return refuse("the program is empty");
        }
        if holds_nul(&self.program) || self.args.iter().any(|arg| holds_nul(arg)) {
            return refuse("the program or an argument holds a NUL");
        }
        for (key, value) in &self.env {
            let bytes = key.as_encoded_bytes();
            if bytes.is_empty() || bytes.contains(&b'=') || holds_nul(key) {
                return refuse("an environment variable's name is empty or holds '=' or a NUL");
            }
            if holds_nul(value) {
                return refuse("an environment variable's value holds a NUL");
            }
        }
        Ok(())
    }
}

/// The host's inherited variables that are set, but a value that reads as
/// a shell function, which a shell would run.
fn inherited() -> Vec<(&'static str, OsString)> {
    INHERITED
        .iter()
        .filter_map(|&key| std::env::var_os(key).map(|value| (key, value)))
        .filter(|(_, value)| !value.as_encoded_bytes().starts_with(b"()"))
        .collect()
}

fn holds_nul(text: &OsStr) -> bool {
    text.as_encoded_bytes().contains(&0)
}

impl fmt::Debug for McpCommand {
    /// The program and the names of the variables given, never the
    /// arguments nor the values, which may hold secrets.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let keys: Vec<&OsStr> = self.env.iter().map(|(key, _)| key.as_os_str()).collect();
        f.debug_struct("McpCommand")
            .field("program", &self.program)
            .field("args", &self.args.len())
            .field("env", &keys)
            .field("current_dir", &self.current_dir)
            .field("connect_timeout", &self.connect_timeout)
            .field("close_grace", &self.close_grace)
            .finish()
    }
}
