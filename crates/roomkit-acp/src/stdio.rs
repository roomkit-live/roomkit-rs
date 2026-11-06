//! The default transport: the agent spawned here, spoken to over its stdio.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use crate::client::AcpClient;
use crate::client_side::ClientSideConnection;
use crate::env::{absolute_path, resolve_spawn_env};
use crate::error::AcpError;
use crate::process::{AgentProcess, SHUTDOWN_GRACE, Started};
use crate::transport::AcpTransport;

/// What [`StdioAcpTransport::new`] takes beside the command and the
/// directory: Python's `env=None, inherit_env=None`.
#[derive(Clone, Default)]
pub struct StdioAcpTransportOptions {
    /// Variables added to the agent's environment, over the default one
    /// (`HOME`, `LOGNAME`, `PATH`, `SHELL`, `TERM` and `USER` of the host's,
    /// on Unix) and over the inherited ones.
    pub env: Option<BTreeMap<String, String>>,
    /// Names of the host's variables the agent is given, read at each
    /// spawn, so that a reconnect takes a rotated value; a name the host
    /// does not set is skipped, and `env` wins over it. The default
    /// environment drops what a coding agent's tooling relies on: without
    /// `SSH_AUTH_SOCK`, every git-over-SSH operation asks for a key's
    /// passphrase on the terminal.
    pub inherit_env: Option<Vec<String>>,
}

impl fmt::Debug for StdioAcpTransportOptions {
    /// The names of the variables, never their values, which may hold
    /// secrets.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StdioAcpTransportOptions")
            .field(
                "env",
                &self.env.as_ref().map(|env| env.keys().collect::<Vec<_>>()),
            )
            .field("inherit_env", &self.inherit_env)
            .finish()
    }
}

/// The agent spawned here as a local process, spoken to over its stdio:
/// Python's `StdioACPTransport`.
///
/// The command runs directly, a program and its arguments, never through a
/// shell, in `cwd`, its environment cleared but for the default variables
/// and the ones [`StdioAcpTransportOptions`] adds. Its standard error is
/// read and dropped, so that it never blocks the agent. The agent's
/// messages are read whole, one JSON-RPC message per line (HITL 3f858b12).
///
/// [`close`](AcpTransport::close) stops the agent as the Python SDK does:
/// the connection closed, its input with it, two seconds to exit, then
/// `SIGTERM` and two seconds more, then `SIGKILL`. Only the agent's process
/// is signalled, not what it started, which ends with its input as a rule.
/// A transport dropped without a close kills its agent.
pub struct StdioAcpTransport {
    command: Vec<String>,
    cwd: PathBuf,
    env: Option<BTreeMap<String, String>>,
    inherit_env: Vec<String>,
    running: Mutex<Option<Running>>,
}

/// The agent of the last [`open`](AcpTransport::open), until the close.
struct Running {
    connection: ClientSideConnection,
    process: AgentProcess,
}

impl StdioAcpTransport {
    /// The transport spawning `command`, the program then its arguments, in
    /// `cwd`, an absolute path (`~` read as the host's `HOME`).
    ///
    /// # Errors
    ///
    /// [`AcpError::Value`], in Python's words, for an empty command, an
    /// empty argument, a relative `cwd`, or an empty name in `inherit_env`.
    pub fn new(
        command: Vec<String>,
        cwd: impl AsRef<Path>,
        options: StdioAcpTransportOptions,
    ) -> Result<Self, AcpError> {
        if command.is_empty() {
            return Err(AcpError::Value(
                "command must be a non-empty sequence of arguments".to_owned(),
            ));
        }
        if command.iter().any(String::is_empty) {
            return Err(AcpError::Value(
                "every command argument must be a non-empty string".to_owned(),
            ));
        }
        let StdioAcpTransportOptions { env, inherit_env } = options;
        let inherit_env = inherit_env.unwrap_or_default();
        if inherit_env.iter().any(String::is_empty) {
            return Err(AcpError::Value(
                "inherit_env must be a sequence of non-empty variable names".to_owned(),
            ));
        }
        Ok(Self {
            command,
            cwd: absolute_path(cwd.as_ref(), "cwd")?,
            env,
            inherit_env,
            running: Mutex::new(None),
        })
    }

    /// The id of the agent's process the last [`open`](AcpTransport::open)
    /// spawned, none before it and after the close: Python's `process`, the
    /// process object (HITL 3592a76c).
    pub fn process(&self) -> Option<u32> {
        self.lock()
            .as_ref()
            .and_then(|running| running.process.id())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Running>> {
        self.running.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Spawns the agent and opens the connection over its stdio, the
    /// variables `inherit_env` names read now.
    fn spawn(&self, client: Arc<dyn AcpClient>) -> Result<ClientSideConnection, AcpError> {
        let env = resolve_spawn_env(&self.inherit_env, self.env.as_ref(), |name: &str| {
            std::env::var_os(name)
        });
        let (program, args) = self.command.split_first().ok_or_else(|| {
            AcpError::Value("command must be a non-empty sequence of arguments".to_owned())
        })?;
        let Started {
            process,
            stdin,
            stdout,
        } = AgentProcess::start(program, args, &self.cwd, env.as_ref())?;
        let connection = ClientSideConnection::new(client, stdout, stdin);
        // An agent an earlier open left running is let go of, which kills
        // it.
        let _replaced = self.lock().replace(Running {
            connection: connection.clone(),
            process,
        });
        Ok(connection)
    }
}

impl AcpTransport for StdioAcpTransport {
    type Connection = ClientSideConnection;

    fn name(&self) -> &'static str {
        "stdio"
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait's open is a future: nothing starts until it is awaited"
    )]
    async fn open(&self, client: Arc<dyn AcpClient>) -> Result<ClientSideConnection, AcpError> {
        self.spawn(client)
    }

    async fn close(&self) {
        let running = self.lock().take();
        if let Some(Running {
            connection,
            process,
        }) = running
        {
            connection.close().await;
            process.stop(SHUTDOWN_GRACE).await;
        }
    }

    fn is_alive(&self) -> bool {
        self.lock()
            .as_ref()
            .is_none_or(|running| !running.process.has_exited())
    }
}

impl fmt::Debug for StdioAcpTransport {
    /// The program and the names of the variables, never the arguments nor
    /// the values, which may hold secrets.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StdioAcpTransport")
            .field("program", &self.command.first())
            .field("args", &self.command.len().saturating_sub(1))
            .field("cwd", &self.cwd)
            .field(
                "env",
                &self.env.as_ref().map(|env| env.keys().collect::<Vec<_>>()),
            )
            .field("inherit_env", &self.inherit_env)
            .field("process", &self.process())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refused(command: &[&str], cwd: &str, inherit_env: Option<&[&str]>) -> String {
        let options = StdioAcpTransportOptions {
            env: None,
            inherit_env: inherit_env.map(|names| names.iter().map(|&name| name.into()).collect()),
        };
        let command = command.iter().map(|&arg| arg.to_owned()).collect();
        match StdioAcpTransport::new(command, cwd, options) {
            Err(AcpError::Value(message)) => message,
            other => panic!("not refused: {other:?}"),
        }
    }

    // Python: tests/test_channels/test_acp.py::TestACPChannel::test_stdio_transport_validates_its_own_arguments
    #[test]
    fn the_transport_validates_its_own_arguments() {
        assert_eq!(
            refused(&[], "/srv", None),
            "command must be a non-empty sequence of arguments"
        );
        assert_eq!(
            refused(&["agent", ""], "/srv", None),
            "every command argument must be a non-empty string"
        );
        assert_eq!(
            refused(&["agent"], "relative", None),
            "cwd must be an absolute path"
        );
        let transport = StdioAcpTransport::new(
            vec!["agent".into()],
            "/srv",
            StdioAcpTransportOptions::default(),
        )
        .unwrap();
        assert_eq!(transport.name(), "stdio");
        assert_eq!(transport.process(), None);
        assert!(transport.is_alive());
    }

    // Python: tests/test_channels/test_acp.py::TestACPChannel::test_inherit_env_validation
    #[test]
    fn inherit_env_names_are_not_empty() {
        assert_eq!(
            refused(&["agent"], "/srv", Some(&["SSH_AUTH_SOCK", ""])),
            "inherit_env must be a sequence of non-empty variable names"
        );
        let options = StdioAcpTransportOptions {
            inherit_env: Some(vec!["SSH_AUTH_SOCK".into()]),
            ..StdioAcpTransportOptions::default()
        };
        let transport = StdioAcpTransport::new(vec!["agent".into()], "/srv", options).unwrap();
        assert_eq!(transport.inherit_env, ["SSH_AUTH_SOCK"]);
        let default = StdioAcpTransport::new(
            vec!["agent".into()],
            "/srv",
            StdioAcpTransportOptions::default(),
        )
        .unwrap();
        assert_eq!(default.inherit_env, Vec::<String>::new());
    }
}
