//! The server's process (RFC §21.2): started without a shell, with the
//! host's environment withheld; stopped at its close, at the kit's
//! shutdown and when its last handle is dropped; released before a failed
//! connection reports; never blocked by its standard error. A process gone
//! from `/proc` was waited for: no zombie is left.

use std::time::Duration;

use tokio::time::Instant;

use pretty_assertions::assert_eq;
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{ToolContext, ToolError, ToolOutput, Toolset};
use roomkit_mcp::{McpCommand, McpError};

use super::support::{GRACE, PidFile, Room, agent, channel, exists, fake, gone_within, room_id};

fn echo(text: &str) -> roomkit::model::ToolCall {
    let arguments = serde_json::json!({ "text": text });
    roomkit::model::ToolCall::new("call_1", "echo", arguments.as_object().cloned().unwrap())
}

fn context() -> ToolContext {
    ToolContext::new(room_id(), channel("assistant"))
}

/// RFC §21.2: the server stops when its connection closes, and the close
/// returns once it was waited for.
#[tokio::test]
async fn close_stops_the_server_and_waits_for_it() {
    let pid_file = PidFile::new();
    let server = pid_file.on(fake("normal")).connect().await.unwrap();
    let pid = pid_file.pid();
    let running = (exists(pid), server.process_id());
    server.close().await;
    assert_eq!(
        (running, exists(pid), server.process_id()),
        ((true, Some(pid)), false, None)
    );
}

/// A server that stays once its input closes is killed past the grace.
#[tokio::test]
async fn a_server_that_stays_once_its_input_closes_is_killed_after_the_grace() {
    let pid_file = PidFile::new();
    let server = pid_file.on(fake("deaf")).connect().await.unwrap();
    let pid = pid_file.pid();
    let started = Instant::now();
    let closed = tokio::time::timeout(Duration::from_secs(5), server.close()).await;
    let took = started.elapsed();
    assert!(closed.is_ok() && !exists(pid) && took >= GRACE, "{took:?}");
}

/// The server stops when the last handle on it is dropped: killed at once,
/// then reaped by the runtime.
#[tokio::test]
async fn dropping_the_last_handle_kills_the_server() {
    let pid_file = PidFile::new();
    let server = pid_file.on(fake("deaf")).connect().await.unwrap();
    let pid = pid_file.pid();
    let clone = server.clone();
    drop(server);
    let after_one = exists(pid);
    drop(clone);
    let gone = gone_within(pid, Duration::from_secs(5)).await;
    assert_eq!((after_one, gone), (true, true));
}

/// A server that exits while its connection is open is waited for at
/// once: no zombie stays until the connection closes. The call it was
/// serving fails.
#[tokio::test]
async fn a_server_that_exits_is_waited_for_at_once() {
    let pid_file = PidFile::new();
    let server = pid_file.on(fake("normal")).connect().await.unwrap();
    let pid = pid_file.pid();
    let mut call = echo("");
    call.name = "crash".to_owned();
    let failed = server.call(call, context()).await;
    let reaped = gone_within(pid, Duration::from_secs(5)).await;
    assert!(
        matches!(&failed, Err(ToolError::Raised { kind, .. }) if kind == "McpError") && reaped,
        "{failed:?}"
    );
    server.close().await;
}

/// A server started through a wrapper (as `npx` or `uvx` start one) is the
/// wrapper's child, and the process an agent's tools reach.
struct Wrapped {
    wrapper: PidFile,
    server: PidFile,
}

impl Wrapped {
    fn new() -> Self {
        Self {
            wrapper: PidFile::new(),
            server: PidFile::new(),
        }
    }

    /// The wrapper, starting the fake server in `mode` as its child.
    fn command(&self, mode: &str) -> McpCommand {
        self.server
            .on_child(self.wrapper.on(fake(&format!("wrap:{mode}"))))
    }

    /// Whether the wrapper is gone at once, and the server, which the
    /// system's init reaps once the wrapper is gone, within a bound.
    async fn gone(&self) -> (bool, bool) {
        let wrapper = !exists(self.wrapper.pid());
        let server = gone_within(self.server.pid(), Duration::from_secs(5)).await;
        (wrapper, server)
    }
}

/// RFC §21.2: a wrapped server that outlives its input's end stops with its
/// wrapper at the close: the wrapper leads a process group of its own, and
/// the group is stopped whole (as the Python SDK kills a server's session).
#[tokio::test]
async fn a_wrapped_server_that_outlives_its_input_stops_at_the_close() {
    let wrapped = Wrapped::new();
    let server = wrapped.command("deaf").connect().await.unwrap();
    server.close().await;
    assert_eq!(wrapped.gone().await, (true, true));
}

/// A process of the server's group that ignores `SIGTERM` is sent
/// `SIGKILL` once the grace is over.
#[tokio::test]
async fn a_process_of_the_group_that_ignores_sigterm_is_killed_after_the_grace() {
    let stubborn = PidFile::new();
    let server = fake("deaf")
        .arg("--stubborn-pid-file")
        .arg(stubborn.path())
        .connect()
        .await
        .unwrap();
    let pid = stubborn.pid();
    let running = exists(pid);
    server.close().await;
    let gone = gone_within(pid, Duration::from_secs(5)).await;
    assert_eq!((running, gone), (true, true));
}

/// A wrapped server stops when the last handle on it is dropped.
#[tokio::test]
async fn a_wrapped_server_stops_when_its_last_handle_is_dropped() {
    let wrapped = Wrapped::new();
    let server = wrapped.command("deaf").connect().await.unwrap();
    let leader = wrapped.wrapper.pid();
    drop(server);
    let wrapper_gone = gone_within(leader, Duration::from_secs(5)).await;
    assert_eq!((wrapper_gone, wrapped.gone().await.1), (true, true));
}

/// A wrapped server stops when its runtime is dropped with the handle on it
/// still held: the watcher, dropped with the runtime, sends the group
/// `SIGKILL`. The wrapper, this process's child, stays a zombie until this
/// process exits, as no runtime is left to reap it.
#[test]
fn a_wrapped_server_stops_when_its_runtime_is_dropped() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let wrapped = Wrapped::new();
    let server = runtime.block_on(wrapped.command("deaf").connect()).unwrap();
    let (leader, child) = (wrapped.wrapper.pid(), wrapped.server.pid());
    drop(runtime);
    let ended = (0..500).find(|_| {
        let ended = !running(leader) && !exists(child);
        if !ended {
            std::thread::sleep(Duration::from_millis(10));
        }
        ended
    });
    drop(server);
    assert!(ended.is_some());
}

/// Whether the process `pid` runs: there, and no zombie.
fn running(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        stat.rsplit_once(')')
            .is_some_and(|(_, after)| !after.trim_start().starts_with('Z'))
    })
}

/// RFC §21.2: a wrapped server that never answers the handshake is stopped
/// with its wrapper before the error is returned.
#[tokio::test]
async fn a_wrapped_server_that_never_answers_is_stopped_before_the_error() {
    let wrapped = Wrapped::new();
    let refused = wrapped
        .command("silent-deaf")
        .connect_timeout(Duration::from_millis(300))
        .connect()
        .await
        .err();
    assert!(matches!(refused, Some(McpError::Timeout { .. })));
    assert_eq!(wrapped.gone().await, (true, true));
}

/// The kit's shutdown closes the agent serving the server's tools, which
/// stops the server.
#[tokio::test]
async fn the_kit_s_shutdown_stops_the_server_of_its_agent() {
    let pid_file = PidFile::new();
    let server = pid_file.on(fake("normal")).connect().await.unwrap();
    let pid = pid_file.pid();
    let model = ScriptedProvider::new([ScriptedRound::answering("Hello.")]);
    let room = Room::with(agent(&model).with_tools(server).unwrap()).await;
    room.says("Hello").await;
    let running = exists(pid);
    room.kit.shutdown().await;
    assert_eq!((running, exists(pid)), (true, false));
}

/// RFC §21.2: a command that names no program fails to start, and no
/// process runs.
#[tokio::test]
async fn a_missing_command_fails_to_start() {
    let refused = McpCommand::new("/nonexistent/roomkit-mcp-server")
        .connect()
        .await
        .err();
    assert!(
        matches!(&refused, Some(McpError::Spawn { program, source })
            if program == "/nonexistent/roomkit-mcp-server"
                && source.kind() == std::io::ErrorKind::NotFound),
        "{refused:?}"
    );
}

/// RFC §21.2: a server that never answers the handshake is cut at the
/// connect bound, and its process is waited for before the error.
#[tokio::test]
async fn a_server_that_never_answers_the_handshake_is_stopped_before_the_error() {
    let pid_file = PidFile::new();
    let command = pid_file
        .on(fake("silent"))
        .connect_timeout(Duration::from_millis(100));
    let refused = command.connect().await.err();
    assert!(
        matches!(refused, Some(McpError::Timeout { after, .. }) if after == Duration::from_millis(100))
            && !exists(pid_file.pid())
    );
}

/// RFC §21.2: a server that exits before it answers fails the handshake,
/// and is waited for before the error.
#[tokio::test]
async fn a_server_that_exits_at_once_fails_the_handshake_and_is_waited_for() {
    let pid_file = PidFile::new();
    let refused = pid_file.on(fake("exit")).connect().await.err();
    assert!(
        matches!(refused, Some(McpError::Handshake { .. })) && !exists(pid_file.pid()),
        "{refused:?}"
    );
}

/// The lines of standard error a failed connection's error ends with, as
/// its display shows them: each indented, never a log line of its own.
fn stderr_lines(error: &McpError) -> Vec<String> {
    let shown = error.to_string();
    let stderr = shown
        .split_once("; its standard error ended with:\n")
        .map(|(_, stderr)| stderr);
    assert!(
        stderr.is_some_and(|stderr| stderr.split('\n').all(|line| line.starts_with("  "))),
        "{shown:?}"
    );
    stderr
        .unwrap()
        .split('\n')
        .map(|line| line.strip_prefix("  ").unwrap().to_owned())
        .collect()
}

/// What a server wrote on its standard error before its connection failed
/// ends the error, whether it failed its handshake, its listing or the
/// connect bound: its last 20 lines, their control characters replaced,
/// why it failed (a missing package, a traceback, an image being pulled).
#[tokio::test]
async fn a_failed_connection_s_error_ends_with_the_server_s_last_stderr_lines() {
    let mut exited: Vec<String> = (6..=24).map(|n| format!("line {n}")).collect();
    exited.push("\u{fffd}[31mModuleNotFoundError\u{fffd}[0m: No module named 'mcp'".to_owned());
    let waiting = vec![
        "pulling the server's image".to_owned(),
        "\u{fffd}[33mlayer 1/3\u{fffd}[0m: waiting".to_owned(),
    ];
    for (mode, expected) in [
        ("stderr-exit", exited),
        ("stderr-listing-error", waiting.clone()),
        ("stderr-hang", waiting),
    ] {
        let refused = fake(mode)
            .connect_timeout(Duration::from_millis(300))
            .connect()
            .await
            .err()
            .unwrap();
        let failed_as = match (mode, &refused) {
            ("stderr-exit", McpError::Handshake { .. })
            | ("stderr-listing-error", McpError::Listing { .. }) => true,
            ("stderr-hang", McpError::Timeout { stderr, .. }) => stderr.len() == 2,
            _ => false,
        };
        assert!(failed_as, "{mode}: {refused:?}");
        assert_eq!(stderr_lines(&refused), expected, "{mode}");
    }
}

/// The standard error a failed connection's error keeps takes 4 KiB at
/// most, each line cut, the last lines kept.
#[tokio::test]
async fn the_stderr_a_failed_connection_s_error_keeps_takes_4_kib_at_most() {
    let refused = fake("stderr-exit-long").connect().await.err().unwrap();
    let lines = stderr_lines(&refused);
    let firsts: Vec<char> = lines
        .iter()
        .filter_map(|line| line.chars().next())
        .collect();
    let bytes: usize = lines.iter().map(String::len).sum();
    assert!(
        firsts == ['5', '6', '7', '8']
            && bytes <= 4096
            && lines.iter().all(|line| line.ends_with('…')),
        "{firsts:?} {bytes}"
    );
}

/// What a server's words make up in an error cannot drive a terminal or
/// forge a log line: its control characters, line separators and bidi
/// controls are replaced, and a line break it sends is indented, so that
/// no line of it starts a line of its own.
#[tokio::test]
async fn a_server_s_words_in_an_error_cannot_forge_a_log_line() {
    let refused = fake("initialize-error").connect().await.err().unwrap();
    let shown = refused.to_string();
    let McpError::Handshake { message, .. } = &refused else {
        panic!("{refused:?}");
    };
    let forbidden = |c: char| {
        (c.is_control() && c != '\n')
            || matches!(c, '\u{2028}' | '\u{2029}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
    };
    assert!(
        message.contains("FORGED")
            && !shown.chars().any(forbidden)
            && shown.split('\n').skip(1).all(|line| line.starts_with("  ")),
        "{shown:?}"
    );
}

/// RFC §21.2: a listing the server fails stops it before the error.
#[tokio::test]
async fn a_listing_that_fails_stops_the_server_before_the_error() {
    let pid_file = PidFile::new();
    let refused = pid_file.on(fake("listing-error")).connect().await.err();
    assert!(
        matches!(&refused, Some(McpError::Listing { message, .. }) if message.contains("tools/list"))
            && !exists(pid_file.pid()),
        "{refused:?}"
    );
}

/// The server's standard error is read as it writes: 4 MiB of it, far
/// over a pipe's buffer, never blocks the server.
#[tokio::test]
async fn a_server_flooding_its_standard_error_is_not_blocked() {
    let server = fake("stderr-flood").connect().await.unwrap();
    let answer = server.call(echo("still here"), context()).await;
    server.close().await;
    assert_eq!(answer, Ok(ToolOutput::Text("still here".to_owned())));
}

/// A command that cannot start a process is refused before anything runs.
#[tokio::test]
async fn a_command_that_cannot_start_a_process_is_refused() {
    let refused = [
        McpCommand::new("").connect().await.err(),
        fake("normal").env("A=B", "1").connect().await.err(),
        fake("normal").env("", "1").connect().await.err(),
        fake("normal").env("KEY", "a\0b").connect().await.err(),
        fake("normal").arg("a\0b").connect().await.err(),
    ];
    assert!(
        refused
            .iter()
            .all(|error| matches!(error, Some(McpError::Command { .. }))),
        "{refused:?}"
    );
}

/// The server inherits only the named variables of the host's
/// environment, and those the command gives: the host's own (a model's
/// key) are not the server's.
#[tokio::test]
async fn the_server_inherits_only_the_named_variables_and_its_own() {
    let server = fake("normal")
        .env("ROOMKIT_FAKE", "1")
        .connect()
        .await
        .unwrap();
    let mut call = echo("");
    call.name = "env".to_owned();
    let answer = server.call(call, context()).await.unwrap();
    server.close().await;
    let ToolOutput::Text(names) = answer else {
        panic!("{answer:?}");
    };
    let allowed = [
        "HOME",
        "LOGNAME",
        "PATH",
        "SHELL",
        "TERM",
        "USER",
        "ROOMKIT_FAKE",
    ];
    assert!(
        names.split(' ').all(|name| allowed.contains(&name)) && names.contains("ROOMKIT_FAKE"),
        "{names}"
    );
}

/// RFC §21.2: the arguments are a list, never read by a shell: one holding
/// a shell's syntax reaches the server as written.
#[tokio::test]
async fn an_argument_reaches_the_server_as_written() {
    let written = "$(touch /tmp/roomkit-mcp-shell); `id` | cat > x; echo *";
    let server = fake("normal")
        .arg("--instructions")
        .arg(written)
        .connect()
        .await
        .unwrap();
    let instructions = server.instructions().map(str::to_owned);
    server.close().await;
    assert_eq!(instructions.as_deref(), Some(written));
}
