//! `StdioAcpTransport` against the `fake-acp-agent` fixture, a real
//! process spoken to over its stdio.

use std::collections::BTreeMap;

use agent_client_protocol_schema::ProtocolVersion;
use agent_client_protocol_schema::v1::{
    InitializeRequest, InitializeResponse, NewSessionRequest, PromptRequest, StopReason,
};
use pretty_assertions::assert_eq;
use roomkit_acp::{
    AcpConnection, AcpError, AcpTransport, ClientSideConnection, StdioAcpTransport,
    StdioAcpTransportOptions,
};
use serde_json::{Value, json};
use tokio::time::Instant;

use crate::support::{PATIENCE, Recorder};

/// The stand-in agent (`fixtures/fake_agent.rs`).
const FAKE: &str = env!("CARGO_BIN_EXE_fake-acp-agent");

/// The directory the agent runs in: this crate's, an absolute path.
const CWD: &str = env!("CARGO_MANIFEST_DIR");

fn transport(mode: &str, options: StdioAcpTransportOptions) -> StdioAcpTransport {
    StdioAcpTransport::new(vec![FAKE.into(), mode.into()], CWD, options).unwrap()
}

async fn initialized(connection: &ClientSideConnection) -> InitializeResponse {
    connection
        .initialize(InitializeRequest::new(ProtocolVersion::V1))
        .await
        .unwrap()
}

/// The agent's `_meta` member `key`, as its `initialize` answer reports it.
fn reported(answer: &InitializeResponse, key: &str) -> Value {
    answer
        .meta
        .as_ref()
        .and_then(|meta| meta.get(key))
        .cloned()
        .unwrap_or(Value::Null)
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_official_sdk_stdio_round_trip
// (the transport's half: the channel's comes with it).
#[tokio::test]
async fn a_prompt_goes_to_the_spawned_agent_and_its_updates_come_back() {
    let client = Recorder::new();
    let transport = transport("echo", StdioAcpTransportOptions::default());
    let connection = transport.open(client.client()).await.unwrap();
    assert!(transport.process().is_some());
    let answer = initialized(&connection).await;
    assert_eq!(answer.protocol_version, ProtocolVersion::V1);
    assert_eq!(reported(&answer, "cwd"), json!(CWD));
    assert_eq!(answer.agent_info.unwrap().name, "fake-acp-agent");
    let session = connection
        .new_session(NewSessionRequest::new(CWD))
        .await
        .unwrap();
    assert_eq!(session.session_id.0.as_ref(), "session-1");
    let prompt = PromptRequest::new(session.session_id, vec!["hello".into(), "there".into()]);
    let answer = connection.prompt(prompt).await.unwrap();
    assert_eq!(answer.stop_reason, StopReason::EndTurn);
    assert_eq!(client.texts(), ["hello", "there"]);
    transport.close().await;
    assert_eq!(transport.process(), None);
    assert!(transport.is_alive());
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_spawn_receives_resolved_env
#[tokio::test]
async fn the_agent_gets_the_default_environment_the_named_variables_then_env() {
    // `cargo test` sets CARGO_PKG_NAME in the test's environment.
    let options = StdioAcpTransportOptions {
        env: Some(BTreeMap::from([(
            "MAX_THINKING_TOKENS".to_owned(),
            "1024".to_owned(),
        )])),
        inherit_env: Some(vec![
            "CARGO_PKG_NAME".into(),
            "ROOMKIT_ACP_NEVER_SET".into(),
        ]),
    };
    let transport = transport("echo", options);
    let connection = transport.open(Recorder::new().client()).await.unwrap();
    let env = reported(&initialized(&connection).await, "env");
    transport.close().await;
    assert_eq!(env["CARGO_PKG_NAME"], "roomkit-acp");
    assert_eq!(env["MAX_THINKING_TOKENS"], "1024");
    assert_eq!(env["ROOMKIT_ACP_NEVER_SET"], Value::Null);
    // The host's other variables stay out.
    assert_eq!(env["CARGO_MANIFEST_DIR"], Value::Null);
    assert_eq!(env["PATH"], json!(std::env::var("PATH").unwrap()));
}

#[tokio::test]
async fn env_wins_over_an_inherited_variable() {
    let options = StdioAcpTransportOptions {
        env: Some(BTreeMap::from([(
            "CARGO_PKG_NAME".to_owned(),
            "explicit".to_owned(),
        )])),
        inherit_env: Some(vec!["CARGO_PKG_NAME".into()]),
    };
    let transport = transport("echo", options);
    let connection = transport.open(Recorder::new().client()).await.unwrap();
    let env = reported(&initialized(&connection).await, "env");
    transport.close().await;
    assert_eq!(env["CARGO_PKG_NAME"], "explicit");
}

// Python: tests/test_channels/test_acp.py::TestACPChannel::test_a_dead_transport_reconnects_and_drops_sessions
// (the transport's half: it tells its agent is gone).
#[tokio::test]
async fn an_agent_that_exits_leaves_its_transport_dead_and_its_connection_closed() {
    let transport = transport("exit", StdioAcpTransportOptions::default());
    let connection = transport.open(Recorder::new().client()).await.unwrap();
    initialized(&connection).await;
    let deadline = Instant::now() + PATIENCE;
    while transport.is_alive() {
        assert!(Instant::now() < deadline, "the agent's exit was not seen");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    let later = connection.new_session(NewSessionRequest::new(CWD)).await;
    assert!(
        matches!(later, Err(AcpError::ConnectionClosed)),
        "{later:?}"
    );
    transport.close().await;
}

#[tokio::test]
async fn the_agent_is_refused_what_the_client_does_not_serve_and_asked_its_permission() {
    let client = Recorder::new();
    let transport = transport("echo", StdioAcpTransportOptions::default());
    let connection = transport.open(client.client()).await.unwrap();
    initialized(&connection).await;
    let prompt = PromptRequest::new("session-1", vec!["ask the client".into()]);
    connection.prompt(prompt).await.unwrap();
    transport.close().await;
    let answers: Vec<Value> = client
        .texts()
        .iter()
        .filter(|text| *text != "waiting")
        .map(|text| serde_json::from_str(text).unwrap())
        .collect();
    let refused = |method: &str, named: &str| {
        json!({
            "method": method,
            "answer": { "error": { "code": -32601, "message": "Method not found", "data": { "method": named } } },
        })
    };
    assert_eq!(
        answers,
        [
            refused("fs/read_text_file", "fs/read_text_file"),
            refused("fs/write_text_file", "fs/write_text_file"),
            refused("terminal/create", "terminal/create"),
            refused("elicitation/create", "elicitation/create"),
            refused("_vendor/extension", "_vendor/extension"),
            json!({
                "method": "session/request_permission",
                "answer": { "result": { "outcome": { "outcome": "selected", "optionId": "allow-once" } } },
            }),
        ]
    );
    assert_eq!(client.permissions().len(), 1);
}

#[tokio::test]
async fn lines_that_are_no_message_are_skipped() {
    let client = Recorder::new();
    let transport = transport("junk", StdioAcpTransportOptions::default());
    let connection = transport.open(client.client()).await.unwrap();
    initialized(&connection).await;
    let prompt = PromptRequest::new("session-1", vec!["still here".into()]);
    connection.prompt(prompt).await.unwrap();
    transport.close().await;
    assert_eq!(client.texts(), ["still here"]);
}

#[tokio::test]
async fn a_program_that_cannot_start_fails_the_open() {
    let transport = StdioAcpTransport::new(
        vec!["/nonexistent/roomkit-acp-agent".into()],
        CWD,
        StdioAcpTransportOptions::default(),
    )
    .unwrap();
    let opened = transport.open(Recorder::new().client()).await;
    assert!(
        matches!(&opened, Err(AcpError::Spawn { program, .. }) if program == "/nonexistent/roomkit-acp-agent"),
        "{opened:?}"
    );
    assert!(transport.is_alive());
    assert_eq!(transport.process(), None);
    // A close after a failed open does nothing.
    transport.close().await;
}

#[tokio::test]
async fn a_protocol_version_the_agent_selects_reaches_the_caller() {
    let transport = transport("version:2", StdioAcpTransportOptions::default());
    let connection = transport.open(Recorder::new().client()).await.unwrap();
    let answer = initialized(&connection).await;
    transport.close().await;
    assert_eq!(answer.protocol_version.as_u16(), 2);
}

#[tokio::test]
async fn the_agents_standard_error_never_blocks_it() {
    let client = Recorder::new();
    let transport = transport("stderr-flood", StdioAcpTransportOptions::default());
    let connection = transport.open(client.client()).await.unwrap();
    initialized(&connection).await;
    transport.close().await;
}

#[tokio::test]
async fn an_open_never_awaited_starts_nothing() {
    let transport = transport("echo", StdioAcpTransportOptions::default());
    drop(transport.open(Recorder::new().client()));
    assert_eq!(transport.process(), None);
}

/// Whether the process `pid` is still there, as Linux lists it.
#[cfg(target_os = "linux")]
async fn gone(pid: u32) {
    let deadline = Instant::now() + PATIENCE;
    while std::path::Path::new(&format!("/proc/{pid}")).exists() {
        assert!(Instant::now() < deadline, "the agent {pid} is still there");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_transport_dropped_without_a_close_kills_its_agent() {
    let transport = transport("echo", StdioAcpTransportOptions::default());
    let connection = transport.open(Recorder::new().client()).await.unwrap();
    initialized(&connection).await;
    let pid = transport.process().unwrap();
    drop(transport);
    gone(pid).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn opening_again_lets_the_first_agent_go() {
    let transport = transport("echo", StdioAcpTransportOptions::default());
    let first = transport.open(Recorder::new().client()).await.unwrap();
    initialized(&first).await;
    let pid = transport.process().unwrap();
    let second = transport.open(Recorder::new().client()).await.unwrap();
    initialized(&second).await;
    assert_ne!(transport.process(), Some(pid));
    gone(pid).await;
    transport.close().await;
}
