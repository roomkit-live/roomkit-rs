//! `ClientSideConnection` over an in-memory pipe, the agent's side written
//! as raw JSON lines, so that it shares none of the connection's code.

use std::sync::Arc;

use agent_client_protocol_schema::ProtocolVersion;
use agent_client_protocol_schema::v1::{
    CancelNotification, CloseSessionRequest, InitializeRequest, PromptRequest, StopReason,
};
use pretty_assertions::assert_eq;
use roomkit_acp::{AcpConnection, AcpError, ClientSideConnection};
use serde_json::{Value, json};
use tokio::io::{
    AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, Lines, ReadHalf, WriteHalf,
};

use crate::support::{PATIENCE, Recorder};

/// The agent's end of the pipe.
struct Agent {
    lines: Lines<BufReader<ReadHalf<DuplexStream>>>,
    writer: WriteHalf<DuplexStream>,
}

impl Agent {
    /// The client's next message.
    async fn read(&mut self) -> Value {
        let line = tokio::time::timeout(PATIENCE, self.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .expect("the client's stream ended");
        serde_json::from_str(&line).unwrap()
    }

    /// Whether the client's stream ended.
    async fn ended(&mut self) -> bool {
        tokio::time::timeout(PATIENCE, self.lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .is_none()
    }

    async fn send(&mut self, message: &Value) {
        let mut line = message.to_string();
        line.push('\n');
        self.writer.write_all(line.as_bytes()).await.unwrap();
    }

    async fn answer(&mut self, id: &Value, result: Value) {
        self.send(&json!({ "jsonrpc": "2.0", "id": id, "result": result }))
            .await;
    }

    async fn update(&mut self, text: &str) {
        self.send(&json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": "session-1",
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "content": { "type": "text", "text": text },
                },
            },
        }))
        .await;
    }
}

fn pipe(client: &Arc<Recorder>) -> (ClientSideConnection, Agent) {
    let (ours, theirs) = tokio::io::duplex(64 * 1024);
    let (reader, writer) = tokio::io::split(ours);
    let connection = ClientSideConnection::new(client.client(), reader, writer);
    let (reader, writer) = tokio::io::split(theirs);
    let agent = Agent {
        lines: BufReader::new(reader).lines(),
        writer,
    };
    (connection, agent)
}

fn initialize() -> InitializeRequest {
    InitializeRequest::new(ProtocolVersion::V1)
}

fn prompt(text: &str) -> PromptRequest {
    PromptRequest::new("session-1", vec![text.into()])
}

#[tokio::test]
async fn requests_are_numbered_from_zero_and_answered_in_any_order() {
    let client = Recorder::new();
    let (connection, mut agent) = pipe(&client);
    let first = connection.initialize(initialize());
    let second = connection.close_session(CloseSessionRequest::new("session-1"));
    let agent_side = async {
        let one = agent.read().await;
        let two = agent.read().await;
        assert_eq!(one["jsonrpc"], "2.0");
        assert_eq!(one["id"], 0);
        assert_eq!(one["method"], "initialize");
        assert_eq!(one["params"]["protocolVersion"], 1);
        assert_eq!(two["id"], 1);
        assert_eq!(two["method"], "session/close");
        assert_eq!(two["params"], json!({ "sessionId": "session-1" }));
        agent.answer(&two["id"], json!({})).await;
        agent
            .answer(
                &one["id"],
                json!({ "protocolVersion": 1, "agentInfo": { "name": "a", "version": "2" } }),
            )
            .await;
    };
    let (first, second, ()) = tokio::join!(first, second, agent_side);
    assert_eq!(first.unwrap().agent_info.unwrap().version, "2");
    second.unwrap();
}

#[tokio::test]
async fn an_error_answer_is_the_agents_error() {
    let client = Recorder::new();
    let (connection, mut agent) = pipe(&client);
    let agent_side = async {
        let request = agent.read().await;
        agent
            .send(&json!({
                "jsonrpc": "2.0",
                "id": request["id"],
                "error": { "code": -32000, "message": "Authentication required", "data": { "why": "no key" } },
            }))
            .await;
    };
    let (answer, ()) = tokio::join!(connection.initialize(initialize()), agent_side);
    match answer {
        Err(AcpError::Rpc {
            code,
            message,
            data,
        }) => {
            assert_eq!(code, -32000);
            assert_eq!(message, "Authentication required");
            assert_eq!(data, Some(json!({ "why": "no key" })));
        }
        other => panic!("not the agent's error: {other:?}"),
    }
}

#[tokio::test]
async fn the_end_of_the_agents_stream_fails_what_waits_and_what_follows() {
    let client = Recorder::new();
    let (connection, mut agent) = pipe(&client);
    let agent_side = async {
        agent.read().await;
        drop(agent);
    };
    let (answer, ()) = tokio::join!(connection.initialize(initialize()), agent_side);
    assert!(
        matches!(answer, Err(AcpError::ConnectionClosed)),
        "{answer:?}"
    );
    let later = connection.initialize(initialize()).await;
    assert!(
        matches!(later, Err(AcpError::ConnectionClosed)),
        "{later:?}"
    );
    let cancel = connection
        .cancel(CancelNotification::new("session-1"))
        .await;
    assert!(
        matches!(cancel, Err(AcpError::ConnectionClosed)),
        "{cancel:?}"
    );
}

#[tokio::test]
async fn close_fails_what_waits_and_shuts_the_agents_input() {
    let client = Recorder::new();
    let (connection, mut agent) = pipe(&client);
    let waiting = tokio::spawn({
        let connection = connection.clone();
        async move { connection.initialize(initialize()).await }
    });
    agent.read().await;
    connection.close().await;
    let answer = waiting.await.unwrap();
    assert!(
        matches!(answer, Err(AcpError::ConnectionClosed)),
        "{answer:?}"
    );
    assert!(agent.ended().await);
    // A second close does nothing.
    connection.close().await;
}

#[tokio::test]
async fn a_request_whose_caller_went_is_forgotten() {
    let client = Recorder::new();
    let (connection, mut agent) = pipe(&client);
    {
        let request = connection.initialize(initialize());
        tokio::pin!(request);
        let sent = tokio::time::timeout(std::time::Duration::from_millis(20), &mut request).await;
        assert!(sent.is_err(), "answered before the agent read it");
    }
    let forgotten = agent.read().await;
    // Its late answer finds no one, and the next request is the next id.
    agent
        .answer(&forgotten["id"], json!({ "protocolVersion": 1 }))
        .await;
    let agent_side = async {
        let request = agent.read().await;
        assert_eq!(request["id"], 1);
        agent
            .answer(&request["id"], json!({ "protocolVersion": 1 }))
            .await;
    };
    let (answer, ()) = tokio::join!(connection.initialize(initialize()), agent_side);
    answer.unwrap();
}

#[tokio::test]
async fn a_prompts_updates_are_taken_in_order_before_its_answer() {
    let client = Recorder::new();
    let (connection, mut agent) = pipe(&client);
    let agent_side = async {
        let request = agent.read().await;
        assert_eq!(request["method"], "session/prompt");
        assert_eq!(
            request["params"],
            json!({ "sessionId": "session-1", "prompt": [{ "type": "text", "text": "hi" }] })
        );
        for text in ["one", "two", "three"] {
            agent.update(text).await;
        }
        agent
            .answer(&request["id"], json!({ "stopReason": "end_turn" }))
            .await;
    };
    let (answer, ()) = tokio::join!(connection.prompt(prompt("hi")), agent_side);
    assert_eq!(answer.unwrap().stop_reason, StopReason::EndTurn);
    assert_eq!(client.texts(), ["one", "two", "three"]);
}

#[tokio::test]
async fn cancel_is_a_notification() {
    let client = Recorder::new();
    let (connection, mut agent) = pipe(&client);
    connection
        .cancel(CancelNotification::new("session-1"))
        .await
        .unwrap();
    assert_eq!(
        agent.read().await,
        json!({ "jsonrpc": "2.0", "method": "session/cancel", "params": { "sessionId": "session-1" } })
    );
}

#[tokio::test]
async fn an_answer_that_is_no_object_reads_as_an_empty_one_where_the_sdk_reads_it_so() {
    let client = Recorder::new();
    let (connection, mut agent) = pipe(&client);
    let agent_side = async {
        let request = agent.read().await;
        agent.answer(&request["id"], Value::Null).await;
    };
    let (answer, ()) = tokio::join!(
        connection.close_session(CloseSessionRequest::new("session-1")),
        agent_side
    );
    answer.unwrap();
    // `initialize`'s answer is read whole: none is refused.
    let agent_side = async {
        let request = agent.read().await;
        agent.answer(&request["id"], Value::Null).await;
    };
    let (answer, ()) = tokio::join!(connection.initialize(initialize()), agent_side);
    assert!(
        matches!(&answer, Err(AcpError::InvalidResponse { method, .. }) if method == "initialize"),
        "{answer:?}"
    );
}

#[tokio::test]
async fn the_agents_requests_but_a_permission_are_method_not_found() {
    let client = Recorder::new();
    let (_connection, mut agent) = pipe(&client);
    let session = json!("session-1");
    let asked = [
        (
            "fs/read_text_file",
            "fs/read_text_file",
            json!({ "sessionId": session, "path": "/etc/hostname" }),
        ),
        (
            "fs/write_text_file",
            "fs/write_text_file",
            json!({ "sessionId": session, "path": "/tmp/x", "content": "x" }),
        ),
        (
            "terminal/create",
            "terminal/create",
            json!({ "sessionId": session, "command": "ls" }),
        ),
        (
            "terminal/output",
            "terminal/output",
            json!({ "sessionId": session, "terminalId": "t-1" }),
        ),
        (
            "terminal/release",
            "terminal/release",
            json!({ "sessionId": session, "terminalId": "t-1" }),
        ),
        (
            "terminal/wait_for_exit",
            "terminal/wait_for_exit",
            json!({ "sessionId": session, "terminalId": "t-1" }),
        ),
        (
            "terminal/kill",
            "terminal/kill",
            json!({ "sessionId": session, "terminalId": "t-1" }),
        ),
        // The Python implementation's client names it so.
        (
            "elicitation/create",
            "session/request_input",
            json!({
                "mode": "form",
                "sessionId": session,
                "message": "Your name?",
                "requestedSchema": { "type": "object", "properties": {} },
            }),
        ),
        ("_vendor/extension", "_vendor/extension", json!({})),
        ("no/such_method", "no/such_method", json!({})),
        ("session/update", "session/update", json!({})),
    ];
    for (id, (method, named, params)) in asked.iter().enumerate() {
        agent
            .send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await;
        assert_eq!(
            agent.read().await,
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32601, "message": "Method not found", "data": { "method": named } },
            })
        );
    }
}

#[tokio::test]
async fn a_permission_is_answered_while_the_reading_goes_on() {
    let client = Recorder::new();
    let (_connection, mut agent) = pipe(&client);
    agent
        .send(&json!({
            "jsonrpc": "2.0",
            "id": "p-1",
            "method": "session/request_permission",
            "params": {
                "sessionId": "session-1",
                "toolCall": { "toolCallId": "tool-1", "title": "Read file" },
                "options": [{ "optionId": "allow-once", "name": "Allow once", "kind": "allow_once" }],
            },
        }))
        .await;
    // The client answers only once it took this update, read after the
    // request.
    agent.update("waiting").await;
    assert_eq!(
        agent.read().await,
        json!({
            "jsonrpc": "2.0",
            "id": "p-1",
            "result": { "outcome": { "outcome": "selected", "optionId": "allow-once" } },
        })
    );
    assert_eq!(client.permissions().len(), 1);
}

#[tokio::test]
async fn a_request_whose_parameters_are_not_valid_is_answered_invalid_params() {
    let client = Recorder::new();
    let (_connection, mut agent) = pipe(&client);
    for (id, method) in ["session/request_permission", "fs/read_text_file"]
        .iter()
        .enumerate()
    {
        agent
            .send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": {} }))
            .await;
        let answer = agent.read().await;
        assert_eq!(answer["id"], id);
        assert_eq!(answer["error"]["code"], -32602);
        assert_eq!(answer["error"]["message"], "Invalid params");
    }
    assert_eq!(client.permissions().len(), 0);
}
