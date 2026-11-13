//! A minimal local ACP peer for the example, not an application server.

#![expect(
    clippy::indexing_slicing,
    reason = "this local fixture asserts its own fixed JSON protocol"
)]

use serde_json::{Value, json};
use std::error::Error;
use std::io::{self, BufRead, Write};

pub(super) fn serve() -> Result<(), Box<dyn Error>> {
    let mut output = io::stdout().lock();
    let mut prompt = Value::Null;
    for line in io::stdin().lock().lines() {
        let message: Value = serde_json::from_str(&line?)?;
        match message["method"].as_str() {
            Some("initialize") => reply(
                &mut output,
                &message,
                &json!({
                    "protocolVersion":1,"agentCapabilities":{"sessionCapabilities":{"close":{}}}
                }),
            )?,
            Some("session/new") => {
                reply(
                    &mut output,
                    &message,
                    &json!({"sessionId":"session-review"}),
                )?;
            }
            Some("session/prompt") => {
                prompt = message;
                request_read(&mut output)?;
            }
            Some("session/close") => reply(&mut output, &message, &json!({}))?,
            None if message["id"] == "permission-1" => finish_read(&mut output, &message, &prompt)?,
            _ => {}
        }
    }
    Ok(())
}

fn request_read(output: &mut impl Write) -> Result<(), Box<dyn Error>> {
    let call = json!({"toolCallId":"read-1","title":"Read","kind":"read",
        "status":"pending","rawInput":{"path":"guide.md"}});
    let mut announced = call.clone();
    announced["sessionUpdate"] = json!("tool_call");
    update(output, &announced)?;
    send(
        output,
        &json!({"jsonrpc":"2.0","id":"permission-1","method":"session/request_permission",
        "params":{"sessionId":"session-review","toolCall":call,"options":[
            {"optionId":"approve","name":"Allow once","kind":"allow_once"},
            {"optionId":"refuse","name":"Reject once","kind":"reject_once"}
        ]}}),
    )
}

fn finish_read(
    output: &mut impl Write,
    permission: &Value,
    prompt: &Value,
) -> Result<(), Box<dyn Error>> {
    assert_eq!(permission["result"]["outcome"]["optionId"], "approve");
    update(
        output,
        &json!({"sessionUpdate":"tool_call_update","toolCallId":"read-1",
        "status":"completed","rawOutput":"guide contents"}),
    )?;
    update(
        output,
        &json!({"sessionUpdate":"agent_message_chunk",
        "content":{"type":"text","text":"The guide describes the public API."}}),
    )?;
    reply(output, prompt, &json!({"stopReason":"end_turn"}))
}

fn update(output: &mut impl Write, update: &Value) -> Result<(), Box<dyn Error>> {
    send(
        output,
        &json!({"jsonrpc":"2.0","method":"session/update",
        "params":{"sessionId":"session-review","update":update}}),
    )
}

fn reply(output: &mut impl Write, request: &Value, result: &Value) -> Result<(), Box<dyn Error>> {
    send(
        output,
        &json!({"jsonrpc":"2.0","id":request["id"],"result":result}),
    )
}

fn send(output: &mut impl Write, message: &Value) -> Result<(), Box<dyn Error>> {
    writeln!(output, "{message}")?;
    output.flush()?;
    Ok(())
}
