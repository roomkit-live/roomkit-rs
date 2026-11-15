//! Call a handler directly under a described turn (RFC §21.4).
//! Run `cargo run --example tool_turn_context`; no network or key is needed.

use std::error::Error;
use std::process::ExitCode;

use roomkit::model::Metadata;
use roomkit_ai::{
    ToolCallContext, ToolHandler, ToolResult, ToolTurnContextOptions, current_response_metadata,
    current_tool_actor_id, current_tool_call, tool_turn_context,
};
use serde_json::json;

fn main() -> Result<ExitCode, Box<dyn Error>> {
    tokio::runtime::Runtime::new()?.block_on(run())
}

async fn run() -> Result<ExitCode, Box<dyn Error>> {
    let call = ToolCallContext {
        room_id: Some("support".parse()?),
        tool_call_id: "lookup-1".into(),
        ..Default::default()
    };
    let handler = ToolHandler::new(|_, _| async {
        if let Some(record) = current_response_metadata() {
            record.insert("source", json!("doc-1"));
        }
        if let Some(call) = current_tool_call() {
            let content = roomkit::model::ResponseMetadata::new();
            content.insert("page", json!(3));
            call.structured_content.replace(Some(content));
        }
        Ok(ToolResult::Text("The answer is on page 3.".into()))
    });
    let options = ToolTurnContextOptions {
        room_id: call.room_id.clone(),
        actor_id: Some("alice".into()),
        call: Some(call.clone()),
        ..Default::default()
    };
    let (answer, actor, record) = tool_turn_context(options, async {
        let answer = handler.call("lookup".into(), Metadata::new()).await;
        (answer, current_tool_actor_id(), current_response_metadata())
    })
    .await?;

    let mut checks = shared::Checks::default();
    checks.expect(
        "handler answer",
        answer? == ToolResult::Text("The answer is on page 3.".into()),
    );
    checks.expect("turn actor", actor.as_deref() == Some("alice"));
    checks.expect(
        "turn source",
        record.and_then(|record| record.get("source")) == Some(json!("doc-1")),
    );
    checks.expect(
        "call's structured result",
        call.structured_content
            .get()
            .and_then(|content| content.get("page"))
            == Some(json!(3)),
    );
    checks.expect("outside the turn", current_tool_actor_id().is_none());
    Ok(checks.finish())
}
