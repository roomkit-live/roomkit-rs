//! A task that did not complete tells its requester why (RFC §23.3 step 8,
//! §23.4).
//!
//! The assistant is handed a review a reviewer could not finish: capped at
//! one round of tool calls, the reviewer reads a file, wants another, and its
//! turn is cut there. The task fails with no answer, and the assistant reads
//! why in what it is handed back: the runtime's words, then a stable code,
//! `failed: it reached its limit of tool rounds before giving an answer
//! (max_rounds)`. The task's line, as `task_status` answers it, names the
//! same code under `end_reason`.
//!
//! The code comes from a closed list: a turn cut short (`max_rounds`,
//! `timeout`, `budget_exceeded`...), a task cancelled (`cancelled`), a
//! worker that answered nothing (`empty_response`), and any other failure
//! `error`, whose detail stays on the task's result. The Python
//! implementation's `examples/task_end_reason.py`.
//!
//! It runs on stand-in models, so it needs no network and no key.
//!
//! ```sh
//! cargo run --example task_end_reason
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::future::Future;
use std::process::ExitCode;
use std::time::Duration;

use roomkit::model::{ChannelId, ChannelType, Metadata, RoomId, TaskStatus};
use roomkit::orchestration::RecentOptions;
use roomkit::tasks::{DelegatedTaskResult, WaitOptions, room_tasks};
use roomkit::{AttachChannelOptions, CreateRoomOptions, DelegateOptions, RoomKit, RoomKitOptions};
use roomkit_ai::{
    AiChannel, AiChannelOptions, AiContext, AiMessage, AiProvider, AiResponse, AiTool, AiToolCall,
    ProviderError, ToolHandler, ToolResult,
};

use shared::{Checks, Log, recording};

const WHY: &str =
    "failed: it reached its limit of tool rounds before giving an answer (max_rounds)";

/// A stand-in reviewer: it asks for another file at every round.
struct Reviewer;

impl AiProvider for Reviewer {
    fn model_name(&self) -> &'static str {
        "reviewer"
    }

    fn generate<'a>(
        &'a self,
        _context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        std::future::ready(Ok(AiResponse {
            tool_calls: vec![AiToolCall::new("r1", "read_file")],
            ..AiResponse::new("")
        }))
    }
}

/// A stand-in assistant: it keeps what it is handed, and says it.
struct Assistant {
    read: Log<String>,
}

impl AiProvider for Assistant {
    fn model_name(&self) -> &'static str {
        "assistant"
    }

    fn generate<'a>(
        &'a self,
        context: &'a AiContext,
    ) -> impl Future<Output = Result<AiResponse, ProviderError>> + Send + 'a {
        let handed = context.messages.last().map(AiMessage::text);
        self.read.push(handed.unwrap_or_default());
        std::future::ready(Ok(AiResponse::new(
            "The review stopped at its limit of tool rounds.",
        )))
    }
}

/// The reviewer's channel, capped at one round of tool calls.
fn reviewer() -> Result<AiChannel<Reviewer>, Box<dyn Error>> {
    let options = AiChannelOptions {
        tools: vec![AiTool::new("read_file", "Read a file of the change.")?.into()],
        tool_handler: Some(ToolHandler::new(|_name, _arguments| async {
            Ok(ToolResult::Text("fn placed() {}".to_owned()))
        })),
        max_tool_rounds: 1,
        ..AiChannelOptions::default()
    };
    Ok(AiChannel::new(
        ChannelId::new("reviewer")?,
        Reviewer,
        options,
    )?)
}

/// A room where the assistant answers, and a reviewer it can be handed.
async fn build_kit(read: &Log<String>) -> Result<(RoomKit, RoomId), Box<dyn Error>> {
    let assistant = AiChannel::new(
        ChannelId::new("assistant")?,
        Assistant { read: read.clone() },
        AiChannelOptions::default(),
    )?;
    let kit = RoomKit::new(RoomKitOptions::default());
    kit.register_channel(recording("ws", ChannelType::WEBSOCKET)?)?;
    kit.register_channel(assistant)?;
    kit.register_channel(reviewer()?)?;
    let room = RoomId::new("review-chat")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in ["ws", "assistant"] {
        let channel = ChannelId::new(channel)?;
        kit.attach_channel(&room, &channel, AttachChannelOptions::default())
            .await?;
    }
    Ok((kit, room))
}

/// What the assistant was handed, once the hand-back's turn ran: it runs
/// after the task ended.
async fn handed(read: &Log<String>) -> Vec<String> {
    for _ in 0..200 {
        if !read.entries().is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    read.entries()
}

/// Whether the run showed what the example says.
fn checks(result: &DelegatedTaskResult, handed: &[String], line: &Metadata) -> ExitCode {
    let mut checks = Checks::default();
    checks.expect(
        "the task failed, its error kept for the host",
        result.status == TaskStatus::Failed
            && result.error.as_deref()
                == Some("The worker's turn ended max_rounds before its answer"),
    );
    checks.expect(
        "the assistant is handed the failure once, with why",
        handed.len() == 1 && handed.iter().all(|text| text.contains(WHY)),
    );
    checks.expect(
        "the task's line names the same code, in the runtime's words too",
        line.get("end_reason").and_then(|code| code.as_str()) == Some("max_rounds")
            && line.get("end_reason_text").and_then(|text| text.as_str())
                == Some("it reached its limit of tool rounds before giving an answer"),
    );
    checks.finish()
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let read = Log::new();
    let (kit, room) = build_kit(&read).await?;

    println!("The host delegates the review to the reviewer, capped at one tool round.");
    let options = DelegateOptions {
        notify: Some(ChannelId::new("assistant")?),
        ..DelegateOptions::default()
    };
    let reviewer = ChannelId::new("reviewer")?;
    let task = kit
        .delegate(&room, &reviewer, "Review the placement change.", options)
        .await?;
    let result = task.wait(WaitOptions::default()).await?;
    println!(
        "The task's result: {:?}, error: {:?}",
        result.status, result.error
    );
    let handed = handed(&read).await;
    for text in &handed {
        println!(
            "The assistant is handed: {}",
            text.lines().nth(1).unwrap_or(text)
        );
    }
    let entries = kit.status_bus().recent(RecentOptions::default()).await;
    let line = room_tasks(entries).into_iter().next().unwrap_or_default();
    println!(
        "The task's line: status {:?}, end_reason {:?}, end_reason_text {:?}",
        line.get("status"),
        line.get("end_reason"),
        line.get("end_reason_text")
    );
    kit.close().await?;
    Ok(checks(&result, &handed, &line))
}
