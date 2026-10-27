//! An AI agent's tools (RFC §6.4, §9.3, §21.1, §21.6): Alice asks a shop by
//! SMS about her order, and the model calls five tools in one round. Each
//! call runs through its gate, then its tool, concurrently with the
//! others, and the model reads each outcome in its next round:
//!
//! - `lookup_order` is served, and its result is what the model reads;
//! - `delete_customer` is refused by the agent's tool policy, before
//!   anything else looks at it (RFC §21.1), so the approval hook never
//!   sees it;
//! - `cancel_order` is blocked by an approval in `BEFORE_TOOL_USE`, the
//!   model reading the hook's words (RFC §9.3);
//! - `refund_order` fails: the model reads the failure named by its
//!   error's type, and only the kit's `ON_TOOL_CALL` observer reads the
//!   error's message, which holds a password (RFC §9.3);
//! - `track_parcel` never answers, and is cut at the agent's bound per call
//!   (RFC §21.6).
//!
//! The room keeps the round's rows, a start and an end per call, then the
//! answer, which carries the turn's record. Alice's SMS receives the
//! round's text and the answer, never a tool-call row: a messaging
//! transport is not handed them. It runs against a local fake of
//! the OpenAI wire, which scripts the model's calls, so it needs no
//! network and no key.
//!
//! ```sh
//! cargo run --example agent_tools
//! ```

#![expect(clippy::print_stdout, reason = "an example prints what it ran")]

use std::error::Error;
use std::process::ExitCode;
use std::time::Duration;

use roomkit::model::{
    ChannelId, ChannelType, EventContent, EventType, InboundMessage, RoomEvent, RoomId,
    ToolCallContent, ToolCallOutcome,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    AttachChannelOptions, CreateRoomOptions, ProcessInboundOptions, ToolHookSpec, ToolUse,
};
use roomkit_ai::{ToolDefinition, ToolError, ToolOutput, ToolPolicy, Tools};
use roomkit_openai::{OpenAiAgent, OpenAiConfig};
use serde_json::{Value, json};

use shared::{Checks, FakeOpenAi, Log, OpenAiEndpoint, Reply};

const SYSTEM_PROMPT: &str = "You help the customers of an online shop with their orders.";

/// The secret the refund's database error holds.
const SECRET: &str = "postgres://admin:hunter2@db";

/// The words the approval refuses a cancellation with.
const NEEDS_APPROVAL: &str = "A manager must approve a cancellation.";

/// The agent's bound on each call: the parcel tracker never answers.
const BOUND: Duration = Duration::from_millis(50);

/// The model's script: one round calling every tool, then the answer.
fn script() -> Vec<Reply> {
    let order = r#"{"order": "42"}"#;
    vec![
        Reply::calling(
            "Let me look into order 42.",
            [
                ("call_1", "lookup_order", order),
                ("call_2", "delete_customer", r#"{"customer": "alice"}"#),
                ("call_3", "cancel_order", order),
                ("call_4", "refund_order", order),
                ("call_5", "track_parcel", order),
            ],
        ),
        Reply::from("Order 42 shipped on Monday; a manager has to approve a cancellation."),
    ]
}

#[tokio::main]
async fn main() -> Result<ExitCode, Box<dyn Error>> {
    let endpoint = OpenAiEndpoint::Fake(FakeOpenAi::start(&script()).await?);
    let (kit, sms, room) = shop_room(&endpoint).await?;
    let (approvals, audit) = watch_the_calls(&kit)?;

    println!("Alice asks about order 42.");
    let message = InboundMessage::new(
        sms.id().clone(),
        "+15551234567",
        EventContent::text("Where is order 42? Cancel it, or refund me."),
    );
    let result = kit
        .process_inbound(
            message,
            ProcessInboundOptions {
                room_id: Some(room.clone()),
                ..Default::default()
            },
        )
        .await?;
    // The calls' observers run in the background: the kit's close lets them
    // finish.
    kit.close().await;

    let timeline = shared::timeline(&kit, &room).await?;
    print_rows(&timeline);
    let audit = audit.entries();
    println!("The audit read:");
    for (tool, outcome, detail) in &audit {
        println!("  {tool}: {outcome:?}, {detail:?}");
    }
    let mut checks = Checks::default();
    checks.expect("the turn did not fail", result.error.is_none());
    check_the_rows(&mut checks, &timeline);
    check_each_call(&mut checks, &timeline, &approvals.entries(), &audit);
    let requests = endpoint
        .fake()
        .map(FakeOpenAi::requests)
        .unwrap_or_default();
    checks.expect(
        "the refund's error message reaches neither the room nor the model",
        !holds(&timeline, SECRET) && !holds(&requests, SECRET),
    );
    checks.expect(
        "the model's next round reads the five results, and the answer carries the turn's record",
        requests.get(1).is_some_and(|request| {
            request
                .get("messages")
                .and_then(Value::as_array)
                .is_some_and(|messages| {
                    messages.iter().filter(|m| m["role"] == "tool").count() == 5
                })
        }) && timeline.last().is_some_and(|answer| {
            answer.metadata.get("loop_end_reason") == Some(&json!("completed"))
        }),
    );
    checks.expect(
        "Alice receives the round's text and the answer by SMS, and no tool-call row",
        sms.deliveries()
            .iter()
            .map(|delivery| delivery.event.event_type)
            .eq([EventType::Message, EventType::Message])
            && sms.texts().first().map(String::as_str) == Some("Let me look into order 42.")
            && sms
                .texts()
                .last()
                .is_some_and(|text| text.starts_with("Order 42 shipped")),
    );
    Ok(checks.finish())
}

/// The room Alice writes to by SMS, with the shop's agent: its tools, a
/// policy that denies deletions, and its bound per call.
async fn shop_room(
    endpoint: &OpenAiEndpoint,
) -> Result<(roomkit::RoomKit, RecordingTransport, RoomId), Box<dyn Error>> {
    let config = OpenAiConfig::new(endpoint.base_url(), endpoint.api_key(), endpoint.model())
        .with_system_prompt(SYSTEM_PROMPT);
    let agent = OpenAiAgent::new(ChannelId::new("assistant")?, config)?
        .with_tools(shop())?
        .with_tool_policy(ToolPolicy::new().deny("delete_*"))
        .with_tool_timeout(Some(BOUND));
    let kit = shared::kit();
    let sms = RecordingTransport::new(ChannelId::new("sms")?, ChannelType::SMS);
    kit.register_channel(sms.clone())?;
    kit.register_channel(agent)?;
    let room = RoomId::new("room_1")?;
    kit.create_room(CreateRoomOptions {
        room_id: Some(room.clone()),
        ..Default::default()
    })
    .await?;
    for channel in ["sms", "assistant"] {
        kit.attach_channel(
            &room,
            &ChannelId::new(channel)?,
            AttachChannelOptions::default(),
        )
        .await?;
    }
    Ok((kit, sms, room))
}

/// What one call's observer reads: the tool, the outcome, what failed.
type Audited = (String, Option<ToolCallOutcome>, Option<String>);

/// The kit's tool hooks: an approval, where a cancellation waits for a
/// manager who is not there, and an audit of every call; what each saw.
fn watch_the_calls(kit: &roomkit::RoomKit) -> Result<(Log<String>, Log<Audited>), Box<dyn Error>> {
    let approvals = Log::new();
    let asked = approvals.clone();
    let spec = ToolHookSpec::new("approve").with_timeout(Duration::from_secs(300));
    kit.before_tool_use(spec, move |event, _context| {
        asked.push(event.call.name.clone());
        let cancels = event.call.name == "cancel_order";
        async move {
            Ok(if cancels {
                ToolUse::block(NEEDS_APPROVAL)
            } else {
                ToolUse::Allow
            })
        }
    })?;
    let audit = Log::new();
    let audited = audit.clone();
    kit.observe_tool_calls("audit", move |event, _context| {
        audited.push((
            event.call.name.clone(),
            event.outcome,
            event.error_detail.clone(),
        ));
        async {}
    })?;
    Ok((approvals, audit))
}

/// The checks of the room's rows: the round's text, a start per call, the
/// ends in call order, then the answer.
fn check_the_rows(checks: &mut Checks, timeline: &[RoomEvent]) {
    let mut expected = vec![EventType::Message, EventType::Message];
    expected.extend([EventType::ToolCallStart; 5]);
    expected.extend([EventType::ToolCallEnd; 5]);
    expected.push(EventType::Message);
    let ends = end_rows(timeline);
    checks.expect(
        "the room keeps the round's text, a start per call, the ends in call order, then the answer",
        kinds(timeline) == expected
            && ends
                .iter()
                .map(|row| row.tool_id.as_str())
                .eq(["call_1", "call_2", "call_3", "call_4", "call_5"]),
    );
}

/// The checks of each call's outcome, and of what the hooks saw.
fn check_each_call(
    checks: &mut Checks,
    timeline: &[RoomEvent],
    approvals: &[String],
    audit: &[Audited],
) {
    let ends = end_rows(timeline);
    let end = |tool: &str| ends.iter().find(|row| row.tool_name == tool);
    let outcome = |tool: &str| end(tool).and_then(|row| row.outcome);
    let read = |tool: &str| {
        end(tool)
            .and_then(|row| row.result.as_str())
            .unwrap_or_default()
            .to_owned()
    };
    checks.expect(
        "lookup_order is served, and the model reads its result",
        outcome("lookup_order") == Some(ToolCallOutcome::Served)
            && read("lookup_order") == r#"{"order":"42","shipped":"Monday","status":"shipped"}"#,
    );
    checks.expect(
        "delete_customer is refused by the policy, and never reaches the approval",
        outcome("delete_customer") == Some(ToolCallOutcome::Refused)
            && read("delete_customer")
                == r#"{"error": "Tool 'delete_customer' is not permitted by the agent's tool policy."}"#
            && !approvals.contains(&"delete_customer".to_owned()),
    );
    checks.expect(
        "cancel_order is blocked by the approval, the model reading its words",
        outcome("cancel_order") == Some(ToolCallOutcome::Refused)
            && read("cancel_order") == format!(r#"{{"error": "{NEEDS_APPROVAL}"}}"#),
    );
    checks.expect(
        "refund_order fails by its error's type; only the observer reads the message",
        read("refund_order") == r#"{"error": "Tool 'refund_order' failed (DatabaseError)"}"#
            && audit.iter().any(|(tool, _, detail)| {
                tool == "refund_order"
                    && detail
                        .as_deref()
                        .is_some_and(|detail| detail.contains(SECRET))
            }),
    );
    checks.expect(
        "track_parcel is cut at the agent's bound, and fails as a timeout",
        read("track_parcel") == r#"{"error": "Tool 'track_parcel' failed (ToolTimeoutError)"}"#,
    );
    checks.expect("the audit reads every call once", audit.len() == 5);
}

/// The shop's tools: each answers its calls one way, the example's point.
fn shop() -> Tools {
    let order = || {
        json!({
            "type": "object",
            "properties": { "order": { "type": "string" } },
            "required": ["order"],
        })
    };
    Tools::new()
        .tool(
            ToolDefinition::new("lookup_order", "Find an order by its number", order()),
            |_call, _context| async {
                Ok(ToolOutput::Json(
                    json!({ "order": "42", "status": "shipped", "shipped": "Monday" }),
                ))
            },
        )
        .tool(
            ToolDefinition::new(
                "delete_customer",
                "Delete a customer's account",
                json!({ "type": "object", "properties": { "customer": { "type": "string" } } }),
            ),
            |_call, _context| async { Ok(ToolOutput::from("deleted")) },
        )
        .tool(
            ToolDefinition::new("cancel_order", "Cancel an order", order()),
            |_call, _context| async { Ok(ToolOutput::from("cancelled")) },
        )
        .tool(
            ToolDefinition::new("refund_order", "Refund an order", order()),
            |_call, _context| async { Err(ToolError::raised(&DatabaseError)) },
        )
        .tool(
            ToolDefinition::new("track_parcel", "Track an order's parcel", order()),
            |_call, _context| std::future::pending(),
        )
}

/// What the refund's database fails with: its message holds a password.
#[derive(Debug)]
struct DatabaseError;

impl std::fmt::Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{SECRET} is unreachable")
    }
}

impl Error for DatabaseError {}

/// The end row of each call, in the room's order.
fn end_rows(timeline: &[RoomEvent]) -> Vec<&ToolCallContent> {
    timeline
        .iter()
        .filter(|event| event.event_type == EventType::ToolCallEnd)
        .filter_map(|event| {
            let EventContent::ToolCall(row) = &event.content else {
                return None;
            };
            Some(row)
        })
        .collect()
}

/// The type of each message and tool-call row the room holds, in order:
/// its lifecycle records aside.
fn kinds(timeline: &[RoomEvent]) -> Vec<EventType> {
    timeline
        .iter()
        .map(|event| event.event_type)
        .filter(|kind| {
            matches!(
                kind,
                EventType::Message | EventType::ToolCallStart | EventType::ToolCallEnd
            )
        })
        .collect()
}

/// Whether any of `records`, written as JSON, holds `text`.
fn holds<T: serde::Serialize>(records: &[T], text: &str) -> bool {
    records
        .iter()
        .any(|record| serde_json::to_string(record).is_ok_and(|written| written.contains(text)))
}

/// Prints the room's rows: each message's text, each call's start, and
/// its end with what the model read of it.
fn print_rows(timeline: &[RoomEvent]) {
    println!("The room holds:");
    for event in timeline {
        let EventContent::ToolCall(row) = &event.content else {
            if event.event_type == EventType::Message {
                let text = roomkit::text_of(&event.content);
                println!("  #{} {}: {text:?}", event.index, event.source.channel_id);
            }
            continue;
        };
        if event.event_type == EventType::ToolCallEnd {
            let read = row.result.as_str().unwrap_or_default();
            println!(
                "  #{} end   {} {:?}: {read}",
                event.index, row.tool_name, row.outcome
            );
        } else {
            let arguments = Value::Object(row.arguments.clone());
            println!("  #{} start {} {arguments}", event.index, row.tool_name);
        }
    }
}
