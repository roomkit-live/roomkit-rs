//! The AI agent's turn in a kit, on a scripted model (RFC §6.4, §8.3, §9.3,
//! §21.1, §21.6): what the shared conformance suite does not cover of the
//! tool loop, its hooks and its depth.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    DirectEvent, EventContent, EventType, Metadata, RoomEvent, ToolCallContent, ToolCallOutcome,
};
use roomkit::{
    HookExecution, HookSpec, HookTrigger, KitError, RegistrationError, ToolUse, ToolVerdict,
    current_turn,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{ToolDefinition, ToolError, ToolOutput, Tools};
use serde_json::{Value, json};
use tokio::sync::Barrier;

use self::support::{Room, channel, served};

/// The end row of the call `id` in `timeline`.
fn end_row<'a>(timeline: &'a [RoomEvent], id: &str) -> &'a ToolCallContent {
    timeline
        .iter()
        .filter(|event| event.event_type == EventType::ToolCallEnd)
        .find_map(|event| {
            let EventContent::ToolCall(row) = &event.content else {
                return None;
            };
            (row.tool_id == id).then_some(row)
        })
        .unwrap()
}

/// What each request of `model` carried as tool results.
fn tool_results(model: &ScriptedProvider) -> Vec<Vec<String>> {
    model
        .requests()
        .iter()
        .map(|request| {
            request
                .messages
                .iter()
                .filter(|message| message.tool_call_id.is_some())
                .map(|message| message.text.clone())
                .collect()
        })
        .collect()
}

/// RFC §9.3: a tool trigger's payload is a tool call, no event.
#[tokio::test]
async fn a_tool_trigger_takes_no_event_hook() {
    let room = Room::new(ScriptedProvider::new([]), Tools::new()).await;
    let spec = HookSpec::new("approve", HookTrigger::BeforeToolUse, HookExecution::Sync);
    let registered = room.kit.hook(spec, |_event, _context| async {
        Ok(roomkit::model::HookResult::allow())
    });
    assert!(matches!(
        registered,
        Err(KitError::Registration(
            RegistrationError::PayloadMismatch { .. }
        ))
    ));
}

/// RFC §9.3: each SYNC hook reads the result as the one before left it;
/// a replacement is what the model reads, and a withheld result fails the
/// call `blocked`.
#[tokio::test]
async fn a_sync_on_tool_call_hook_replaces_then_withholds_a_result() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup_order", r#"{"order": "1"}"#)]),
        ScriptedRound::calling("", [("c2", "lookup_order", r#"{"order": "2"}"#)]),
        ScriptedRound::answering("Done."),
    ]);
    let room = Room::new(model.clone(), served()).await;
    let seen = Arc::new(Mutex::new(Vec::new()));
    let saw = Arc::clone(&seen);
    room.kit
        .on_tool_call("redact", move |event, _context| {
            saw.lock().unwrap().push(event.result.clone());
            let second = event.call.id == "c2";
            async move {
                Ok(if second {
                    ToolVerdict::Block("Withheld by policy".to_owned())
                } else {
                    ToolVerdict::Replace(json!({ "order": "1", "status": "[redacted]" }))
                })
            }
        })
        .unwrap();
    room.says("Orders 1 and 2?").await;
    let timeline = room.timeline().await;
    let (first, second) = (end_row(&timeline, "c1"), end_row(&timeline, "c2"));
    assert_eq!(
        (
            (first.outcome, first.result.clone()),
            (second.outcome, second.result.clone()),
            tool_results(&model)[2].clone(),
            seen.lock().unwrap().len(),
        ),
        (
            (
                Some(ToolCallOutcome::Served),
                json!(r#"{"order":"1","status":"[redacted]"}"#)
            ),
            (
                Some(ToolCallOutcome::Blocked),
                json!(r#"{"error": "Withheld by policy"}"#)
            ),
            vec![
                r#"{"order":"1","status":"[redacted]"}"#.to_owned(),
                r#"{"error": "Withheld by policy"}"#.to_owned()
            ],
            2
        )
    );
}

/// RFC §21.1: the call runs with the arguments `BEFORE_TOOL_USE` left, and
/// arguments it left that do not fit the schema are refused as rewritten.
#[tokio::test]
async fn a_rewritten_call_runs_with_its_new_arguments_unless_they_do_not_fit() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "lookup_order", r#"{"order": "1"}"#)]),
        ScriptedRound::calling("", [("c2", "lookup_order", r#"{"order": "2"}"#)]),
        ScriptedRound::answering("Done."),
    ]);
    let room = Room::new(model, served()).await;
    room.kit
        .before_tool_use("rewrite", |event, _context| {
            let mut arguments = Metadata::new();
            if event.call.id == "c1" {
                arguments.insert("order".to_owned(), "7".into());
            }
            async move { Ok(ToolUse::Rewrite(arguments)) }
        })
        .unwrap();
    room.says("Orders?").await;
    let timeline = room.timeline().await;
    let (first, second) = (end_row(&timeline, "c1"), end_row(&timeline, "c2"));
    assert_eq!(
        (
            (Value::Object(first.arguments.clone()), first.result.clone()),
            (second.outcome, second.result.clone()),
        ),
        (
            (
                json!({ "order": "7" }),
                json!(r#"{"order":"7","status":"shipped"}"#)
            ),
            (
                Some(ToolCallOutcome::Refused),
                json!(
                    r#"{"error": "Invalid rewritten arguments for 'lookup_order': missing required argument 'order'"}"#
                )
            ),
        )
    );
}

/// RFC §21.6: a call past its bound is cut, and fails as a tool that
/// raised; what bound it goes to the observers alone.
#[tokio::test]
async fn a_call_past_its_bound_fails_as_a_timeout() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "wait", "{}")]),
        ScriptedRound::answering("Sorry."),
    ]);
    let hangs = Tools::new().tool(
        ToolDefinition::new("wait", "Never answers", json!({ "type": "object" })),
        |_call, _context| std::future::pending(),
    );
    let room = Room::equipped(model, hangs, |agent| {
        agent.with_tool_timeout(Some(Duration::from_millis(20)))
    })
    .await;
    let details = room.observed_details();
    room.says("Wait?").await;
    room.kit.idle().await;
    let timeline = room.timeline().await;
    assert_eq!(
        (
            end_row(&timeline, "c1").result.clone(),
            details.lock().unwrap().clone()
        ),
        (
            json!(r#"{"error": "Tool 'wait' failed (ToolTimeoutError)"}"#),
            vec![Some(
                "ToolTimeoutError: tool 'wait' did not answer within 0.02 s".to_owned()
            )]
        )
    );
}

/// The calls of one round run concurrently: two that each wait for the
/// other end only together.
#[tokio::test]
async fn a_round_s_calls_run_together() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "meet", "{}"), ("c2", "meet", "{}")]),
        ScriptedRound::answering("Met."),
    ]);
    let barrier = Arc::new(Barrier::new(2));
    let meets = Tools::new().tool(
        ToolDefinition::new("meet", "Waits for the other", json!({ "type": "object" })),
        move |_call, _context| {
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                Ok(ToolOutput::Text("met".to_owned()))
            }
        },
    );
    let room = Room::equipped(model, meets, |agent| {
        agent.with_tool_timeout(Some(Duration::from_secs(5)))
    })
    .await;
    tokio::time::timeout(Duration::from_secs(5), room.says("Meet?"))
        .await
        .unwrap();
    let timeline = room.timeline().await;
    assert_eq!(
        [
            end_row(&timeline, "c1").outcome,
            end_row(&timeline, "c2").outcome
        ],
        [Some(ToolCallOutcome::Served); 2]
    );
}

/// RFC §6.4 "A turn cut short": the provider failing after a round keeps
/// the rounds, records the turn's end `error`, and reports the error.
#[tokio::test]
async fn a_provider_failing_after_a_round_keeps_the_round_and_reports_its_error() {
    let model = ScriptedProvider::new([ScriptedRound::calling(
        "Let me check.",
        [("c1", "lookup_order", r#"{"order": "1"}"#)],
    )]);
    let room = Room::new(model, served()).await;
    let result = room.says("Order 1?").await;
    let timeline = room.timeline().await;
    let message = timeline
        .iter()
        .find(|event| event.source.channel_id == channel("assistant"))
        .unwrap();
    assert_eq!(
        (
            result.error.map(|error| error.message),
            message.metadata["loop_end_reason"].clone(),
            end_row(&timeline, "c1").outcome,
        ),
        (
            Some("the scripted model has no round left".to_owned()),
            json!("error"),
            Some(ToolCallOutcome::Served)
        )
    );
}

/// A provider failing before any round fails the turn: nothing of it is
/// stored.
#[tokio::test]
async fn a_provider_failing_on_the_first_round_fails_the_turn() {
    let room = Room::new(ScriptedProvider::new([]), served()).await;
    let result = room.says("Hello?").await;
    let answered = room
        .timeline()
        .await
        .iter()
        .any(|event| event.source.channel_id == channel("assistant"));
    assert_eq!(
        (result.error.map(|error| error.message), answered),
        (
            Some("the scripted model has no round left".to_owned()),
            false
        )
    );
}

/// RFC §21.4, §8.3: a tool writes to its room through its turn, without a
/// kit of its own, at least as deep as the turn's response.
#[tokio::test]
async fn a_tool_writes_to_its_room_at_the_turn_s_depth() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", [("c1", "notify", "{}")]),
        ScriptedRound::answering("Notified."),
    ]);
    let notifies = Tools::new().tool(
        ToolDefinition::new("notify", "Tells the room", json!({ "type": "object" })),
        |_call, context| async move {
            let turn = context.turn().cloned().or_else(current_turn).unwrap();
            let notice = DirectEvent::new(channel("sms"), EventContent::text("Order on its way"));
            let sent = turn
                .send_event(notice)
                .await
                .map_err(|error| ToolError::raised(&error))?;
            Ok(ToolOutput::Text(format!(
                "sent at depth {}",
                sent.chain_depth
            )))
        },
    );
    let room = Room::new(model, notifies).await;
    room.says("Notify me").await;
    room.kit.idle().await;
    let timeline = room.timeline().await;
    let notice = timeline
        .iter()
        .find(|event| roomkit::text_of(&event.content) == "Order on its way")
        .unwrap();
    assert_eq!(
        (notice.chain_depth, end_row(&timeline, "c1").result.clone()),
        (1, json!("sent at depth 1"))
    );
}

/// RFC §8.3, RMK-978: an observer of a tool call posts into the room, the
/// agent answers the post and calls again, and the next post follows. Each
/// post is at least as deep as the turn was when its call was made, once
/// the turn is over too, so the loop ends at `max_chain_depth`.
#[tokio::test]
async fn an_observer_s_loop_through_its_agent_ends_at_the_depth_limit() {
    for after_the_turn in [false, true] {
        let depths = support::observer_loop(after_the_turn).await;
        assert_eq!(depths, [1, 2], "after the turn: {after_the_turn}");
    }
}
