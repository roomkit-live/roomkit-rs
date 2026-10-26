//! The AI agent's turn in a kit, on a scripted model (RFC §6.4, §8.3, §9.3,
//! §21.1, §21.6): what the shared conformance suite does not cover of the
//! tool loop, its hooks and its depth.

#![expect(clippy::unwrap_used, reason = "a test fails on a broken fixture")]

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use pretty_assertions::assert_eq;
use roomkit::model::{
    ChannelType, DirectEvent, EventContent, EventType, InboundMessage, Metadata, RoomEvent,
    ToolCallContent, ToolCallOutcome,
};
use roomkit::testing::RecordingTransport;
use roomkit::{
    ConversationStore, HookExecution, HookSpec, HookTrigger, KitError, RegistrationError, RoomKit,
    ToolUse, ToolVerdict, current_turn,
};
use roomkit_ai::testing::{ScriptedProvider, ScriptedRound};
use roomkit_ai::{AiAgent, ToolDefinition, ToolError, ToolOutput, Tools};
use serde_json::{Value, json};
use tokio::sync::Barrier;

use self::support::{Room, channel, room_id, served};

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

/// A round runs at most 32 of the calls its generation made, the first
/// ones, as roomkit's `_MAX_TOOL_CALLS_PER_ROUND`: the rest never run, are
/// not stored, and the model reads neither them nor results for them
/// (review of step 4 milestone 6: one completion asked 5,000).
#[tokio::test]
async fn a_round_runs_its_first_thirty_two_calls_only() {
    let ids: Vec<String> = (0..40).map(|n| format!("c{n}")).collect();
    let calls = ids
        .iter()
        .map(|id| (id.as_str(), "lookup_order", r#"{"order": "42"}"#));
    let model = ScriptedProvider::new([
        ScriptedRound::calling("", calls),
        ScriptedRound::answering("All shipped."),
    ]);
    let room = Room::new(model.clone(), served()).await;
    room.says("Where are my orders?").await;
    let ended: Vec<String> = room
        .timeline()
        .await
        .iter()
        .filter(|event| event.event_type == EventType::ToolCallEnd)
        .filter_map(|event| {
            let EventContent::ToolCall(row) = &event.content else {
                return None;
            };
            Some(row.tool_id.clone())
        })
        .collect();
    let asked = model
        .requests()
        .get(1)
        .and_then(|request| {
            request
                .messages
                .iter()
                .find(|message| !message.tool_calls.is_empty())
                .map(|message| message.tool_calls.len())
        })
        .unwrap();
    assert_eq!(
        (ended, asked, tool_results(&model)[1].len()),
        (ids[..32].to_vec(), 32, 32)
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

/// RFC §8.3, §10.3: a change a turn's tool makes to the room runs its
/// trigger's hooks under the turn's floor: an `ON_CHANNEL_ATTACHED`
/// observer of a tool's attach, or an `ON_CHANNEL_MUTED` one of a tool's
/// mute, posts at least as deep as the turn's response, so that an agent
/// whose every answer attaches a channel a greeting answers ends at the
/// depth limit (RMK-1004; such a post was at depth 0, each greeting a new
/// chain).
#[tokio::test]
async fn a_hook_on_a_change_a_tool_makes_posts_at_the_turn_s_depth() {
    for (tool, trigger) in [
        ("attach_ws", HookTrigger::OnChannelAttached),
        ("mute_ws", HookTrigger::OnChannelMuted),
    ] {
        let kit = RoomKit::new();
        for (id, channel_type) in [("sms", ChannelType::SMS), ("ws", ChannelType::WEBSOCKET)] {
            kit.register_channel(RecordingTransport::new(channel(id), channel_type))
                .unwrap();
        }
        let weak = kit.downgrade();
        let changes = Tools::new().tool(
            ToolDefinition::new(tool, "Changes the room", json!({ "type": "object" })),
            move |_call, _context| {
                let kit = weak.upgrade().unwrap();
                async move {
                    if tool == "attach_ws" {
                        kit.attach(&room_id(), &channel("ws")).await.unwrap();
                    } else {
                        kit.mute(&room_id(), &channel("ws")).await.unwrap();
                    }
                    Ok(ToolOutput::Text("done".to_owned()))
                }
            },
        );
        let model = ScriptedProvider::new([
            ScriptedRound::calling("", [("c1", tool, "{}")]),
            ScriptedRound::answering("Done."),
            ScriptedRound::answering("Hello."),
        ]);
        let agent = AiAgent::new(channel("assistant"), model)
            .with_tools(changes)
            .unwrap();
        kit.register_channel(agent).unwrap();
        kit.create_room(&room_id()).await.unwrap();
        let bound: &[&str] = if tool == "attach_ws" {
            &["sms", "assistant"]
        } else {
            &["sms", "ws", "assistant"]
        };
        for id in bound {
            kit.attach(&room_id(), &channel(id)).await.unwrap();
        }
        let weak = kit.downgrade();
        kit.observe("greeting", trigger, move |_event, _context| {
            let kit = weak.upgrade().unwrap();
            async move {
                let greeting = DirectEvent::new(channel("sms"), EventContent::text("Welcome."));
                kit.send_event(&room_id(), greeting).await.unwrap();
            }
        })
        .unwrap();
        let message = InboundMessage::new(channel("sms"), "+15550001", EventContent::text("Go."));
        kit.process_inbound_in(&room_id(), message).await.unwrap();
        kit.idle().await;
        let greeting = kit
            .store()
            .list_events(&room_id(), true)
            .await
            .unwrap()
            .into_iter()
            .find(|event| roomkit::text_of(&event.content) == "Welcome.")
            .unwrap();
        assert_eq!(greeting.chain_depth, 1, "{tool}");
        kit.shutdown().await;
    }
}

/// RFC §8.3 "Reentry cap": a turn is one stream, read to its end whichever
/// pass started it, its rows drawing no pass each, as the reference
/// implementation's turn: 6 rounds of 4 calls, 55 rows, and the answer is
/// delivered (review F1).
#[tokio::test]
async fn a_long_turn_draws_one_pass_and_its_answer_is_delivered() {
    let ids: Vec<Vec<String>> = (0..6)
        .map(|round| (0..4).map(|call| format!("c{round}_{call}")).collect())
        .collect();
    let mut rounds: Vec<ScriptedRound> = ids
        .iter()
        .enumerate()
        .map(|(round, calls)| {
            let calls = calls
                .iter()
                .map(|id| (id.as_str(), "lookup_order", r#"{"order": "1"}"#));
            ScriptedRound::calling(format!("Round {round}."), calls)
        })
        .collect();
    rounds.push(ScriptedRound::answering("The final answer."));
    let room = Room::new(ScriptedProvider::new(rounds), served()).await;
    room.says("Check them all").await;
    let timeline = room.timeline().await;
    let blocked = timeline
        .iter()
        .filter(|event| event.blocked_by.is_some())
        .count();
    assert_eq!(
        (blocked, room.sms.texts().last().cloned()),
        (0, Some("The final answer.".to_owned()))
    );
}

/// RFC §8.3 "Reentry cap": a turn is a chained stream when another event's
/// delivery asks for it, and draws its pass before it runs, so the turns
/// one message sets off stay within the cascade's budget, 50 passes by
/// default, beside the two the message itself asks for: what the
/// reference implementation spends, 52 turns (probe
/// tasks/probes/step4-m5/light/f1b_cap_probe.py).
#[tokio::test]
async fn two_agents_answering_each_other_take_at_most_the_budget_s_turns() {
    let turns = support::two_agents(5).await;
    assert!(turns <= 52, "{turns} turns");
}

/// RFC §8.3, §9.5.1: a streamed turn the delivery of a call's own event
/// asks for draws no pass of its own; the call drew what it costs as it
/// started, when the room's work made it. A hook that calls into the room
/// on every row of the agent's turns, each call asking the agent again, is
/// bounded by the cascade's budget: a locked hook's `send_event`, an entry
/// the hook's operation adopts, one pass each, 50 at the default depth
/// limit; an observer's `post`, a hook body's call, one hook call each, 100
/// at a depth limit of 10. Past the budget the call is refused, its event
/// stored refused, and the agent is never asked. (The security review's
/// finding on the F1 rework, a streamed turn asked from a non-Response set
/// running free, does not apply: no such turn runs uncharged.)
#[tokio::test]
async fn a_turn_asked_by_a_hook_s_call_runs_on_the_call_s_draw() {
    for (echo, depth, budget) in [
        (support::Echo::HookSend, 5, 50),
        (support::Echo::ObserverPost, 10, 100),
    ] {
        let (turns, capped) = support::a_hook_calling_per_row(echo, depth).await;
        assert!(
            turns <= budget + 1 && capped > 0,
            "{echo:?}: {turns} turns, {capped} capped"
        );
    }
}

/// A messaging transport is not handed the turn's tool-call rows, so that
/// its delivery of the event's text never sends `[Tool call: x]`; a
/// WebSocket channel is, as the reference implementation's WebSocket shows
/// the turn's activity (review F2, probe
/// tasks/probes/step4-m5/f2_text_transport_probe.py).
#[tokio::test]
async fn a_messaging_transport_is_handed_no_tool_row_and_a_websocket_is() {
    let model = ScriptedProvider::new([
        ScriptedRound::calling(
            "Let me check.",
            [("c1", "lookup_order", r#"{"order": "1"}"#)],
        ),
        ScriptedRound::answering("Order 1 shipped."),
    ]);
    let room = Room::new(model, served()).await;
    let web = RecordingTransport::new(channel("web"), ChannelType::WEBSOCKET);
    room.kit.register_channel(web.clone()).unwrap();
    room.kit.attach(&room_id(), &channel("web")).await.unwrap();
    room.says("Order 1?").await;
    let kinds = |transport: &RecordingTransport| -> Vec<EventType> {
        transport
            .deliveries()
            .iter()
            .map(|delivery| delivery.event.event_type)
            .collect()
    };
    assert_eq!(
        (room.sms.texts(), kinds(&web)),
        (
            vec!["Let me check.".to_owned(), "Order 1 shipped.".to_owned()],
            vec![
                EventType::Message,
                EventType::Message,
                EventType::ToolCallStart,
                EventType::ToolCallEnd,
                EventType::Message,
            ]
        )
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
